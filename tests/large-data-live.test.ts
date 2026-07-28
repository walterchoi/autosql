import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";
import { makeRows, makeKeylessRows, LANGUAGE_SAMPLES } from "./utils/fakeData";

// Larger-scale integration: insert a few thousand rows of mixed-type, multilingual data and
// verify integrity (row count + exact round-trip of a sampled subset), that re-ingestion
// upserts rather than appends, and that the surrogate path handles bulk keyless data. This is
// the coverage the mocked `chunked-insert` unit test can't give. Requires `npm run db:up`.

const N = 2000;
const ROWS = makeRows(N);

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`large multilingual dataset (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "large_data_test";
        const KEYLESS = "large_keyless_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const refKeyless = `${qi("test_schema")}.${qi(KEYLESS)}`;
        const temps = [TABLE, KEYLESS].map(t => `${qi("test_schema")}.${qi("temp_staging__" + t)}`);
        let db: Database;

        const dropAll = async (d: Database) => {
            for (const t of [...temps, ref, refKeyless]) {
                await d.runQuery({ query: `DROP TABLE IF EXISTS ${t}`, params: [] }).catch(() => {});
            }
        };

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false });
            await db.establishConnection();
            await dropAll(db);
        });

        afterAll(async () => {
            await dropAll(db);
            await db.closeConnection();
        });

        const count = async (r: string) => {
            const res = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${r}`, params: [] });
            return Number(Object.values(res.results![0])[0]);
        };
        const noteById = async (id: number) => {
            const res = await db.runQuery({ query: `SELECT ${qi("note")} AS n FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
            return res.results!.length ? String(Object.values(res.results![0])[0]) : null;
        };

        test(`inserts all ${N} rows`, async () => {
            const res = await db.autoSQL(TABLE, ROWS);
            expect(res.success).toBe(true);
            expect(await count(ref)).toBe(N);
        });

        test("every script round-trips exactly (sampled across the dataset)", async () => {
            // First full language cycle (all scripts) + a spread across the table.
            const sampleIdx = [
                ...LANGUAGE_SAMPLES.map((_, i) => i), // 0..len-1: one row per language
                250, 999, 1000, 1500, N - 1,
            ];
            for (const i of sampleIdx) {
                expect(await noteById(ROWS[i].id)).toBe(ROWS[i].note);
            }
        });

        test("re-ingesting the same rows upserts (count unchanged)", async () => {
            const res = await db.autoSQL(TABLE, ROWS);
            expect(res.success).toBe(true);
            expect(await count(ref)).toBe(N); // upsert by id, not append
        });

        test("updated values are reflected on re-ingest", async () => {
            // Edit `score` (a fixed-width int) on the full dataset: schema-stable, so this
            // exercises pure upsert value propagation without triggering an ALTER.
            const edited = ROWS.map((r, i) => (i < 5 ? { ...r, score: 555 } : r));
            const res = await db.autoSQL(TABLE, edited);
            expect(res.success).toBe(true);
            expect(await count(ref)).toBe(N); // still no new rows
            const r = await db.runQuery({ query: `SELECT ${qi("score")} AS s FROM ${ref} WHERE ${qi("id")} = ${ROWS[0].id}`, params: [] });
            expect(Number(Object.values(r.results![0])[0])).toBe(555);
        });

        test("surrogate key handles bulk keyless data (all rows kept, unique ids)", async () => {
            const keyless = makeKeylessRows(500);
            const sdb = Database.create({ ...config, schema: "test_schema", useWorkers: false, surrogateKey: true });
            await sdb.establishConnection();
            try {
                const res = await sdb.autoSQL(KEYLESS, keyless);
                expect(res.success).toBe(true);
                const r = await sdb.runQuery({
                    query: `SELECT COUNT(*) AS c, COUNT(DISTINCT ${qi("autosql_id")}) AS d FROM ${refKeyless}`,
                    params: [],
                });
                const row = r.results![0] as Record<string, any>;
                expect(Number(row.c)).toBe(500);
                expect(Number(row.d)).toBe(500); // every row got a distinct surrogate id
            } finally {
                await sdb.closeConnection();
            }
        });
    });
});
