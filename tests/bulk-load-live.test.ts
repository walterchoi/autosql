import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// End-to-end bulkLoad (both dialects): staging inserts go through the fast path —
// Postgres COPY FROM STDIN, MySQL LOAD DATA LOCAL INFILE — instead of parameterized INSERT.
// Guards that the tab-delimited serialization round-trips real-world data (multilingual text,
// emoji, embedded tabs/newlines/backslashes, NULLs) and that upsert-on-reingest still merges.

const BS = String.fromCharCode(92); // backslash
const TAB = String.fromCharCode(9);
const NL = String.fromCharCode(10);

const ROWS = [
    { id: 1, name: "alpha", note: "plain ascii", amount: 10.5 },
    { id: 2, name: "日本語テスト", note: "emoji 😀🚀 mix", amount: 20.0 },
    { id: 3, name: "Zürich", note: "tab" + TAB + "and" + NL + "newline", amount: 30.25 },
    { id: 4, name: "path", note: "C:" + BS + "temp" + BS + "x", amount: 0 },
    { id: 5, name: "nullnote", note: null, amount: 5.5 },
];

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`bulkLoad end-to-end (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "bulk_load_e2e_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false, bulkLoad: true });
            await db.establishConnection();
            // MySQL LOAD DATA LOCAL INFILE requires the server flag; it resets on container restart.
            if (config.sqlDialect === "mysql") {
                await db.runQuery({ query: "SET GLOBAL local_infile=1", params: [] }).catch(() => {});
            }
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        });

        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        const selectNote = async (id: number): Promise<string | null> => {
            const r = await db.runQuery({
                query: `SELECT ${qi("note")} AS n FROM ${ref} WHERE ${qi("id")} = ${id}`,
                params: [],
            });
            return (r.results![0] as any).n;
        };

        const count = async (): Promise<number> => {
            const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            return Number(Object.values(c.results![0])[0]);
        };

        test("create + bulk load round-trips multilingual / emoji / special-char data", async () => {
            expect((await db.autoSQL(TABLE, ROWS)).success).toBe(true);
            expect(await count()).toBe(5);

            // Values with embedded specials must survive the tab-delimited COPY/INFILE encoding.
            expect(await selectNote(1)).toBe("plain ascii");
            expect(await selectNote(2)).toBe("emoji 😀🚀 mix");
            expect(await selectNote(3)).toBe("tab" + TAB + "and" + NL + "newline");
            expect(await selectNote(4)).toBe("C:" + BS + "temp" + BS + "x");
            expect(await selectNote(5)).toBeNull(); // NULL survives as NULL, not the literal "\N"
        });

        test("re-ingest via bulk load upserts (row count stable, values updated)", async () => {
            const updated = ROWS.map((r) => ({ ...r, note: r.note === null ? null : r.note + " v2" }));
            expect((await db.autoSQL(TABLE, updated)).success).toBe(true);
            expect(await count()).toBe(5); // merged, not duplicated
            expect(await selectNote(1)).toBe("plain ascii v2");
            expect(await selectNote(2)).toBe("emoji 😀🚀 mix v2");
        });
    });
});
