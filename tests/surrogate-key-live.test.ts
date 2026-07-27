import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live verification for the opt-in surrogate key. Covers what unit tests cannot: that the
// INSERT actually omits the surrogate and the DB auto-generates it (both dialects, through the
// default staging INSERT…SELECT path), and — critically — that a second ingest is idempotent
// (no schema thrash) and appends. Requires the test databases (`npm run db:up`); excluded from
// the DB-free unit config.

// A fully duplicated row guarantees no natural single/composite key, forcing the surrogate.
const data = [
    { region: "west", product: "x", qty: 5 },
    { region: "west", product: "x", qty: 5 },
    { region: "east", product: "y", qty: 3 },
];

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`surrogate key (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "surrogate_key_live_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            // Workers disabled (worker.js only exists post-build); staging left default-on to
            // exercise the staging INSERT…SELECT surrogate exclusion.
            db = Database.create({ ...config, schema: "test_schema", surrogateKey: true, useWorkers: false });
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        });

        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        const counts = async () => {
            const r = await db.runQuery({
                query: `SELECT COUNT(*) AS c, COUNT(DISTINCT ${qi("autosql_id")}) AS d, COUNT(${qi("autosql_id")}) AS nn FROM ${ref}`,
                params: [],
            });
            const row = r.results![0] as Record<string, any>;
            return { total: Number(row.c), distinct: Number(row.d), nonNull: Number(row.nn) };
        };

        test("first ingest creates the surrogate PK and auto-generates it", async () => {
            const res = await db.autoSQL(TABLE, data);
            expect(res.success).toBe(true);
            const { total, distinct, nonNull } = await counts();
            expect(total).toBe(3);
            expect(nonNull).toBe(3);   // surrogate populated on every row (not NULL)
            expect(distinct).toBe(3);  // each row got a unique DB-generated id
        });

        test("re-ingestion is idempotent (no schema error) and appends", async () => {
            const res = await db.autoSQL(TABLE, data);
            expect(res.success).toBe(true); // would throw/fail if run 2 thrashed the PK or dropped the surrogate
            const { total, distinct } = await counts();
            expect(total).toBe(6);     // appended (surrogate is unique per insert → no upsert match)
            expect(distinct).toBe(6);  // all ids distinct and continuing
        });
    });
});
