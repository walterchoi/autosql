import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Regression: ingesting (with addTimestamps + staging on, both default) into an externally
// created table that ALREADY HAS ROWS and lacks the dwh_* columns. Previously the timestamp
// columns were added to the insert metadata after schema comparison, so the ALTER never created
// them on the real table and the staging temp (copied from it) failed with
// `column "dwh_created_at" ... does not exist`. Timestamps are now added before comparison, and
// the NOT NULL dwh_created_at is ADDed with DEFAULT CURRENT_TIMESTAMP so pre-existing rows
// backfill. The populated-table case is what exercises the NOT NULL path (an empty table hides it).

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`timestamps on populated external table (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "ext_timestamps_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false }); // addTimestamps + staging default on
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            // Externally-created table WITHOUT any dwh_* columns...
            await db.runQuery({ query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("name")} VARCHAR(50))`, params: [] });
            // ...that already contains rows (this is what forces the NOT NULL backfill path).
            await db.runQuery({ query: `INSERT INTO ${ref} (${qi("id")}, ${qi("name")}) VALUES (100, 'pre-a'), (200, 'pre-b')`, params: [] });
        });

        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        test("autoSQL adds dwh columns, backfills existing rows, and inserts the new one", async () => {
            const res = await db.autoSQL(TABLE, [{ id: 300, name: "new-c" }]);
            expect(res.success).toBe(true); // pre-fix: 'column "dwh_created_at" of relation "temp_staging__..." does not exist'

            const total = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            expect(Number(Object.values(total.results![0])[0])).toBe(3); // 2 pre-existing + 1 new

            // The NOT NULL created-at column must be populated on every row, including the two
            // that pre-dated the ALTER (backfilled via DEFAULT CURRENT_TIMESTAMP).
            const nulls = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref} WHERE ${qi("dwh_created_at")} IS NULL`, params: [] });
            expect(Number(Object.values(nulls.results![0])[0])).toBe(0);
        });
    });
});
