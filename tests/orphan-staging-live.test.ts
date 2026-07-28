import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// R2 (live): a staging temp table orphaned by a crashed run (with a now-stale schema) must not
// corrupt the next load. Previously the temp was created with CREATE TABLE IF NOT EXISTS, so a
// leftover kept its stale schema and the insert failed on a column mismatch. The staging create
// now drops any leftover first, so the temp always matches the current real table.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`orphaned staging table recovery (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "orphan_staging_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false });
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        });

        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        test("a stale leftover temp table does not break the next load", async () => {
            // First load creates the real table (id, name).
            const r1 = await db.autoSQL(TABLE, [{ id: 1000, name: "a" }, { id: 1001, name: "b" }]);
            expect(r1.success).toBe(true);

            // Simulate a crashed prior run: a leftover temp table with a STALE (wrong) schema.
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `CREATE TABLE ${tempRef} (${qi("id")} INT)`, params: [] }); // missing `name`

            // Next load must drop the stale temp, recreate it to match, and succeed.
            const r2 = await db.autoSQL(TABLE, [{ id: 1000, name: "A" }, { id: 1002, name: "c" }]);
            expect(r2.success).toBe(true); // pre-fix: insert into temp failed on the missing `name` column

            const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            expect(Number(Object.values(r.results![0])[0])).toBe(3); // 1000 (upserted), 1001, 1002
        });
    });
});
