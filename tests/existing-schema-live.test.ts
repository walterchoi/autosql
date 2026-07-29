import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// N1 / v1b (live): the realistic Roots flow — a first run returns its resolved schema
// (QueryResult.metaData); a later run passes it back as `existingSchema` so AutoSQL skips
// introspection. It must still upsert correctly and produce no schema drift.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`existingSchema fast path (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "existing_schema_test";
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

        const count = async () => {
            const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            return Number(Object.values(r.results![0])[0]);
        };

        test("re-ingest with the cached resolved schema upserts, with no drift", async () => {
            const r1 = await db.autoSQL(TABLE, [{ id: 1000, name: "a" }, { id: 1001, name: "b" }]);
            expect(r1.success).toBe(true);
            expect(r1.metaData).toBeDefined();
            const cached = r1.metaData!;              // the resolved schema (incl. dwh_* columns)
            expect(cached.dwh_created_at).toBeDefined();

            // Second run: pass the cached schema back → introspection is skipped.
            const r2 = await db.autoSQL(TABLE, [{ id: 1000, name: "A" }, { id: 1002, name: "c" }], undefined, undefined, {
                existingSchema: cached,
            });
            expect(r2.success).toBe(true);
            expect(await count()).toBe(3);            // upsert (1000 updated), 1001, 1002 — no drift/dup

            const r = await db.runQuery({ query: `SELECT ${qi("name")} AS n FROM ${ref} WHERE ${qi("id")} = 1000`, params: [] });
            expect(String(Object.values(r.results![0])[0])).toBe("A"); // 1000 upserted in place
        });
    });
});
