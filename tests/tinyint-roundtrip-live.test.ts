import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// R10 (live): a small-integer key (<=255 -> MySQL tinyint) must survive re-ingest. Previously the
// column round-tripped as boolean, and a spurious boolean->int conversion collapsed every id to
// 0/1 (data loss, then a PRIMARY-key duplicate). A genuine boolean (tinyint(1)) must still work.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`small-int key round-trip (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "tinyint_roundtrip_test";
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

        test("small-int ids are preserved on re-ingest (not collapsed to 0/1)", async () => {
            // ids 5/6 are <=255 -> MySQL tinyint; `active` is a genuine boolean.
            const r1 = await db.autoSQL(TABLE, [
                { id: 5, active: true, label: "five" },
                { id: 6, active: false, label: "six" },
            ]);
            expect(r1.success).toBe(true);

            const r2 = await db.autoSQL(TABLE, [
                { id: 5, active: true, label: "FIVE" }, // upsert existing
                { id: 7, active: false, label: "seven" }, // new
            ]);
            expect(r2.success).toBe(true); // pre-fix (MySQL): Duplicate entry '1' for key 'PRIMARY'

            // If ids had collapsed to 0/1, the upsert would have merged rows and COUNT would be < 3.
            const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            expect(Number(Object.values(c.results![0])[0])).toBe(3); // ids 5, 6, 7 all distinct

            const r = await db.runQuery({ query: `SELECT ${qi("label")} AS l FROM ${ref} WHERE ${qi("id")} = 5`, params: [] });
            expect(String(Object.values(r.results![0])[0])).toBe("FIVE"); // id 5 upserted in place
        });

        test("a boolean added via ALTER round-trips (no non-converging re-ALTER)", async () => {
            const T2 = "tinyint_bool_add_test";
            const ref2 = `${qi("test_schema")}.${qi(T2)}`;
            const temp2 = `${qi("test_schema")}.${qi("temp_staging__" + T2)}`;
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${temp2}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref2}`, params: [] }).catch(() => {});
            try {
                // Step 2 adds a boolean column via ALTER (the path my introspection change made
                // load-bearing: it must be stored as tinyint(1), or it reads back as int and
                // re-ALTERs forever). `active` is nullable (one row omits it) to avoid the separate
                // NOT-NULL-column-added-to-a-populated-table issue on Postgres (R11).
                expect((await db.autoSQL(T2, [{ id: 5, label: "five" }, { id: 6, label: "six" }])).success).toBe(true);
                const addBool = [{ id: 5, active: true, label: "five" }, { id: 6, active: false, label: "six" }, { id: 7, label: "seven" }];
                expect((await db.autoSQL(T2, addBool)).success).toBe(true);

                // The boolean round-trips as boolean (not int) — that's the round-trip the fix closes.
                const { currentMetaData } = await (db as any).autoSQLHandler.fetchTableMetadata(T2);
                expect(currentMetaData.active.type).toBe("boolean");

                // Identical re-ingest must converge: succeed and add no rows.
                expect((await db.autoSQL(T2, addBool)).success).toBe(true);
                const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref2}`, params: [] });
                expect(Number(Object.values(c.results![0])[0])).toBe(3);
            } finally {
                await db.runQuery({ query: `DROP TABLE IF EXISTS ${temp2}`, params: [] }).catch(() => {});
                await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref2}`, params: [] }).catch(() => {});
            }
        });
    });
});
