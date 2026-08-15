import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// C5 (BYOD hardening): a failed query must surface the driver's STRUCTURED error code on
// QueryResult.errorCode (mysql2's string `code`, Postgres's SQLSTATE), so a caller can branch on the
// exact failure (e.g. tell a user which GRANT is missing) without string-matching the message.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`QueryResult.errorCode (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;
        beforeAll(async () => { db = Database.create({ ...config, schema: "test_schema", useWorkers: false }); await db.establishConnection(); });
        afterAll(async () => { await db.closeConnection(); });

        test("a failed query surfaces the driver's error code AND message", async () => {
            const r = await db.runQuery({ query: `SELECT * FROM ${qi("test_schema")}.${qi("no_such_table_xyz_123")}`, params: [] });
            expect(r.success).toBe(false);
            // The human-readable message is still preserved...
            expect(typeof r.error).toBe("string");
            expect(r.error!.length).toBeGreaterThan(0);
            // ...and now the structured code is too.
            expect(typeof r.errorCode).toBe("string");
            expect(r.errorCode!.length).toBeGreaterThan(0);
            if (config.sqlDialect === "mysql") {
                expect(r.errorCode).toBe("ER_NO_SUCH_TABLE");
            } else if (config.sqlDialect === "pgsql") {
                expect(r.errorCode).toBe("42P01"); // undefined_table SQLSTATE
            }
        });

        test("a successful query carries no errorCode", async () => {
            const r = await db.runQuery({ query: "SELECT 1 AS one", params: [] });
            expect(r.success).toBe(true);
            expect(r.errorCode).toBeUndefined();
        });

        // A failing autoSQL() must surface errorCode on its TOP-LEVEL result — the driver code is
        // threaded through throwIfFailedResults to the top-level catch — for both the worker and the
        // direct (useWorkers:false) execution paths.
        for (const useWorkers of [false, true]) {
            test(`autoSQL() failure surfaces errorCode (useWorkers:${useWorkers})`, async () => {
                const TABLE = `errorcode_autosql_${useWorkers ? "w" : "d"}`;
                const ref = `${qi("test_schema")}.${qi(TABLE)}`;
                await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
                // A CHECK the incoming row violates — autosql doesn't model CHECKs, so the DB rejects it.
                await db.runQuery({ query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("val")} INT, CONSTRAINT ${qi("val_nn")} CHECK (${qi("val")} >= 0))`, params: [] });
                try {
                    const loader = Database.create({ ...config, schema: "test_schema", useWorkers, useStagingInsert: false, primaryKey: ["id"], addTimestamps: false });
                    await loader.establishConnection();
                    const r = await loader.autoSQL(TABLE, [{ id: 1, val: -5 }]);
                    await loader.closeConnection();

                    expect(r.success).toBe(false);
                    expect(typeof r.errorCode).toBe("string");
                    expect(r.errorCode!.length).toBeGreaterThan(0);
                    if (config.sqlDialect === "pgsql") expect(r.errorCode).toBe("23514"); // check_violation SQLSTATE
                } finally {
                    await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
                }
            });
        }
    });
});
