import { DB_CONFIG, Database } from "./utils/testConfig";

// A25b / P7: runTransactionsWithConcurrency caps concurrency at (pool size − held schema locks), so a
// held lock (which pins a connection/transaction) can't leave the concurrent phase over-subscribing the
// pool. getHeldSchemaLockCount() tracks the held count; the cap keeps ≥1 so a batch always progresses.

// All three dialects: mysql GET_LOCK, pgsql pg_advisory_lock, SQL Server sp_getapplock (held on a dedicated
// transaction). getHeldSchemaLockCount() reflects the held count on each. SQL Server isn't in DB_CONFIG
// (its live suites use an inline config), so add it explicitly here.
const SS: any = { sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd", database: "master", port: 1433, useWorkers: false };
[...Object.values(DB_CONFIG), SS].forEach((rawConfig: any) => {
    const config = { ...rawConfig, schema: "test_schema", useWorkers: false };
    describe(`held-lock concurrency cap (A25b) for ${String(config.sqlDialect).toUpperCase()}`, () => {
        test("getHeldSchemaLockCount reflects acquired locks; a batch still completes while one is held", async () => {
            const db: any = Database.create(config);
            await db.establishConnection();
            await db.runQuery(db.getCreateSchemaQuery("test_schema")).catch(() => {});
            try {
                expect(db.getHeldSchemaLockCount()).toBe(0);
                await db.acquireSchemaLock("t_lock_cap", 30);
                expect(db.getHeldSchemaLockCount()).toBe(1);

                // Concurrent batch: with a lock held, poolSize is capped but stays ≥1, so this completes.
                const res = await db.runTransactionsWithConcurrency([
                    [{ query: "SELECT 1 AS a", params: [] }],
                    [{ query: "SELECT 2 AS a", params: [] }],
                ]);
                expect(res.length).toBe(2);
                expect(res.every((r: any) => r.success)).toBe(true);

                await db.releaseSchemaLock("t_lock_cap");
                expect(db.getHeldSchemaLockCount()).toBe(0);
            } finally { await db.closeConnection(); }
        });
    });
});
