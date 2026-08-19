import { Database } from "./utils/testConfig";
import { SchemaLockTimeoutError } from "../src/errors";

// SQL Server schema lock via sp_getapplock, held on a dedicated transaction. Verifies the real semantics
// (not just that acquire doesn't throw): the lock is mutually exclusive across connections, times out with
// SchemaLockTimeoutError while held, and becomes acquirable again after release. Regression guard for the
// azure-sql-edge `@ret is not a parameter` bug where acquire was outright broken (useSchemaLock latent).

const SS: any = { sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd", database: "master", port: 1433, useWorkers: false, schema: "test_schema" };
// Unique per test-process run: a Transaction-scoped applock leaked by an abnormally-killed prior run (which
// SQL Server only reaps when it detects the dead connection) can't block a fresh run on a fresh resource.
const TABLE = `t_schema_lock_live_${process.pid}`;

describe("SQL Server schema lock (sp_getapplock, live)", () => {
    test("acquire holds; a second connection times out while held; releases and re-acquires", async () => {
        const holder: any = Database.create(SS);
        const contender: any = Database.create(SS);
        await holder.establishConnection();
        await contender.establishConnection();
        try {
            // 1) Acquire on the holder — must succeed and be reflected in the held count.
            await holder.acquireSchemaLock(TABLE, 30);
            expect(holder.getHeldSchemaLockCount()).toBe(1);

            // 2) A different connection can't take the same lock — it times out (short timeout) and throws.
            await expect(contender.acquireSchemaLock(TABLE, 1)).rejects.toBeInstanceOf(SchemaLockTimeoutError);
            expect(contender.getHeldSchemaLockCount()).toBe(0);

            // 3) Release on the holder frees it; the contender can now acquire.
            await holder.releaseSchemaLock(TABLE);
            expect(holder.getHeldSchemaLockCount()).toBe(0);
            await contender.acquireSchemaLock(TABLE, 30);
            expect(contender.getHeldSchemaLockCount()).toBe(1);
            await contender.releaseSchemaLock(TABLE);
            expect(contender.getHeldSchemaLockCount()).toBe(0);
        } finally {
            await holder.closeConnection();
            await contender.closeConnection();
        }
    });
});
