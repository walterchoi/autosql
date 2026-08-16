import { Database } from "../src/db/database";

// A16: runTransaction documents "always resolves to a QueryResult, never rejects" — bare call sites in
// autosql.ts rely on it so a pool-acquire failure can't crash the caller's process. The acquire was
// awaited OUTSIDE the try, so an acquire rejection escaped as an unhandled rejection. This pins the
// contract directly by stubbing acquireConnection to reject.
describe("runTransaction never rejects (A16)", () => {
    test("a failing acquireConnection resolves to {success:false}, it does not reject", async () => {
        const db: any = Database.create({
            sqlDialect: "pgsql", host: "localhost", port: 5432, user: "x", password: "x", database: "x",
        });
        db.connection = {}; // truthy → skip establishConnection so we exercise the acquire path
        jest.spyOn(db, "acquireConnection").mockRejectedValue(new Error("pool exhausted"));
        jest.spyOn(db, "getPermanentErrors").mockResolvedValue([]);

        const r = await db.runTransaction([{ query: "SELECT 1", params: [] }]);
        expect(r.success).toBe(false);
        expect(String(r.error)).toMatch(/pool exhausted/);
    }, 15000);
});

// A15: runQuery retries internally by default; a caller that owns its own retry loop can pass an
// attempts override so a NON-idempotent statement isn't re-executed (and duplicated) on an ambiguous
// failure. perRowInsertWithRetry passes 1 for exactly this.
describe("runQuery honours a per-call attempts override (A15)", () => {
    const makeDb = () => {
        const db: any = Database.create({ sqlDialect: "pgsql", host: "localhost", port: 5432, user: "x", password: "x", database: "x" });
        jest.spyOn(db, "getPermanentErrors").mockResolvedValue([]); // treat the error as transient
        return db;
    };

    test("override = 1 executes exactly once (no retry) on a transient failure", async () => {
        const db = makeDb();
        const spy = jest.spyOn(db, "executeQuery").mockRejectedValue(Object.assign(new Error("boom"), { code: "TRANSIENT" }));
        const r = await db.runQuery({ query: "INSERT INTO t (id) VALUES (1)", params: [] }, 1);
        expect(r.success).toBe(false);
        expect(spy).toHaveBeenCalledTimes(1);
    });

    test("default (no override) still retries up to 3 times", async () => {
        const db = makeDb();
        const spy = jest.spyOn(db, "executeQuery").mockRejectedValue(Object.assign(new Error("boom"), { code: "TRANSIENT" }));
        const r = await db.runQuery({ query: "INSERT INTO t (id) VALUES (1)", params: [] });
        expect(r.success).toBe(false);
        expect(spy).toHaveBeenCalledTimes(3);
    }, 15000);
});
