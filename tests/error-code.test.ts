import { Database } from "../src/db/database";

// §10/§11 — unit coverage for QueryResult.errorCode across every branch, DB-free by stubbing the
// dialect's executeQuery / connection plumbing to throw shaped driver errors. (error-code-live.test.ts
// covers the real-driver end-to-end path; this exercises the branches a live test can't shape.)

function stubDb(opts: { execError?: any; permanent?: boolean } = {}): any {
    const db: any = Database.create({ sqlDialect: "mysql", host: "h", user: "u", password: "p", database: "d" });
    db.connection = {}; // pretend already connected
    const code = opts.execError?.code;
    db.getPermanentErrors = async () => (opts.permanent !== false && code != null && code !== "" ? [code] : []);
    db.executeQuery = async () => { if (opts.execError) throw opts.execError; return { rows: [], affectedRows: 0 }; };
    // transaction plumbing
    db.acquireConnection = async () => ({});
    db.releaseConnection = () => {};
    db.startTransaction = async () => {};
    db.commit = async () => {};
    db.rollback = async () => {};
    return db;
}

describe("QueryResult.errorCode — unit branches", () => {
    test("runQuery: a driver error surfaces error + errorCode", async () => {
        const r = await stubDb({ execError: Object.assign(new Error("CREATE command denied"), { code: "ER_TABLEACCESS_DENIED_ERROR" }) }).runQuery({ query: "x", params: [] });
        expect(r.success).toBe(false);
        expect(r.error).toMatch(/denied/);
        expect(r.errorCode).toBe("ER_TABLEACCESS_DENIED_ERROR");
    });

    test("runQuery: a NON-driver error (no .code) → errorCode undefined, message preserved", async () => {
        const r = await stubDb({ execError: new Error("some logic error") }).runQuery({ query: "x", params: [] });
        expect(r.success).toBe(false);
        expect(r.error).toBe("some logic error");
        expect(r.errorCode).toBeUndefined();
    });

    test("runQuery: falsy-but-present code — '' → undefined, 0 → '0'", async () => {
        const empty = await stubDb({ execError: Object.assign(new Error("e"), { code: "" }), permanent: false }).runQuery({ query: "x", params: [] });
        expect(empty.errorCode).toBeUndefined(); // empty string is not a real code
        const zero = await stubDb({ execError: Object.assign(new Error("e"), { code: 0 }), permanent: false }).runQuery({ query: "x", params: [] });
        expect(zero.errorCode).toBe("0"); // numeric zero is preserved
    });

    test("runQuery: a numeric code (SQL Server style) is stringified", async () => {
        const r = await stubDb({ execError: Object.assign(new Error("Invalid object name"), { code: 208 }) }).runQuery({ query: "x", params: [] });
        expect(r.errorCode).toBe("208");
    });

    test("runQuery: retries-exhausted path also carries errorCode", async () => {
        // Non-permanent error → runQuery retries then returns from the retries-exhausted site.
        const r = await stubDb({ execError: Object.assign(new Error("deadlock"), { code: "ER_LOCK_DEADLOCK" }), permanent: false }).runQuery({ query: "x", params: [] });
        expect(r.success).toBe(false);
        expect(r.errorCode).toBe("ER_LOCK_DEADLOCK");
    }, 15000);

    test("runQuery: a successful query carries no errorCode", async () => {
        const r = await stubDb().runQuery({ query: "SELECT 1", params: [] });
        expect(r.success).toBe(true);
        expect(r.errorCode).toBeUndefined();
    });

    test("runTransaction: a failing statement surfaces errorCode", async () => {
        const r = await stubDb({ execError: Object.assign(new Error("check violation"), { code: "23514" }) }).runTransaction([{ query: "x", params: [] }]);
        expect(r.success).toBe(false);
        expect(r.errorCode).toBe("23514");
    });

    test("runTransaction: a connect failure surfaces errorCode", async () => {
        const db = stubDb();
        db.connection = null; // force establishConnection
        db.establishConnection = async () => { throw Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" }); };
        const r = await db.runTransaction([{ query: "x", params: [] }]);
        expect(r.success).toBe(false);
        expect(r.errorCode).toBe("ECONNREFUSED");
    });
});
