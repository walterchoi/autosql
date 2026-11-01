import { Database } from "../src/db/database";

// runTransactionsWithConcurrency's connect-failure branch: "must not reject the whole batch —
// return one failure result per group." Only the happy/lock-held path was ever tested
// (schema-lock-concurrency-cap.test.ts); this specific contract had zero coverage.

function makeDb() {
    const db = Database.create({ sqlDialect: "mysql", host: "h", user: "u", password: "p", database: "d" }) as any;
    db.connection = undefined; // force the establishConnection() branch
    db.establishConnection = jest.fn().mockRejectedValue(new Error("ECONNREFUSED"));
    return db;
}

describe("Database.runTransactionsWithConcurrency — connect failure", () => {
    test("returns one {success:false} per group, not a rejected promise", async () => {
        const db = makeDb();
        const groups = [[{ query: "A", params: [] }], [{ query: "B", params: [] }], [{ query: "C", params: [] }]];

        const results = await db.runTransactionsWithConcurrency(groups);

        expect(results).toHaveLength(3);
        for (const r of results) {
            expect(r.success).toBe(false);
            expect(r.error).toBe("ECONNREFUSED");
        }
    });

    test("an empty queryGroups array still resolves (to an empty array), not a connect attempt wasted on nothing", async () => {
        const db = makeDb();
        const results = await db.runTransactionsWithConcurrency([]);
        expect(results).toEqual([]);
    });

    test("outlier: a non-Error rejection (e.g. a string throw) still produces a well-formed message", async () => {
        const db = makeDb();
        db.establishConnection = jest.fn().mockRejectedValue("connection pool exhausted");
        const results = await db.runTransactionsWithConcurrency([[{ query: "A", params: [] }]]);
        expect(results).toHaveLength(1);
        expect(results[0].success).toBe(false);
        expect(results[0].error).toBe("connection pool exhausted");
    });

    test("regression guard: an already-established connection skips establishConnection entirely", async () => {
        const db = makeDb();
        db.connection = {}; // already connected
        db.runTransaction = jest.fn().mockResolvedValue({ success: true });
        db.getMaxConnections = () => 5;

        await db.runTransactionsWithConcurrency([[{ query: "A", params: [] }]]);

        expect(db.establishConnection).not.toHaveBeenCalled();
    });
});
