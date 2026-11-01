import { Database } from "../src/db/database";
import type { DatabaseConfig } from "../src/config/types";

// SQL Server never participates in database-retry.test.ts's generic retry/permanent-error suite
// (it loops over DB_CONFIG, which in this environment only has mysql/pgsql — SQL Server's own
// tests all use a hardcoded CONFIG instead). That left the base retry loop's actual behavior on
// SQL Server, and normalizeError's err.number -> err.code mapping it depends on, with zero
// coverage: not even the mechanical "permanent error doesn't retry" contract every other dialect
// has.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};

describe("SQL Server retry logic (mocked establishDatabaseConnection)", () => {
    // These set .code directly (not .number) — the field the base loop's getPermanentErrors()
    // check actually reads. normalizeError's own .number->.code mapping (query-time errors only)
    // is proven separately below, against the real driver.
    test("a temporary error (not in the permanent list) retries up to maxAttempts", async () => {
        const db = Database.create(CONFIG);
        let attempts = 0;
        jest.spyOn(db as any, "establishDatabaseConnection").mockImplementation(async () => {
            attempts++;
            if (attempts < 3) {
                const err: any = new Error("deadlock victim");
                err.code = "1205"; // deliberately NOT in SQL_SERVER_PERMANENT_ERRORS
                throw err;
            }
        });
        await db.establishConnection();
        expect(attempts).toBe(3);
    }, 15000);

    test("a permanent QUERY-time error (invalid object name, 208) throws immediately without retrying", async () => {
        const db = Database.create(CONFIG);
        let attempts = 0;
        jest.spyOn(db as any, "establishDatabaseConnection").mockImplementation(async () => {
            attempts++;
            const err: any = new Error("Invalid object name 'x'.");
            err.code = "208";
            throw err;
        });
        await expect(db.establishConnection()).rejects.toThrow(/Invalid object name/);
        expect(attempts).toBe(1); // no retries
    });

    test("a permanent CONNECT-time error (ELOGIN) throws immediately without retrying", async () => {
        const db = Database.create(CONFIG);
        let attempts = 0;
        jest.spyOn(db as any, "establishDatabaseConnection").mockImplementation(async () => {
            attempts++;
            const err: any = new Error("Login failed for user 'sa'.");
            err.code = "ELOGIN";
            throw err;
        });
        await expect(db.establishConnection()).rejects.toThrow(/Login failed/);
        expect(attempts).toBe(1);
    });
});

describe("SQL Server normalizeError (live, real driver)", () => {
    test("a real query-time failure's err.number is mapped onto err.code", async () => {
        const db = Database.create(CONFIG);
        await db.establishConnection();
        try {
            let caught: any;
            try {
                await (db as any).executeQuery({ query: "SELECT * FROM no_such_table_xyz_abc", params: [] });
            } catch (e) {
                caught = e;
            }
            expect(caught).toBeDefined();
            expect(caught.code).toBe("208"); // Invalid object name — mapped from the driver's real err.number
        } finally {
            await db.closeConnection();
        }
    }, 15000);

    // A real connect-time login failure never carries err.number at all (confirmed directly
    // against the driver) — tedious's own "ELOGIN" is already on err.code, and the FIX this test
    // guards is that SQL_SERVER_PERMANENT_ERRORS now includes it, so this fails fast (one attempt)
    // instead of retrying 3x and losing the real message behind a generic timeout error.
    test("a real login failure fails fast (ELOGIN is permanent) and keeps its real message", async () => {
        const db = Database.create({ ...CONFIG, password: "definitely-wrong-password" });
        const start = Date.now();
        let caught: any;
        try {
            await db.establishConnection();
        } catch (e) {
            caught = e;
        }
        const elapsedMs = Date.now() - start;
        expect(caught).toBeDefined();
        expect(caught.code).toBe("ELOGIN");
        expect(caught.message).toMatch(/Login failed/i);
        // No retry delay (the base loop waits 1000ms between attempts) — one attempt, not three.
        expect(elapsedMs).toBeLessThan(1000);
    }, 15000);
});
