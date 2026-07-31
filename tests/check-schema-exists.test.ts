import { Database } from "../src/db/database";

// F10: checkSchemaExists must distinguish "the schema is absent" from "we could NOT
// determine existence" (connectivity/auth). Previously any failure was masked as
// { schema: false }, which would mislead a caller into creating a schema that already
// exists or skipping one that does. White-box: stub runQuery, no live DB needed.

const CONFIG = { sqlDialect: "pgsql" as const, host: "localhost", user: "u", password: "p", database: "d" };

describe("checkSchemaExists — F10: 'absent' vs 'could not determine'", () => {
    test("THROWS (does not report absent) when the existence query fails", async () => {
        const db: any = Database.create(CONFIG);
        db.runQuery = jest.fn().mockResolvedValue({ success: false, error: "ECONNREFUSED" });
        await expect(db.checkSchemaExists("my_schema")).rejects.toThrow(/could not determine/i);
    });

    test("reports true when a successful query shows the schema present", async () => {
        const db: any = Database.create(CONFIG);
        db.runQuery = jest.fn().mockResolvedValue({ success: true, results: [{ my_schema: 1 }] });
        await expect(db.checkSchemaExists("my_schema")).resolves.toEqual({ my_schema: true });
    });

    test("reports false when a successful query shows the schema absent", async () => {
        const db: any = Database.create(CONFIG);
        db.runQuery = jest.fn().mockResolvedValue({ success: true, results: [{}] });
        await expect(db.checkSchemaExists("my_schema")).resolves.toEqual({ my_schema: false });
    });

    test("handles the array form (per-schema booleans)", async () => {
        const db: any = Database.create(CONFIG);
        db.runQuery = jest.fn().mockResolvedValue({ success: true, results: [{ a: 1, b: 0 }] });
        await expect(db.checkSchemaExists(["a", "b"])).resolves.toEqual({ a: true, b: false });
    });
});
