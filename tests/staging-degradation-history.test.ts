import { Database } from "../src/db/database";

// The staging-degradation history compensation (getDeleteHistoryRowsQuery) is implemented for
// mysql/pgsql only; SQL Server row-level history is unverified (decisions.md D-F), so the base
// implementation must fail loudly rather than leave history over-recorded.
describe("staging-path degradation history compensation — unsupported dialect", () => {
    test("getDeleteHistoryRowsQuery throws a clear error for SQL Server", () => {
        const db: any = Database.create({ sqlDialect: "sqlserver", host: "h", user: "u", password: "p", database: "d" });
        expect(() =>
            db.getDeleteHistoryRowsQuery("t__history", ["id"], [{ id: 1 }], "2026-08-01 00:00:00")
        ).toThrow(/not supported for dialect "sqlserver"/i);
    });

    test("mysql/pgsql build a delete keyed on the exact as_at + primary key tuple", () => {
        for (const dialect of ["mysql", "pgsql"] as const) {
            const db: any = Database.create({ sqlDialect: dialect, host: "h", user: "u", password: "p", database: "d", schema: "s" });
            const q = db.getDeleteHistoryRowsQuery("t__history", ["id"], [{ id: 2 }, { id: 5 }], "2026-08-01 00:00:00");
            expect(q.query).toMatch(/DELETE FROM/i);
            expect(q.query).toMatch(/dwh_as_at/);
            expect(q.params[0]).toBe("2026-08-01 00:00:00"); // as_at bound first
            expect(q.params).toContain(2);
            expect(q.params).toContain(5);
        }
    });
});
