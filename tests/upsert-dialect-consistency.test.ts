import { Database } from "../src/db/database";

// A11: cross-dialect upsert consistency. When there are NO updatable columns (every non-key column is a
// protected calculated field) but a primary key exists, a duplicate key must be SKIPPED on every
// dialect — MySQL previously fell through to a bare INSERT and errored. Also: the SQL Server MERGE must
// hold a lock so two concurrent upserts of the same key can't both insert.
describe("upsert with no updatable columns is consistent across dialects (A11)", () => {
    const header: any = {
        id: { type: "int", primary: true },
        calc: { type: "int", calculated: true, updatedCalculated: false }, // protected -> not updatable
    };
    const rows = [{ id: 1, calc: 5 }];
    const mk = (d: string) => Database.create({ sqlDialect: d as any, host: "h", user: "u", password: "p", database: "d" }) as any;

    test("MySQL emits a no-op self-update (skip), not a bare INSERT that errors", () => {
        const { query } = mk("mysql").getInsertStatementQuery("t", rows, header, "UPDATE");
        expect(query).toMatch(/ON DUPLICATE KEY UPDATE `id` = `id`/);
    });

    test("Postgres emits ON CONFLICT ... DO NOTHING (skip)", () => {
        const { query } = mk("pgsql").getInsertStatementQuery("t", rows, header, "UPDATE");
        expect(query).toMatch(/ON CONFLICT \("id"\) DO NOTHING/);
    });

    test("SQL Server MERGE holds a lock (WITH (HOLDLOCK)) and skips (no WHEN MATCHED)", () => {
        const { query } = mk("sqlserver").getInsertStatementQuery("t", rows, header, "UPDATE");
        expect(query).toMatch(/MERGE INTO .+ WITH \(HOLDLOCK\) AS target/);
        expect(query).not.toMatch(/WHEN MATCHED/); // nothing to update -> skip on match
    });
});
