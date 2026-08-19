import { Database } from "../src/db/database";
import { MetadataHeader } from "../src/config/types";

// Unit coverage for the atomic staging-degradation-with-history plumbing: the single-PK filter used to
// run each PK's before-image + merge in one transaction builds the right WHERE clause / params for all
// three dialects (SQL Server's pkFilter path landed in spec-4 §3.8; live proof in
// sqlserver-atomic-degradation-live).

const meta: MetadataHeader = { id: { type: "int", primary: true }, val: { type: "int" } };

describe("atomic staging degradation + history — plumbing", () => {
    test("getInsertFromStagingQuery scopes the merge to one primary key when pkFilter is given", () => {
        for (const [dialect, placeholder] of [["mysql", "= ?"], ["pgsql", "= $1"], ["sqlserver", "= @p0"]] as const) {
            const db: any = Database.create({ sqlDialect: dialect, host: "h", user: "u", password: "p", database: "d", schema: "s" });
            const q = db.getInsertFromStagingQuery("t", meta, "UPDATE", { id: 5 });
            expect(q.query).toMatch(/WHERE/i);
            expect(q.query).toContain(placeholder); // the PK-filter binding
            expect(q.params).toEqual([5]);
            // Without a filter, no WHERE / no params (the whole-table merge is unchanged).
            const whole = db.getInsertFromStagingQuery("t", meta, "UPDATE");
            expect(whole.params).toEqual([]);
            expect(whole.query).not.toMatch(/WHERE/i);
        }
    });

    test("getInsertChangedRowsToHistoryQuery scopes the before-image to one primary key when pkFilter is given", () => {
        for (const dialect of ["mysql", "pgsql", "sqlserver"] as const) {
            const db: any = Database.create({ sqlDialect: dialect, host: "h", user: "u", password: "p", database: "d", schema: "s" });
            const q = db.getInsertChangedRowsToHistoryQuery("t", meta, { id: 5 });
            expect(q.query).toMatch(/dwh_as_at/);
            expect(q.query).toMatch(/= \?|= \$1|= @p0/); // the PK-filter binding (mysql / pgsql / sqlserver)
            expect(q.params).toEqual([5]);
            // Without a filter, the whole-table before-image carries no params (server-clock NOW()).
            const whole = db.getInsertChangedRowsToHistoryQuery("t", meta);
            expect(whole.params).toEqual([]);
        }
    });
});
