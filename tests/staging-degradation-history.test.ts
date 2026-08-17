import { Database } from "../src/db/database";
import { MetadataHeader } from "../src/config/types";

// Unit coverage for the atomic staging-degradation-with-history plumbing:
//  - the opt-in combo (rejectedRowsTable + addHistory) errors up-front on SQL Server, whose
//    row-level history is unverified (decisions.md D-F);
//  - the single-PK filter used to run each PK's before-image + merge in one transaction builds the
//    right WHERE clause / params for mysql & pgsql.

const meta: MetadataHeader = { id: { type: "int", primary: true }, val: { type: "int" } };

describe("atomic staging degradation + history — plumbing", () => {
    test("configureHistoryTables errors up-front for SQL Server (row-level history unverified, D-F)", async () => {
        const db: any = Database.create({ sqlDialect: "sqlserver", host: "h", user: "u", password: "p", database: "d", schema: "s" });
        await expect(
            // configureHistoryTables moved to the HistoryCoordinator collaborator (R1 Slice 2, PR 2a);
            // same SQL-Server guard, now reached via handler.history.
            db.autoSQLHandler.history.configureHistoryTables([{ table: "t", data: [], metaData: meta, previousMetaData: null }])
        ).rejects.toThrow(/not supported for SQL Server/i);
    });

    test("getInsertFromStagingQuery scopes the merge to one primary key when pkFilter is given", () => {
        for (const [dialect, placeholder] of [["mysql", "= ?"], ["pgsql", "= $1"]] as const) {
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
        for (const dialect of ["mysql", "pgsql"] as const) {
            const db: any = Database.create({ sqlDialect: dialect, host: "h", user: "u", password: "p", database: "d", schema: "s" });
            const q = db.getInsertChangedRowsToHistoryQuery("t", meta, { id: 5 });
            expect(q.query).toMatch(/dwh_as_at/);
            expect(q.query).toMatch(/= \?|= \$1/); // the PK-filter binding
            expect(q.params).toEqual([5]);
            // Without a filter, the whole-table before-image carries no params (server-clock NOW()).
            const whole = db.getInsertChangedRowsToHistoryQuery("t", meta);
            expect(whole.params).toEqual([]);
        }
    });
});
