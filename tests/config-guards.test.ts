import { validateConfig } from "../src/helpers/utilities";

// SQL Server parity (spec-2 slices 3 & 4): rejectedRowsTable and schemaHistory USED to fail loud on SQL
// Server (their builders were Postgres-only). They now emit T-SQL, so validateConfig must allow them —
// the guards were removed in the same slices that landed (and live-tested) the replacements. Live proof
// that they actually work lives in sqlserver-rejected-rows-live / sqlserver-schema-history-live.
describe("config guards — SQL Server now accepts rejectedRowsTable + schemaHistory (spec-2)", () => {
    test("rejectedRowsTable is allowed on SQL Server (T-SQL dead-letter builder, slice 4)", () => {
        expect(() => validateConfig({ sqlDialect: "sqlserver", rejectedRowsTable: "rejects" } as any)).not.toThrow();
    });
    test("schemaHistory is allowed on SQL Server (T-SQL bootstrap, slice 3)", () => {
        expect(() => validateConfig({ sqlDialect: "sqlserver", schemaHistory: true } as any)).not.toThrow();
    });
    test("both remain allowed on MySQL / Postgres", () => {
        expect(() => validateConfig({ sqlDialect: "pgsql", rejectedRowsTable: "rejects" } as any)).not.toThrow();
        expect(() => validateConfig({ sqlDialect: "mysql", schemaHistory: true } as any)).not.toThrow();
    });
    test("the rejectedRowsTable + addHistory ATOMIC combo is now allowed on SQL Server too (spec-4 §3.8)", () => {
        // The SQL Server atomic before-image+merge path (incl. the per-PK pkFilter form) landed, so the
        // combo is no longer rejected on any dialect (live proof: sqlserver-atomic-degradation-live).
        expect(() => validateConfig({ sqlDialect: "sqlserver", rejectedRowsTable: "rejects", addHistory: true, useStagingInsert: true, historyTables: ["t"] } as any)).not.toThrow();
        expect(() => validateConfig({ sqlDialect: "pgsql", rejectedRowsTable: "rejects", addHistory: true, useStagingInsert: true, historyTables: ["t"] } as any)).not.toThrow();
    });
});
