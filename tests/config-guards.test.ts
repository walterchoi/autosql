import { validateConfig } from "../src/helpers/utilities";

// Fail-loud config guards (A5/A19): features whose builders are Postgres-only must be rejected up
// front on SQL Server, rather than running and then silently dropping rows / silently no-op'ing.
describe("config guards — unsupported SQL Server combinations fail loud", () => {
    test("rejectedRowsTable on SQL Server throws (builder emits Postgres-only DDL → would drop rows)", () => {
        expect(() => validateConfig({ sqlDialect: "sqlserver", rejectedRowsTable: "rejects" } as any))
            .toThrow(/rejectedRowsTable is not yet supported on SQL Server/);
    });
    test("schemaHistory on SQL Server throws (bootstrap emits Postgres-only DDL → would no-op)", () => {
        expect(() => validateConfig({ sqlDialect: "sqlserver", schemaHistory: true } as any))
            .toThrow(/schemaHistory is not yet supported on SQL Server/);
    });
    test("both are allowed on MySQL / Postgres", () => {
        expect(() => validateConfig({ sqlDialect: "pgsql", rejectedRowsTable: "rejects" } as any)).not.toThrow();
        expect(() => validateConfig({ sqlDialect: "mysql", schemaHistory: true } as any)).not.toThrow();
    });
});
