import { parseDatabaseMetaData } from "../src/helpers/utilities";

// parseDatabaseMetaData turns a raw INFORMATION_SCHEMA-shaped result set into a MetadataHeader (or
// a per-table Record<string, MetadataHeader> when multiple tables are present). Only its MySQL
// tinyint-boolean branch had a dedicated test; the rest of its real surface — the mixed-rows
// throw, multi-table grouping, and each dialect's distinct auto-increment signal — had none.

const row = (overrides: Record<string, unknown>) => ({
    column_name: "col", data_type: "varchar", is_nullable: "YES", column_key: "", ...overrides,
});

describe("parseDatabaseMetaData", () => {
    test("an empty rows array returns null", () => {
        expect(parseDatabaseMetaData([])).toBeNull();
    });

    test("rows with no column_name are skipped (column is a header-only artifact, not a real one)", () => {
        const result = parseDatabaseMetaData([{ data_type: "varchar" }]) as any;
        expect(result).toBeNull();
    });

    test("mixed rows (some with table_name, some without) throw", () => {
        expect(() => parseDatabaseMetaData([
            row({ table_name: "t1" }),
            row({}), // no table_name
        ])).toThrow(/Inconsistent data/);
    });

    test("a single-table result set (no table_name on any row) returns a flat MetadataHeader", () => {
        const result = parseDatabaseMetaData([row({ column_name: "id" }), row({ column_name: "name" })]) as any;
        expect(result.id).toBeDefined();
        expect(result.name).toBeDefined();
        expect(result.t1).toBeUndefined(); // not nested under any table key
    });

    test("a multi-table result set groups columns correctly per table", () => {
        const result = parseDatabaseMetaData([
            row({ table_name: "users", column_name: "id" }),
            row({ table_name: "users", column_name: "email" }),
            row({ table_name: "orders", column_name: "order_id" }),
        ]) as any;
        expect(Object.keys(result).sort()).toEqual(["orders", "users"]);
        expect(Object.keys(result.users).sort()).toEqual(["email", "id"]);
        expect(Object.keys(result.orders)).toEqual(["order_id"]);
    });

    test("MySQL auto-increment signal: extra contains 'auto_increment'", () => {
        const result = parseDatabaseMetaData([row({ column_name: "id", extra: "auto_increment" })]) as any;
        expect(result.id.autoIncrement).toBe(true);
    });

    test("Postgres auto-increment signal: column_default contains 'nextval'", () => {
        const result = parseDatabaseMetaData([row({ column_name: "id", column_default: "nextval('seq')" })]) as any;
        expect(result.id.autoIncrement).toBe(true);
    });

    test("SQL Server auto-increment signal: is_identity as the string \"1\"", () => {
        const result = parseDatabaseMetaData([row({ column_name: "id", is_identity: "1" })]) as any;
        expect(result.id.autoIncrement).toBe(true);
    });

    test("SQL Server auto-increment signal: is_identity as a real boolean true", () => {
        const result = parseDatabaseMetaData([row({ column_name: "id", is_identity: true })]) as any;
        expect(result.id.autoIncrement).toBe(true);
    });

    test("a column with none of the three signals is not flagged auto-increment", () => {
        const result = parseDatabaseMetaData([row({ column_name: "id" })]) as any;
        expect(result.id.autoIncrement).toBe(false);
    });

    test("column_key maps to primary/unique/index flags", () => {
        const result = parseDatabaseMetaData([
            row({ column_name: "id", column_key: "PRIMARY" }),
            row({ column_name: "code", column_key: "UNIQUE" }),
            row({ column_name: "name", column_key: "INDEX" }),
            row({ column_name: "plain", column_key: "" }),
        ]) as any;
        expect(result.id).toMatchObject({ primary: true, unique: false, index: false });
        expect(result.code).toMatchObject({ primary: false, unique: true, index: false });
        expect(result.name).toMatchObject({ primary: false, unique: false, index: true });
        expect(result.plain).toMatchObject({ primary: false, unique: false, index: false });
    });

    test("unique_index_name passes through as uniqueName, absent when empty", () => {
        const result = parseDatabaseMetaData([
            row({ column_name: "code", unique_index_name: "uq_code" }),
            row({ column_name: "plain", unique_index_name: "" }),
        ]) as any;
        expect(result.code.uniqueName).toBe("uq_code");
        expect(result.plain.uniqueName).toBeUndefined();
    });

    test("column_default is carried as ddlDefault, not default (A3 — never bound as a row value)", () => {
        const result = parseDatabaseMetaData([row({ column_name: "id", column_default: "0" })]) as any;
        expect(result.id.ddlDefault).toBe("0");
        expect(result.id.default).toBeUndefined();
    });

    test("case-insensitive column names (e.g. Oracle/SQL Server uppercase catalogs) are normalized", () => {
        const result = parseDatabaseMetaData([
            { COLUMN_NAME: "id", DATA_TYPE: "int", IS_NULLABLE: "NO", COLUMN_KEY: "" },
        ]) as any;
        expect(result.id).toBeDefined();
        expect(result.id.allowNull).toBe(false);
    });
});
