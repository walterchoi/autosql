import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// Spec-4 slice E (§3.6): SQL Server rejects ALTER COLUMN that changes a column's TYPE while an index or
// unique constraint depends on it (err 5074). The alter builder now wraps a type-change ALTER: drop the
// single-column indexes / unique constraints on the column, alter, then recreate them (one batch).
// Length/nullability changes are unaffected (they don't hit 5074).

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");
const ref = (t: string) => `${qi("test_schema")}.${qi(t)}`;

describe("SQL Server ALTER COLUMN on an indexed column (live, spec-4 slice E)", () => {
    let db: Database;
    const drop = async (t: string) => { await db.runQuery(db.getDropTableQuery(t)).catch(() => {}); };
    const colType = async (t: string, col: string): Promise<string> => {
        const r = await db.runQuery({ query: `SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='test_schema' AND TABLE_NAME='${t}' AND COLUMN_NAME='${col}'`, params: [] });
        return String((r.results![0] as any).DATA_TYPE);
    };
    const indexExists = async (t: string, name: string): Promise<boolean> => {
        const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM sys.indexes WHERE name='${name}' AND object_id=OBJECT_ID('test_schema.${t}')`, params: [] });
        return Number((r.results![0] as any).c) > 0;
    };
    const count = async (t: string) => Number(Object.values((await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(t)}`, params: [] })).results![0])[0]);

    beforeAll(async () => { db = Database.create(CONFIG); await db.establishConnection(); await db.runQuery(db.getCreateSchemaQuery("test_schema")); });
    afterAll(async () => { await db.closeConnection(); });

    test("type change (int→bigint) on a NON-UNIQUE indexed column: index dropped, altered, recreated", async () => {
        const TABLE = "ss_alt_idx";
        await drop(TABLE);
        await db.runQuery({ query: `CREATE TABLE ${ref(TABLE)} (${qi("id")} INT PRIMARY KEY, ${qi("code")} INT)`, params: [] });
        await db.runQuery({ query: `CREATE INDEX ${qi("ix_code")} ON ${ref(TABLE)} (${qi("code")})`, params: [] });
        await db.runQuery({ query: `INSERT INTO ${ref(TABLE)} (${qi("id")}, ${qi("code")}) VALUES (1, 100)`, params: [] });

        // 5,000,000,000 > INT max → forces code INT→BIGINT on an indexed column.
        const res = await db.autoSQL(TABLE, [{ id: 2, code: 5000000000 }]);
        expect(res.success).toBe(true);
        expect(await colType(TABLE, "code")).toBe("bigint"); // altered
        expect(await indexExists(TABLE, "ix_code")).toBe(true); // recreated
        expect(await count(TABLE)).toBe(2);
        await drop(TABLE);
    });

    test("type change on a UNIQUE-CONSTRAINT column: constraint dropped, altered, recreated", async () => {
        const TABLE = "ss_alt_uq";
        await drop(TABLE);
        await db.runQuery({ query: `CREATE TABLE ${ref(TABLE)} (${qi("id")} INT PRIMARY KEY, ${qi("sku")} INT, CONSTRAINT ${qi("uq_sku")} UNIQUE (${qi("sku")}))`, params: [] });
        await db.runQuery({ query: `INSERT INTO ${ref(TABLE)} (${qi("id")}, ${qi("sku")}) VALUES (1, 100)`, params: [] });

        const res = await db.autoSQL(TABLE, [{ id: 2, sku: 6000000000 }]);
        expect(res.success).toBe(true);
        expect(await colType(TABLE, "sku")).toBe("bigint");
        expect(await indexExists(TABLE, "uq_sku")).toBe(true); // unique constraint (sys.indexes) recreated
        await drop(TABLE);
    });

    test("length-only widen on an indexed column still works (no wrap needed)", async () => {
        const TABLE = "ss_alt_len";
        await drop(TABLE);
        await db.runQuery({ query: `CREATE TABLE ${ref(TABLE)} (${qi("id")} INT PRIMARY KEY, ${qi("name")} NVARCHAR(10))`, params: [] });
        await db.runQuery({ query: `CREATE INDEX ${qi("ix_name")} ON ${ref(TABLE)} (${qi("name")})`, params: [] });
        await db.runQuery({ query: `INSERT INTO ${ref(TABLE)} (${qi("id")}, ${qi("name")}) VALUES (1, 'short')`, params: [] });

        const res = await db.autoSQL(TABLE, [{ id: 2, name: "a much longer value than ten characters" }]);
        expect(res.success).toBe(true);
        expect(await indexExists(TABLE, "ix_name")).toBe(true);
        await drop(TABLE);
    });
});
