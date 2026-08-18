import { Database } from "../src/db/database";
import { DatabaseConfig, MetadataHeader } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// Spec-4 slice C — bulk-copy (request.bulk / TDS) on SQL Server (live). bulkLoadRows was a hard throw
// (soft-fallback to INSERT); it now builds a typed sql.Table from the column metadata and bulk-copies.
// Any failure still falls back to INSERT, so correctness is unconditional — these tests prove the fast
// path actually runs (not just the fallback) and round-trips typed values.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");
const ref = (t: string) => `${qi("test_schema")}.${qi(t)}`;

describe("SQL Server bulk-copy (live, spec-4 slice C)", () => {
    let db: Database;
    const count = async (t: string) => Number(Object.values((await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(t)}`, params: [] })).results![0])[0]);
    const drop = async (t: string) => { await db.runQuery(db.getDropTableQuery(t)).catch(() => {}); };

    beforeAll(async () => { db = Database.create(CONFIG); await db.establishConnection(); await db.runQuery(db.getCreateSchemaQuery("test_schema")); });
    afterAll(async () => { await db.closeConnection(); });

    test("bulkLoadRows request.bulk loads typed rows (with string→typed coercion)", async () => {
        const TABLE = "ss_bulk_direct";
        await drop(TABLE);
        await db.runQuery({ query: `CREATE TABLE ${ref(TABLE)} (${qi("id")} INT NOT NULL PRIMARY KEY, ${qi("name")} NVARCHAR(50), ${qi("amount")} DECIMAL(10,2), ${qi("active")} BIT, ${qi("ts")} DATETIME2)`, params: [] });
        const header: MetadataHeader = {
            id: { type: "int", allowNull: false, primary: true },
            name: { type: "varchar", length: 50 },
            amount: { type: "decimal", length: 10, decimal: 2 },
            active: { type: "boolean" },
            ts: { type: "datetime" },
        } as any;
        const columns = ["id", "name", "amount", "active", "ts"];
        // Values as strings (as the sqlized staging path produces) to exercise coercion.
        const rows = [
            ["1", "日本語 😀 café", "9.99", "true", "2024-01-15 10:00:00"],
            ["2", "b", "3.50", "false", "2024-07-20 18:30:00"],
        ];
        const res = await (db as any).bulkLoadRows(TABLE, columns, rows, header);
        expect(res.success).toBe(true);
        expect(res.affectedRows).toBe(2);
        expect(await count(TABLE)).toBe(2);
        const r = await db.runQuery({ query: `SELECT ${qi("amount")} AS amount, ${qi("active")} AS active, ${qi("name")} AS name FROM ${ref(TABLE)} WHERE ${qi("id")} = 1`, params: [] });
        expect(Number((r.results![0] as any).amount)).toBe(9.99);
        expect(Number((r.results![0] as any).active)).toBe(1);
        expect((r.results![0] as any).name).toBe("日本語 😀 café");
        await drop(TABLE);
    });

    test("autoSQL with bulkLoad:true lands rows via the bulk fast path (no INSERT fallback)", async () => {
        const TABLE = "ss_bulk_autosql";
        const warns: string[] = [];
        const logger = { warn: (m: string) => warns.push(m), error: () => {}, info: () => {}, debug: () => {} };
        const bdb = Database.create({ ...CONFIG, bulkLoad: true, logger: logger as any });
        await bdb.establishConnection();
        try {
            await bdb.runQuery(bdb.getDropTableQuery(TABLE)).catch(() => {});
            const data = Array.from({ length: 50 }, (_, i) => ({ id: i + 1, name: "n" + i, amount: Number(((i + 1) * 1.5).toFixed(2)), active: i % 2 === 0 }));
            const res = await bdb.autoSQL(TABLE, data);
            expect(res.success).toBe(true);
            const c = Number(Object.values((await bdb.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(TABLE)}`, params: [] })).results![0])[0]);
            expect(c).toBe(50);
            // Bulk succeeded → no "falling back to INSERT" warning was emitted.
            expect(warns.some(w => /falling back to INSERT/i.test(w))).toBe(false);
            await bdb.runQuery(bdb.getDropTableQuery(TABLE)).catch(() => {});
        } finally { await bdb.closeConnection(); }
    });
});
