import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// Spec-2 slice 2 — VERIFICATION (no build): row-level history (addHistory) and split tables (autoSplit)
// were implemented for SQL Server but had no live coverage (⚠️ in the matrix). The shared
// history-incremental-live / split tests filter SQL Server out (D-F caution). This runs the same
// assertions against the live azure-sql-edge `sqlserver` service to promote ⚠️ → ✅.
// NOTE: the atomic history+degradation combo guard is deliberately NOT touched here — that requires
// rejectedRowsTable (slice 4) and a loadStrategy routing change, so it stays as-is (see spec-1 §5.b).

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");

describe("SQL Server row-level history (live, spec-2 slice 2)", () => {
    const TABLE = "ss_history_live";
    const HISTORY = TABLE + "__history";
    const ref = `${qi("test_schema")}.${qi(TABLE)}`;
    const histRef = `${qi("test_schema")}.${qi(HISTORY)}`;
    const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
    const baseConfig = { ...CONFIG, addTimestamps: false, addHistory: true, historyTables: [TABLE] };

    let admin: Database;
    const dropAll = async () => {
        for (const r of [tempRef, histRef, ref]) await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
    };
    const count = async (tableRef: string) => {
        const r = await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM ${tableRef}`, params: [] });
        return Number(Object.values(r.results![0])[0]);
    };
    const valOf = async (id: number) => {
        const r = await admin.runQuery({ query: `SELECT ${qi("val")} AS v FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
        return r.results!.length ? Number(Object.values(r.results![0])[0]) : null;
    };

    beforeAll(async () => { admin = Database.create(baseConfig); await admin.establishConnection(); await admin.runQuery(admin.getCreateSchemaQuery("test_schema")); });
    afterAll(async () => { await dropAll(); await admin.closeConnection(); });
    beforeEach(async () => { await dropAll(); });

    test("updating 1 of 3 rows writes exactly ONE before-image (INNER JOIN, not per table row)", async () => {
        const db = Database.create(baseConfig); await db.establishConnection();
        try {
            expect((await db.autoSQL(TABLE, [{ id: 1, val: 10 }, { id: 2, val: 20 }, { id: 3, val: 30 }])).success).toBe(true);
            await admin.runQuery({ query: `DELETE FROM ${histRef}`, params: [] });
            expect(await count(histRef)).toBe(0);

            expect((await db.autoSQL(TABLE, [{ id: 2, val: 25 }])).success).toBe(true);
            expect(await valOf(2)).toBe(25);

            expect(await count(histRef)).toBe(1); // exactly one before-image
            const hist = await admin.runQuery({ query: `SELECT ${qi("id")} AS id, ${qi("val")} AS val FROM ${histRef}`, params: [] });
            expect(Number((hist.results![0] as any).id)).toBe(2);
            expect(Number((hist.results![0] as any).val)).toBe(20); // the BEFORE value
        } finally { await db.closeConnection(); }
    });

    test("re-loading an unchanged batch writes NO before-image", async () => {
        const db = Database.create(baseConfig); await db.establishConnection();
        try {
            expect((await db.autoSQL(TABLE, [{ id: 1, val: 10 }, { id: 2, val: 20 }])).success).toBe(true);
            await admin.runQuery({ query: `DELETE FROM ${histRef}`, params: [] });
            expect((await db.autoSQL(TABLE, [{ id: 1, val: 10 }, { id: 2, val: 20 }])).success).toBe(true);
            expect(await count(histRef)).toBe(0);
        } finally { await db.closeConnection(); }
    });
});

describe("SQL Server split tables (live, spec-2 slice 2)", () => {
    const TABLE = "ss_split_live";
    const baseConfig = { ...CONFIG, addTimestamps: false, autoSplit: true };
    // MAX_COLUMN_COUNT is 100, so a >100-column table must partition. Build a wide keyed row.
    const wideRow = (id: number): Record<string, any> => {
        const r: Record<string, any> = { id };
        for (let i = 0; i < 120; i++) r["c" + i] = "v" + (i % 7);
        return r;
    };
    let admin: Database;

    const partTables = async (): Promise<string[]> => {
        const r = await admin.runQuery({
            query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @p0 AND TABLE_NAME LIKE @p1 ORDER BY TABLE_NAME`,
            params: ["test_schema", TABLE + "__part_%"],
        });
        return (r.results || []).map((row: any) => row.TABLE_NAME ?? row.table_name);
    };
    const dropAll = async () => {
        for (const t of await partTables()) await admin.runQuery({ query: `DROP TABLE IF EXISTS ${qi("test_schema")}.${qi(t)}`, params: [] }).catch(() => {});
        await admin.runQuery({ query: `DROP TABLE IF EXISTS ${qi("test_schema")}.${qi(TABLE)}`, params: [] }).catch(() => {});
    };

    beforeAll(async () => { admin = Database.create(baseConfig); await admin.establishConnection(); await admin.runQuery(admin.getCreateSchemaQuery("test_schema")); await dropAll(); });
    afterAll(async () => { await dropAll(); await admin.closeConnection(); });

    test("a wide table partitions into __part_N tables that all share the primary key", async () => {
        const db = Database.create(baseConfig); await db.establishConnection();
        try {
            // 121 columns incl. PK, MAX_COLUMN_COUNT 100 → must split across multiple partition tables.
            expect((await db.autoSQL(TABLE, [wideRow(1), wideRow(2)])).success).toBe(true);
            const parts = await partTables();
            expect(parts.length).toBeGreaterThanOrEqual(2); // actually split
            // Every partition carries the id PK so they can be re-joined.
            for (const p of parts) {
                const cols = await admin.runQuery({ query: `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @p0 AND TABLE_NAME = @p1`, params: ["test_schema", p] });
                const names = (cols.results || []).map((r: any) => (r.COLUMN_NAME ?? r.column_name));
                expect(names).toContain("id");
            }
        } finally { await db.closeConnection(); }
    });
});
