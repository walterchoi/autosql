import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// Spec-4 slice D (§3.8): the atomic staging-degradation + row-level-history combo (rejectedRowsTable +
// addHistory) now works on SQL Server. Each PK's before-image capture and its merge run in ONE
// transaction (via the new SQL Server pkFilter path), so a diverted row leaves neither a data change nor
// a spurious before-image. Mirrors staging-degradation-history-live (pg/mysql), hardcoded for SQL Server.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");

describe("SQL Server atomic degradation + history (live, spec-4 slice D)", () => {
    const TABLE = "ss_atomic_degr";
    const HISTORY = TABLE + "__history";
    const REJECTED = "ss_atomic_degr_rejected";
    const ref = `${qi("test_schema")}.${qi(TABLE)}`;
    const histRef = `${qi("test_schema")}.${qi(HISTORY)}`;
    const rejRef = `${qi("test_schema")}.${qi(REJECTED)}`;
    const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
    const baseConfig = { ...CONFIG, addTimestamps: false, addHistory: true, historyTables: [TABLE], rejectedRowsTable: REJECTED };

    let admin: Database;
    const dropAll = async () => { for (const r of [tempRef, histRef, ref, rejRef]) await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {}); };
    const valOf = async (id: number) => { const r = await admin.runQuery({ query: `SELECT ${qi("val")} AS v FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] }); return r.results!.length ? Number(Object.values(r.results![0])[0]) : null; };
    const rowCount = async (t: string) => Number(Object.values((await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM ${t}`, params: [] })).results![0])[0]);
    const historyIds = async () => { const r = await admin.runQuery({ query: `SELECT ${qi("id")} AS id, ${qi("val")} AS val FROM ${histRef} ORDER BY ${qi("id")}`, params: [] }); return (r.results ?? []).map((row: any) => ({ id: Number(row.id), val: Number(row.val) })); };

    beforeAll(async () => { admin = Database.create(baseConfig); await admin.establishConnection(); await admin.runQuery(admin.getCreateSchemaQuery("test_schema")); });
    afterAll(async () => { await dropAll(); await admin.closeConnection(); });
    beforeEach(async () => {
        await dropAll();
        await admin.runQuery({ query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("val")} INT, CONSTRAINT ${qi("ss_val_nonneg")} CHECK (${qi("val")} >= 0))`, params: [] });
        await admin.runQuery({ query: `INSERT INTO ${ref} (${qi("id")}, ${qi("val")}) VALUES (1, 10), (2, 20)`, params: [] });
    });

    test("diverts the CHECK-violating row with no before-image; the good row lands with its before-image", async () => {
        const db = Database.create(baseConfig); await db.establishConnection();
        try {
            const r = await db.autoSQL(TABLE, [{ id: 1, val: 15 }, { id: 2, val: -5 }]);
            expect(r.success).toBe(true);
            expect(await valOf(1)).toBe(15);   // id=1 updated
            expect(await valOf(2)).toBe(20);   // id=2 diverted, unchanged
            expect(await rowCount(rejRef)).toBe(1);
            expect(await historyIds()).toEqual([{ id: 1, val: 10 }]); // only the changed row's before-image
        } finally { await db.closeConnection(); }
    });

    test("discriminating: a per-PK merge failure rolls back its before-image (proves single transaction)", async () => {
        const db = Database.create(baseConfig); await db.establishConnection();
        const failing = { query: `SELECT * FROM ${qi("test_schema")}.${qi("__no_such_table_xyz__")}`, params: [] as any[] };
        const mergeSpy = jest.spyOn(db, "getInsertFromStagingQuery").mockReturnValue(failing);
        try {
            const r = await db.autoSQL(TABLE, [{ id: 1, val: 15 }]);
            expect(r.success).toBe(true);       // degraded: diverted
            expect(await valOf(1)).toBe(10);    // merge never applied
            expect(await rowCount(rejRef)).toBe(1);
            expect(await historyIds()).toEqual([]); // before-image shared the failing txn → rolled back
        } finally { mergeSpy.mockRestore(); await db.closeConnection(); }
    });
});
