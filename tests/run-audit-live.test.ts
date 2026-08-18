import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier, escapeLiteral } from "../src/db/utils/escape";

// Spec-3 (F1): the engine persists one row per top-level load to a managed run-audit table (autosql_runs),
// opt-in via `runAudit`, best-effort (a failed audit write never fails the load). Mirrors schemaHistory.

const SS: any = { sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd", database: "master", port: 1433, useWorkers: false };
const CONFIGS: any[] = [...Object.values(DB_CONFIG), SS];

CONFIGS.forEach((rawConfig) => {
    const config = { ...rawConfig, schema: "test_schema", useWorkers: false };
    const dialect = config.sqlDialect;
    const qi = (n: string) => escapeIdentifier(n, dialect);
    const lit = (v: string) => escapeLiteral(v, dialect);

    describe(`run-audit table (live) for ${String(dialect).toUpperCase()}`, () => {
        const TABLE = "run_audit_target";
        const AUDIT = "autosql_runs";
        const auditRef = `${qi("test_schema")}.${qi(AUDIT)}`;
        let admin: any;

        beforeAll(async () => { admin = Database.create(config); await admin.establishConnection(); await admin.runQuery(admin.getCreateSchemaQuery("test_schema")); });
        afterAll(async () => { await admin.runQuery({ query: `DROP TABLE IF EXISTS ${auditRef}`, params: [] }).catch(() => {}); await admin.closeConnection(); });

        const dropTarget = async () => { await admin.runQuery(admin.getDropTableQuery(TABLE)).catch(() => {}); };
        const clearAudit = async () => { await admin.runQuery({ query: `DELETE FROM ${auditRef} WHERE table_name = ${lit(TABLE)}`, params: [] }).catch(() => {}); };
        const auditRows = async (): Promise<any[]> => {
            const r = await admin.runQuery({ query: `SELECT table_name, success, rows_in, affected_rows, duration_ms, load_ms, error FROM ${auditRef} WHERE table_name = ${lit(TABLE)}`, params: [] }).catch(() => ({ results: [] }));
            return (r.results || []) as any[];
        };
        const auditTableExists = async (): Promise<boolean> => {
            const r = await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM information_schema.tables WHERE table_schema = ${lit("test_schema")} AND table_name = ${lit(AUDIT)}`, params: [] });
            return Number(Object.values(r.results![0])[0]) > 0;
        };

        beforeEach(async () => { await dropTarget(); await admin.runQuery({ query: `DROP TABLE IF EXISTS ${auditRef}`, params: [] }).catch(() => {}); });

        test("records exactly one row per autoSQL load, with the run metrics", async () => {
            const db = Database.create({ ...config, runAudit: true }); await db.establishConnection();
            try {
                expect((await db.autoSQL(TABLE, [{ id: 1, v: 10 }, { id: 2, v: 20 }, { id: 3, v: 30 }])).success).toBe(true);
                const rows = await auditRows();
                expect(rows.length).toBe(1);
                expect(rows[0].table_name).toBe(TABLE);
                expect(Number(rows[0].success)).toBe(1);           // BIT/TINYINT/BOOLEAN → 1/true
                expect(Number(rows[0].rows_in)).toBe(3);
                expect(Number(rows[0].duration_ms)).toBeGreaterThanOrEqual(0);
                expect(rows[0].error == null).toBe(true);
            } finally { await db.closeConnection(); }
        });

        test("autoSQLChunked writes exactly ONE row (not one per chunk)", async () => {
            const db = Database.create({ ...config, runAudit: true }); await db.establishConnection();
            try {
                async function* chunks() { yield [{ id: 1, v: 1 }]; yield [{ id: 2, v: 2 }]; yield [{ id: 3, v: 3 }]; }
                expect((await db.autoSQLChunked(TABLE, chunks())).success).toBe(true);
                expect((await auditRows()).length).toBe(1);
            } finally { await db.closeConnection(); }
        });

        test("records a failed load with success=false and a populated error", async () => {
            // A CHECK the batch violates forces a hard failure (no rejectedRowsTable).
            await admin.runQuery({ query: `CREATE TABLE ${qi("test_schema")}.${qi(TABLE)} (${qi("id")} INT PRIMARY KEY, ${qi("v")} INT, CONSTRAINT ${qi("ra_nonneg")} CHECK (${qi("v")} >= 0))`, params: [] });
            const db = Database.create({ ...config, runAudit: true }); await db.establishConnection();
            try {
                const res = await db.autoSQL(TABLE, [{ id: 1, v: -5 }]);
                expect(res.success).toBe(false);
                const rows = await auditRows();
                expect(rows.length).toBe(1);
                expect(Number(rows[0].success)).toBe(0);
                expect(String(rows[0].error || "").length).toBeGreaterThan(0);
            } finally { await db.closeConnection(); }
        });

        test("disabled (default) writes nothing — the audit table is never created", async () => {
            const db = Database.create({ ...config }); await db.establishConnection();
            try {
                expect((await db.autoSQL(TABLE, [{ id: 1, v: 1 }])).success).toBe(true);
                expect(await auditTableExists()).toBe(false);
            } finally { await db.closeConnection(); }
        });

        test("a broken audit table never fails the load (best-effort)", async () => {
            // Pre-create an incompatible autosql_runs (missing columns) → the INSERT fails → warn, load OK.
            await admin.runQuery({ query: `CREATE TABLE ${auditRef} (${qi("id")} INT)`, params: [] });
            const warns: string[] = [];
            const db = Database.create({ ...config, runAudit: true, logger: { warn: (m: string) => warns.push(m), error: () => {}, info: () => {}, debug: () => {} } as any });
            await db.establishConnection();
            try {
                expect((await db.autoSQL(TABLE, [{ id: 1, v: 1 }])).success).toBe(true); // load still succeeds
                expect(warns.some(w => /runAudit/i.test(w))).toBe(true);                 // and warned
            } finally { await db.closeConnection(); }
        });
    });
});
