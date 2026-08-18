import { Database } from '../db/database';
import { DatabaseConfig } from '../config/types';
import { escapeIdentifier, escapeLiteral } from '../db/utils/escape';

// Persisted run-audit table (F1): the engine writes one row per top-level load to a managed table, the
// same bootstrap-a-table + write-a-row pattern as schemaHistory but for RUNS, not DDL. Opt-in via
// `runAudit`. DIVERGENCE from schemaHistory: everything here is BEST-EFFORT — a failed bootstrap or insert
// warns and returns, never throwing into the load (an observability feature must never cause an outage).

/** One run-audit row. Built at each entry point from QueryStats (success) or a partial (failure). */
export interface RunAuditInput {
    table: string;
    success: boolean;
    rows: number;
    affectedRows: number;
    durationMs: number;
    rowsPerSecond: number;
    phases?: { prepare?: number; configure?: number; load?: number };
    staged: boolean;
    bulkLoad: boolean;
    error?: string;
    errorCode?: string;
}

// Escaped, dialect-quoted reference to the run-audit table (mirrors schemaHistory's historyTableRef).
function runAuditTableRef(config: DatabaseConfig): string {
    const table = config.runAuditTable || 'autosql_runs';
    const schema = config.runAuditSchema || config.schema;
    const dialect = config.sqlDialect;
    const t = escapeIdentifier(table, dialect);
    return schema ? `${escapeIdentifier(schema, dialect)}.${t}` : t;
}

// Bootstrap — CREATE TABLE IF NOT EXISTS, per dialect (three real branches: pgsql ≠ sqlserver). The core
// metrics are typed scalar columns so they're queryable without JSON functions. Best-effort: warns and
// returns on failure (does NOT throw — unlike schemaHistory's fail-loud bootstrap).
async function bootstrapRunAuditTable(db: Database): Promise<void> {
    const ref = runAuditTableRef(db.getConfig());
    const dialect = db.getConfig().sqlDialect;
    let ddl: string;
    if (dialect === 'mysql') {
        ddl = `CREATE TABLE IF NOT EXISTS ${ref} (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_at          DATETIME     NOT NULL,
  schema_name     VARCHAR(255),
  table_name      VARCHAR(255) NOT NULL,
  success         TINYINT(1)   NOT NULL,
  rows_in         BIGINT,
  affected_rows   BIGINT,
  duration_ms     BIGINT,
  rows_per_second BIGINT,
  prepare_ms      INT,
  configure_ms    INT,
  load_ms         INT,
  staged          TINYINT(1),
  bulk_load       TINYINT(1),
  error           TEXT,
  error_code      VARCHAR(64)
);`;
    } else if (dialect === 'sqlserver') {
        ddl = `IF OBJECT_ID(${escapeLiteral(ref, 'sqlserver')}, 'U') IS NULL
CREATE TABLE ${ref} (
  id              BIGINT IDENTITY(1,1) PRIMARY KEY,
  run_at          DATETIME2     NOT NULL,
  schema_name     NVARCHAR(255),
  table_name      NVARCHAR(255) NOT NULL,
  success         BIT           NOT NULL,
  rows_in         BIGINT,
  affected_rows   BIGINT,
  duration_ms     BIGINT,
  rows_per_second BIGINT,
  prepare_ms      INT,
  configure_ms    INT,
  load_ms         INT,
  staged          BIT,
  bulk_load       BIT,
  error           NVARCHAR(MAX),
  error_code      NVARCHAR(64)
);`;
    } else {
        ddl = `CREATE TABLE IF NOT EXISTS ${ref} (
  id              BIGSERIAL PRIMARY KEY,
  run_at          TIMESTAMPTZ  NOT NULL,
  schema_name     VARCHAR(255),
  table_name      VARCHAR(255) NOT NULL,
  success         BOOLEAN      NOT NULL,
  rows_in         BIGINT,
  affected_rows   BIGINT,
  duration_ms     BIGINT,
  rows_per_second BIGINT,
  prepare_ms      INTEGER,
  configure_ms    INTEGER,
  load_ms         INTEGER,
  staged          BOOLEAN,
  bulk_load       BOOLEAN,
  error           TEXT,
  error_code      VARCHAR(64)
);`;
    }
    const res = await db.runTransaction([{ query: ddl, params: [] }]);
    if (!res.success) throw new Error(res.error || 'run-audit bootstrap failed');
}

const AUDIT_COLS = 'run_at, schema_name, table_name, success, rows_in, affected_rows, duration_ms, rows_per_second, prepare_ms, configure_ms, load_ms, staged, bulk_load, error, error_code';

// Parameterised INSERT of one row, per dialect (? / $n / @pN). run_at is app UTC (a Date), not the DB
// clock, so multi-host runs are comparable (mirrors schemaHistory's applied_at handling).
async function insertRunAudit(db: Database, input: RunAuditInput): Promise<void> {
    const config = db.getConfig();
    const ref = runAuditTableRef(config);
    const dialect = config.sqlDialect;
    const schemaName = config.runAuditSchema || config.schema || null;
    const params = [
        new Date(), schemaName, input.table, input.success,
        input.rows, input.affectedRows, input.durationMs, input.rowsPerSecond,
        input.phases?.prepare ?? null, input.phases?.configure ?? null, input.phases?.load ?? null,
        input.staged, input.bulkLoad, input.error ?? null, input.errorCode ?? null,
    ];
    const n = params.length;
    let placeholders: string;
    if (dialect === 'mysql') placeholders = Array(n).fill('?').join(', ');
    else if (dialect === 'sqlserver') placeholders = Array.from({ length: n }, (_, i) => `@p${i}`).join(', ');
    else placeholders = Array.from({ length: n }, (_, i) => `$${i + 1}`).join(', ');
    const res = await db.runQuery({ query: `INSERT INTO ${ref} (${AUDIT_COLS}) VALUES (${placeholders})`, params });
    if (!res.success) throw new Error(res.error || 'run-audit insert failed'); // so writeRunAudit warns
}

/**
 * Best-effort: bootstrap the run-audit table (once, idempotent) and write one row. No-ops unless
 * `runAudit` is enabled. NEVER throws into the load — any failure warns and returns, so an audit-table
 * problem (no CREATE grant, incompatible pre-existing table) degrades the observability feature, not the load.
 */
export async function writeRunAudit(db: Database, input: RunAuditInput): Promise<void> {
    if (!db.getConfig().runAudit) return;
    try {
        await bootstrapRunAuditTable(db);
        await insertRunAudit(db, input);
    } catch (e) {
        db.warn(`runAudit: could not record the run for '${input.table}' (${e instanceof Error ? e.message : String(e)}); the load succeeded — only the audit row is missing.`);
    }
}
