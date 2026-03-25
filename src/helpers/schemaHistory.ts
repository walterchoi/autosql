import crypto from 'crypto';
import os from 'os';
import { Database } from '../db/database';
import { MetadataHeader, DatabaseConfig, QueryResult } from '../config/types';
import { SchemaDriftError } from '../errors';

// ---------------------------------------------------------------------------
// Stable JSON stringify (key-sorted, recursive) for deterministic checksums
// ---------------------------------------------------------------------------
function stableStringify(val: unknown): string {
    if (Array.isArray(val)) return `[${val.map(stableStringify).join(',')}]`;
    if (val !== null && typeof val === 'object') {
        const keys = Object.keys(val as object).sort();
        return `{${keys.map(k => `${JSON.stringify(k)}:${stableStringify((val as any)[k])}`).join(',')}}`;
    }
    return JSON.stringify(val);
}

export function computeChecksum(schema: MetadataHeader): string {
    return crypto.createHash('sha256').update(stableStringify(schema)).digest('hex');
}

function getAppliedBy(): string {
    return `${os.hostname()}:${process.pid}`;
}

function historyTableRef(config: DatabaseConfig): string {
    const table = config.schemaHistoryTable || 'autosql_schema_history';
    const schema = config.schemaHistorySchema || config.schema;
    return schema ? `${schema}.${table}` : table;
}

// ---------------------------------------------------------------------------
// Bootstrap — CREATE TABLE IF NOT EXISTS (dialect-aware via dialect on config)
// ---------------------------------------------------------------------------
export async function bootstrapSchemaHistoryTable(db: Database): Promise<void> {
    const ref = historyTableRef(db.getConfig());
    const dialect = db.getConfig().sqlDialect;
    let ddl: string;
    if (dialect === 'mysql') {
        ddl = `CREATE TABLE IF NOT EXISTS \`${ref.replace('.', '\`.\`')}\` (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  table_name      VARCHAR(255) NOT NULL,
  version         INT UNSIGNED NOT NULL,
  status          VARCHAR(20)  NOT NULL,
  applied_at      DATETIME     NOT NULL,
  applied_by      VARCHAR(255),
  previous_schema JSON         NOT NULL,
  new_schema      JSON,
  changes         JSON         NOT NULL,
  checksum        CHAR(64),
  UNIQUE KEY uq_table_version (table_name, version)
);`;
    } else {
        // PostgreSQL — split schema.table if present
        const parts = ref.includes('.') ? ref.split('.') : [null, ref];
        const schemaQ = parts[0] ? `"${parts[0]}"."${parts[1]}"` : `"${parts[1]}"`;
        ddl = `CREATE TABLE IF NOT EXISTS ${schemaQ} (
  id              BIGSERIAL PRIMARY KEY,
  table_name      VARCHAR(255) NOT NULL,
  version         INTEGER      NOT NULL,
  status          VARCHAR(20)  NOT NULL,
  applied_at      TIMESTAMPTZ  NOT NULL,
  applied_by      VARCHAR(255),
  previous_schema JSONB        NOT NULL,
  new_schema      JSONB,
  changes         JSONB        NOT NULL,
  checksum        CHAR(64),
  UNIQUE (table_name, version)
);`;
    }
    // runQuery validates single-statement — use executeQuery via runTransaction
    await db.runTransaction([{ query: ddl, params: [] }]);
}

// ---------------------------------------------------------------------------
// Record start of migration (status = 'pending')
// Returns the inserted record id.
// ---------------------------------------------------------------------------
export async function recordMigrationStart(
    db: Database,
    table: string,
    previousSchema: MetadataHeader,
    changes: object
): Promise<number> {
    const ref = historyTableRef(db.getConfig());
    const dialect = db.getConfig().sqlDialect;
    const appliedBy = getAppliedBy();
    const now = new Date();

    let query: string;
    if (dialect === 'mysql') {
        const [s, t] = ref.includes('.') ? ref.split('.') : ['', ref];
        const tableRef = s ? `\`${s}\`.\`${t}\`` : `\`${t}\``;
        query = `INSERT INTO ${tableRef} (table_name, version, status, applied_at, applied_by, previous_schema, changes)
SELECT ?, COALESCE(MAX(version), 0) + 1, 'pending', ?, ?, ?, ?
FROM ${tableRef} WHERE table_name = ?`;
        const result = await db.runQuery({
            query,
            params: [
                table,
                now.toISOString().slice(0, 19).replace('T', ' '),
                appliedBy,
                JSON.stringify(previousSchema),
                JSON.stringify(changes),
                table
            ]
        });
        // Return the inserted id via last insert id
        const idResult = await db.runQuery({ query: 'SELECT LAST_INSERT_ID() AS id', params: [] });
        return Number(idResult.results?.[0]?.id ?? 0);
    } else {
        const [s, t] = ref.includes('.') ? ref.split('.') : ['', ref];
        const tableRef = s ? `"${s}"."${t}"` : `"${t}"`;
        query = `INSERT INTO ${tableRef} (table_name, version, status, applied_at, applied_by, previous_schema, changes)
SELECT $1, COALESCE(MAX(version), 0) + 1, 'pending', $2, $3, $4::jsonb, $5::jsonb
FROM ${tableRef} WHERE table_name = $1
RETURNING id`;
        const result = await db.runQuery({
            query,
            params: [
                table,
                now.toISOString(),
                appliedBy,
                JSON.stringify(previousSchema),
                JSON.stringify(changes)
            ]
        });
        return Number(result.results?.[0]?.id ?? 0);
    }
}

// ---------------------------------------------------------------------------
// Update status after migration completes
// ---------------------------------------------------------------------------
async function updateHistoryStatus(
    db: Database,
    id: number,
    status: 'applied' | 'failed' | 'rolled_back',
    newSchema?: MetadataHeader
): Promise<void> {
    const ref = historyTableRef(db.getConfig());
    const dialect = db.getConfig().sqlDialect;
    const checksum = newSchema ? computeChecksum(newSchema) : null;

    if (dialect === 'mysql') {
        const [s, t] = ref.includes('.') ? ref.split('.') : ['', ref];
        const tableRef = s ? `\`${s}\`.\`${t}\`` : `\`${t}\``;
        await db.runQuery({
            query: `UPDATE ${tableRef} SET status = ?, new_schema = ?, checksum = ? WHERE id = ?`,
            params: [status, newSchema ? JSON.stringify(newSchema) : null, checksum, id]
        });
    } else {
        const [s, t] = ref.includes('.') ? ref.split('.') : ['', ref];
        const tableRef = s ? `"${s}"."${t}"` : `"${t}"`;
        await db.runQuery({
            query: `UPDATE ${tableRef} SET status = $1, new_schema = $2::jsonb, checksum = $3 WHERE id = $4`,
            params: [status, newSchema ? JSON.stringify(newSchema) : null, checksum, id]
        });
    }
}

export async function recordMigrationSuccess(db: Database, id: number, newSchema: MetadataHeader): Promise<void> {
    await updateHistoryStatus(db, id, 'applied', newSchema);
}

export async function recordMigrationRolledBack(db: Database, id: number): Promise<void> {
    await updateHistoryStatus(db, id, 'rolled_back');
}

export async function recordMigrationFailed(db: Database, id: number): Promise<void> {
    await updateHistoryStatus(db, id, 'failed');
}

// ---------------------------------------------------------------------------
// Drift detection
// ---------------------------------------------------------------------------

/**
 * Compare the live schema checksum to the last 'applied' record.
 * Returns { drifted: false } if no history exists yet (first run).
 */
export async function detectSchemaDrift(
    db: Database,
    table: string
): Promise<{ drifted: boolean; expected: string | null; actual: string }> {
    const config = db.getConfig();
    const ref = historyTableRef(config);
    const dialect = config.sqlDialect;

    const liveSchema = await db.getTableMetaData(config.schema || config.database || '', table);
    if (!liveSchema) {
        return { drifted: false, expected: null, actual: '' };
    }
    const actual = computeChecksum(liveSchema);

    let query: string;
    if (dialect === 'mysql') {
        const [s, t] = ref.includes('.') ? ref.split('.') : ['', ref];
        const tableRef = s ? `\`${s}\`.\`${t}\`` : `\`${t}\``;
        query = `SELECT checksum FROM ${tableRef} WHERE table_name = ? AND status = 'applied' ORDER BY version DESC LIMIT 1`;
    } else {
        const [s, t] = ref.includes('.') ? ref.split('.') : ['', ref];
        const tableRef = s ? `"${s}"."${t}"` : `"${t}"`;
        query = `SELECT checksum FROM ${tableRef} WHERE table_name = $1 AND status = 'applied' ORDER BY version DESC LIMIT 1`;
    }

    const result = await db.runQuery({ query, params: [table] });
    if (!result.success || !result.results?.length) {
        return { drifted: false, expected: null, actual };
    }

    const expected: string = result.results[0].checksum;
    const drifted = expected !== actual;

    if (drifted) {
        const msg = `Schema drift detected on table '${table}': live checksum (${actual.slice(0, 8)}…) does not match last recorded checksum (${expected.slice(0, 8)}…). The table may have been modified outside autosql.`;
        if (config.strictDriftDetection) {
            throw new SchemaDriftError(msg);
        }
        db.warn(msg);
    }

    return { drifted, expected, actual };
}

// ---------------------------------------------------------------------------
// Point-in-time schema reconstruction
// ---------------------------------------------------------------------------

/**
 * Returns the MetadataHeader that was in effect at `at` by reading the last
 * 'applied' history record with applied_at <= at.
 * Returns null if no applied record exists before that date.
 */
export async function getSchemaAt(
    db: Database,
    table: string,
    at: Date
): Promise<MetadataHeader | null> {
    const config = db.getConfig();
    const ref = historyTableRef(config);
    const dialect = config.sqlDialect;

    let query: string;
    if (dialect === 'mysql') {
        const [s, t] = ref.includes('.') ? ref.split('.') : ['', ref];
        const tableRef = s ? `\`${s}\`.\`${t}\`` : `\`${t}\``;
        query = `SELECT new_schema FROM ${tableRef} WHERE table_name = ? AND status = 'applied' AND applied_at <= ? ORDER BY applied_at DESC LIMIT 1`;
    } else {
        const [s, t] = ref.includes('.') ? ref.split('.') : ['', ref];
        const tableRef = s ? `"${s}"."${t}"` : `"${t}"`;
        query = `SELECT new_schema FROM ${tableRef} WHERE table_name = $1 AND status = 'applied' AND applied_at <= $2 ORDER BY applied_at DESC LIMIT 1`;
    }

    const result = await db.runQuery({ query, params: [table, at.toISOString()] });
    if (!result.success || !result.results?.length) return null;

    const raw = result.results[0].new_schema;
    if (!raw) return null;

    return (typeof raw === 'string' ? JSON.parse(raw) : raw) as MetadataHeader;
}
