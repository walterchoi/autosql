import crypto from 'crypto';
import os from 'os';
import { Database } from '../db/database';
import { MetadataHeader, DatabaseConfig, QueryResult } from '../config/types';
import { SchemaDriftError } from '../errors';
import { escapeIdentifier } from '../db/utils/escape';

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

// Escaped, dialect-quoted reference to the history table (A13). Routes schema and table each through
// escapeIdentifier — the old version returned a raw `schema.table` string that every caller then
// re-split on '.', which dropped 3rd+ segments, corrupted any name containing a dot, and bypassed the
// injection-safe escaping the rest of the codebase uses.
function historyTableRef(config: DatabaseConfig): string {
    const table = config.schemaHistoryTable || 'autosql_schema_history';
    const schema = config.schemaHistorySchema || config.schema;
    const dialect = config.sqlDialect;
    const t = escapeIdentifier(table, dialect);
    return schema ? `${escapeIdentifier(schema, dialect)}.${t}` : t;
}

// ---------------------------------------------------------------------------
// Bootstrap — CREATE TABLE IF NOT EXISTS (dialect-aware via dialect on config)
// ---------------------------------------------------------------------------
export async function bootstrapSchemaHistoryTable(db: Database): Promise<void> {
    const ref = historyTableRef(db.getConfig());
    const dialect = db.getConfig().sqlDialect;
    let ddl: string;
    if (dialect === 'mysql') {
        ddl = `CREATE TABLE IF NOT EXISTS ${ref} (
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
        ddl = `CREATE TABLE IF NOT EXISTS ${ref} (
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
    const res = await db.runTransaction([{ query: ddl, params: [] }]);
    if (!res.success) {
        // schemaHistory is opt-in; if its table cannot be created the feature cannot work. Fail loud
        // rather than silently record nothing and later report "no drift" when actually blind (A19).
        throw new Error(`schemaHistory: failed to create the history table ${ref}: ${res.error ?? "unknown error"}. Grant the load user CREATE on the schema/database, or disable schemaHistory.`);
    }
}

// ---------------------------------------------------------------------------
// Record start of migration (status = 'pending')
// Returns the inserted record id.
// ---------------------------------------------------------------------------
// A `pending` migration start older than this is treated as orphaned (a crashed run) and
// swept to `failed`, so it can't linger forever. Comfortably longer than any real migration.
const STALE_PENDING_MS = 60 * 60 * 1000;

/** Best-effort: mark this table's orphaned `pending` starts (from crashed runs) as failed. */
async function sweepStalePending(db: Database, table: string): Promise<void> {
    const ref = historyTableRef(db.getConfig());
    const dialect = db.getConfig().sqlDialect;
    const cutoff = new Date(Date.now() - STALE_PENDING_MS);
    const tableRef = ref; // already dialect-escaped (A13)
    if (dialect === 'mysql') {
        await db.runQuery({
            query: `UPDATE ${tableRef} SET status = 'failed' WHERE table_name = ? AND status = 'pending' AND applied_at < ?`,
            params: [table, cutoff.toISOString().slice(0, 19).replace('T', ' ')]
        }).catch(() => {});
    } else {
        await db.runQuery({
            query: `UPDATE ${tableRef} SET status = 'failed' WHERE table_name = $1 AND status = 'pending' AND applied_at < $2`,
            params: [table, cutoff.toISOString()]
        }).catch(() => {});
    }
}

const UNIQUE_VIOLATION = /duplicate|unique/i;

export async function recordMigrationStart(
    db: Database,
    table: string,
    previousSchema: MetadataHeader,
    changes: object
): Promise<number | undefined> {
    const ref = historyTableRef(db.getConfig());
    const dialect = db.getConfig().sqlDialect;
    const appliedBy = getAppliedBy();

    await sweepStalePending(db, table);

    // The version is computed as MAX(version)+1 against a UNIQUE(table_name, version)
    // constraint. Without a schema lock, two concurrent migrations can compute the same
    // version and one hits a unique violation — recompute and retry a few times.
    const maxAttempts = 5;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const now = new Date();
        let result: QueryResult;
        if (dialect === 'mysql') {
            const tableRef = ref; // already dialect-escaped (A13)
            const query = `INSERT INTO ${tableRef} (table_name, version, status, applied_at, applied_by, previous_schema, changes)
SELECT ?, COALESCE(MAX(version), 0) + 1, 'pending', ?, ?, ?, ?
FROM ${tableRef} WHERE table_name = ?`;
            // Run the INSERT and LAST_INSERT_ID() in one transaction so they share a single
            // connection — LAST_INSERT_ID() is connection-scoped, so reading it on a separate
            // pooled connection returns 0/unrelated and leaves the row stuck at 'pending'.
            result = await db.runTransaction([
                {
                    query,
                    params: [
                        table,
                        now.toISOString().slice(0, 19).replace('T', ' '),
                        appliedBy,
                        JSON.stringify(previousSchema),
                        JSON.stringify(changes),
                        table
                    ]
                },
                { query: 'SELECT LAST_INSERT_ID() AS id', params: [] }
            ]);
        } else {
            const tableRef = ref; // already dialect-escaped (A13)
            // $1 is used both in the SELECT list and the WHERE clause; without an explicit cast
            // Postgres infers it as text in one place and varchar in the other and rejects the
            // statement with "inconsistent types deduced for parameter $1".
            const query = `INSERT INTO ${tableRef} (table_name, version, status, applied_at, applied_by, previous_schema, changes)
SELECT $1::varchar, COALESCE(MAX(version), 0) + 1, 'pending', $2, $3, $4::jsonb, $5::jsonb
FROM ${tableRef} WHERE table_name = $1
RETURNING id`;
            result = await db.runQuery({
                query,
                params: [
                    table,
                    now.toISOString(),
                    appliedBy,
                    JSON.stringify(previousSchema),
                    JSON.stringify(changes)
                ]
            });
        }

        const id = Number(result.results?.[0]?.id ?? 0);
        if (id > 0) return id;
        if (attempt < maxAttempts && UNIQUE_VIOLATION.test(result.error ?? '')) continue;
        // Failed to record the start — warn (the change proceeds, just untracked) and return undefined
        // so callers' `id !== undefined` guard skips the follow-up UPDATE (a 0 would run
        // UPDATE ... WHERE id = 0). Previously this returned silently (A19).
        db.warn(`schemaHistory: could not record migration start for '${table}' (${result.error ?? 'unknown error'}); the schema change will proceed but won't be recorded in history.`);
        return undefined;
    }
    db.warn(`schemaHistory: could not record migration start for '${table}' after ${maxAttempts} attempts; the schema change will proceed but won't be recorded in history.`);
    return undefined;
}

// ---------------------------------------------------------------------------
// Update status after migration completes
// ---------------------------------------------------------------------------
async function updateHistoryStatus(
    db: Database,
    id: number,
    status: 'applied' | 'failed' | 'rolled_back',
    newSchema?: MetadataHeader,
    checksumSchema?: MetadataHeader | null
): Promise<void> {
    const ref = historyTableRef(db.getConfig());
    const dialect = db.getConfig().sqlDialect;
    // The drift baseline must be comparable to the drift-time check, which reads the LIVE (introspected)
    // schema. So checksum the introspected `checksumSchema` when the caller re-introspects post-migration
    // (A6): both sides then come from introspection and match for an unchanged table — inferred-vs-
    // introspected would false-positive on legitimate type round-trips. `checksumSchema === undefined`
    // (caller didn't re-introspect) falls back to `newSchema`; an explicit `null` (introspection read
    // nothing) stores a null checksum → treated as "no baseline" at drift time, not a false positive.
    // `new_schema` itself stays the inferred schema (used for point-in-time reconstruction).
    const checksumSource = checksumSchema !== undefined ? checksumSchema : newSchema;
    const checksum = checksumSource ? computeChecksum(checksumSource) : null;

    if (dialect === 'mysql') {
        const tableRef = ref; // already dialect-escaped (A13)
        await db.runQuery({
            query: `UPDATE ${tableRef} SET status = ?, new_schema = ?, checksum = ? WHERE id = ?`,
            params: [status, newSchema ? JSON.stringify(newSchema) : null, checksum, id]
        });
    } else {
        const tableRef = ref; // already dialect-escaped (A13)
        await db.runQuery({
            query: `UPDATE ${tableRef} SET status = $1, new_schema = $2::jsonb, checksum = $3 WHERE id = $4`,
            params: [status, newSchema ? JSON.stringify(newSchema) : null, checksum, id]
        });
    }
}

export async function recordMigrationSuccess(db: Database, id: number, newSchema: MetadataHeader, checksumSchema?: MetadataHeader | null): Promise<void> {
    await updateHistoryStatus(db, id, 'applied', newSchema, checksumSchema);
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

    // Read the recorded baseline FIRST. No applied record (or a null checksum) → no reliable baseline
    // for this table: a first run (the table legitimately doesn't exist yet) or a migration whose
    // post-migration introspection couldn't read the table. Nothing to compare — return quietly, and
    // crucially DON'T warn about a "missing" table that isn't supposed to exist yet (A6/A19).
    let query: string;
    if (dialect === 'mysql') {
        const tableRef = ref; // already dialect-escaped (A13)
        query = `SELECT checksum FROM ${tableRef} WHERE table_name = ? AND status = 'applied' ORDER BY version DESC LIMIT 1`;
    } else {
        const tableRef = ref; // already dialect-escaped (A13)
        query = `SELECT checksum FROM ${tableRef} WHERE table_name = $1 AND status = 'applied' ORDER BY version DESC LIMIT 1`;
    }

    const result = await db.runQuery({ query, params: [table] });
    const expected: string | null = (result.success && result.results?.length) ? (result.results[0].checksum ?? null) : null;
    if (expected === null) {
        return { drifted: false, expected: null, actual: '' };
    }

    // We HAVE a baseline — read the live (introspected) schema and compare. Both the stored baseline
    // (recorded from a post-migration re-introspection) and this are introspection-derived, so an
    // unchanged table matches exactly.
    const liveSchema = await db.getTableMetaData(config.schema || config.database || '', table);
    if (!liveSchema) {
        // Had a baseline but can't read the live table now — NOT a clean bill. Warn instead of
        // silently reporting "no drift" (A19).
        db.warn(`Schema drift check for '${table}' could not read the live table schema (introspection returned nothing); skipping the check. This is not a clean bill — verify the table if it persists.`);
        return { drifted: false, expected, actual: '' };
    }
    const actual = computeChecksum(liveSchema);
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
        const tableRef = ref; // already dialect-escaped (A13)
        query = `SELECT new_schema FROM ${tableRef} WHERE table_name = ? AND status = 'applied' AND applied_at <= ? ORDER BY applied_at DESC LIMIT 1`;
    } else {
        const tableRef = ref; // already dialect-escaped (A13)
        query = `SELECT new_schema FROM ${tableRef} WHERE table_name = $1 AND status = 'applied' AND applied_at <= $2 ORDER BY applied_at DESC LIMIT 1`;
    }

    const result = await db.runQuery({ query, params: [table, at.toISOString()] });
    if (!result.success || !result.results?.length) return null;

    const raw = result.results[0].new_schema;
    if (!raw) return null;

    return (typeof raw === 'string' ? JSON.parse(raw) : raw) as MetadataHeader;
}
