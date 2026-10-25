import crypto from 'crypto';
import type { MetadataHeader, DatabaseConfig, QueryInput, supportedDialects } from '../config/types';
import { escapeIdentifier, escapeLiteral, assertSafeLength } from '../db/utils/escape';

export function generateRunId(): string {
    return crypto.randomBytes(4).toString('hex');
}

export function buildStreamStagingTableName(table: string, prefix: string, runId: string): string {
    return `${prefix}${table}__${runId}`;
}

export function orphanPattern(table: string, prefix: string): string {
    return `${prefix}${table}__%`;
}

/** Regex to confirm a table name is an autosql stream staging table (8-hex suffix) */
export function isAutosqlStreamTable(tableName: string, table: string, prefix: string): boolean {
    const expected = `${prefix}${table}__`;
    if (!tableName.startsWith(expected)) return false;
    const suffix = tableName.slice(expected.length);
    return /^[0-9a-f]{8}$/.test(suffix);
}

/** Marker segment separating a resumable (session-keyed) staging table from a per-run one. */
const RESUMABLE_MARKER = 'rs_';

/**
 * Name a resumable staging table from a caller-owned session key — a pure function of the key,
 * so a later run resolves the same table. Hashed so the caller-owned string can't affect the
 * identifier's shape or length.
 */
export function buildResumableStagingTableName(table: string, prefix: string, sessionKey: string): string {
    const hash = crypto.createHash('sha256').update(sessionKey).digest('hex').slice(0, 8);
    return `${prefix}${table}__${RESUMABLE_MARKER}${hash}`;
}

/**
 * Orphan cleanup skips these — owned by a caller's session, so outliving the run that created it
 * is normal, not a crash. An explicit check rather than relying on {@link isAutosqlStreamTable}'s
 * regex rejecting `rs_`, so the exclusion survives that regex changing.
 */
export function isResumableStreamTable(tableName: string, table: string, prefix: string): boolean {
    const expected = `${prefix}${table}__${RESUMABLE_MARKER}`;
    if (!tableName.startsWith(expected)) return false;
    return /^[0-9a-f]{8}$/.test(tableName.slice(expected.length));
}

/**
 * Columns of an existing staging table, so an adopted stream validates writes against the shape
 * that is really there rather than one inferred from its own first chunk.
 */
export function buildStagingColumnsQuery(stagingTable: string, config: DatabaseConfig): QueryInput {
    const schema = config.schema || config.database || '';
    const where = config.sqlDialect === 'mysql' ? '? AND table_name = ?'
        : config.sqlDialect === 'sqlserver' ? '@p0 AND table_name = @p1'
        : '$1 AND table_name = $2';
    return {
        query: `SELECT column_name FROM information_schema.columns WHERE table_schema = ${where} ORDER BY ordinal_position`,
        params: [schema, stagingTable]
    };
}

// --- Identifier escaping. Column names come from arbitrary caller JSON keys and table/schema names
//     from config, so every interpolated identifier must be quote-escaped. ---
function qi(name: string, dialect: supportedDialects): string {
    return escapeIdentifier(name, dialect);
}
function schemaPrefixFor(config: DatabaseConfig): string {
    return config.schema ? `${escapeIdentifier(config.schema, config.sqlDialect)}.` : '';
}

/**
 * Staging-only bookkeeping, never merged into the target. `_autosql_seq` carries the ordering: an
 * opaque cursor can't be sorted and `_autosql_staged_at` ties within MySQL's one-second resolution.
 * See docs/streaming.md, "Resuming across runs with a cursor".
 */
export const STAGING_CURSOR_COLUMN = '_autosql_cursor';
export const STAGING_STAGED_AT_COLUMN = '_autosql_staged_at';
export const STAGING_SEQ_COLUMN = '_autosql_seq';
export const STAGING_COLUMN_PREFIX = '_autosql_';
export const STAGING_META_COLUMNS = [STAGING_SEQ_COLUMN, STAGING_CURSOR_COLUMN, STAGING_STAGED_AT_COLUMN];

/** True for a column autosql injects into staging, so callers' data columns can be told apart. */
export function isStagingMetaColumn(column: string): boolean {
    return column.startsWith(STAGING_COLUMN_PREFIX);
}

/** CREATE TABLE DDL for the untyped (all TEXT) staging table. Columns come from the first chunk. */
export function buildCreateStreamStagingTableQuery(
    stagingTable: string,
    columns: string[],
    config: DatabaseConfig
): QueryInput {
    const dialect = config.sqlDialect;
    const schemaPrefix = schemaPrefixFor(config);
    const colType = dialect === 'mysql' ? 'LONGTEXT' : dialect === 'sqlserver' ? 'NVARCHAR(MAX)' : 'TEXT';
    const colDefs = columns.map(col => `${qi(col, dialect)} ${colType}`).join(',\n  ');
    const tableRef = `${schemaPrefix}${qi(stagingTable, dialect)}`;

    // DB clock, not the JS clock: a load spread across hosts would interleave their skew.
    const seq = qi(STAGING_SEQ_COLUMN, dialect);
    const stagedAt = qi(STAGING_STAGED_AT_COLUMN, dialect);
    const cursor = qi(STAGING_CURSOR_COLUMN, dialect);
    const metaDefs = dialect === 'mysql'
        ? `${seq} BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n  ${cursor} LONGTEXT NULL,\n  ${stagedAt} DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP`
        : dialect === 'sqlserver'
        ? `${seq} BIGINT IDENTITY(1,1) PRIMARY KEY,\n  ${cursor} NVARCHAR(MAX) NULL,\n  ${stagedAt} DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()`
        : `${seq} BIGSERIAL PRIMARY KEY,\n  ${cursor} TEXT NULL,\n  ${stagedAt} TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP`;

    const allDefs = `${metaDefs},\n  ${colDefs}`;
    if (dialect === 'sqlserver') {
        // No CREATE TABLE IF NOT EXISTS — guard with IF OBJECT_ID.
        return { query: `IF OBJECT_ID(${escapeLiteral(tableRef, 'sqlserver')}, 'U') IS NULL CREATE TABLE ${tableRef} (\n  ${allDefs}\n);`, params: [] };
    }
    return { query: `CREATE TABLE IF NOT EXISTS ${tableRef} (\n  ${allDefs}\n);`, params: [] };
}

/** Cursors ordered by their first row's seq; staged_at alone ties on MySQL's one-second resolution. */
export function buildStagingStateQuery(stagingTable: string, config: DatabaseConfig): QueryInput {
    const dialect = config.sqlDialect;
    const tableRef = `${schemaPrefixFor(config)}${qi(stagingTable, dialect)}`;
    const seq = qi(STAGING_SEQ_COLUMN, dialect);
    const cursor = qi(STAGING_CURSOR_COLUMN, dialect);
    const stagedAt = qi(STAGING_STAGED_AT_COLUMN, dialect);
    return {
        query: `SELECT ${cursor} AS cursor_value, COUNT(*) AS row_count, MIN(${seq}) AS first_seq, `
             + `MIN(${stagedAt}) AS first_staged_at, MAX(${stagedAt}) AS last_staged_at `
             + `FROM ${tableRef} GROUP BY ${cursor} ORDER BY MIN(${seq})`,
        params: []
    };
}

/**
 * Widen an existing stream staging table.
 *
 * Staging is all-text, so there is never a type to widen, only columns to add. Added columns stay
 * nullable because rows already staged genuinely had no value for them.
 */
export function buildAddStreamStagingColumnsQuery(
    stagingTable: string,
    newColumns: string[],
    config: DatabaseConfig
): QueryInput {
    const dialect = config.sqlDialect;
    const tableRef = `${schemaPrefixFor(config)}${qi(stagingTable, dialect)}`;
    const colType = dialect === 'mysql' ? 'LONGTEXT' : dialect === 'sqlserver' ? 'NVARCHAR(MAX)' : 'TEXT';
    // MySQL and SQL Server take several ADDs in one statement; Postgres needs ADD COLUMN per column.
    const adds = dialect === 'pgsql'
        ? newColumns.map(c => `ADD COLUMN ${qi(c, dialect)} ${colType}`).join(', ')
        : dialect === 'sqlserver'
        ? `ADD ${newColumns.map(c => `${qi(c, dialect)} ${colType}`).join(', ')}`
        : newColumns.map(c => `ADD COLUMN ${qi(c, dialect)} ${colType}`).join(', ');
    return { query: `ALTER TABLE ${tableRef} ${adds}`, params: [] };
}

// Objects/arrays serialise to JSON, not String()'s "[object Object]" / "a,b,c" (A18) — matching
// sqlize's JSON handling on the batch path, so a JSON-bearing source round-trips the same via
// openStream as via autoSQL.
function streamCell(v: any): string | null {
    if (v === null || v === undefined) return null;
    if (typeof v === "object") return JSON.stringify(v);
    return String(v);
}

/** INSERT one chunk. Values are cast to strings (TEXT columns) and parameter-bound. */
export function buildInsertIntoStreamStagingQuery(
    stagingTable: string,
    columns: string[],
    rows: Record<string, any>[],
    config: DatabaseConfig,
    cursor?: string
): QueryInput {
    const dialect = config.sqlDialect;
    const tableRef = `${schemaPrefixFor(config)}${qi(stagingTable, dialect)}`;
    // seq and staged_at are DB-defaulted, so the cursor is the only one ever bound.
    const insertColumns = cursor !== undefined ? [...columns, STAGING_CURSOR_COLUMN] : columns;
    const cellFor = (row: Record<string, any>, col: string) =>
        col === STAGING_CURSOR_COLUMN ? cursor! : streamCell(row[col]);
    const escapedCols = insertColumns.map(c => qi(c, dialect)).join(', ');

    const params: any[] = [];
    const rowPlaceholders: string[] = [];

    if (dialect === 'mysql') {
        for (const row of rows) {
            rowPlaceholders.push(`(${insertColumns.map(() => '?').join(', ')})`);
            for (const col of insertColumns) {
                params.push(cellFor(row, col));
            }
        }
    } else if (dialect === 'sqlserver') {
        // @pN zero-indexed. The caller (write()) chunks rows so a single request stays under SQL
        // Server's 2,100-parameter cap.
        let paramIdx = 0;
        for (const row of rows) {
            const holders = insertColumns.map(() => `@p${paramIdx++}`);
            rowPlaceholders.push(`(${holders.join(', ')})`);
            for (const col of insertColumns) {
                params.push(cellFor(row, col));
            }
        }
    } else {
        let paramIdx = 1;
        for (const row of rows) {
            const holders = insertColumns.map(() => `$${paramIdx++}`);
            rowPlaceholders.push(`(${holders.join(', ')})`);
            for (const col of insertColumns) {
                params.push(cellFor(row, col));
            }
        }
    }
    return {
        query: `INSERT INTO ${tableRef} (${escapedCols}) VALUES ${rowPlaceholders.join(', ')}`,
        params
    };
}

export function buildSelectFromStreamStagingQuery(
    stagingTable: string,
    config: DatabaseConfig
): QueryInput {
    const tableRef = `${schemaPrefixFor(config)}${qi(stagingTable, config.sqlDialect)}`;
    return { query: `SELECT * FROM ${tableRef}`, params: [] };
}

export function buildDropStreamStagingTableQuery(
    stagingTable: string,
    config: DatabaseConfig
): QueryInput {
    const tableRef = `${schemaPrefixFor(config)}${qi(stagingTable, config.sqlDialect)}`;
    return { query: `DROP TABLE IF EXISTS ${tableRef}`, params: [] };
}

export function buildOrphanSearchQuery(
    table: string,
    prefix: string,
    config: DatabaseConfig
): QueryInput {
    const schema = config.schema || config.database || '';
    const pattern = orphanPattern(table, prefix);
    const where = config.sqlDialect === 'mysql' ? '? AND table_name LIKE ?'
        : config.sqlDialect === 'sqlserver' ? '@p0 AND table_name LIKE @p1'
        : '$1 AND table_name LIKE $2';
    return {
        query: `SELECT table_name FROM information_schema.tables WHERE table_schema = ${where}`,
        params: [schema, pattern]
    };
}

// --- Type cast helpers for merge query ---

const MYSQL_NUMERIC = ['int', 'bigint', 'smallint', 'tinyint', 'mediumint'];
const MYSQL_FLOAT   = ['float', 'double'];
const MYSQL_DATE    = ['date'];
const MYSQL_DATETIME = ['datetime', 'datetimetz', 'timestamp'];
const MYSQL_TIME    = ['time'];
const MYSQL_BOOL    = ['boolean'];

// Postgres has no tinyint/mediumint — map to smallint/integer (matches the CREATE-path
// translation). Without this a tinyint column falls through uncast and the INSERT is rejected
// (`column "id" is of type smallint but expression is of type text`). Previously masked because
// 0/1 inferred as boolean; now it's a small integer (R3), so the merge must cast it.
const PG_INT     = ['int', 'integer', 'mediumint'];
const PG_BIGINT  = ['bigint'];
const PG_SMALL   = ['smallint', 'tinyint'];
const PG_FLOAT   = ['float', 'double'];
const PG_DATE    = ['date'];
const PG_TS      = ['datetime', 'datetimetz', 'timestamp', 'timestamptz'];
const PG_TIME    = ['time'];
const PG_BOOL    = ['boolean'];

// SQL Server: no tinyint (signed) — map to SMALLINT; datetime family → DATETIME2; boolean → BIT (SQL
// Server casts 'true'/'false' and '1'/'0' to BIT).
const SS_INT      = ['int', 'integer', 'mediumint'];
const SS_BIGINT   = ['bigint'];
const SS_SMALL    = ['smallint', 'tinyint'];
const SS_FLOAT    = ['float', 'double'];
const SS_DATE     = ['date'];
const SS_DATETIME = ['datetime', 'datetimetz', 'timestamp', 'timestamptz'];
const SS_TIME     = ['time'];
const SS_BOOL     = ['boolean'];

// Shared control flow across dialects (previously three near-identical mysqlCast/pgCast/
// sqlserverCast copies): each checks its type groups in order, decimal is special-cased, and an
// unmatched type passes through unwrapped as text. Only the type→target mapping and cast syntax
// vary per dialect.
interface CastGroup { types: string[]; target: string; }
interface CastDialect {
    wrap: (quotedCol: string, target: string) => string;
    groups: CastGroup[]; // checked in order; first match wins
    decimalTarget: (len: number, dec: number) => string;
}

const CAST_DIALECTS: Record<'mysql' | 'pgsql' | 'sqlserver', CastDialect> = {
    mysql: {
        wrap: (q, target) => `CAST(${q} AS ${target})`,
        decimalTarget: (len, dec) => `DECIMAL(${len},${dec})`,
        groups: [
            { types: MYSQL_NUMERIC, target: 'SIGNED' },
            { types: MYSQL_FLOAT, target: 'DECIMAL' },
            { types: MYSQL_DATETIME, target: 'DATETIME' },
            { types: MYSQL_DATE, target: 'DATE' },
            { types: MYSQL_TIME, target: 'TIME' },
            { types: MYSQL_BOOL, target: 'SIGNED' },
        ],
    },
    pgsql: {
        wrap: (q, target) => `${q}::${target}`,
        decimalTarget: (len, dec) => `DECIMAL(${len},${dec})`,
        groups: [
            { types: PG_INT, target: 'INTEGER' },
            { types: PG_BIGINT, target: 'BIGINT' },
            { types: PG_SMALL, target: 'SMALLINT' },
            { types: PG_FLOAT, target: 'FLOAT' },
            { types: PG_TS, target: 'TIMESTAMP' },
            { types: PG_DATE, target: 'DATE' },
            { types: PG_TIME, target: 'TIME' },
            { types: PG_BOOL, target: 'BOOLEAN' },
        ],
    },
    // NULLIF(x,'') so an empty staged string becomes NULL rather than a cast error (mirrors
    // alterTableTypeConversion's text→numeric guard).
    sqlserver: {
        wrap: (q, target) => `CAST(NULLIF(${q}, '') AS ${target})`,
        decimalTarget: (len, dec) => `DECIMAL(${len},${dec})`,
        groups: [
            { types: SS_INT, target: 'INT' },
            { types: SS_BIGINT, target: 'BIGINT' },
            { types: SS_SMALL, target: 'SMALLINT' },
            { types: SS_FLOAT, target: 'FLOAT' },
            { types: SS_DATETIME, target: 'DATETIME2' },
            { types: SS_DATE, target: 'DATE' },
            { types: SS_TIME, target: 'TIME' },
            { types: SS_BOOL, target: 'BIT' },
        ],
    },
};

function castColumn(dialect: 'mysql' | 'pgsql' | 'sqlserver', col: string, colDef: { type: string | null; length?: number; decimal?: number }): string {
    const cfg = CAST_DIALECTS[dialect];
    const q = qi(col, dialect);
    const t = (colDef.type || '').toLowerCase();
    if (t === 'decimal') {
        const len = assertSafeLength(colDef.length  ?? 10, 'length');
        const dec = assertSafeLength(colDef.decimal ?? 2, 'decimal');
        return cfg.wrap(q, cfg.decimalTarget(len, dec));
    }
    for (const group of cfg.groups) {
        if (group.types.includes(t)) return cfg.wrap(q, group.target);
    }
    return q; // text-like: no cast needed
}

/** The merge used at end(): INSERT INTO main SELECT ... FROM staging, with per-dialect casts. */
export function buildMergeFromStreamQuery(
    table: string,
    stagingTable: string,
    metaData: MetadataHeader,
    insertType: 'UPDATE' | 'INSERT',
    config: DatabaseConfig,
    dedupOnPrimaryKey?: boolean
): QueryInput {
    const dialect = config.sqlDialect;
    const schemaPrefix = schemaPrefixFor(config);

    const columns = Object.keys(metaData);
    const primaryKeys = columns.filter(c => metaData[c].primary);
    const mainRef    = `${schemaPrefix}${qi(table, dialect)}`;
    const rawStagingRef = `${schemaPrefix}${qi(stagingTable, dialect)}`;

    // A resumed run re-sends the page it died on, so staging can hold a key twice. Postgres and SQL
    // Server reject duplicate source keys outright (only MySQL tolerates it), so collapse to the
    // last-staged row per key: the re-run wins.
    const dedupKeys = Object.keys(metaData).filter(c => metaData[c].primary);
    const wantsDedup = !!dedupOnPrimaryKey && dedupKeys.length > 0;
    const stagingRef = wantsDedup
        ? `(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY `
          + `${dedupKeys.map(k => qi(k, dialect)).join(', ')} ORDER BY ${qi(STAGING_SEQ_COLUMN, dialect)} DESC`
          + `) AS ${qi('_autosql_rn', dialect)} FROM ${rawStagingRef}) ${qi('_autosql_d', dialect)} `
          + `WHERE ${qi('_autosql_rn', dialect)} = 1) ${qi('_autosql_s', dialect)}`
        : rawStagingRef;
    const colList  = columns.map(c => qi(c, dialect)).join(', ');

    if (dialect === 'mysql') {
        const castList = columns.map(c => castColumn('mysql', c, metaData[c])).join(', ');
        let q = `INSERT INTO ${mainRef} (${colList}) SELECT ${castList} FROM ${stagingRef}`;

        if (insertType === 'UPDATE') {
            const updateCols = columns.filter(c => !primaryKeys.includes(c) && !(metaData[c].calculated && metaData[c].updatedCalculated === false));
            if (updateCols.length > 0) {
                const set = updateCols.map(c => `${qi(c, dialect)} = VALUES(${qi(c, dialect)})`).join(', ');
                q += ` ON DUPLICATE KEY UPDATE ${set}`;
            }
        }
        return { query: q, params: [] };
    } else if (dialect === 'sqlserver') {
        const castList = columns.map(c => castColumn('sqlserver', c, metaData[c])).join(', ');
        // Merge only on written PKs; a surrogate PK (excluded elsewhere) or no PK → plain append (§3.7).
        const mergeKeys = primaryKeys.filter(pk => columns.includes(pk));
        if (insertType === 'UPDATE' && mergeKeys.length > 0) {
            const onMatch = mergeKeys.map(pk => `target.${qi(pk, dialect)} = source.${qi(pk, dialect)}`).join(' AND ');
            const updateCols = columns.filter(c => !mergeKeys.includes(c) && !(metaData[c].calculated && metaData[c].updatedCalculated === false));
            // WITH (HOLDLOCK) serialises match+insert (A11), same as the direct/staging MERGE.
            let q = `MERGE INTO ${mainRef} WITH (HOLDLOCK) AS target USING (SELECT ${castList} FROM ${stagingRef}) AS source (${colList}) ON ${onMatch} `;
            if (updateCols.length > 0) {
                q += `WHEN MATCHED THEN UPDATE SET ${updateCols.map(c => `target.${qi(c, dialect)} = source.${qi(c, dialect)}`).join(', ')} `;
            }
            q += `WHEN NOT MATCHED THEN INSERT (${colList}) VALUES (${columns.map(c => `source.${qi(c, dialect)}`).join(', ')});`;
            return { query: q, params: [] };
        }
        return { query: `INSERT INTO ${mainRef} (${colList}) SELECT ${castList} FROM ${stagingRef};`, params: [] };
    } else {
        const castList = columns.map(c => castColumn('pgsql', c, metaData[c])).join(', ');
        let q = `INSERT INTO ${mainRef} (${colList}) SELECT ${castList} FROM ${stagingRef}`;

        if (insertType === 'UPDATE' && primaryKeys.length > 0) {
            const updateCols = columns.filter(c => !primaryKeys.includes(c) && !(metaData[c].calculated && metaData[c].updatedCalculated === false));
            const conflict = `ON CONFLICT (${primaryKeys.map(c => qi(c, dialect)).join(', ')})`;
            if (updateCols.length > 0) {
                const set = updateCols.map(c => `${qi(c, dialect)} = EXCLUDED.${qi(c, dialect)}`).join(', ');
                q += ` ${conflict} DO UPDATE SET ${set}`;
            } else {
                q += ` ${conflict} DO NOTHING`;
            }
        }
        return { query: q, params: [] };
    }
}

export function buildBootstrapRejectedRowsQuery(config: DatabaseConfig): QueryInput[] {
    const dialect   = config.sqlDialect;
    const schema    = config.rejectedRowsSchema || config.schema;
    const tableName = config.rejectedRowsTable!;
    const prefix    = schema ? `${escapeIdentifier(schema, dialect)}.` : '';
    const tableRef  = `${prefix}${qi(tableName, dialect)}`;

    let ddl: string;
    if (dialect === 'mysql') {
        ddl = `CREATE TABLE IF NOT EXISTS ${tableRef} (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  target_table  VARCHAR(255) NOT NULL,
  rejected_at   DATETIME     NOT NULL,
  error_message TEXT         NOT NULL,
  raw_data      JSON         NOT NULL
);`;
    } else if (dialect === 'sqlserver') {
        // No CREATE TABLE IF NOT EXISTS (IF OBJECT_ID guard), no BIGSERIAL (BIGINT IDENTITY),
        // no JSONB (raw_data is NVARCHAR(MAX) JSON text), TIMESTAMPTZ→DATETIME2.
        ddl = `IF OBJECT_ID(${escapeLiteral(tableRef, 'sqlserver')}, 'U') IS NULL
CREATE TABLE ${tableRef} (
  id            BIGINT IDENTITY(1,1) PRIMARY KEY,
  target_table  NVARCHAR(255) NOT NULL,
  rejected_at   DATETIME2     NOT NULL,
  error_message NVARCHAR(MAX) NOT NULL,
  raw_data      NVARCHAR(MAX) NOT NULL
);`;
    } else {
        ddl = `CREATE TABLE IF NOT EXISTS ${tableRef} (
  id            BIGSERIAL PRIMARY KEY,
  target_table  VARCHAR(255) NOT NULL,
  rejected_at   TIMESTAMPTZ  NOT NULL,
  error_message TEXT         NOT NULL,
  raw_data      JSONB        NOT NULL
);`;
    }
    return [{ query: ddl, params: [] }];
}

export function buildInsertRejectedRowsQuery(
    config: DatabaseConfig,
    targetTable: string,
    failures: { row: Record<string, any>; error: string }[]
): QueryInput {
    const dialect   = config.sqlDialect;
    const schema    = config.rejectedRowsSchema || config.schema;
    const tableName = config.rejectedRowsTable!;
    const prefix    = schema ? `${escapeIdentifier(schema, dialect)}.` : '';
    const tableRef  = `${prefix}${qi(tableName, dialect)}`;

    const now = new Date().toISOString().slice(0, 19).replace('T', ' ');

    if (dialect === 'mysql') {
        const placeholders = failures.map(() => '(?, ?, ?, ?)').join(', ');
        const params: any[] = [];
        for (const f of failures) {
            params.push(targetTable, now, f.error, JSON.stringify(f.row));
        }
        return {
            query: `INSERT INTO ${tableRef} (target_table, rejected_at, error_message, raw_data) VALUES ${placeholders}`,
            params
        };
    } else if (dialect === 'sqlserver') {
        // @pN placeholders zero-indexed in params-array order; raw_data is NVARCHAR(MAX) text (no
        // ::jsonb); rejected_at binds a Date to DATETIME2.
        let idx = 0;
        const placeholders = failures.map(() => `(@p${idx++}, @p${idx++}, @p${idx++}, @p${idx++})`).join(', ');
        const nowDate = new Date();
        const params: any[] = [];
        for (const f of failures) {
            params.push(targetTable, nowDate, f.error, JSON.stringify(f.row));
        }
        return {
            query: `INSERT INTO ${tableRef} (target_table, rejected_at, error_message, raw_data) VALUES ${placeholders}`,
            params
        };
    } else {
        let idx = 1;
        const placeholders = failures.map(() => `($${idx++}, $${idx++}, $${idx++}, $${idx++}::jsonb)`).join(', ');
        const params: any[] = [];
        for (const f of failures) {
            params.push(targetTable, new Date().toISOString(), f.error, JSON.stringify(f.row));
        }
        return {
            query: `INSERT INTO ${tableRef} (target_table, rejected_at, error_message, raw_data) VALUES ${placeholders}`,
            params
        };
    }
}
