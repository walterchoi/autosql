import crypto from 'crypto';
import { MetadataHeader, DatabaseConfig, QueryInput, supportedDialects } from '../config/types';
import { escapeIdentifier, assertSafeLength } from '../db/utils/escape';

// Generate an 8-char hex run ID for unique staging table names
export function generateRunId(): string {
    return crypto.randomBytes(4).toString('hex');
}

export function buildStreamStagingTableName(table: string, prefix: string, runId: string): string {
    return `${prefix}${table}__${runId}`;
}

/** Pattern to identify autosql-owned stream staging tables for a given table */
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

// --- Identifier escaping. Column names originate from arbitrary caller JSON keys and table/
//     schema names from config, so every interpolated identifier must be quote-escaped (the
//     rest of the query builders already route through escape.ts; the streaming path did not). ---
function qi(name: string, dialect: supportedDialects): string {
    return escapeIdentifier(name, dialect);
}
function schemaPrefixFor(config: DatabaseConfig): string {
    return config.schema ? `${escapeIdentifier(config.schema, config.sqlDialect)}.` : '';
}

/**
 * Build CREATE TABLE DDL for the untyped (all TEXT) stream staging table.
 * Columns come from the first write() chunk.
 */
export function buildCreateStreamStagingTableQuery(
    stagingTable: string,
    columns: string[],
    config: DatabaseConfig
): QueryInput {
    const dialect = config.sqlDialect;
    const schemaPrefix = schemaPrefixFor(config);
    const colType = dialect === 'mysql' ? 'LONGTEXT' : 'TEXT';
    const colDefs = columns.map(col => `${qi(col, dialect)} ${colType}`).join(',\n  ');
    const tableRef = `${schemaPrefix}${qi(stagingTable, dialect)}`;
    return { query: `CREATE TABLE IF NOT EXISTS ${tableRef} (\n  ${colDefs}\n);`, params: [] };
}

/**
 * Build INSERT for a single chunk of rows into the stream staging table.
 * All values are cast to strings (TEXT columns) and parameter-bound.
 */
export function buildInsertIntoStreamStagingQuery(
    stagingTable: string,
    columns: string[],
    rows: Record<string, any>[],
    config: DatabaseConfig
): QueryInput {
    const dialect = config.sqlDialect;
    const tableRef = `${schemaPrefixFor(config)}${qi(stagingTable, dialect)}`;
    const escapedCols = columns.map(c => qi(c, dialect)).join(', ');

    const params: any[] = [];
    const rowPlaceholders: string[] = [];

    if (dialect === 'mysql') {
        for (const row of rows) {
            rowPlaceholders.push(`(${columns.map(() => '?').join(', ')})`);
            for (const col of columns) {
                const v = row[col];
                params.push(v === null || v === undefined ? null : String(v));
            }
        }
    } else {
        let paramIdx = 1;
        for (const row of rows) {
            const holders = columns.map(() => `$${paramIdx++}`);
            rowPlaceholders.push(`(${holders.join(', ')})`);
            for (const col of columns) {
                const v = row[col];
                params.push(v === null || v === undefined ? null : String(v));
            }
        }
    }
    return {
        query: `INSERT INTO ${tableRef} (${escapedCols}) VALUES ${rowPlaceholders.join(', ')}`,
        params
    };
}

/**
 * Build SELECT * FROM stream staging table query (for reading all rows at end()).
 */
export function buildSelectFromStreamStagingQuery(
    stagingTable: string,
    config: DatabaseConfig
): QueryInput {
    const tableRef = `${schemaPrefixFor(config)}${qi(stagingTable, config.sqlDialect)}`;
    return { query: `SELECT * FROM ${tableRef}`, params: [] };
}

/**
 * Build DROP TABLE for a stream staging table.
 */
export function buildDropStreamStagingTableQuery(
    stagingTable: string,
    config: DatabaseConfig
): QueryInput {
    const tableRef = `${schemaPrefixFor(config)}${qi(stagingTable, config.sqlDialect)}`;
    return { query: `DROP TABLE IF EXISTS ${tableRef}`, params: [] };
}

/**
 * Build query to find orphaned stream staging tables matching the pattern.
 */
export function buildOrphanSearchQuery(
    table: string,
    prefix: string,
    config: DatabaseConfig
): QueryInput {
    const schema = config.schema || config.database || '';
    const pattern = orphanPattern(table, prefix);
    return {
        query: `SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE ?`,
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

const PG_INT     = ['int', 'integer'];
const PG_BIGINT  = ['bigint'];
const PG_SMALL   = ['smallint'];
const PG_FLOAT   = ['float', 'double'];
const PG_DATE    = ['date'];
const PG_TS      = ['datetime', 'datetimetz', 'timestamp', 'timestamptz'];
const PG_TIME    = ['time'];
const PG_BOOL    = ['boolean'];

function mysqlCast(col: string, colDef: { type: string | null; length?: number; decimal?: number }): string {
    const q = qi(col, 'mysql');
    const t = (colDef.type || '').toLowerCase();
    if (MYSQL_NUMERIC.includes(t)) return `CAST(${q} AS SIGNED)`;
    if (MYSQL_FLOAT.includes(t))   return `CAST(${q} AS DECIMAL)`;
    if (t === 'decimal') {
        const len = assertSafeLength(colDef.length  ?? 10, 'length');
        const dec = assertSafeLength(colDef.decimal ?? 2, 'decimal');
        return `CAST(${q} AS DECIMAL(${len},${dec}))`;
    }
    if (MYSQL_DATETIME.includes(t)) return `CAST(${q} AS DATETIME)`;
    if (MYSQL_DATE.includes(t))     return `CAST(${q} AS DATE)`;
    if (MYSQL_TIME.includes(t))     return `CAST(${q} AS TIME)`;
    if (MYSQL_BOOL.includes(t))     return `CAST(${q} AS SIGNED)`;
    return q; // text-like: no cast needed
}

function pgCast(col: string, colDef: { type: string | null; length?: number; decimal?: number }): string {
    const q = qi(col, 'pgsql');
    const t = (colDef.type || '').toLowerCase();
    if (PG_INT.includes(t))    return `${q}::INTEGER`;
    if (PG_BIGINT.includes(t)) return `${q}::BIGINT`;
    if (PG_SMALL.includes(t))  return `${q}::SMALLINT`;
    if (PG_FLOAT.includes(t))  return `${q}::FLOAT`;
    if (t === 'decimal') {
        const len = assertSafeLength(colDef.length  ?? 10, 'length');
        const dec = assertSafeLength(colDef.decimal ?? 2, 'decimal');
        return `${q}::DECIMAL(${len},${dec})`;
    }
    if (PG_TS.includes(t))   return `${q}::TIMESTAMP`;
    if (PG_DATE.includes(t)) return `${q}::DATE`;
    if (PG_TIME.includes(t)) return `${q}::TIME`;
    if (PG_BOOL.includes(t)) return `${q}::BOOLEAN`;
    return q;
}

/**
 * Build INSERT INTO main SELECT ... FROM stagingTable with type casts.
 * This is the primary merge query used at end().
 */
export function buildMergeFromStreamQuery(
    table: string,
    stagingTable: string,
    metaData: MetadataHeader,
    insertType: 'UPDATE' | 'INSERT',
    config: DatabaseConfig
): QueryInput {
    const dialect = config.sqlDialect;
    const schemaPrefix = schemaPrefixFor(config);

    const columns = Object.keys(metaData);
    const primaryKeys = columns.filter(c => metaData[c].primary);
    const mainRef    = `${schemaPrefix}${qi(table, dialect)}`;
    const stagingRef = `${schemaPrefix}${qi(stagingTable, dialect)}`;
    const colList  = columns.map(c => qi(c, dialect)).join(', ');

    if (dialect === 'mysql') {
        const castList = columns.map(c => mysqlCast(c, metaData[c])).join(', ');
        let q = `INSERT INTO ${mainRef} (${colList}) SELECT ${castList} FROM ${stagingRef}`;

        if (insertType === 'UPDATE') {
            const updateCols = columns.filter(c => !primaryKeys.includes(c) && !(metaData[c].calculated && metaData[c].updatedCalculated === false));
            if (updateCols.length > 0) {
                const set = updateCols.map(c => `${qi(c, dialect)} = VALUES(${qi(c, dialect)})`).join(', ');
                q += ` ON DUPLICATE KEY UPDATE ${set}`;
            }
        }
        return { query: q, params: [] };
    } else {
        const castList = columns.map(c => pgCast(c, metaData[c])).join(', ');
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

/**
 * Build CREATE TABLE + INSERT for the rejected rows table (opt-in).
 */
export function buildBootstrapRejectedRowsQuery(config: DatabaseConfig): QueryInput[] {
    const dialect   = config.sqlDialect;
    const schema    = config.rejectedRowsSchema || config.schema;
    const tableName = config.rejectedRowsTable!;
    const prefix    = schema ? `${escapeIdentifier(schema, dialect)}.` : '';
    const tableRef  = `${prefix}${qi(tableName, dialect)}`;

    const ddl = dialect === 'mysql'
        ? `CREATE TABLE IF NOT EXISTS ${tableRef} (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  target_table  VARCHAR(255) NOT NULL,
  rejected_at   DATETIME     NOT NULL,
  error_message TEXT         NOT NULL,
  raw_data      JSON         NOT NULL
);`
        : `CREATE TABLE IF NOT EXISTS ${tableRef} (
  id            BIGSERIAL PRIMARY KEY,
  target_table  VARCHAR(255) NOT NULL,
  rejected_at   TIMESTAMPTZ  NOT NULL,
  error_message TEXT         NOT NULL,
  raw_data      JSONB        NOT NULL
);`;
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
