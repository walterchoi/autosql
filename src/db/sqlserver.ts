import crypto from "crypto";
import { Database } from "./database";
import { sqlServerPermanentErrors } from "./permanentErrors/sqlserver";
import { QueryInput, ColumnDefinition, DatabaseConfig, AlterTableChanges, InsertResult, MetadataHeader, InsertInput, QueryResult } from "../config/types";
import { sqlServerConfig } from "./config/sqlServerConfig";
import { isValidSingleQuery } from "./utils/validateQuery";
import { escapeIdentifier, escapeLiteral } from "./utils/escape";
import { compareMetaData } from "../helpers/metadata";
import { SqlServerTableQueryBuilder } from "./queryBuilders/sqlserver/tableBuilder";
import { SqlServerIndexQueryBuilder } from "./queryBuilders/sqlserver/indexBuilder";
import { SqlServerInsertQueryBuilder } from "./queryBuilders/sqlserver/insertBuilder";
import { AutoSQLHandler } from "./autosql";
import { normalizeResultKeys, isMetadataHeader } from "../helpers/utilities";
import { SchemaLockTimeoutError } from "../errors";

const dialectConfig = sqlServerConfig;

// Map an autosql local column type to the mssql bulk-copy type object (used to build the typed sql.Table
// for request.bulk). `sql` is the mssql module. Anything unrecognised → NVARCHAR(MAX) (staging is text).
function sqlServerBulkType(sql: any, meta: ColumnDefinition): any {
    const t = (meta.type || "").toLowerCase();
    const len = typeof meta.length === "number" ? meta.length : undefined;
    const dec = typeof meta.decimal === "number" ? meta.decimal : undefined;
    switch (t) {
        case "int": case "integer": case "mediumint": return sql.Int;
        case "bigint": return sql.BigInt;
        case "smallint": case "tinyint": return sql.SmallInt;
        case "float": case "double": return sql.Float;
        case "decimal": return sql.Decimal(len ?? 18, dec ?? 2);
        case "boolean": return sql.Bit;
        case "date": return sql.Date;
        case "time": return sql.Time;
        case "datetime": case "timestamp": case "datetimetz": case "datetimeoffset": return sql.DateTime2;
        case "varchar": return (len && len <= 4000) ? sql.NVarChar(len) : sql.NVarChar(sql.MAX);
        default: return sql.NVarChar(sql.MAX); // text / json / unknown
    }
}

// Coerce a (possibly sqlized-string) value to the JS type the mssql bulk API expects for `meta`. On any
// mismatch it returns null / a value the driver rejects → the whole bulk throws → INSERT fallback.
function sqlServerBulkCoerce(v: any, meta: ColumnDefinition): any {
    if (v === null || v === undefined) return null;
    const t = (meta.type || "").toLowerCase();
    if (["int", "integer", "mediumint", "bigint", "smallint", "tinyint", "float", "double", "decimal"].includes(t)) {
        if (v === "") return null;
        const n = Number(v);
        return Number.isNaN(n) ? null : n;
    }
    if (t === "boolean") {
        if (v === "") return null;
        return v === true || v === 1 || v === "1" || String(v).toLowerCase() === "true";
    }
    if (["date", "time", "datetime", "timestamp", "datetimetz", "datetimeoffset"].includes(t)) {
        if (v === "") return null;
        if (v instanceof Date) return v;
        const d = new Date(v);
        return isNaN(d.getTime()) ? null : d;
    }
    return String(v);
}

/**
 * SQL Server / Azure SQL adapter (Class A row-store, T-SQL). Uses the `mssql` driver.
 *
 * Transaction model: unlike pg/mysql (a pinned pool connection running BEGIN/COMMIT as SQL), mssql
 * pins a `sql.Transaction`; `startTransaction/commit/rollback` drive that API and
 * `executeQuery(query, client)` runs on `new sql.Request(client)` when pinned, else `pool.request()`.
 * Parameters are named `@p0, @p1, ...` (zero-indexed, params-array order), matching the builders.
 */
export class SqlServerDatabase extends Database {
    private sql: any;
    private schemaLockTx: Map<string, any> = new Map();

    constructor(config: DatabaseConfig) {
        super(config);
        this.autoSQLHandler = new AutoSQLHandler(this);
    }

    async establishDatabaseConnection(): Promise<void> {
        try {
            this.sql = require("mssql");
        } catch (err) {
            throw new Error("Missing required dependency 'mssql'. Please install it to use SqlServerDatabase.");
        }
        // Map the driver-agnostic `ssl` option onto tedious/mssql TLS options (options.encrypt,
        // options.trustServerCertificate, options.cryptoCredentialsDetails); see sqlServerTlsOptions.
        // Omitted/false keeps the local-container default (no forced TLS, trust the self-signed cert)
        // so existing behaviour and the test harness are unchanged.
        const sslOptions = this.sqlServerTlsOptions();
        const pool = new this.sql.ConnectionPool({
            server: this.config.host || "localhost",
            port: this.config.port || 1433,
            user: this.config.user,
            password: this.config.password,
            database: this.config.database,
            pool: { max: this.config.connectionLimit || 5, min: 0, idleTimeoutMillis: 30000 },
            options: {
                ...sslOptions,
                enableArithAbort: true,
            },
        });
        try {
            await pool.connect();
        } catch (error) {
            // Map err.number -> err.code (A21) so the base connect-retry loop's permanent-error check
            // classifies an unrecoverable connect failure — 18456 (login failed), 4060 (cannot open
            // database) — and aborts instead of retrying it maxAttempts times.
            throw this.normalizeError(error);
        }
        this.connection = pool as any;
    }

    /**
     * Translate `DatabaseConfig.ssl` into tedious/mssql TLS options.
     *  - omitted / false → local-dev default: no forced TLS, trust the (self-signed) server cert.
     *  - `true`          → encrypt on, VERIFY against the system CA store.
     *  - object          → encrypt on; `rejectUnauthorized:false` → `trustServerCertificate` (skip
     *                      verification, with a warning); `ca`/`cert`/`key`/`servername` →
     *                      `cryptoCredentialsDetails` (custom CA / mutual TLS / SNI).
     * @internal Exposed for unit testing the mapping; not part of the stable public API.
     */
    public sqlServerTlsOptions(): Record<string, any> {
        const ssl = this.config.ssl;
        if (!ssl) {
            return { encrypt: false, trustServerCertificate: true };
        }
        const options: Record<string, any> = { encrypt: true };
        if (ssl === true) {
            options.trustServerCertificate = false; // verify against system CAs
            return options;
        }
        options.trustServerCertificate = ssl.rejectUnauthorized === false;
        if (options.trustServerCertificate) {
            this.config.logger?.warn?.("autosql: ssl.rejectUnauthorized is false — TLS certificate verification is DISABLED (use only for dev/self-signed).");
        }
        const cc: Record<string, any> = {};
        if (ssl.ca) cc.ca = ssl.ca;
        if (ssl.cert) cc.cert = ssl.cert;
        if (ssl.key) cc.key = ssl.key;
        if (ssl.servername) cc.servername = ssl.servername;
        if (Object.keys(cc).length) options.cryptoCredentialsDetails = cc;
        return options;
    }

    public getMaxConnections(): number {
        return (this.connection as any)?.config?.pool?.max ?? this.config.connectionLimit ?? 5;
    }

    public async closeConnection(): Promise<{ success: boolean; error?: string }> {
        if (!this.connection) return { success: true };
        try {
            await (this.connection as any).close();
            return { success: true };
        } catch (error) {
            return { success: false, error: error instanceof Error ? error.message : "Unknown error" };
        } finally {
            this.connection = undefined;
            this.config.sshClient = undefined;
            this.config.sshStream = undefined;
        }
    }

    public async bulkLoadRows(table: string, columns: string[], rows: any[][], header?: MetadataHeader): Promise<QueryResult> {
        // TDS bulk-copy via the mssql `request.bulk` API — the same wire path as BULK INSERT, but with
        // no server-side file. It needs a fully-typed sql.Table, derived from the column metadata. Any
        // failure here (unsupported type/value, driver quirk) throws; bulkLoadStaging catches it and
        // falls back to parameterised INSERT, so correctness never depends on this fast path (spec-4 §3.5).
        const start = new Date();
        if (!this.connection) await this.establishConnection();
        if (rows.length === 0) return { start, end: new Date(), duration: 0, success: true, affectedRows: 0 };
        if (!header) throw new Error("bulkLoadRows requires column metadata on SQL Server");

        const sql = this.sql;
        const schema = this.getConfig().schema;
        const qualified = `${schema ? `${escapeIdentifier(schema, "sqlserver")}.` : ""}${escapeIdentifier(table, "sqlserver")}`;
        const tvp = new sql.Table(qualified);
        tvp.create = false;
        for (const col of columns) {
            const meta = header[col] || ({ type: "varchar" } as ColumnDefinition);
            tvp.columns.add(col, sqlServerBulkType(sql, meta), { nullable: meta.allowNull !== false });
        }
        for (const row of rows) {
            tvp.rows.add(...row.map((v, i) => sqlServerBulkCoerce(v, header[columns[i]] || ({ type: "varchar" } as ColumnDefinition))));
        }
        await new sql.Request(this.connection).bulk(tvp);
        const end = new Date();
        return { start, end, duration: end.getTime() - start.getTime(), success: true, affectedRows: rows.length };
    }

    // ---- Transaction hooks (mssql Transaction API) ----

    protected async acquireConnection(): Promise<any> {
        if (!this.connection) await this.establishConnection();
        // A not-yet-begun transaction. startTransaction() begins it; executeQuery binds requests to it.
        return new this.sql.Transaction(this.connection);
    }

    protected releaseConnection(_client: any): void {
        // mssql releases the underlying pooled connection on commit()/rollback(); nothing to do.
    }

    protected destroyConnection(_client: any): void {
        // mssql transactions release their pooled connection on settle; there is no per-client handle to
        // destroy (A24). No-op — the pool manages connection health.
    }

    public async startTransaction(client: any): Promise<void> {
        if (!client) throw new Error("startTransaction requires a pinned connection; use runTransaction().");
        await client.begin();
    }

    public async commit(client: any): Promise<void> {
        if (!client) throw new Error("commit requires a pinned connection; use runTransaction().");
        await client.commit();
    }

    public async rollback(client: any): Promise<void> {
        if (!client) throw new Error("rollback requires a pinned connection; use runTransaction().");
        await client.rollback();
    }

    public getDialectConfig() {
        return dialectConfig;
    }

    protected async getPermanentErrors(): Promise<string[]> {
        return sqlServerPermanentErrors;
    }

    async testQuery(queryOrParams: QueryInput): Promise<any> {
        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
        if (!isValidSingleQuery(query)) {
            throw new Error("Each query in the transaction must be a single statement.");
        }
        if (!this.connection) await this.establishConnection();

        // Syntax-check without executing (SET PARSEONLY validates the batch and returns nothing).
        const request = (this.connection as any).request();
        try {
            await request.batch(`SET PARSEONLY ON; ${query}; SET PARSEONLY OFF;`);
            return { success: true };
        } catch (error) {
            this.error(`SQL Server testQuery failed: ${error}`);
            throw this.normalizeError(error);
        }
    }

    /** mssql surfaces the real error number on err.number; map it to err.code so the retry logic
     *  (which matches on error.code against getPermanentErrors) works. */
    private normalizeError(error: any): any {
        if (error && error.number !== undefined && error.code === undefined) {
            try { error.code = String(error.number); } catch { /* frozen error; ignore */ }
        } else if (error && error.number !== undefined) {
            try { error.code = String(error.number); } catch { /* ignore */ }
        }
        return error;
    }

    protected async executeQuery(query: string, client?: any): Promise<any>;
    protected async executeQuery(QueryInput: QueryInput, client?: any): Promise<any>;
    protected async executeQuery(queryOrParams: QueryInput, client?: any): Promise<{ rows: any[]; affectedRows: number }> {
        if (!this.connection) await this.establishConnection();

        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
        const params = typeof queryOrParams === "string" ? [] : queryOrParams.params || [];

        // Bind to the pinned transaction when running inside runTransaction; otherwise a pool request.
        const request = client ? new this.sql.Request(client) : (this.connection as any).request();
        params.forEach((value: any, i: number) => request.input(`p${i}`, value));

        try {
            // .batch() supports multi-statement T-SQL (needed for the guarded CREATE/DDL blocks and
            // the DECLARE-based drop-PK). Parameters are still bound.
            const result = await request.batch(query);
            const rows = result.recordset || [];
            const affectedRows = Array.isArray(result.rowsAffected)
                ? result.rowsAffected.reduce((a: number, b: number) => a + b, 0)
                : (result.rowsAffected ?? rows.length ?? 0);
            return { rows, affectedRows };
        } catch (error) {
            throw this.normalizeError(error);
        }
    }

    // ---- Schema lock via sp_getapplock (held on a dedicated transaction) ----

    private getLockResource(table: string): string {
        // A stable, bounded resource name (sp_getapplock @Resource max 255 chars).
        const h = crypto.createHash("sha256").update(`autosql_schema__${table}`).digest("hex");
        return `autosql_schema_${h.slice(0, 32)}`;
    }

    public async acquireSchemaLock(table: string, timeoutSeconds: number): Promise<void> {
        if (!this.connection) await this.establishConnection();
        const resource = this.getLockResource(table);
        const tx = new this.sql.Transaction(this.connection);
        await tx.begin();
        try {
            const request = new this.sql.Request(tx);
            request.input("res", resource);
            request.input("timeout", timeoutSeconds * 1000);
            // @LockOwner='Transaction' ties the lock to this open transaction; released on commit/rollback.
            const result = await request.execute("sp_getapplock_wrapper").catch(async () => {
                // sp_getapplock returns a result code (>=0 success, <0 failure) via an output param.
                const r = new this.sql.Request(tx);
                r.input("Resource", resource);
                r.input("LockMode", "Exclusive");
                r.input("LockOwner", "Transaction");
                r.input("LockTimeout", timeoutSeconds * 1000);
                r.output("ret", this.sql.Int);
                await r.execute("sp_getapplock");
                return { returnValue: r.parameters?.ret?.value ?? r.returnValue };
            });
            const code = (result as any)?.returnValue ?? 0;
            if (typeof code === "number" && code < 0) {
                await tx.rollback().catch(() => {});
                throw new SchemaLockTimeoutError(
                    `Could not acquire schema lock for table '${table}' within ${timeoutSeconds}s (sp_getapplock code ${code}).`
                );
            }
            const stale = this.schemaLockTx.get(table);
            if (stale && stale !== tx) await stale.rollback().catch(() => {});
            this.schemaLockTx.set(table, tx);
        } catch (error) {
            if (this.schemaLockTx.get(table) !== tx) await tx.rollback().catch(() => {});
            throw error;
        }
    }

    public async releaseSchemaLock(table: string): Promise<void> {
        const tx = this.schemaLockTx.get(table);
        if (!tx) return;
        try {
            // Committing the holding transaction releases the Transaction-scoped applock.
            await tx.commit();
        } catch {
            await tx.rollback().catch(() => {});
        } finally {
            this.schemaLockTx.delete(table);
        }
    }

    // ---- DDL / DML query builders (delegate to the SQL Server builders) ----

    getCreateSchemaQuery(schemaName: string): QueryInput {
        const s = escapeLiteral(schemaName, "sqlserver");
        // CREATE SCHEMA must be the first statement in its batch, so wrap in dynamic SQL.
        return {
            query: `IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ${s}) EXEC('CREATE SCHEMA ${escapeIdentifier(schemaName, "sqlserver")}');`,
            params: [],
        };
    }

    getCheckSchemaQuery(schemaName: string | string[]): QueryInput {
        if (Array.isArray(schemaName)) {
            return {
                query: `SELECT ${schemaName
                    .map((db) => `(CASE WHEN EXISTS (SELECT 1 FROM sys.schemas WHERE name = ${escapeLiteral(db, "sqlserver")}) THEN 1 ELSE 0 END) AS ${escapeIdentifier(db, "sqlserver")}`)
                    .join(", ")};`,
                params: [],
            };
        }
        return {
            query: `SELECT (CASE WHEN EXISTS (SELECT 1 FROM sys.schemas WHERE name = ${escapeLiteral(schemaName, "sqlserver")}) THEN 1 ELSE 0 END) AS ${escapeIdentifier(schemaName, "sqlserver")};`,
            params: [],
        };
    }

    getCreateTableQuery(table: string, headers: MetadataHeader): QueryInput[] {
        return SqlServerTableQueryBuilder.getCreateTableQuery(table, headers, this.getConfig());
    }

    async getAlterTableQuery(table: string, alterTableChangesOrOldHeaders: AlterTableChanges | MetadataHeader, newHeaders?: MetadataHeader): Promise<QueryInput[]> {
        let alterTableChanges: AlterTableChanges;
        let updatedMetaData: MetadataHeader;
        const stagingPrefix = this.getConfig().stagingPrefix ?? "temp_staging__";
        const isStagingTable = table.startsWith(stagingPrefix);
        const alterPrimaryKey = (this.config.updatePrimaryKey ?? false) && !isStagingTable;

        if (isMetadataHeader(alterTableChangesOrOldHeaders)) {
            if (!newHeaders) throw new Error("Missing new headers for ALTER TABLE query");
            ({ changes: alterTableChanges, updatedMetaData } = compareMetaData(alterTableChangesOrOldHeaders, newHeaders, this.getDialectConfig(), this.config.logger));
            this.updateTableMetadata(table, updatedMetaData, "metaData");
        } else {
            alterTableChanges = alterTableChangesOrOldHeaders as AlterTableChanges;
        }
        const queries: QueryInput[] = [];

        // R9: surface schema changes that are computed but blocked by the safe-default flags.
        if (!isStagingTable) this.warnBlockedSchemaChanges(table, alterTableChanges);

        // SQL Server DDL is fully transactional, so (unlike Postgres) no COMMIT/BEGIN interleaving is
        // needed around a PK change. Drop the old PK first, apply column changes, then add the new PK.
        if (alterTableChanges.primaryKeyChanges.length > 0 && alterPrimaryKey) {
            queries.push(this.getDropPrimaryKeyQuery(table));
        }

        // Only drop the unique when the caller opted in (A10). Off (default) → keep it; the load fails
        // loud / diverts on the collision. warnBlockedSchemaChanges emits the warning.
        if (alterTableChanges.noLongerUnique.length > 0 && this.getConfig().dropUniqueConstraints) {
            const uniqueIndexesResult = await this.runQuery(this.getUniqueIndexesQuery(table));
            if (!uniqueIndexesResult.success || !uniqueIndexesResult.results) {
                throw new Error(`Failed to fetch unique indexes for table ${table}: ${uniqueIndexesResult.error}`);
            }
            const schemaPrefix = this.getConfig().schema ? `${escapeIdentifier(this.getConfig().schema!, "sqlserver")}.` : "";
            const indexesToDrop = (uniqueIndexesResult.results || [])
                .map((row) => normalizeResultKeys(row))
                .filter((row) => row.columns)
                .filter(({ columns }) => columns.split(", ").some((col: string) => alterTableChanges.noLongerUnique.includes(col)))
                .map(({ index_name }) => `DROP INDEX ${escapeIdentifier(index_name, "sqlserver")} ON ${schemaPrefix}${escapeIdentifier(table, "sqlserver")}`);
            if (indexesToDrop.length > 0) {
                queries.push({ query: indexesToDrop.join("; ") + ";", params: [] });
            }
        }

        const alterQueries = SqlServerTableQueryBuilder.getAlterTableQuery(table, alterTableChanges, this.getConfig().schema, this.getConfig());
        queries.push(...alterQueries);

        if (alterTableChanges.primaryKeyChanges.length > 0 && alterPrimaryKey) {
            queries.push(this.getAddPrimaryKeyQuery(table, alterTableChanges.primaryKeyChanges));
        }

        return queries;
    }

    getDropTableQuery(table: string): QueryInput {
        return SqlServerTableQueryBuilder.getDropTableQuery(table, this.getConfig().schema);
    }

    getTableExistsQuery(schema: string, table: string): QueryInput {
        return SqlServerTableQueryBuilder.getTableExistsQuery(schema, table);
    }

    getTableMetaDataQuery(schema: string, table: string): QueryInput {
        return SqlServerTableQueryBuilder.getTableMetaDataQuery(schema, table);
    }

    getPrimaryKeysQuery(table: string): QueryInput {
        return SqlServerIndexQueryBuilder.getPrimaryKeysQuery(table, this.getConfig().schema);
    }

    getForeignKeyConstraintsQuery(table: string): QueryInput {
        return SqlServerIndexQueryBuilder.getForeignKeyConstraintsQuery(table, this.getConfig().schema);
    }

    getViewDependenciesQuery(table: string): QueryInput {
        return SqlServerIndexQueryBuilder.getViewDependenciesQuery(table, this.getConfig().schema);
    }

    getDropPrimaryKeyQuery(table: string): QueryInput {
        return SqlServerIndexQueryBuilder.getDropPrimaryKeyQuery(table, this.getConfig().schema);
    }

    getDropUniqueConstraintQuery(table: string, indexName: string): QueryInput {
        return SqlServerIndexQueryBuilder.getDropUniqueConstraintQuery(table, indexName, this.getConfig().schema);
    }

    getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput {
        return SqlServerIndexQueryBuilder.getAddPrimaryKeyQuery(table, primaryKeys, this.getConfig().schema);
    }

    getUniqueIndexesQuery(table: string, column_name?: string): QueryInput {
        return SqlServerIndexQueryBuilder.getUniqueIndexesQuery(table, column_name, this.getConfig().schema);
    }

    getSplitTablesQuery(table: string): QueryInput {
        return SqlServerTableQueryBuilder.getSplitTablesQuery(table, this.getConfig().schema);
    }

    getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, insertInput?: "UPDATE" | "INSERT"): QueryInput {
        return SqlServerInsertQueryBuilder.getInsertStatementQuery(tableOrInput, data, metaData, this.getConfig(), insertInput);
    }

    getInsertFromStagingQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, insertInput?: "UPDATE" | "INSERT"): QueryInput {
        return SqlServerInsertQueryBuilder.getInsertFromStagingQuery(tableOrInput, metaData, this.getConfig(), insertInput);
    }

    getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader): QueryInput {
        return SqlServerInsertQueryBuilder.getInsertChangedRowsToHistoryQuery(tableOrInput, metaData, this.getConfig());
    }

    getCreateTempTableQuery(table: string, stagingPrefix?: string): QueryInput {
        return SqlServerTableQueryBuilder.getCreateTempTableQuery(table, this.getConfig().schema, stagingPrefix);
    }

    getConstraintConflictQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] }, stagingPrefix?: string): QueryInput {
        return SqlServerIndexQueryBuilder.generateConstraintConflictBreakdownQuery(table, structure, this.getConfig().schema, stagingPrefix);
    }
}
