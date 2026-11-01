import crypto from "crypto";
import { Database } from "./database";
import { SQL_SERVER_PERMANENT_ERRORS } from "./permanentErrors/sqlserver";
import type { QueryInput, ColumnDefinition, DatabaseConfig, AlterTableChanges, MetadataHeader, InsertInput, QueryResult, DataRow } from "../config/types";
import { SQL_SERVER_CONFIG } from "./config/sqlServerConfig";
import { isValidSingleQuery } from "./utils/validateQuery";
import { escapeIdentifier, escapeLiteral } from "./utils/escape";
import { compareMetaData } from "../helpers/metadata";
import { SqlServerTableQueryBuilder } from "./queryBuilders/sqlserver/tableBuilder";
import { SqlServerIndexQueryBuilder } from "./queryBuilders/sqlserver/indexBuilder";
import { SqlServerInsertQueryBuilder } from "./queryBuilders/sqlserver/insertBuilder";
import { AutoSQLHandler } from "./autosql";
import { normalizeResultKeys, isMetadataHeader } from "../helpers/utilities";
import { SchemaLockTimeoutError } from "../errors";

const DIALECT_CONFIG = SQL_SERVER_CONFIG;

// mssql ships no TypeScript types at all (no "types"/"typings" field, no .d.ts, no @types/mssql
// package) — same "optional peer dependency, lazily require()'d" reasoning as the
// no-require-imports exceptions for this driver, just one level up: there's nothing to import even
// as a type. Used for the module namespace, connection/transaction/request objects it hands back,
// and anything else that's genuinely the mssql driver's own untypeable shape.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type MssqlDriver = any;

// Map an autosql local column type to the mssql bulk-copy type object (used to build the typed sql.Table
// for request.bulk). `sql` is the mssql module. Anything unrecognised → NVARCHAR(MAX) (staging is text).
function sqlServerBulkType(sql: MssqlDriver, meta: ColumnDefinition): MssqlDriver {
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

// Already-sqlized values are expected to be a string/number/boolean/Date; stringifying anything
// else (an object reaching here would mean sqlize let something through it shouldn't have) is a
// real bug, not a false positive — kept as its own function (not inlined) so the String() calls
// below are safe.
function stringifyScalar(v: string | number | boolean | bigint | Date): string {
    return String(v);
}

// Coerce a (possibly sqlized-string) value to the JS type the mssql bulk API expects for `meta`. On any
// mismatch it returns null / a value the driver rejects → the whole bulk throws → INSERT fallback.
// Behavior (incl. for a Date reaching a column whose declared type isn't a date type) matches the
// pre-fix version exactly — only the type annotations and the final-else guard are new.
function sqlServerBulkCoerce(v: unknown, meta: ColumnDefinition): number | boolean | Date | string | null {
    if (v === null || v === undefined) return null;
    if (typeof v !== "string" && typeof v !== "number" && typeof v !== "boolean" && !(v instanceof Date)) {
        throw new Error(`sqlServerBulkCoerce: unsupported value type "${typeof v}" — values must already be sqlized to a string/number/boolean/Date.`);
    }
    const t = (meta.type || "").toLowerCase();
    if (["int", "integer", "mediumint", "bigint", "smallint", "tinyint", "float", "double", "decimal"].includes(t)) {
        if (v === "") return null;
        const n = Number(v);
        return Number.isNaN(n) ? null : n;
    }
    if (t === "boolean") {
        if (v === "") return null;
        return v === true || v === 1 || v === "1" || stringifyScalar(v).toLowerCase() === "true";
    }
    if (["date", "time", "datetime", "timestamp", "datetimetz", "datetimeoffset"].includes(t)) {
        if (v === "") return null;
        if (v instanceof Date) return v;
        // A boolean here is a genuine type/column mismatch (same as the function's documented "on
        // any mismatch" contract) — preserved exactly: Date's constructor coerces it (true → 1ms
        // past epoch) same as the original untyped `new Date(v)` did.
        const d = new Date(v as string | number);
        return isNaN(d.getTime()) ? null : d;
    }
    return stringifyScalar(v);
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
    // `declare` — no field is actually emitted here (the base class's constructor already sets
    // `connection`); this only narrows the TYPE this subclass sees it as, back from the base's
    // `unknown` to the (untypeable) mssql driver shape, so the many uncasted `this.connection.*`
    // accesses below keep compiling.
    protected declare connection: MssqlDriver;
    private sql: MssqlDriver;
    private schemaLockTx: Map<string, MssqlDriver> = new Map();

    constructor(config: DatabaseConfig) {
        super(config);
        this.autoSQLHandler = new AutoSQLHandler(this);
    }

    async establishDatabaseConnection(): Promise<void> {
        try {
            // Lazy require, not a static import: mssql is an OPTIONAL peer dependency — a static
            // import would force every consumer to install all three drivers.
            // eslint-disable-next-line @typescript-eslint/no-require-imports
            this.sql = require("mssql");
        } catch {
            throw new Error("Missing required dependency 'mssql'. Please install it to use SqlServerDatabase.");
        }
        // See sqlServerTlsOptions for the `ssl` -> tedious/mssql options mapping.
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
            // A connect-time failure (bad password, inaccessible database) arrives as tedious's own
            // "ELOGIN" code, already classified permanent (A21) — normalizeError only has real work
            // to do on a query-time error, but runs unconditionally here for one code path either
            // way, so the base connect-retry loop's permanent-error check aborts instead of
            // retrying it maxAttempts times.
            throw this.normalizeError(error);
        }
        this.connection = pool;
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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- the mssql driver's own connection-options shape (encrypt/trustServerCertificate/cryptoCredentialsDetails); not worth a hard dependency on its types at this boundary.
    public sqlServerTlsOptions(): Record<string, any> {
        const ssl = this.config.ssl;
        if (!ssl) {
            return { encrypt: false, trustServerCertificate: true };
        }
        // eslint-disable-next-line @typescript-eslint/no-explicit-any -- same mssql driver options shape as above.
        const options: Record<string, any> = { encrypt: true };
        if (ssl === true) {
            options.trustServerCertificate = false; // verify against system CAs
            return options;
        }
        options.trustServerCertificate = ssl.rejectUnauthorized === false;
        this.warnInsecureTlsAndTunnelConflict(ssl);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any -- same mssql driver options shape as above.
        const cc: Record<string, any> = {};
        if (ssl.ca) cc.ca = ssl.ca;
        if (ssl.cert) cc.cert = ssl.cert;
        if (ssl.key) cc.key = ssl.key;
        if (ssl.servername) cc.servername = ssl.servername;
        if (Object.keys(cc).length) options.cryptoCredentialsDetails = cc;
        return options;
    }

    public getMaxConnections(): number {
        return this.connection?.config?.pool?.max ?? this.config.connectionLimit ?? 5;
    }

    protected getHeldSchemaLockCount(): number { return this.schemaLockTx.size; }

    public async closeConnection(): Promise<{ success: boolean; error?: string }> {
        if (!this.connection) return { success: true };
        try {
            await this.connection.close();
            return { success: true };
        } catch (error) {
            return { success: false, error: error instanceof Error ? error.message : "Unknown error" };
        } finally {
            this.connection = undefined;
            this.config.sshClient = undefined;
            this.config.sshStream = undefined;
        }
    }

    public async bulkLoadRows(table: string, columns: string[], rows: unknown[][], header?: MetadataHeader): Promise<QueryResult> {
        // TDS bulk-copy via mssql's `request.bulk` — same wire path as BULK INSERT, no server-side
        // file. Any failure here throws; bulkLoadStaging catches it and falls back to INSERT (spec-4 §3.5).
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
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- col is from the caller's `columns` list, not Object.keys(header); an unmapped column genuinely isn't a header key (noUncheckedIndexedAccess isn't set).
            const meta = header[col] || { type: "varchar" };
            tvp.columns.add(col, sqlServerBulkType(sql, meta), { nullable: meta.allowNull !== false });
        }
        for (const row of rows) {
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- same unmapped-column case as above.
            tvp.rows.add(...row.map((v, i) => sqlServerBulkCoerce(v, header[columns[i]] || { type: "varchar" })));
        }
        await new sql.Request(this.connection).bulk(tvp);
        const end = new Date();
        return { start, end, duration: end.getTime() - start.getTime(), success: true, affectedRows: rows.length };
    }

    // ---- Transaction hooks (mssql Transaction API) ----

    protected async acquireConnection(): Promise<MssqlDriver> {
        if (!this.connection) await this.establishConnection();
        // A not-yet-begun transaction. startTransaction() begins it; executeQuery binds requests to it.
        return new this.sql.Transaction(this.connection);
    }

    protected releaseConnection(_client: MssqlDriver): void {
        // mssql releases the underlying pooled connection on commit()/rollback(); nothing to do.
    }

    protected destroyConnection(_client: MssqlDriver): void {
        // mssql transactions release their pooled connection on settle; there is no per-client handle to
        // destroy (A24). No-op — the pool manages connection health.
    }

    public async startTransaction(client: MssqlDriver): Promise<void> {
        if (!client) throw new Error("startTransaction requires a pinned connection; use runTransaction().");
        await client.begin();
    }

    public async commit(client: MssqlDriver): Promise<void> {
        if (!client) throw new Error("commit requires a pinned connection; use runTransaction().");
        await client.commit();
    }

    public async rollback(client: MssqlDriver): Promise<void> {
        if (!client) throw new Error("rollback requires a pinned connection; use runTransaction().");
        await client.rollback();
    }

    public getDialectConfig() {
        return DIALECT_CONFIG;
    }

    protected getPermanentErrors(): Promise<string[]> {
        return Promise.resolve(SQL_SERVER_PERMANENT_ERRORS);
    }

    async testQuery(queryOrParams: QueryInput): Promise<{ success: true }> {
        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
        if (!isValidSingleQuery(query)) {
            throw new Error("Each query in the transaction must be a single statement.");
        }
        if (!this.connection) await this.establishConnection();

        // Syntax-check without executing (SET PARSEONLY validates the batch and returns nothing).
        const request = this.connection.request();
        try {
            await request.batch(`SET PARSEONLY ON; ${query}; SET PARSEONLY OFF;`);
            return { success: true };
        } catch (error) {
            this.error(`SQL Server testQuery failed: ${error instanceof Error ? error.message : String(error)}`);
            throw this.normalizeError(error);
        }
    }

    /** mssql surfaces a QUERY-time error's real number on err.number (code otherwise stays the
     *  generic "EREQUEST"); map it to err.code so the retry logic (which matches on error.code
     *  against getPermanentErrors) works. A CONNECT-time failure (bad password, inaccessible
     *  database) never gets a number at all — tedious's connection-level "ELOGIN" code is already
     *  on err.code, with nothing for this function to do; SQL_SERVER_PERMANENT_ERRORS lists
     *  "ELOGIN" alongside the real numeric codes so that case classifies correctly too. */
    private normalizeError(error: unknown): unknown {
        if (typeof error === "object" && error !== null && "number" in error && error.number !== undefined) {
            try { (error as { code?: string }).code = stringifyScalar(error.number as number); } catch { /* frozen error; ignore */ }
        }
        return error;
    }

    protected async executeQuery(queryOrParams: QueryInput, client?: MssqlDriver): Promise<{ rows: DataRow[]; affectedRows: number }> {
        if (!this.connection) await this.establishConnection();

        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
        const params = typeof queryOrParams === "string" ? [] : queryOrParams.params || [];

        // Bind to the pinned transaction when running inside runTransaction; otherwise a pool request.
        const request = client ? new this.sql.Request(client) : this.connection.request();
        params.forEach((value: unknown, i: number) => request.input(`p${i}`, value));

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
            request.input("res", this.sql.NVarChar(255), resource);
            request.input("timeout", this.sql.Int, timeoutSeconds * 1000);
            // sp_getapplock returns a status code (>=0 success, <0 failure) via its RETURN value, NOT an
            // output param — capture it into a local and SELECT it. @LockOwner='Transaction' ties the lock
            // to this open transaction; it releases on commit/rollback.
            const result = await request.query(
                "DECLARE @code int; " +
                "EXEC @code = sp_getapplock @Resource = @res, @LockMode = 'Exclusive', " +
                "@LockOwner = 'Transaction', @LockTimeout = @timeout; " +
                "SELECT @code AS code;"
            );
            const code = Number(result?.recordset?.[0]?.code ?? 0);
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
            alterTableChanges = alterTableChangesOrOldHeaders;
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
                throw new Error(`Failed to fetch unique indexes for table ${table}: ${uniqueIndexesResult.error ?? "unknown error"}`);
            }
            const schema = this.getConfig().schema;
            const schemaPrefix = schema ? `${escapeIdentifier(schema, "sqlserver")}.` : "";
            const indexesToDrop = uniqueIndexesResult.results
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

    getInsertStatementQuery(tableOrInput: string | InsertInput, data?: DataRow[], metaData?: MetadataHeader, insertInput?: "UPDATE" | "INSERT"): QueryInput {
        return SqlServerInsertQueryBuilder.getInsertStatementQuery(tableOrInput, data, metaData, this.getConfig(), insertInput);
    }

    getInsertFromStagingQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, insertInput?: "UPDATE" | "INSERT", pkFilter?: DataRow): QueryInput {
        return SqlServerInsertQueryBuilder.getInsertFromStagingQuery(tableOrInput, metaData, this.getConfig(), insertInput, pkFilter);
    }

    getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, pkFilter?: DataRow): QueryInput {
        return SqlServerInsertQueryBuilder.getInsertChangedRowsToHistoryQuery(tableOrInput, metaData, this.getConfig(), pkFilter);
    }

    getCreateTempTableQuery(table: string, stagingPrefix?: string): QueryInput {
        return SqlServerTableQueryBuilder.getCreateTempTableQuery(table, this.getConfig().schema, stagingPrefix);
    }

    getConstraintConflictQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] }, stagingPrefix?: string): QueryInput {
        return SqlServerIndexQueryBuilder.generateConstraintConflictBreakdownQuery(table, structure, this.getConfig().schema, stagingPrefix);
    }
}
