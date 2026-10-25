import { AsyncLocalStorage } from 'async_hooks';
import { isValidSingleQuery } from './utils/validateQuery';
import type { QueryInput, DatabaseConfig, DialectConfig, AlterTableChanges, MetadataHeader, QueryResult, InsertInput } from '../config/types';
import { validateConfig, parseDatabaseMetaData } from '../helpers/utilities';
import { MAX_QUERY_ATTEMPTS } from "../config/defaults";
import type { AutoSQLHandler } from "./autosql";
import { setSSH } from '../helpers/ssh';

/**
 * Normalise a driver error's structured code for QueryResult.errorCode: non-empty string (mysql2
 * `code` / pg SQLSTATE) or number (SQL Server). Empty-string `code` → undefined; numeric `0` → "0".
 */
function driverErrorCode(err: any): string | undefined {
    const code = err?.code;
    return code != null && code !== "" ? String(code) : undefined;
}

export abstract class Database {
    protected connection: any = null;
    protected config: DatabaseConfig;
    public autoSQLHandler!: AutoSQLHandler;
    protected abstract getPermanentErrors(): Promise<string[]>;
    // Per-operation schema override, so concurrent calls with different schemas don't race on a
    // shared, mutated this.config.schema.
    private schemaContext = new AsyncLocalStorage<string>();
    // Per-operation number-format override, so a resolved {thousands, decimal} pair reaches
    // inference/sqlize/worker dispatch for one load without mutating shared config.
    private separatorContext = new AsyncLocalStorage<{ thousands: string; decimal: string }>();

    constructor(config: DatabaseConfig) {
        this.config = validateConfig(config);
    }

    public getConfig() {
        const ctxSchema = this.schemaContext.getStore();
        const ctxSep = this.separatorContext.getStore();
        if (ctxSchema === undefined && ctxSep === undefined) return this.config;
        const cfg = { ...this.config };
        if (ctxSchema !== undefined) cfg.schema = ctxSchema;
        if (ctxSep !== undefined) {
            cfg.thousandsSeparator = ctxSep.thousands;
            cfg.decimalSeparator = ctxSep.decimal;
        }
        return cfg;
    }

    /** Run `fn` with `schema` as the effective config schema, without mutating shared config. A falsy schema runs `fn` unchanged. */
    public runWithSchema<T>(schema: string | undefined, fn: () => T): T {
        if (!schema) return fn();
        return this.schemaContext.run(schema, fn);
    }

    /** Run `fn` with resolved number-format separators as the effective config, without mutating shared config. A falsy pair runs `fn` unchanged. */
    public runWithSeparators<T>(separators: { thousands: string; decimal: string } | undefined, fn: () => T): T {
        if (!separators) return fn();
        return this.separatorContext.run(separators, fn);
    }

    public updateTableMetadata(table: string, metaData: MetadataHeader, type: "metaData" | "existingMetaData" = "metaData"): void {
        if (!this.config[type]) {
            this.config[type] = {};
        }

        this.config[type][table] = {
            ...this.config[type][table],
            ...metaData
        };
    }

    public updateSchema(schema: string | undefined): void {
        this.config.schema = schema;
    }

    public log(msg: string): void { this.config.logger?.log?.(msg); }
    public warn(msg: string): void { this.config.logger?.warn?.(msg); }
    public error(msg: string): void { this.config.logger?.error?.(msg); }

    /**
     * Warn once when a computed schema change (drop/PK change/unique-drop) is blocked by its
     * opt-in flag defaulting to false (R9), naming the columns and the flag to flip. Doesn't fire
     * on an unchanged re-ingest: `primaryKeyChanges` only populates on a genuine delta, and
     * `dropColumns` is rename-folded.
     */
    protected warnBlockedSchemaChanges(table: string, changes: AlterTableChanges): void {
        if (changes.dropColumns?.length && !this.config.deleteColumns) {
            this.warn(`Schema change not applied to '${table}': ${changes.dropColumns.length} column(s) would be dropped (${changes.dropColumns.join(", ")}) but 'deleteColumns' is off. Set deleteColumns: true to apply the drop, or leave it off to keep the columns.`);
        }
        if (changes.primaryKeyChanges?.length && !this.config.updatePrimaryKey) {
            this.warn(`Schema change not applied to '${table}': a primary-key change is pending (${changes.primaryKeyChanges.join(", ")}) but 'updatePrimaryKey' is off. Set updatePrimaryKey: true to apply it.`);
        }
        if (changes.noLongerUnique?.length && !this.config.dropUniqueConstraints) {
            this.warn(`Schema change not applied to '${table}': ${changes.noLongerUnique.length} column(s) (${changes.noLongerUnique.join(", ")}) now contain duplicate values that would require DROPPING their UNIQUE constraint, but 'dropUniqueConstraints' is off. The constraint is KEPT (the load may then fail on the duplicate rows); set dropUniqueConstraints: true to relax it.`);
        }
    }

    /**
     * Opt-in charset upgrade (R8): return DDL converting text columns whose charset differs from the
     * target. No-op in the base impl, because only MySQL has the 3-byte `utf8` vs 4-byte `utf8mb4`
     * split; Postgres `UTF8` already stores 4-byte chars.
     */
    public getCharsetUpgradeQueries(_table: string): Promise<QueryInput[]> {
        return Promise.resolve([]);
    }

    static create(config: DatabaseConfig): Database {
        const dialects: Record<string, new (config: DatabaseConfig) => Database> = {
            mysql: MySQLDatabase,
            pgsql: PostgresDatabase,
            sqlserver: SqlServerDatabase
        };

        const dialect = config.sqlDialect?.toLowerCase();
        if (!dialects[dialect]) {
            throw new Error(`Unsupported SQL dialect: ${dialect}`);
        }

        return new dialects[dialect](config);
    }

    public getDialect() {
        return this.config.sqlDialect
    }

    public abstract getDialectConfig(): DialectConfig;

    public abstract establishDatabaseConnection(): Promise<void>;
    public abstract testQuery(queryOrParams: QueryInput): Promise<any>;
    // When `client` is provided the query runs on that pinned connection and the connection is
    // NOT released here (the transaction owner manages its lifecycle). Without a client, a
    // throwaway pool connection is acquired and released per call (standalone queries).
    protected abstract executeQuery(queryOrParams: QueryInput, client?: any): Promise<any>;
    // Acquire/release a single pooled connection so a transaction can run all of its
    // statements (START/…/COMMIT/ROLLBACK) on the same connection.
    protected abstract acquireConnection(): Promise<any>;
    protected abstract releaseConnection(client: any): void;
    // Discard a connection instead of returning it to the pool — used when a ROLLBACK failed, so the
    // next borrower isn't handed a connection stuck in an aborted-transaction state (A24).
    protected abstract destroyConnection(client: any): void;

    public async establishConnection(): Promise<void> {
        let attempts = 0;
        const maxAttempts = MAX_QUERY_ATTEMPTS || 3;

        while (attempts < maxAttempts) {
            try {
                if (this.config.sshConfig) {
                    const { stream, sshClient } = await setSSH(this.config.sshConfig, this.config.logger);
                    this.config.sshStream = stream;
                    this.config.sshClient = sshClient;
                }
                await this.establishDatabaseConnection();
                return;
            } catch (error: any) {
                this.error(`Database connection attempt ${attempts + 1} failed: ${error.message}`);

                // Close any SSH tunnel opened on this attempt so a retry/final-throw doesn't leak an
                // SSH connection per failed attempt.
                if (this.config.sshClient) {
                    try { this.config.sshClient.end(); } catch { /* already closed / never connected */ }
                    this.config.sshClient = undefined;
                    this.config.sshStream = undefined;
                }

                const permanentErrors = await this.getPermanentErrors();
                if (permanentErrors.includes(error.code)) {
                    this.error("Permanent error detected. Aborting retry.");
                    throw error;
                }

                attempts++;
                if (attempts < maxAttempts) {
                    await new Promise((res) => setTimeout(res, 1000));
                }
            }
        }

        throw new Error("Database connection failed after multiple attempts.");
    }

    public async testConnection(): Promise<boolean> {
        try {
            const result = await this.runQuery({ query: "SELECT 1 AS solution;"});
            return !!result;
        } catch (error) {
            this.error(`Test connection failed: ${error}`);
            return false;
        }
    }

    public async checkSchemaExists(schemaName: string | string[]): Promise<Record<string, boolean>> {
        const query = this.getCheckSchemaQuery(schemaName);
        const result = await this.runQuery(query);

        // A failed query means we could NOT determine existence (connectivity/auth/permissions), not
        // that the schema is absent. Masking as `false` would mislead a caller into re-creating an
        // existing schema or skipping one that exists — surface the failure instead of guessing.
        if (!result.success) {
            throw new Error(`checkSchemaExists: could not determine schema existence — ${result.error}`);
        }

        const resultsArray = result.results ?? [];

        if (Array.isArray(schemaName)) {
            return schemaName.reduce<Record<string, boolean>>((acc, db) => {
                acc[db] = resultsArray[0]?.[db] === 1;
                return acc;
            }, {});
        }

        return { [schemaName]: resultsArray[0]?.[schemaName] === 1 };
    }

    /**
     * Create the schema if it doesn't exist. Resolves `{ [schemaName]: true }`, plus a deprecated
     * `success: true`. THROWS when the CREATE fails, with the driver code on `error.code`, so a
     * login without create rights surfaces the real cause rather than a later "schema not found".
     */
    public async createSchema(schemaName: string): Promise<Record<string, boolean>> {
        let result: QueryResult;
        try {
            result = await this.runQuery(this.getCreateSchemaQuery(schemaName));
        } catch (error) {
            throw new Error(`Failed to create schema "${schemaName}": ${error}`);
        }
        // runQuery reports a SQL error as success:false rather than throwing.
        if (!result.success) {
            const err: any = new Error(`Failed to create schema "${schemaName}": ${result.error ?? "unknown error"}${result.errorCode ? ` [${result.errorCode}]` : ""}`);
            if (result.errorCode) err.code = result.errorCode;
            throw err;
        }
        // `success` is kept for callers written against the old `{ success: true }` return (deprecated).
        return { [schemaName]: true, success: true };
    }

    public createTableQuery(table: string, headers: MetadataHeader): QueryInput[] {
        if (!table || !headers || Object.keys(headers).length === 0) {
            throw new Error("Invalid table configuration: table name and headers are required.");
        }
    
        return this.getCreateTableQuery(table, headers);
    }

    public async alterTableQuery(table: string, oldHeaders: MetadataHeader, newHeaders: MetadataHeader): Promise<QueryInput[]> {
        if (!table || !oldHeaders || !newHeaders) {
            throw new Error("Invalid table configuration: table name and headers are required.");
        }
    
        return this.getAlterTableQuery(table, oldHeaders, newHeaders);
    }

    public dropTableQuery(table: string): QueryInput {
        if (!table) {
            throw new Error("Invalid table configuration: table name is required.");
        }
    
        return this.getDropTableQuery(table);
    }
    

    // Require a pinned connection — a transaction spread across throwaway pool connections isn't
    // atomic. Call runTransaction for atomic multi-statement work rather than these directly.
    public async startTransaction(client: any): Promise<void> {
        if (!client) throw new Error("startTransaction requires a pinned connection; use runTransaction() for atomic transactions.");
        await this.executeQuery("START TRANSACTION;", client);
    }

    public async commit(client: any): Promise<void> {
        if (!client) throw new Error("commit requires a pinned connection; use runTransaction().");
        await this.executeQuery("COMMIT;", client);
    }

    public async rollback(client: any): Promise<void> {
        if (!client) throw new Error("rollback requires a pinned connection; use runTransaction().");
        await this.executeQuery("ROLLBACK;", client);
    }

    public async closeConnection(): Promise<{ success: boolean; error?: string }> {
        if (!this.connection)  return { success: true };

        try {
            if ("end" in this.connection) {
                await this.connection.end(); // only MySQL/PostgreSQL expose this
            }
            if (this.config.sshClient) {
                this.config.sshClient.end();
            }
            return { success: true };
        } catch (error) {
            return { success: false, error: error instanceof Error ? error.message : "Unknown error" };
        } finally {
            this.connection = undefined;        
            this.config.sshClient = undefined;
            this.config.sshStream = undefined;
        }
    }
    
    public async runQuery(queryOrParams: QueryInput, maxAttemptsOverride?: number): Promise<QueryResult> {
        // `maxAttemptsOverride` lets a caller opt out of the internal retry — matters for a
        // non-idempotent statement (single-row INSERT), where retrying after an ambiguous failure
        // (conn dropped after the server applied the row) would duplicate it. `perRowInsertWithRetry`
        // passes 1 for exactly this (A15).
        const maxAttempts = maxAttemptsOverride ?? MAX_QUERY_ATTEMPTS ?? 3;
        let attempts = 0;
        let _error: any;
        const start = new Date();
        let end: Date;

        const query: QueryInput = typeof queryOrParams === "string" ? { query: queryOrParams, params: [] } : queryOrParams;

        if (!isValidSingleQuery(query.query)) {
            return { start, end: new Date(), duration: 0, success: false, error: "Multiple SQL statements detected. Use transactions instead." };
        }

        while (attempts < maxAttempts) {
            try {
                const { rows, affectedRows } = await this.executeQuery(query);

                end = new Date();
                return {
                    start,
                    end,
                    duration: end.getTime() - start.getTime(),
                    affectedRows,
                    success: true,
                    results: rows
                };

            } catch (error: any) {
                const permanentErrors = await this.getPermanentErrors();
                if (permanentErrors.includes(error.code)) {
                    end = new Date();
                    return { start, end, duration: end.getTime() - start.getTime(), success: false, error: error.message, errorCode: driverErrorCode(error) };
                }
    
                attempts++;
                if (attempts < maxAttempts) {
                    await new Promise(res => setTimeout(res, 1000));
                } else {
                    _error = error;
                }
            }
        }
    
        end = new Date();
        return { start, end, duration: end.getTime() - start.getTime(), success: false, error: _error?.message, errorCode: driverErrorCode(_error) };
    }

    public async runTransaction(queriesOrStrings: QueryInput[]): Promise<QueryResult> {
        const start = new Date();
        // Establishing the connection can fail (bad creds, host down, SSH tunnel failure). Keep it
        // inside the failure envelope so runTransaction always resolves to a QueryResult and never
        // rejects (matches runQuery's contract) — a caller's process can't crash on a connect error.
        try {
            if (!this.connection) {
                await this.establishConnection();
            }
        } catch (error: any) {
            const end = new Date();
            return { start, end, duration: end.getTime() - start.getTime(), success: false, error: error instanceof Error ? error.message : String(error), errorCode: driverErrorCode(error) };
        }
        let end: Date;
        let _error: any;

        const queries = queriesOrStrings.map(query =>
            typeof query === "string" ? { query, params: [] } : query
        );

        // Retry the whole transaction, not individual statements: a deadlock or serialization failure
        // aborts the entire transaction, so a per-statement retry on the same connection cannot
        // succeed. A DDL-containing transaction runs exactly once, because MySQL implicitly commits
        // before each DDL and re-running would re-apply the already-committed DML.
        const containsDDL = queries.some(q => /^\s*(create|alter|drop|truncate|rename)\b/i.test(q.query || ""));
        const maxAttempts = containsDDL ? 1 : (MAX_QUERY_ATTEMPTS || 3);

        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            let client: any;
            let rollbackFailed = false;
            let results: any[] = [];
            let totalAffectedRows = 0;
            try {
                // Acquire INSIDE the try: pool exhaustion, an acquire timeout, or a connection
                // dropped between establish and acquire must become a {success:false} QueryResult
                // rather than an unhandled rejection, since callers rely on never-rejects (A16).
                client = await this.acquireConnection();
                await this.startTransaction(client);

                for (const query of queries) {
                    if (!query?.query?.trim()) continue;
                    const { rows, affectedRows } = await this.executeQuery(query, client);
                    results = results.concat(rows);
                    totalAffectedRows += affectedRows || 0;
                }

                await this.commit(client);
                end = new Date();
                return {
                    start,
                    end,
                    duration: end.getTime() - start.getTime(),
                    affectedRows: totalAffectedRows || results.length,
                    success: true,
                    results
                };
            } catch (error: any) {
                this.error(`Transaction error (attempt ${attempt}/${maxAttempts}): ${error}`);
                if (client) {
                    try {
                        await this.rollback(client);
                    } catch (rollbackError) {
                        rollbackFailed = true;
                        this.error(`Rollback failed: ${rollbackError}`);
                    }
                }

                const permanentErrors = await this.getPermanentErrors();
                if (permanentErrors.includes(error.code) || attempt >= maxAttempts) {
                    _error = error;
                    break;
                }
                await new Promise(res => setTimeout(res, 1000));
            } finally {
                if (client) {
                    // A failed rollback leaves the connection dirty (esp. Postgres: aborted txn);
                    // destroy it rather than return a poisoned connection to the pool (A24).
                    if (rollbackFailed) this.destroyConnection(client);
                    else this.releaseConnection(client);
                }
            }
        }

        end = new Date();
        return {
            start,
            end,
            duration: end.getTime() - start.getTime(),
            success: false,
            error: _error?.message,
            errorCode: driverErrorCode(_error)
        };
    }

    public async runTransactionsWithConcurrency(queryGroups: QueryInput[][]): Promise<QueryResult[]> {
        // A connect failure must not reject the whole batch — return one failure result per group so
        // callers still get a well-formed array (matching the per-group failure path in runNext below).
        try {
            if (!this.connection) {
                await this.establishConnection();
            }
        } catch (error: any) {
            const now = new Date();
            const msg = error instanceof Error ? error.message : String(error);
            return queryGroups.map(() => ({ start: now, end: now, duration: 0, success: false, error: msg }));
        }
        const results: QueryResult[] = [];
        const running: Promise<void>[] = [];
        // Cap concurrency at the connections actually AVAILABLE — the pool size minus any connections
        // this instance has pinned for held schema locks (A25b). At least 1 so a batch always progresses.
        const poolSize = Math.max(1, this.getMaxConnections() - this.getHeldSchemaLockCount())

        let index = 0;
      
        const runNext = async () => {
          const i = index++;
          if (i >= queryGroups.length) return;
      
          this.log(`Running transaction for group #${i + 1}`);
          try {
            const result = await this.runTransaction(queryGroups[i]);
            results[i] = result;
          } catch (error: any) {
            results[i] = {
              start: new Date(),
              end: new Date(),
              duration: 0,
              success: false,
              error: error?.message || "Unknown error"
            };
          }
      
          await runNext();
        };
      
        for (let i = 0; i < Math.min(poolSize, queryGroups.length); i++) {
          running.push(runNext());
        }
      
        await Promise.all(running);
      
        return results;
    }

    public async getTableMetaData(schema: string, table: string): Promise<MetadataHeader | null> {
        try {
            if (!this.connection) throw new Error("Database connection not established.");
    
            const existsQueryInput = this.getTableExistsQuery(schema, table);
            const exists = await this.runQuery(existsQueryInput);
            // `exists` is always a (truthy) QueryResult, so the old `if (!exists)` never fired (A19).
            // Surface a failed existence probe, then read the COUNT(*) to short-circuit an absent table
            // (key casing varies by dialect, so read the first column value).
            if (!exists.success) {
                throw new Error(`Failed to check whether ${schema}.${table} exists: ${exists.error ?? "unknown error"}`);
            }
            const countRow = exists.results?.[0];
            const tableExists = countRow ? Number(Object.values(countRow)[0] ?? 0) > 0 : false;
            if (!tableExists) return null;

            const metaQuery = this.getTableMetaDataQuery(schema, table);
            const result = await this.runQuery(metaQuery);
            if (!result.success || !result.results) {
                throw new Error(`Failed to fetch metadata: ${result.error}`);
            }
    
            const parsedMetadata = parseDatabaseMetaData(result.results, this.getDialectConfig());
    
            if (!parsedMetadata) return null;
            if (typeof parsedMetadata === "object" && !Array.isArray(parsedMetadata)) {
                return parsedMetadata as MetadataHeader; // Single table case
            }
    
            throw new Error("Unexpected metadata format: Multiple tables returned for a single-table query.");
        } catch (error) {
            this.error(`Error fetching table metadata: ${error}`);
            return null;
        }
    }
    
    public abstract getCreateSchemaQuery(schemaName: string): QueryInput;
    public abstract getCheckSchemaQuery(schemaName: string | string[]): QueryInput;
    public abstract getCreateTableQuery(table: string, headers: MetadataHeader): QueryInput[]
    public abstract getAlterTableQuery(table: string, alterTableChangesOrOldHeaders: AlterTableChanges | MetadataHeader, newHeaders?: MetadataHeader): Promise<QueryInput[]>;
    public abstract getDropTableQuery(table: string): QueryInput;
    public abstract getPrimaryKeysQuery(table: string): QueryInput;
    public abstract getForeignKeyConstraintsQuery(table: string): QueryInput;
    public abstract getViewDependenciesQuery(table: string): QueryInput;
    public abstract getDropPrimaryKeyQuery(table: string): QueryInput;
    public abstract getDropUniqueConstraintQuery(table: string, indexName: string): QueryInput;
    public abstract getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput;
    public abstract getUniqueIndexesQuery(table: string, column_name?: string): QueryInput;
    public abstract getTableExistsQuery(schema: string, table: string): QueryInput;
    public abstract getTableMetaDataQuery(schema: string, table: string): QueryInput;
    public abstract getSplitTablesQuery(table: string): QueryInput;
    public abstract getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, insertType?: "UPDATE" | "INSERT"): QueryInput; 
    // `pkFilter` (optional) scopes the merge to a single primary key — used by the atomic staging
    // degradation path, which runs a per-PK merge in the same transaction as that PK's before-image.
    public abstract getInsertFromStagingQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, insertType?: "UPDATE" | "INSERT", pkFilter?: Record<string, any>): QueryInput;
    // `pkFilter` (optional) scopes the before-image capture to a single primary key (same atomic path).
    public abstract getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, pkFilter?: Record<string, any>): QueryInput;
    public abstract getCreateTempTableQuery(table: string, stagingPrefix?: string): QueryInput;
    public abstract getConstraintConflictQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] }, stagingPrefix?: string): QueryInput;

    public abstract getMaxConnections(): number;

    /**
     * How many schema locks this instance holds (each pins a pool connection or transaction).
     * `runTransactionsWithConcurrency` subtracts this from the pool size to avoid
     * over-subscribing while locks are held (A25b).
     */
    protected getHeldSchemaLockCount(): number { return 0; }

    /**
     * Shared `rejectUnauthorized:false` + `ssl`/`sshConfig` conflict warnings for all 3 dialects —
     * previously copy-pasted, which once let SQL Server's copy drift out of sync (PR #79). The
     * `sshConfig` conflict branch is a no-op on SQL Server (validateConfig already refuses that
     * combination there), but still routes through one copy of the message.
     */
    protected warnInsecureTlsAndTunnelConflict(ssl: unknown): void {
        if (ssl && typeof ssl === "object" && (ssl as { rejectUnauthorized?: boolean }).rejectUnauthorized === false) {
            this.config.logger?.warn?.("autosql: ssl.rejectUnauthorized is false — TLS certificate verification is DISABLED (use only for dev/self-signed).");
        }
        if (ssl && (this.config.sshConfig || this.config.sshStream)) {
            this.config.logger?.warn?.("autosql: both `ssl` and `sshConfig` are set — pick one encrypted path (direct TLS OR an SSH tunnel); TLS over an SSH stream is not a supported combination.");
        }
    }

    /**
     * acquireSchemaLock's connection-registration step (shared by mysql.ts/pgsql.ts): release any
     * stale connection already registered for this table before pointing the map at the new one,
     * so a leftover entry can't leak a pooled connection.
     */
    protected registerSchemaLockConnection<T extends { release(): void }>(connections: Map<string, T>, table: string, client: T): void {
        const stale = connections.get(table);
        if (stale && stale !== client) stale.release();
        connections.set(table, client);
    }

    /**
     * acquireSchemaLock's catch-path cleanup (shared by mysql.ts/pgsql.ts): release `client` if
     * the map was never updated to point at it (lock not acquired, or acquiring threw first).
     */
    protected releaseSchemaLockConnectionOnError<T extends { release(): void }>(connections: Map<string, T>, table: string, client: T | undefined): void {
        if (client && connections.get(table) !== client) client.release();
    }

    /**
     * Bulk-load already-sqlized value rows using the dialect's native mechanism. `columns` is the
     * ordered list each row's values align to. Throws on failure so the caller can fall back to
     * parameterised INSERT.
     */
    public abstract bulkLoadRows(table: string, columns: string[], rows: any[][], header?: MetadataHeader): Promise<QueryResult>;

    /**
     * Acquire a database-native advisory lock scoped to the given table (MySQL `GET_LOCK` on a
     * dedicated pool connection, Postgres `pg_try_advisory_lock` on a dedicated client). Held until
     * `releaseSchemaLock`.
     */
    public abstract acquireSchemaLock(table: string, timeoutSeconds: number): Promise<void>;

    /** Release the lock from `acquireSchemaLock`. Safe to call when none is held. */
    public abstract releaseSchemaLock(table: string): Promise<void>;

    get autoSQL() {
        return this.autoSQLHandler.autoSQL.bind(this.autoSQLHandler);
    }

    // Dry run: what an autoSQL(table, data, …) call WOULD do (schema diff + DDL), computed without
    // writing. See AutoSQLHandler.preview.
    get preview() {
        return this.autoSQLHandler.preview.bind(this.autoSQLHandler);
    }

    get autoCreateTable() {
        return this.autoSQLHandler.autoCreateTable.bind(this.autoSQLHandler);
    }

    get autoAlterTable() {
        return this.autoSQLHandler.autoAlterTable.bind(this.autoSQLHandler);
    }

    get fetchTableMetadata() {
        return this.autoSQLHandler.fetchTableMetadata.bind(this.autoSQLHandler);
    }

    get splitTableData() {
        return this.autoSQLHandler.splitTableData.bind(this.autoSQLHandler);
    }

    get autoInsertData() {
        return this.autoSQLHandler.autoInsertData.bind(this.autoSQLHandler);
    }

    get handleMetadata() {
        return this.autoSQLHandler.handleMetadata.bind(this.autoSQLHandler);
    }

    get autoConfigureTable() {
        return this.autoSQLHandler.autoConfigureTable.bind(this.autoSQLHandler);
    }

    get autoSQLChunked() {
        return this.autoSQLHandler.autoSQLChunked.bind(this.autoSQLHandler);
    }

    get openStream() {
        return this.autoSQLHandler.openStream.bind(this.autoSQLHandler);
    }
}

import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { SqlServerDatabase } from "./sqlserver";

export { MySQLDatabase, PostgresDatabase, SqlServerDatabase };