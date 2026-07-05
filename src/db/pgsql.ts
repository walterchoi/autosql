import crypto from "crypto";
import type { Pool, PoolClient } from "pg";
import { Database } from "./database";
import { pgsqlPermanentErrors } from './permanentErrors/pgsql';
import { QueryInput, ColumnDefinition, DatabaseConfig, AlterTableChanges, InsertResult, MetadataHeader, InsertInput, QueryResult } from "../config/types";
import { pgsqlConfig } from "./config/pgsqlConfig";
import { isValidSingleQuery } from './utils/validateQuery';
import { escapeIdentifier, escapeLiteral } from './utils/escape';
import { compareMetaData } from '../helpers/metadata';
import { PostgresTableQueryBuilder } from "./queryBuilders/pgsql/tableBuilder";
import { PostgresIndexQueryBuilder } from "./queryBuilders/pgsql/indexBuilder";
import { PostgresInsertQueryBuilder } from "./queryBuilders/pgsql/insertBuilder";
import { AutoSQLHandler } from "./autosql";
import { normalizeResultKeys, isMetadataHeader } from "../helpers/utilities";
import { SchemaLockTimeoutError } from "../errors";
const dialectConfig = pgsqlConfig

export class PostgresDatabase extends Database {
    private schemaLockConnections: Map<string, PoolClient> = new Map();

    constructor(config: DatabaseConfig) {
        super(config);
        this.autoSQLHandler = new AutoSQLHandler(this);
    }

    async establishDatabaseConnection(): Promise<void> {
        let Pg: typeof import("pg");
        try {
            Pg = require("pg");
        } catch (err) {
            throw new Error("Missing required dependency 'pg'. Please install it to use PgDatabase.");
        }

        this.connection = new Pg.Pool({
            host: this.config.host,
            user: this.config.user,
            password: this.config.password,
            database: this.config.database,
            port: this.config.port || 5432,
            max: 5,
            stream: this.config.sshStream ? () => this.config.sshStream! : undefined
        });
    }

    public getMaxConnections(): number {
        return (this.connection as Pool)?.options?.max ?? 5;
    }

    /**
     * Two int4 keys (from sha256) for the pg_advisory_lock(int4, int4) form — a 64-bit lock
     * space. A single 32-bit djb2 key made distinct tables collide onto the same advisory
     * lock and serialize against each other.
     */
    private getLockKey(table: string): [number, number] {
        const h = crypto.createHash("sha256").update(`autosql_schema__${table}`).digest();
        return [h.readInt32BE(0), h.readInt32BE(4)];
    }

    public async acquireSchemaLock(table: string, timeoutSeconds: number): Promise<void> {
        const [lockKey1, lockKey2] = this.getLockKey(table);
        if (!this.connection) await this.establishConnection();
        let client: PoolClient | undefined;
        try {
            client = await (this.connection as Pool).connect();
            const deadline = Date.now() + timeoutSeconds * 1000;
            let acquired = false;
            while (Date.now() < deadline) {
                const result = await client.query('SELECT pg_try_advisory_lock($1, $2) AS acquired', [lockKey1, lockKey2]);
                if (result.rows[0]?.acquired === true) {
                    acquired = true;
                    break;
                }
                await new Promise(r => setTimeout(r, 500));
            }
            if (!acquired) {
                // Do not release here — the catch below releases exactly once (the map was
                // never set for this table, so its guard fires). Releasing here too would
                // double-release the connection on the timeout path.
                throw new SchemaLockTimeoutError(
                    `Could not acquire schema lock for table '${table}' within ${timeoutSeconds}s. ` +
                    `Another process may be modifying this table's schema. Increase schemaLockTimeout or retry later.`
                );
            }
            // Release any stale connection registered for this table before overwriting, so a
            // leftover entry can't leak a pooled connection.
            const stale = this.schemaLockConnections.get(table);
            if (stale && stale !== client) stale.release();
            this.schemaLockConnections.set(table, client);
        } catch (error) {
            if (client && this.schemaLockConnections.get(table) !== client) client.release();
            throw error;
        }
    }

    public async releaseSchemaLock(table: string): Promise<void> {
        const [lockKey1, lockKey2] = this.getLockKey(table);
        const client = this.schemaLockConnections.get(table);
        if (!client) return;
        try {
            await client.query('SELECT pg_advisory_unlock($1, $2)', [lockKey1, lockKey2]);
        } finally {
            client.release();
            this.schemaLockConnections.delete(table);
        }
    }

    public getDialectConfig() {
        return dialectConfig;
    }

    protected async getPermanentErrors(): Promise<string[]> {
        return pgsqlPermanentErrors
    }

    async testQuery(queryOrParams: QueryInput): Promise<any> {
        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
    
        if (!isValidSingleQuery(query)) {
            throw new Error("Each query in the transaction must be a single statement.");
        }
    
        if (!this.connection) {
            await this.establishConnection();
        }
    
        let client: PoolClient | null = null;
        try {
            client = await (this.connection as Pool).connect();
    
            if (
                query.trim().toLowerCase().startsWith("select") ||
                query.trim().toLowerCase().startsWith("insert") ||
                query.trim().toLowerCase().startsWith("update") ||
                query.trim().toLowerCase().startsWith("delete")
            ) {
                // Use PREPARE for DML queries
                await client.query(`PREPARE validate_stmt AS ${query}`);
                await client.query(`DEALLOCATE validate_stmt`); // Cleanup
            } else {
                // Use a transaction with ROLLBACK for DDL queries (CREATE TABLE, ALTER TABLE)
                await client.query("BEGIN;");
                await client.query(query);
                await client.query("ROLLBACK;"); // Prevent execution
            }
    
            return { success: true };
        } catch (error) {
            if(client) await client.query("ROLLBACK;");
            this.error(`PostgreSQL testQuery failed: ${error}`);
            throw error;
        } finally {
            if (client) client.release();
        }
    }    

    protected async acquireConnection(): Promise<PoolClient> {
        if (!this.connection) {
            await this.establishConnection();
        }
        return await (this.connection as Pool).connect();
    }

    protected releaseConnection(client: PoolClient): void {
        if (client) client.release();
    }

    protected async executeQuery(query: string, client?: PoolClient): Promise<any>;
    protected async executeQuery(QueryInput: QueryInput, client?: PoolClient): Promise<any>;
    protected async executeQuery(queryOrParams: QueryInput, client?: PoolClient): Promise<{ rows: any[]; affectedRows: number }> {
        if (!this.connection) {
            await this.establishConnection();
        }

        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
        const params = typeof queryOrParams === "string" ? [] : queryOrParams.params || [];

        const pinned = !!client;
        let conn: PoolClient | null = client ?? null;
        try {
            if (!conn) conn = await (this.connection as Pool).connect();
            const result = await conn.query(query, params);

            const rows = result.rows || [];
            const affectedRows = result.rowCount ?? rows.length ?? 0;

            return { rows, affectedRows };
        } catch (error) {
            // Standalone query: roll back its throwaway connection. Pinned (transaction)
            // connection: leave the ROLLBACK to runTransaction so the whole transaction aborts
            // on this same connection.
            if (!pinned && conn) { try { await conn.query("ROLLBACK;"); } catch { /* autocommit: nothing to roll back */ } }
            throw error;
        } finally {
            if (!pinned && conn) conn.release();
        }
    }

    getCreateSchemaQuery(schemaName: string): QueryInput {
        return { query: `CREATE SCHEMA IF NOT EXISTS ${escapeIdentifier(schemaName, "pgsql")};`};
    }

    getCheckSchemaQuery(schemaName: string | string[]): QueryInput {
        if (Array.isArray(schemaName)) {
            return { query: `SELECT ${schemaName
                .map(
                    (db) =>
                        `(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ${escapeLiteral(db, "pgsql")}) THEN 1 ELSE 0 END) AS ${escapeIdentifier(db, "pgsql")}`
                )
                .join(", ")};`};
        }
        return { query: `SELECT (CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ${escapeLiteral(schemaName, "pgsql")}) THEN 1 ELSE 0 END) AS ${escapeIdentifier(schemaName, "pgsql")};`};
    }

    getCreateTableQuery(table: string, headers: MetadataHeader): QueryInput[] {
            return PostgresTableQueryBuilder.getCreateTableQuery(table, headers, this.getConfig());
        }
    
    async getAlterTableQuery(table: string, alterTableChangesOrOldHeaders: AlterTableChanges | MetadataHeader, newHeaders?: MetadataHeader): Promise<QueryInput[]> {
        let alterTableChanges: AlterTableChanges;
        let updatedMetaData: MetadataHeader
        // Staging temp tables are throwaway bulk-load intermediaries created via CREATE TABLE
        // AS SELECT (columns only, no keys). They need no primary key — reconciling one would
        // emit DROP/ADD PRIMARY KEY against a keyless table, which errors. Skip PK reconciliation
        // for staging tables (keyed off the staging-name prefix; the real target is untouched).
        const stagingPrefix = this.getConfig().stagingPrefix ?? "temp_staging__";
        const isStagingTable = table.startsWith(stagingPrefix);
        const alterPrimaryKey = (this.config.updatePrimaryKey ?? false) && !isStagingTable;

        if (isMetadataHeader(alterTableChangesOrOldHeaders)) {
                    // If old headers are provided in MetadataHeader format, compare them with newHeaders
                    if (!newHeaders) {
                        throw new Error("Missing new headers for ALTER TABLE query");
                    }
                    ({ changes: alterTableChanges, updatedMetaData }  = compareMetaData(alterTableChangesOrOldHeaders, newHeaders, this.getDialectConfig(), this.config.logger));
                    this.updateTableMetadata(table, updatedMetaData, "metaData")
                } else {
                    alterTableChanges = alterTableChangesOrOldHeaders as AlterTableChanges;
                }
        const queries: QueryInput[] = [];
        const schemaPrefix = this.getConfig().schema ? `${escapeIdentifier(this.getConfig().schema!, "pgsql")}.` : "";

        if (alterTableChanges.primaryKeyChanges.length > 0 && alterPrimaryKey) {
            queries.push(this.getDropPrimaryKeyQuery(table));
            queries.push({ query: "COMMIT;", params: [] });
            queries.push({ query: "BEGIN;", params: [] });
        }

        // ✅ Only fetch unique indexes if there are columns to remove uniqueness from
        let indexesToDrop: string[] = [];
        if (alterTableChanges.noLongerUnique.length > 0) {
            // Extract the results from the QueryResult response
            const uniqueIndexesResult = await this.runQuery(this.getUniqueIndexesQuery(table));
        
            if (!uniqueIndexesResult.success || !uniqueIndexesResult.results) {
                throw new Error(`Failed to fetch unique indexes for table ${table}: ${uniqueIndexesResult.error}`);
            }
        
            const uniqueIndexes = (uniqueIndexesResult.results || [])
                .map(row => normalizeResultKeys(row))
                .filter(row => row.columns);
        
            const indexesToDrop = uniqueIndexes
                .filter(({ columns }) => columns.split(", ").some((col: string) => alterTableChanges.noLongerUnique.includes(col)))
                .map(({ index_name }) => `DROP INDEX IF EXISTS ${escapeIdentifier(index_name, "pgsql")}`);
        
                if (indexesToDrop.length > 0) {
                    queries.push({
                        query: indexesToDrop.join("; ") + ";",
                        params: []
                    });
                }
        }
        // Get actual ALTER TABLE queries
        const alterQueries = PostgresTableQueryBuilder.getAlterTableQuery(table, alterTableChanges, this.getConfig().schema, this.getConfig());
        queries.push(...alterQueries);

        // Add New Primary Key (if changed and allowed)
        if (alterTableChanges.primaryKeyChanges.length > 0 && alterPrimaryKey) {
            queries.push({ query: "COMMIT;", params: [] });
            queries.push({ query: "BEGIN;", params: [] });
            queries.push(this.getAddPrimaryKeyQuery(table, alterTableChanges.primaryKeyChanges));
        }

        return queries;
    }

    getDropTableQuery(table: string): QueryInput {
        return PostgresTableQueryBuilder.getDropTableQuery(table, this.getConfig().schema);
    }

    getTableExistsQuery(schema: string, table: string): QueryInput {
        return PostgresTableQueryBuilder.getTableExistsQuery(schema, table);
    }

    getTableMetaDataQuery(schema: string, table: string): QueryInput {
        return PostgresTableQueryBuilder.getTableMetaDataQuery(schema, table);
    }

    getPrimaryKeysQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getPrimaryKeysQuery(table, this.getConfig().schema);
    }

    getForeignKeyConstraintsQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getForeignKeyConstraintsQuery(table, this.getConfig().schema);
    }

    getViewDependenciesQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getViewDependenciesQuery(table, this.getConfig().schema);
    }

    getDropPrimaryKeyQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getDropPrimaryKeyQuery(table, this.getConfig().schema);
    }

    getDropUniqueConstraintQuery(table: string, indexName: string): QueryInput {
        return PostgresIndexQueryBuilder.getDropUniqueConstraintQuery(table, indexName, this.getConfig().schema);
    }

    getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput {
        return PostgresIndexQueryBuilder.getAddPrimaryKeyQuery(table, primaryKeys, this.getConfig().schema);
    }

    getUniqueIndexesQuery(table: string, column_name?: string): QueryInput {
        return PostgresIndexQueryBuilder.getUniqueIndexesQuery(table, column_name, this.getConfig().schema);
    }

    getSplitTablesQuery(table: string): QueryInput {
        return PostgresTableQueryBuilder.getSplitTablesQuery(table, this.getConfig().schema);
    }

    getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, insertInput?: "UPDATE"|"INSERT"): QueryInput {
        return PostgresInsertQueryBuilder.getInsertStatementQuery(tableOrInput, data, metaData, this.getConfig(), insertInput)
    }

    getInsertFromStagingQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, insertInput?: "UPDATE"|"INSERT"): QueryInput {
        return PostgresInsertQueryBuilder.getInsertFromStagingQuery(tableOrInput, metaData, this.getConfig(), insertInput)
    }

    getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader): QueryInput {
        return PostgresInsertQueryBuilder.getInsertChangedRowsToHistoryQuery(tableOrInput, metaData, this.getConfig())
    }

    getCreateTempTableQuery(table: string, stagingPrefix?: string): QueryInput {
        return PostgresTableQueryBuilder.getCreateTempTableQuery(table, this.getConfig().schema, stagingPrefix)
    }

    getConstraintConflictQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] }, stagingPrefix?: string): QueryInput {
        return PostgresIndexQueryBuilder.generateConstraintConflictBreakdownQuery(table, structure, this.getConfig().schema, stagingPrefix)
    }
}