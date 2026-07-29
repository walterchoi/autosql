import type { ResultSetHeader, FieldPacket, PoolConnection, Pool } from "mysql2/promise";
import { Database } from "./database";
import { mysqlPermanentErrors } from './permanentErrors/mysql';
import { QueryInput, ColumnDefinition, DatabaseConfig, AlterTableChanges, InsertResult, MetadataHeader, InsertInput, QueryResult } from "../config/types";
import { normalizeResultKeys, isMetadataHeader } from "../helpers/utilities";
import { serializeRowsToCopyText } from "../helpers/bulkLoad";
import { mysqlConfig } from "./config/mysqlConfig";
import { isValidSingleQuery } from './utils/validateQuery';
import { escapeIdentifier, escapeLiteral } from './utils/escape';
import { compareMetaData } from '../helpers/metadata';
import { MySQLTableQueryBuilder } from "./queryBuilders/mysql/tableBuilder";
import { MySQLIndexQueryBuilder } from "./queryBuilders/mysql/indexBuilder";
import { MySQLInsertQueryBuilder } from "./queryBuilders/mysql/insertBuilder";
import { AutoSQLHandler } from "./autosql";
import { SchemaLockTimeoutError } from "../errors";
const dialectConfig = mysqlConfig

export class MySQLDatabase extends Database {
    private schemaLockConnections: Map<string, PoolConnection> = new Map();

    constructor(config: DatabaseConfig) {
        super(config);
        this.autoSQLHandler = new AutoSQLHandler(this);
    }

    async establishDatabaseConnection(): Promise<void> {
        let mysql: any;
        try {
            mysql = require("mysql2/promise");
        } catch (err) {
            throw new Error("Missing required dependency 'mysql2'. Please install it to use MySQLDatabase.");
        }
        this.connection = mysql.createPool({
            host: this.config.host,
            user: this.config.user,
            password: this.config.password,
            database: this.config.database || this.config.schema,
            port: this.config.port || 3306,
            connectionLimit: this.config.connectionLimit || 5,
            // Pin the connection charset to a 4-byte-capable encoding. Without this mysql2
            // negotiates its default (historically 3-byte utf8_general_ci), so a 4-byte
            // character (emoji, some CJK) throws `Incorrect string value: '\xF0\x9F...'` on
            // INSERT even when the target table is utf8mb4 — the bytes can't cross the wire.
            charset: this.config.charset || dialectConfig.charset,
            ...(this.config.sshStream ? { stream: this.config.sshStream } : {})
        });
    }

    public getMaxConnections(): number {
        // mysql2's promise wrapper keeps the pool config on the underlying `.pool` (not `.config`).
        const conn = this.connection as any;
        return conn?.pool?.config?.connectionLimit ?? conn?.config?.connectionLimit ?? 5;
    }

    public async bulkLoadRows(table: string, columns: string[], rows: any[][]): Promise<QueryResult> {
        const start = new Date();
        if (!this.connection) await this.establishConnection();
        if (rows.length === 0) {
            return { start, end: new Date(), duration: 0, success: true, affectedRows: 0 };
        }
        const { Readable } = require("stream");
        const schema = this.getConfig().schema;
        const schemaPrefix = schema ? `${escapeIdentifier(schema, "mysql")}.` : "";
        const colList = columns.map(c => escapeIdentifier(c, "mysql")).join(", ");
        const body = serializeRowsToCopyText(rows);
        const client = await (this.connection as Pool).getConnection();
        try {
            // The stream body matches serializeRowsToCopyText: TAB fields, newline rows, `\` escape,
            // `\N` NULL. CHARACTER SET utf8mb4 so 4-byte characters load intact.
            const sql =
                `LOAD DATA LOCAL INFILE 'autosql-bulk' INTO TABLE ${schemaPrefix}${escapeIdentifier(table, "mysql")} ` +
                `CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' (${colList})`;
            // mysql2 streams the infile as Buffers (it calls Buffer.copy), so yield a Buffer.
            await client.query({ sql, infileStreamFactory: () => Readable.from([Buffer.from(body, "utf8")]) } as any);
            const end = new Date();
            return { start, end, duration: end.getTime() - start.getTime(), success: true, affectedRows: rows.length };
        } finally {
            client.release();
        }
    }

    public async acquireSchemaLock(table: string, timeoutSeconds: number): Promise<void> {
        const lockKey = `autosql_schema__${table}`;
        if (!this.connection) await this.establishConnection();
        let client: PoolConnection | undefined;
        try {
            client = await (this.connection as Pool).getConnection();
            const [rows] = await client.query('SELECT GET_LOCK(?, ?) AS acquired', [lockKey, timeoutSeconds]) as [any[], any];
            const acquired = (rows as any[])[0]?.acquired;
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
        const lockKey = `autosql_schema__${table}`;
        const client = this.schemaLockConnections.get(table);
        if (!client) return;
        try {
            await client.query('SELECT RELEASE_LOCK(?)', [lockKey]);
        } finally {
            client.release();
            this.schemaLockConnections.delete(table);
        }
    }

    public getDialectConfig() {
        return dialectConfig;
    }

    protected async getPermanentErrors(): Promise<string[]> {
        return mysqlPermanentErrors;
    }

    async testQuery(queryOrParams: QueryInput): Promise<any> {

        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
    
        if (!isValidSingleQuery(query)) {
            throw new Error("Each query in the transaction must be a single statement.");
        }
    
        if (!this.connection) {
            await this.establishConnection();
        }

        let client: PoolConnection | null = null;
        try {
            client = await (this.connection as Pool).getConnection();
    
            // Use PREPARE to validate syntax without executing
            await client.query(`PREPARE stmt FROM ?`, [query]);
            await client.query(`DEALLOCATE PREPARE stmt`); // Cleanup
    
            return { success: true };
        } catch (error: any) {
            if(client) await client.query("ROLLBACK;");
            this.error(`MySQL testQuery failed: ${error}`);
            throw error;
        } finally {
            if (client) client.release();
        }
    }

    protected async acquireConnection(): Promise<PoolConnection> {
        if (!this.connection) {
            await this.establishConnection();
        }
        return await (this.connection as Pool).getConnection();
    }

    protected releaseConnection(client: PoolConnection): void {
        if (client) client.release();
    }

    protected async executeQuery(query: string, client?: PoolConnection): Promise<any>;
    protected async executeQuery(QueryInput: QueryInput, client?: PoolConnection): Promise<any>;
    protected async executeQuery(queryOrParams: QueryInput, client?: PoolConnection): Promise<{ rows: any[]; affectedRows: number }> {
        if (!this.connection) {
            await this.establishConnection();
        }

        const pinned = !!client;
        let conn: PoolConnection | null = client ?? null;
        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
        const params = typeof queryOrParams === "string" ? [] : queryOrParams.params || [];

        try {
            if (!conn) conn = await (this.connection as Pool).getConnection();
            const [rowsOrResult, maybeHeader] = await conn.query(query, params) as [any, ResultSetHeader | FieldPacket[]];

            const rows = Array.isArray(rowsOrResult) ? rowsOrResult : [];
            let affectedRows = 0;

            // For INSERT/UPDATE/DELETE, mysql2 returns a ResultSetHeader as rowsOrResult (not an array).
            // For SELECT, rowsOrResult is the rows array and maybeHeader is FieldPacket[].
            if (!Array.isArray(rowsOrResult) && rowsOrResult != null && 'affectedRows' in rowsOrResult) {
                affectedRows = (rowsOrResult as ResultSetHeader).affectedRows;
            } else if (maybeHeader && !Array.isArray(maybeHeader) && 'affectedRows' in maybeHeader) {
                affectedRows = (maybeHeader as ResultSetHeader).affectedRows;
            } else if (rows.length > 0) {
                affectedRows = rows.length;
            }

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
        return { query: `CREATE SCHEMA IF NOT EXISTS ${escapeIdentifier(schemaName, "mysql")};` };
    }

    getCheckSchemaQuery(schemaName: string | string[]): QueryInput {
        if (Array.isArray(schemaName)) {
            return { query: `SELECT ${schemaName
                .map(
                    (db) =>
                        `(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ${escapeLiteral(db, "mysql")}) THEN 1 ELSE 0 END) AS ${escapeIdentifier(db, "mysql")}`
                )
                .join(", ")};`};
        }
        return { query: `SELECT (CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ${escapeLiteral(schemaName, "mysql")}) THEN 1 ELSE 0 END) AS ${escapeIdentifier(schemaName, "mysql")};`};
    }

    getCreateTableQuery(table: string, headers: MetadataHeader): QueryInput[] {
        return MySQLTableQueryBuilder.getCreateTableQuery(table, headers, this.getConfig());
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
        const schemaPrefix = this.getConfig().schema ? `${escapeIdentifier(this.getConfig().schema!, "mysql")}.` : "";

        // R9: surface schema changes that are computed but blocked by the safe-default flags.
        if (!isStagingTable) this.warnBlockedSchemaChanges(table, alterTableChanges);

        if (alterTableChanges.primaryKeyChanges.length > 0 && alterPrimaryKey) {
            queries.push(this.getDropPrimaryKeyQuery(table));
        }

        let indexesToDrop: string[] = [];
        if (alterTableChanges.noLongerUnique.length > 0) {
            const uniqueIndexesResult = await this.runQuery(this.getUniqueIndexesQuery(table));
        
            if (!uniqueIndexesResult.success || !uniqueIndexesResult.results) {
                throw new Error(`Failed to fetch unique indexes for table ${table}: ${uniqueIndexesResult.error}`);
            }
        
            const uniqueIndexes = (uniqueIndexesResult.results || [])
                .map(row => normalizeResultKeys(row))
                .filter(row => row.columns);
        
            const indexesToDrop = uniqueIndexes
                .filter(({ columns }) => columns.split(", ").some((col: string) => alterTableChanges.noLongerUnique.includes(col)))
                .map(({ index_name }) => `DROP INDEX ${escapeIdentifier(index_name, "mysql")}`);
        
            if (indexesToDrop.length > 0) {
                queries.push({
                    query: `ALTER TABLE ${schemaPrefix}${escapeIdentifier(table, "mysql")} ${indexesToDrop.join(", ")};`,
                    params: []
                });
            }
        }
        
        const alterQueries = MySQLTableQueryBuilder.getAlterTableQuery(table, alterTableChanges, this.getConfig().schema, this.getConfig());
        queries.push(...alterQueries);
        if (alterTableChanges.primaryKeyChanges.length > 0 && alterPrimaryKey) {
            queries.push(this.getAddPrimaryKeyQuery(table, alterTableChanges.primaryKeyChanges));
        }
        return queries;
    }

    /**
     * R8: detect text columns on a pre-existing table whose charset differs from the target
     * (`charset`, default utf8mb4) and, if any, return one `CONVERT TO CHARACTER SET` statement.
     * Numeric/date columns have a NULL `CHARACTER_SET_NAME` and are ignored. Convergent: once every
     * text column is the target charset the detect finds nothing and this returns `[]`.
     */
    public async getCharsetUpgradeQueries(table: string): Promise<QueryInput[]> {
        const targetCharset = this.config.charset || dialectConfig.charset;
        const targetCollate = this.config.collate || dialectConfig.collate;
        // charset/collate are config-derived identifiers, not row data — validate before
        // interpolating (they can't be parameter-bound in a CONVERT clause).
        const idOk = /^[A-Za-z0-9_]+$/;
        if (!idOk.test(targetCharset) || !idOk.test(targetCollate)) {
            throw new Error(`Invalid charset/collate for upgrade: ${JSON.stringify({ targetCharset, targetCollate })}`);
        }
        const schema = this.getConfig().schema;
        const detect: QueryInput = {
            query:
                `SELECT COUNT(*) AS c FROM information_schema.COLUMNS ` +
                `WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ` +
                `AND CHARACTER_SET_NAME IS NOT NULL AND CHARACTER_SET_NAME <> ?`,
            params: [schema, table, targetCharset],
        };
        const res = await this.runQuery(detect);
        const count = Number(Object.values(res.results?.[0] ?? { c: 0 })[0] ?? 0);
        if (!count) return [];
        const schemaPrefix = schema ? `${escapeIdentifier(schema, "mysql")}.` : "";
        return [{
            query: `ALTER TABLE ${schemaPrefix}${escapeIdentifier(table, "mysql")} CONVERT TO CHARACTER SET ${targetCharset} COLLATE ${targetCollate};`,
            params: [],
        }];
    }

    getDropTableQuery(table: string): QueryInput {
        return MySQLTableQueryBuilder.getDropTableQuery(table, this.getConfig().schema);
    }

    getTableExistsQuery(schema: string, table: string): QueryInput {
        return MySQLTableQueryBuilder.getTableExistsQuery(schema, table);
    }

    getTableMetaDataQuery(schema: string, table: string): QueryInput {
        return MySQLTableQueryBuilder.getTableMetaDataQuery(schema, table);
    }

    getPrimaryKeysQuery(table: string): QueryInput {
        return MySQLIndexQueryBuilder.getPrimaryKeysQuery(table, this.getConfig().schema);
    }

    getForeignKeyConstraintsQuery(table: string): QueryInput {
        return MySQLIndexQueryBuilder.getForeignKeyConstraintsQuery(table, this.getConfig().schema);
    }

    getViewDependenciesQuery(table: string): QueryInput {
        return MySQLIndexQueryBuilder.getViewDependenciesQuery(table, this.getConfig().schema);
    }

    getDropPrimaryKeyQuery(table: string): QueryInput {
        return MySQLIndexQueryBuilder.getDropPrimaryKeyQuery(table, this.getConfig().schema);
    }

    getDropUniqueConstraintQuery(table: string, indexName: string): QueryInput {
        return MySQLIndexQueryBuilder.getDropUniqueConstraintQuery(table, indexName, this.getConfig().schema);
    }

    getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput {
        return MySQLIndexQueryBuilder.getAddPrimaryKeyQuery(table, primaryKeys, this.getConfig().schema);
    }

    getUniqueIndexesQuery(table: string, column_name?: string): QueryInput {
        return MySQLIndexQueryBuilder.getUniqueIndexesQuery(table, column_name, this.getConfig().schema);
    }

    getSplitTablesQuery(table: string): QueryInput {
        return MySQLTableQueryBuilder.getSplitTablesQuery(table, this.getConfig().schema);
    }

    getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, insertInput?: "UPDATE"|"INSERT"): QueryInput {
        return MySQLInsertQueryBuilder.getInsertStatementQuery(tableOrInput, data, metaData, this.getConfig(), insertInput)
    }

    getInsertFromStagingQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, insertInput?: "UPDATE"|"INSERT"): QueryInput {
        return MySQLInsertQueryBuilder.getInsertFromStagingQuery(tableOrInput, metaData, this.getConfig(), insertInput)
    }

    getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader): QueryInput {
        return MySQLInsertQueryBuilder.getInsertChangedRowsToHistoryQuery(tableOrInput, metaData, this.getConfig())
    }

    getCreateTempTableQuery(table: string, stagingPrefix?: string): QueryInput {
        return MySQLTableQueryBuilder.getCreateTempTableQuery(table, this.getConfig().schema, stagingPrefix)
    }

    getConstraintConflictQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] }, stagingPrefix?: string): QueryInput {
        return MySQLIndexQueryBuilder.generateConstraintConflictBreakdownQuery(table, structure, this.getConfig().schema, stagingPrefix)
    }
}