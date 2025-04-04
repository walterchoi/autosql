import type { Pool, PoolClient } from "pg";
import { Database } from "./database";
import { pgsqlPermanentErrors } from './permanentErrors/pgsql';
import { QueryInput, ColumnDefinition, DatabaseConfig, AlterTableChanges, InsertResult, MetadataHeader, isMetadataHeader, InsertInput, QueryResult } from "../config/types";
import { pgsqlConfig } from "./config/pgsqlConfig";
import { isValidSingleQuery } from './utils/validateQuery';
import { compareMetaData } from '../helpers/metadata';
import { PostgresTableQueryBuilder } from "./queryBuilders/pgsql/tableBuilder";
import { PostgresIndexQueryBuilder } from "./queryBuilders/pgsql/indexBuilder";
import { PostgresInsertQueryBuilder } from "./queryBuilders/pgsql/insertBuilder";
import { AutoSQLHandler } from "./autosql";
import { normalizeResultKeys } from "../helpers/utilities";
const dialectConfig = pgsqlConfig

export class PostgresDatabase extends Database {
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
            console.error("PostgreSQL testQuery failed:", error);
            throw error;
        } finally {
            if (client) client.release();
        }
    }    

    protected async executeQuery(query: string): Promise<any>;
    protected async executeQuery(QueryInput: QueryInput): Promise<any>;
    protected async executeQuery(queryOrParams: QueryInput): Promise<{ rows: any[]; affectedRows: number }> {
        if (!this.connection) {
            await this.establishConnection();
        }
    
        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
        const params = typeof queryOrParams === "string" ? [] : queryOrParams.params || [];
    
        let client: PoolClient | null = null;
        try {
            client = await (this.connection as Pool).connect();
            const result = await client.query(query, params);
    
            const rows = result.rows || [];
            const affectedRows = result.rowCount ?? rows.length ?? 0;
    
            return { rows, affectedRows };
        } catch (error) {
            if (client) await client.query("ROLLBACK;");
            throw error;
        } finally {
            if (client) client.release();
        }
    }    

    getCreateSchemaQuery(schemaName: string): QueryInput {
        return { query: `CREATE SCHEMA IF NOT EXISTS "${schemaName}";`};
    }

    getCheckSchemaQuery(schemaName: string | string[]): QueryInput {
        if (Array.isArray(schemaName)) {
            return { query: `SELECT ${schemaName
                .map(
                    (db) =>
                        `(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${db}') THEN 1 ELSE 0 END) AS "${db}"`
                )
                .join(", ")};`};
        }
        return { query: `SELECT (CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${schemaName}') THEN 1 ELSE 0 END) AS "${schemaName}";`};
    }

    getCreateTableQuery(table: string, headers: MetadataHeader): QueryInput[] {
            return PostgresTableQueryBuilder.getCreateTableQuery(table, headers, this.config);
        }
    
    async getAlterTableQuery(table: string, alterTableChangesOrOldHeaders: AlterTableChanges | MetadataHeader, newHeaders?: MetadataHeader): Promise<QueryInput[]> {
        let alterTableChanges: AlterTableChanges;
        let updatedMetaData: MetadataHeader
        const alterPrimaryKey = this.config.updatePrimaryKey ?? false;

        if (isMetadataHeader(alterTableChangesOrOldHeaders)) {
                    // If old headers are provided in MetadataHeader format, compare them with newHeaders
                    if (!newHeaders) {
                        throw new Error("Missing new headers for ALTER TABLE query");
                    }
                    ({ changes: alterTableChanges, updatedMetaData }  = compareMetaData(alterTableChangesOrOldHeaders, newHeaders, this.getDialectConfig()));
                    this.updateTableMetadata(table, updatedMetaData, "metaData")
                } else {
                    alterTableChanges = alterTableChangesOrOldHeaders as AlterTableChanges;
                }
        const queries: QueryInput[] = [];
        const schemaPrefix = this.config.schema ? `"${this.config.schema}".` : "";

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
                .map(({ index_name }) => `DROP INDEX IF EXISTS "${index_name}"`);
        
                if (indexesToDrop.length > 0) {
                    queries.push({
                        query: indexesToDrop.join("; ") + ";",
                        params: []
                    });
                }
        }
        // Get actual ALTER TABLE queries
        const alterQueries = PostgresTableQueryBuilder.getAlterTableQuery(table, alterTableChanges, this.config.schema, this.getConfig());
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
        return PostgresTableQueryBuilder.getDropTableQuery(table, this.config.schema);
    }

    getTableExistsQuery(schema: string, table: string): QueryInput {
        return PostgresTableQueryBuilder.getTableExistsQuery(schema, table);
    }

    getTableMetaDataQuery(schema: string, table: string): QueryInput {
        return PostgresTableQueryBuilder.getTableMetaDataQuery(schema, table);
    }

    getPrimaryKeysQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getPrimaryKeysQuery(table, this.config.schema);
    }

    getForeignKeyConstraintsQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getForeignKeyConstraintsQuery(table, this.config.schema);
    }

    getViewDependenciesQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getViewDependenciesQuery(table, this.config.schema);
    }

    getDropPrimaryKeyQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getDropPrimaryKeyQuery(table, this.config.schema);
    }

    getDropUniqueConstraintQuery(table: string, indexName: string): QueryInput {
        return PostgresIndexQueryBuilder.getDropUniqueConstraintQuery(table, indexName, this.config.schema);
    }

    getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput {
        return PostgresIndexQueryBuilder.getAddPrimaryKeyQuery(table, primaryKeys, this.config.schema);
    }

    getUniqueIndexesQuery(table: string, column_name?: string): QueryInput {
        return PostgresIndexQueryBuilder.getUniqueIndexesQuery(table, column_name, this.config.schema);
    }

    getSplitTablesQuery(table: string): QueryInput {
        return PostgresTableQueryBuilder.getSplitTablesQuery(table, this.config.schema);
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

    getCreateTempTableQuery(table: string): QueryInput {
        return PostgresTableQueryBuilder.getCreateTempTableQuery(table, this.config.schema)
    }

    getConstraintConflictQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] }): QueryInput {
        return PostgresIndexQueryBuilder.generateConstraintConflictBreakdownQuery(table, structure, this.config.schema)
    }
}