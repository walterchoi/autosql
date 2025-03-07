import { Pool, PoolClient } from "pg";
import { Database } from "./database";
import { pgsqlPermanentErrors } from './permanentErrors/pgsql';
import { QueryInput, ColumnDefinition, DatabaseConfig, AlterTableChanges } from "../config/types";
import { pgsqlConfig } from "./config/pgsql";
import { isValidSingleQuery } from './utils/validateQuery';
import { compareHeaders } from '../helpers/headers';
import { PostgresTableQueryBuilder } from "./queryBuilders/pgsql/tableBuilder";
import { PostgresIndexQueryBuilder } from "./queryBuilders/pgsql/indexBuilder";
const dialectConfig = pgsqlConfig

export class PostgresDatabase extends Database {
    constructor(config: DatabaseConfig) {
        super(config); // Ensure constructor calls `super()`
    }

    async establishDatabaseConnection(): Promise<void> {
        this.connection = new Pool({
            host: this.config.host,
            user: this.config.user,
            password: this.config.password,
            database: this.config.database,
            port: this.config.port || 5432,
            max: 3
        });
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
            console.error("PostgreSQL testQuery failed:", error);
            throw error;
        } finally {
            if (client) client.release();
        }
    }    

    protected async executeQuery(query: string): Promise<any>;
    protected async executeQuery(QueryInput: QueryInput): Promise<any>;
    protected async executeQuery(queryOrParams: QueryInput): Promise<any> {
        if (!this.connection) {
            await this.establishConnection();
        }
    
        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
        const params = typeof queryOrParams === "string" ? [] : queryOrParams.params || [];
    
        let client: PoolClient | null = null;
        try {
            client = await (this.connection as Pool).connect();
            const result = await client.query(query, params);
            return result.rows;
        } catch (error) {
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

    getCreateTableQuery(table: string, headers: { [column: string]: ColumnDefinition }[]): QueryInput[] {
            return PostgresTableQueryBuilder.getCreateTableQuery(table, headers);
        }
    
    getAlterTableQuery(table: string, alterTableChangesOrOldHeaders: AlterTableChanges | { [column: string]: ColumnDefinition }[], newHeaders?: { [column: string]: ColumnDefinition }[]): QueryInput[] {
        let alterTableChanges: AlterTableChanges;
        if (Array.isArray(alterTableChangesOrOldHeaders) && newHeaders) {
            alterTableChanges = compareHeaders(alterTableChangesOrOldHeaders, newHeaders);
        } else if (Array.isArray(alterTableChangesOrOldHeaders)) {
            throw new Error("Missing new headers for ALTER TABLE query");
        } else {
            alterTableChanges = alterTableChangesOrOldHeaders;
        }
        return PostgresTableQueryBuilder.getAlterTableQuery(table, alterTableChanges);
    }

    getDropTableQuery(table: string): QueryInput {
        return PostgresTableQueryBuilder.getDropTableQuery(table);
    }

    getPrimaryKeysQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getPrimaryKeysQuery(table);
    }

    getForeignKeyConstraintsQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getForeignKeyConstraintsQuery(table);
    }

    getViewDependenciesQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getViewDependenciesQuery(table);
    }

    getDropPrimaryKeyQuery(table: string): QueryInput {
        return PostgresIndexQueryBuilder.getDropPrimaryKeyQuery(table);
    }

    getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput {
        return PostgresIndexQueryBuilder.getAddPrimaryKeyQuery(table, primaryKeys);
    }
}
