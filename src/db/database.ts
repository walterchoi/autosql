import { Pool } from "mysql2/promise";
import { Pool as PgPool } from "pg";
import { isValidSingleQuery } from './utils/validateQuery';
import { QueryInput, DatabaseConfig, DialectConfig, ColumnDefinition, AlterTableChanges, InsertResult, MetadataHeader } from '../config/types';
import { validateConfig, parseDatabaseMetaData } from '../helpers/utilities';

// Abstract database class to define common methods.
export abstract class Database {
    protected connection: Pool | PgPool | null = null;
    protected config: DatabaseConfig;
    startDate : Date = new Date();
    protected abstract getPermanentErrors(): Promise<string[]>;

    constructor(config: DatabaseConfig) {
        this.config = validateConfig(config);
    }

    public getConfig() {
        return this.config;
    }

    static create(config: DatabaseConfig): Database {
        const DIALECTS: Record<string, new (config: DatabaseConfig) => Database> = {
            mysql: MySQLDatabase,
            pgsql: PostgresDatabase
        };

        const dialect = config.sqlDialect?.toLowerCase();
        if (!DIALECTS[dialect]) {
            throw new Error(`Unsupported SQL dialect: ${dialect}`);
        }

        return new DIALECTS[dialect](config);
    }

    public getDialect() {
        return this.config.sqlDialect
    }

    public abstract getDialectConfig(): DialectConfig;

    public abstract establishDatabaseConnection(): Promise<void>;
    public abstract testQuery(queryOrParams: QueryInput): Promise<any>;
    protected abstract executeQuery(queryOrParams: QueryInput): Promise<any>;

    public async establishConnection(): Promise<void> {
        let attempts = 0;
        const maxAttempts = 3;

        while (attempts < maxAttempts) {
            try {
                await this.establishDatabaseConnection();
                return;
            } catch (error: any) {
                console.error(`Database connection attempt ${attempts + 1} failed:`, error.message);

                const permanentErrors = await this.getPermanentErrors();
                if (permanentErrors.includes(error.code)) {
                    console.error("Permanent error detected. Aborting retry.");
                    throw error;
                }

                attempts++;
                if (attempts < maxAttempts) {
                    await new Promise((res) => setTimeout(res, 1000)); // Wait 1 second before retrying
                }
            }
        }

        throw new Error("Database connection failed after multiple attempts.");
    }
    
    public async runQuery(queryOrParams: QueryInput): Promise<any> {
        let attempts = 0;
        const maxAttempts = 3;
        let _error;
    
        const QueryInput =
            typeof queryOrParams === "string"
                ? { query: queryOrParams, params: [] }
                : queryOrParams;
    
        if (!isValidSingleQuery(QueryInput.query)) {
            throw new Error("Multiple SQL statements detected. Use transactions instead.");
        }
    
        while (attempts < maxAttempts && !_error) {
            try {
                return await this.executeQuery(QueryInput);
            } catch (error: any) {
                const permanentErrors = await this.getPermanentErrors();
                if (permanentErrors.includes(error.code)) {
                    _error = error;
                }
    
                attempts++;
                if (attempts < maxAttempts) {
                    await new Promise((res) => setTimeout(res, 1000));
                } else {
                    _error = error;
                }
            }
        }
        throw _error;
    }

    // Test database connection.
    public async testConnection(): Promise<boolean> {
        try {
            const result = await this.runQuery({ query: "SELECT 1 AS solution;"});
            return !!result;
        } catch (error) {
            console.error("Test connection failed:", error);
            return false;
        }
    }

    // Check if schema(s) exist.
    public async checkSchemaExists(schemaName: string | string[]): Promise<Record<string, boolean>> {
        try {
            const QueryInput = this.getCheckSchemaQuery(schemaName);
            const result = await this.runQuery(QueryInput);

            if (Array.isArray(schemaName)) {
                return schemaName.reduce((acc, db) => {
                    acc[db] = result[0][db] === 1;
                    return acc;
                }, {} as Record<string, boolean>);
            }

            return { [schemaName]: result[0][schemaName] === 1 };
        } catch (error) {
            return { [schemaName.toString()]: false };
        }
    }

    public async createSchema(schemaName: string): Promise<Record<string, boolean>> {
        try {
            const QueryInput = this.getCreateSchemaQuery(schemaName);
            const result = await this.runQuery(QueryInput);
            return { success: true };
        } catch (error) {
            throw new Error(`Failed to create schema: ${error}`);
        }
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
    
        return await this.getAlterTableQuery(table, oldHeaders, newHeaders);
    }

    public dropTableQuery(table: string): QueryInput {
        if (!table) {
            throw new Error("Invalid table configuration: table name is required.");
        }
    
        return this.getDropTableQuery(table);
    }
    

    // Begin transaction.
    public async startTransaction(): Promise<void> {
        await this.executeQuery("START TRANSACTION;");
    }

    // Commit transaction.
    public async commit(): Promise<void> {
        await this.executeQuery("COMMIT;");
    }

    // Rollback transaction.
    public async rollback(): Promise<void> {
        await this.executeQuery("ROLLBACK;");
    }

    public async closeConnection(): Promise<{ success: boolean; error?: string }> {
        if (!this.connection)  return { success: true };

        try {
            if ("end" in this.connection) {
                await this.connection.end(); // MySQL & PostgreSQL close method
            }
            return { success: true };
        } catch (error) {
            return { success: false, error: error instanceof Error ? error.message : "Unknown error" };
        } finally {
            this.connection = null;
        }
    }

    public async runTransaction(queriesOrStrings: QueryInput[]): Promise<{ success: boolean; results?: any[]; error?: string }> {
        if (!this.connection) {
            await this.establishConnection();
        }
    
        let results: any[] = [];
        let attempts: number;
        const maxAttempts = 3;
    
        // Convert string queries into QueryInput
        const queries = queriesOrStrings.map(query =>
            typeof query === "string" ? { query, params: [] } : query
        );
    
        try {
            await this.startTransaction();
            let _error;
    
            for (const QueryInput of queries) {
                const query = QueryInput.query;
                const params = QueryInput.params || [];
    
                if (!isValidSingleQuery(query)) {
                    _error = "Each query in the transaction must be a single statement.";
                    throw new Error(_error);
                }
    
                attempts = 0;
                if (_error) {
                    console.log(`Not running query: ${query}`);
                    console.log(`Due to error: ${_error}`);
                }
    
                while (attempts < maxAttempts && !_error) {
                    try {
                        const result = await this.executeQuery(QueryInput);
                        results.push(result);
                        break;
                    } catch (error: any) {
                        attempts++;
    
                        const permanentErrors = await this.getPermanentErrors();
                        if (permanentErrors.includes(error.code)) {
                            _error = error;
                        }
    
                        if (attempts >= maxAttempts) {
                            _error = error;
                        }
    
                        await new Promise((res) => setTimeout(res, 1000));
                    }
                }
            }
    
            if (_error) {
                throw _error;
            }
    
            await this.commit();
            return { success: true, results };
        } catch (error: any) {
            await this.rollback();
            return { success: false, error: error.message };
        }
    }

    public async getTableMetaData(schema: string, table: string): Promise<MetadataHeader | null> {
        try {
            if (!this.connection) throw new Error("Database connection not established.");
    
            const existsQueryInput = this.getTableExistsQuery(schema, table);
            const exists = await this.runQuery(existsQueryInput);
            if (!exists) return null;

            const MetaQueryInput = this.getTableMetaDataQuery(schema, table);
            const result = await this.runQuery(MetaQueryInput);
            return parseDatabaseMetaData(result, this.getDialectConfig());
        } catch (error) {
            console.error("Error fetching table metadata:", error);
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
    public abstract getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput;
    public abstract getUniqueIndexesQuery(table: string, column_name?: string): QueryInput;
    public abstract autoSQL(table: string, data: Record<string, any>[]): Promise<InsertResult>;
    public abstract getTableExistsQuery(schema: string, table: string): QueryInput;
    public abstract getTableMetaDataQuery(schema: string, table: string): QueryInput;
}

import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";

export { MySQLDatabase, PostgresDatabase };