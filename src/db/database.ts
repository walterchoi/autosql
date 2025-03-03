import { Pool } from "mysql2/promise";
import { Pool as PgPool } from "pg";
import { isValidSingleQuery } from './utils/validateQuery';
import { ColumnDefinition } from '../config/types';

export interface DatabaseConfig {
    sql_dialect: string;
    host?: string;
    user?: string;
    password?: string;
    database?: string;
    port?: number;
    table?: string;
    headers?: ColumnDefinition[]
}

// Abstract database class to define common methods.
export abstract class Database {
    protected connection: Pool | PgPool | null = null;
    protected config: DatabaseConfig;
    protected abstract getPermanentErrors(): Promise<string[]>;

    constructor(config: DatabaseConfig) {
        this.config = config;
    }

    static create(config: DatabaseConfig): Database {
        const DIALECTS: Record<string, new (config: DatabaseConfig) => Database> = {
            mysql: MySQLDatabase,
            pgsql: PostgresDatabase
        };

        const dialect = config.sql_dialect?.toLowerCase();
        if (!DIALECTS[dialect]) {
            throw new Error(`Unsupported SQL dialect: ${dialect}`);
        }

        return new DIALECTS[dialect](config);
    }

    public getDialect() {
        return this.config.sql_dialect
    }

    public abstract getDialectConfig(): DialectConfig;

    abstract establishDatabaseConnection(): Promise<void>;
    abstract testQuery(query: string): Promise<any>;
    protected abstract executeQuery(query: string, params?: any[]): Promise<any>;

    async establishConnection(): Promise<void> {
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
    
    async runQuery(query: string, params: any[] = []): Promise<any> {
        let attempts = 0;
        const maxAttempts = 3;
        let _error
        if (!isValidSingleQuery(query)) {
            throw new Error("Multiple SQL statements detected. Use transactions instead.");
        }
        while (attempts < maxAttempts && !_error) {
            try {
                return await this.executeQuery(query, params);
            } catch (error: any) {
                const permanentErrors = await this.getPermanentErrors();
                if (permanentErrors.includes(error.code)) {
                    _error = error
                }

                attempts++;
                if (attempts < maxAttempts) {
                    await new Promise((res) => setTimeout(res, 1000)); // Wait before retrying
                } else {
                    _error = error
                }
            }
        }

        throw _error
    }

    // Test database connection.
    async testConnection(): Promise<boolean> {
        try {
            const result = await this.runQuery("SELECT 1 AS solution;");
            return !!result;
        } catch (error) {
            console.error("Test connection failed:", error);
            return false;
        }
    }

    // Check if schema(s) exist.
    async checkSchemaExists(schemaName: string | string[]): Promise<Record<string, boolean>> {
        try {
            const query = this.getCheckSchemaQuery(schemaName);
            const result = await this.runQuery(query);

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

    async createSchema(schemaName: string): Promise<{ success: boolean; error?: string }> {
        try {
            const query = this.getCreateSchemaQuery(schemaName);
            await this.runQuery(query);
            return { success: true };
        } catch (error) {
            return { success: false, error: error instanceof Error ? error.message : "Unknown error" };
        }
    }

    createTableQuery(table: string, headers: { [column: string]: ColumnDefinition }[]): string[] {
        if (!table || !headers || headers.length === 0) {
          throw new Error("Invalid table configuration: table name and headers are required.");
        }
    
        return this.getCreateTableQuery(table, headers);
    }

    alterTableQuery(table: string, oldHeaders: { [column: string]: ColumnDefinition }[], newHeaders: { [column: string]: ColumnDefinition }[]): string[] {
        if (!table || !oldHeaders || !newHeaders) {
            throw new Error("Invalid table configuration: table name and headers are required.");
        }
    
        return this.getAlterTableQuery(table, oldHeaders, newHeaders);
    }

    dropTableQuery(table: string): string {
        if (!table) {
            throw new Error("Invalid table configuration: table name is required.");
        }
    
        return this.getDropTableQuery(table);
    }
    

    // Begin transaction.
    async startTransaction(): Promise<void> {
        await this.executeQuery("START TRANSACTION;");
    }

    // Commit transaction.
    async commit(): Promise<void> {
        await this.executeQuery("COMMIT;");
    }

    // Rollback transaction.
    async rollback(): Promise<void> {
        await this.executeQuery("ROLLBACK;");
    }

    async closeConnection(): Promise<{ success: boolean; error?: string }> {
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

    async runTransaction(queries: string[]): Promise<{ success: boolean; results?: any[]; error?: string }> {
        if (!this.connection) {
            await this.establishConnection();
        }
    
        let results: any[] = [];
        let attempts: number;
        const maxAttempts = 3;
    
        try {
            await this.startTransaction(); // Begin transaction
            let _error;
            for (const query of queries) {
                if (!isValidSingleQuery(query)) {
                    _error = "Each query in the transaction must be a single statement."
                    throw new Error("Each query in the transaction must be a single statement.");
                }
                attempts = 0;
                if(_error) {
                    console.log(`Not running query: ${query}`)
                }
                while (attempts < maxAttempts && !_error) {
                    try {
                        const result = await this.executeQuery(query);
                        results.push(result);
                        break; // Query succeeded, move to next query
                    } catch (error: any) {
                        attempts++;
    
                        // Check if it's a permanent error
                        const permanentErrors = await this.getPermanentErrors();
                        if (permanentErrors.includes(error.code)) {
                            _error = error
                        }
    
                        // If max retries reached, rollback and stop
                        if (attempts >= maxAttempts) {
                            _error = error
                        }
    
                        await new Promise((res) => setTimeout(res, 1000)); // Wait before retrying
                    }
                }
            }
            if(_error) {
                throw _error
            }
            await this.commit(); // Commit only if all queries succeed
            return { success: true, results };
        } catch (error: any) {
            await this.rollback();
            return { success: false, error: error.message };
        }
    }

    protected abstract getCreateSchemaQuery(schemaName: string): string;
    protected abstract getCheckSchemaQuery(schemaName: string | string[]): string;
    protected abstract getCreateTableQuery(table: string, headers: { [column: string]: ColumnDefinition }[]): string[]
    protected abstract getAlterTableQuery(table: string, oldHeaders: { [column: string]: ColumnDefinition }[], newHeaders: { [column: string]: ColumnDefinition }[]): string[]
    protected abstract getDropTableQuery(table: string): string;
}

import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { DialectConfig } from "./config/interfaces";

export { MySQLDatabase, PostgresDatabase };