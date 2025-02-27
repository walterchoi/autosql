import { Pool } from "mysql2/promise";
import { Pool as PgPool } from "pg";

export interface DatabaseConfig {
    sql_dialect: string;
    host?: string;
    user?: string;
    password?: string;
    database?: string;
    port?: number;
}

// Abstract database class to define common methods.
export abstract class Database {
    protected connection: Pool | PgPool | null = null;
    protected config: DatabaseConfig;

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

    abstract establishDatabaseConnection(): Promise<void>;
    abstract runQuery(query: string, params?: any[]): Promise<any>;

    async establishConnection(): Promise<void> {
        try {
            await this.establishDatabaseConnection();
        } catch (error) {
            console.error("Database connection failed:", error instanceof Error ? error.message : String(error));
            await this.closeConnection(); // Ensure the connection is closed on failure
            throw error; // Re-throw so tests can handle it
        }
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
            console.error(`Error checking schema existence: ${error}`);
            return { [schemaName.toString()]: false };
        }
    }

    async createSchema(schemaName: string): Promise<{ success: boolean; error?: string }> {
        try {
            const query = this.getCreateSchemaQuery(schemaName);
            await this.runQuery(query);
            return { success: true };
        } catch (error) {
            console.error(`Error creating schema ${schemaName}:`, error);
            return { success: false, error: error instanceof Error ? error.message : "Unknown error" };
        }
    }

    // Begin transaction.
    async startTransaction(): Promise<void> {
        await this.runQuery("START TRANSACTION;");
    }

    // Commit transaction.
    async commit(): Promise<void> {
        await this.runQuery("COMMIT;");
    }

    // Rollback transaction.
    async rollback(): Promise<void> {
        await this.runQuery("ROLLBACK;");
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
    
        try {
            await this.startTransaction(); // Begin transaction
    
            for (const query of queries) {
                const result = await this.runQuery(query);
                results.push(result);
            }
    
            await this.commit(); // Commit if all queries succeed
            return { success: true, results };
        } catch (error) {
            console.error("Transaction failed, rolling back:", error);
            await this.rollback(); // Rollback on error
            return { success: false, error: error instanceof Error ? error.message : "Unknown error" };
        }
    }    

    protected abstract getCreateSchemaQuery(schemaName: string): string;
    protected abstract getCheckSchemaQuery(schemaName: string | string[]): string;
}

import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./postgresql";

export { MySQLDatabase, PostgresDatabase };