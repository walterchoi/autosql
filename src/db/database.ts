import { Pool } from "mysql2/promise";
import { Pool as PgPool } from "pg";

// Abstract database class to define common methods.

export abstract class Database {
    protected connection: Pool | PgPool | null = null;
    protected config: any;

    constructor(config: any) {
        this.config = config;
    }

    abstract establishConnection(): Promise<void>;
    abstract runQuery(query: string, params?: any[]): Promise<any>;

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

    async closeConnection(): Promise<void> {
        if (this.connection) {
            try {
                await this.connection.end(); // MySQL && PostgreSQL
            } catch (error) {
                console.error("Error closing database connection:", error);
            }
            this.connection = null;
        }
    }

    protected abstract getCreateSchemaQuery(schemaName: string): string;
    protected abstract getCheckSchemaQuery(schemaName: string | string[]): string;
}
