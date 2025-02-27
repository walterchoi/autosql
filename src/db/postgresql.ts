import { Pool } from "pg";
import { Database, DatabaseConfig } from "./database";

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

    async runQuery(query: string, params?: any[]): Promise<any> {
        if (!this.connection) {
            await this.establishConnection();
        }
        const client = await (this.connection as Pool).connect();
        try {
            const result = await client.query(query, params);
            return result.rows;
        } catch (error) {
            throw error;
        } finally {
            client.release();
        }
    }

    protected getCreateSchemaQuery(schemaName: string): string {
        return `CREATE SCHEMA IF NOT EXISTS "${schemaName}";`;
    }

    protected getCheckSchemaQuery(schemaName: string | string[]): string {
        if (Array.isArray(schemaName)) {
            return `SELECT ${schemaName
                .map(
                    (db) =>
                        `(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${db}') THEN 1 ELSE 0 END) AS "${db}"`
                )
                .join(", ")};`;
        }
        return `SELECT (CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${schemaName}') THEN 1 ELSE 0 END) AS "${schemaName}";`;
    }
}
