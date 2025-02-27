import mysql, { Pool, PoolConnection } from "mysql2/promise";
import { Database, DatabaseConfig } from "./database"; // Ensure correct import

export class MySQLDatabase extends Database {
    constructor(config: DatabaseConfig) {
        super(config); // Ensure constructor calls `super()`
    }

    async establishDatabaseConnection(): Promise<void> {
        this.connection = mysql.createPool({
            host: this.config.host,
            user: this.config.user,
            password: this.config.password,
            database: this.config.database,
            port: this.config.port || 3306,
            connectionLimit: 3
        });
    }

    async runQuery(query: string, params?: any[]): Promise<any> {
        if (!this.connection) {
            await this.establishConnection();
        }
        let client: PoolConnection | null = null;

        try {
            client = await (this.connection as Pool).getConnection(); // Get a connection from the pool
            const [rows] = await client.query(query, params);
            return rows;
        } catch (error) {
            throw error;
        } finally {
            if (client) client.release(); // Always release connection
        }
    }

    protected getCreateSchemaQuery(schemaName: string): string {
        return `CREATE SCHEMA IF NOT EXISTS \`${schemaName}\`;`;
    }

    protected getCheckSchemaQuery(schemaName: string | string[]): string {
        if (Array.isArray(schemaName)) {
            return `SELECT ${schemaName
                .map(
                    (db) =>
                        `(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${db}') THEN 1 ELSE 0 END) AS '${db}'`
                )
                .join(", ")};`;
        }
        return `SELECT (CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${schemaName}') THEN 1 ELSE 0 END) AS '${schemaName}';`;
    }
}