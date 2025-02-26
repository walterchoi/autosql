import mysql, { Pool } from "mysql2/promise";
import { Database } from "./database";

export class MySQLDatabase extends Database {
    async establishConnection(): Promise<void> {
        this.connection = await mysql.createPool({
            host: this.config.host,
            user: this.config.user,
            password: this.config.password,
            database: this.config.database,
            port: this.config.port || 3306,
            connectionLimit: 10
        });
    }

    async runQuery(query: string, params?: any[]): Promise<any> {
        if (!this.connection) await this.establishConnection();

        // Use `.query()` instead of `.execute()` for transactions
        if (query.startsWith("START TRANSACTION") || query.startsWith("COMMIT") || query.startsWith("ROLLBACK")) {
            const [rows] = await (this.connection as Pool).query(query, params);
            return rows;
        }

        const [rows] = await (this.connection as Pool).execute(query, params);
        return rows;
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
