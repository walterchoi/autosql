import mysql, { Pool, PoolConnection } from "mysql2/promise";
import { Database, DatabaseConfig } from "./database";
import { mysqlPermanentErrors } from './permanentErrors/mysql';
import { ColumnDefinition } from "../config/types";
import { mysqlConfig } from "./config/mysql";
import { isValidSingleQuery } from './utils/validateQuery';
const dialectConfig = mysqlConfig

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

    protected async getPermanentErrors(): Promise<string[]> {
        return mysqlPermanentErrors;
    }

    public getDialectConfig() {
        return dialectConfig;
    }

    async testQuery(query: string): Promise<any> {
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
            console.error("MySQL testQuery failed:", error);
            throw error;
        } finally {
            if (client) client.release();
        }
    }

    protected async executeQuery(query: string, params: any[] = []): Promise<any> {
        if (!this.connection) {
            await this.establishConnection();
        }

        let client: PoolConnection | null = null;
        try {
            client = await (this.connection as Pool).getConnection();
            const [rows] = await client.query(query, params);
            return rows;
        } catch (error) {
            throw error;
        } finally {
            if (client) client.release();
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

    protected getCreateTableQuery(table: string, headers: { [column: string]: ColumnDefinition }[]): string[] {
        let sqlQueries: string[] = [];
        let sqlQuery = `CREATE TABLE IF NOT EXISTS \`${table}\` (\n`;
        let primaryKeys: string[] = [];
        let uniqueKeys: string[] = [];
        let indexes: string[] = [];
    
        for (const columnObj of headers) {
            const columnName = Object.keys(columnObj)[0]; // Extract column name
            const column = columnObj[columnName];
    
            if (!column.type) throw new Error(`Missing type for column ${columnName}`);
    
            let columnType = column.type.toLowerCase();
            if (dialectConfig.translate.local_to_server[columnType]) {
                columnType = dialectConfig.translate.local_to_server[columnType];
            }
    
            let columnDef = `\`${columnName}\` ${columnType}`;
    
            // Handle column lengths
            if (column.length && dialectConfig.require_length.includes(columnType)) {
                columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
            }
    
            // Convert BOOLEAN → TINYINT(1) for MySQL
            if (column.type === "boolean") {
                columnDef = `\`${columnName}\` TINYINT(1)`;
            }
    
            // Apply AUTO_INCREMENT only to valid integer types
            if (column.autoIncrement) {
                if (["int", "bigint", "smallint", "tinyint"].includes(columnType)) {
                    columnDef += " AUTO_INCREMENT";
                } else {
                    throw new Error(`AUTO_INCREMENT is not supported on type ${columnType} in MySQL`);
                }
            }
    
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) {
                const replacement = dialectConfig.default_translation[column.default];
                columnDef += ` DEFAULT ${replacement ? replacement : column.default}`;
            }
    
            if (column.primary) primaryKeys.push(`\`${columnName}\``);
            if (column.unique) uniqueKeys.push(`\`${columnName}\``);
            if (column.index) indexes.push(`\`${columnName}\``);
    
            sqlQuery += `${columnDef},\n`;
        }
    
        if (primaryKeys.length) sqlQuery += `PRIMARY KEY (${primaryKeys.join(", ")}),\n`;
        if (uniqueKeys.length) sqlQuery += `${uniqueKeys.map((key) => `UNIQUE(${key})`).join(", ")},\n`;
    
        sqlQuery = sqlQuery.slice(0, -2) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;";
        sqlQueries.push(sqlQuery); // Store CREATE TABLE query as first item
    
        // Create indexes separately
        for (const index of indexes) {
            sqlQueries.push(`CREATE INDEX \`${index.replace(/`/g, "")}_idx\` ON \`${table}\` (\`${index.replace(/`/g, "")}\`);`);
        }
    
        return sqlQueries;
    }    
     
}