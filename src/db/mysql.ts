import mysql, { Pool, PoolConnection } from "mysql2/promise";
import { Database, DatabaseConfig } from "./database";
import { mysqlPermanentErrors } from './permanentErrors/mysql';
import { ColumnDefinition } from "../config/types";
import { mysqlConfig } from "./config/mysql";
import { isValidSingleQuery } from './utils/validateQuery';
import { compareHeaders } from '../helpers/headers';
import { MySQLTableQueryBuilder } from "./queryBuilders/mysql/tableBuilder";
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

    public getDialectConfig() {
        return dialectConfig;
    }

    protected async getPermanentErrors(): Promise<string[]> {
        return mysqlPermanentErrors;
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
        return MySQLTableQueryBuilder.getCreateTableQuery(table, headers);
    }

    protected getAlterTableQuery(table: string, oldHeaders: { [column: string]: ColumnDefinition }[], newHeaders: { [column: string]: ColumnDefinition }[]): string[] {
        return MySQLTableQueryBuilder.getAlterTableQuery(table, oldHeaders, newHeaders);
    }

    protected getDropTableQuery(table: string): string {
        return MySQLTableQueryBuilder.getDropTableQuery(table);
    }
    
}