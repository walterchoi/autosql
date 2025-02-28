import { Pool, PoolClient } from "pg";
import { Database, DatabaseConfig } from "./database";
import { pgsqlPermanentErrors } from './permanentErrors/pgsql';
import { ColumnDefinition } from "../helpers/metadata";
import { pgsqlConfig } from "./config/pgsql";
import { isValidSingleQuery } from './validateQuery';
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

    protected async getPermanentErrors(): Promise<string[]> {
        return pgsqlPermanentErrors
    }

    async testQuery(query: string): Promise<any> {
        if (!isValidSingleQuery(query)) {
            throw new Error("Each query in the transaction must be a single statement.");
        }
        if (!this.connection) {
            await this.establishConnection();
        }
    
        let client: PoolClient | null = null;
        try {
            client = await (this.connection as Pool).connect();
    
            if (query.trim().toLowerCase().startsWith("select") ||
                query.trim().toLowerCase().startsWith("insert") ||
                query.trim().toLowerCase().startsWith("update") ||
                query.trim().toLowerCase().startsWith("delete")) {
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

    protected async executeQuery(query: string, params: any[] = []): Promise<any> {
        if (!this.connection) {
            await this.establishConnection();
        }

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

    protected getCreateTableQuery(table: string, headers: { [column: string]: ColumnDefinition }[]): string[] {
        let sqlQueries: string[] = [];
        let sqlQuery = `CREATE TABLE IF NOT EXISTS "${table}" (\n`;
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
    
            let columnDef = `"${columnName}" ${columnType}`;
    
            // Handle column lengths
            if (column.length && dialectConfig.require_length.includes(columnType)) {
                columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
            }
    
            // Use SERIAL for Auto-incrementing Primary Keys
            if (column.autoIncrement) {
                if (columnType === "int" || columnType === "bigint") {
                    columnDef = `"${columnName}" SERIAL`;
                } else {
                    throw new Error(`AUTO_INCREMENT (SERIAL) is not supported on type ${columnType} in PostgreSQL`);
                }
            }
    
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) {
                const replacement = dialectConfig.default_translation[column.default] || column.default;
                columnDef += ` DEFAULT ${replacement}`;
            }
    
            if (column.primary) primaryKeys.push(`"${columnName}"`);
            if (column.unique) uniqueKeys.push(`"${columnName}"`);
            if (column.index) indexes.push(`"${columnName}"`);
    
            sqlQuery += `${columnDef},\n`;
        }
    
        if (primaryKeys.length) sqlQuery += `PRIMARY KEY (${primaryKeys.join(", ")}),\n`;
        if (uniqueKeys.length) sqlQuery += `${uniqueKeys.map((key) => `UNIQUE(${key})`).join(", ")},\n`;
    
        sqlQuery = sqlQuery.slice(0, -2) + "\n);";
        sqlQueries.push(sqlQuery); // Store CREATE TABLE query as first item
    
        // Create indexes separately
        for (const index of indexes) {
            sqlQueries.push(`CREATE INDEX "${index.replace(/"/g, "")}_idx" ON "${table}" ("${index.replace(/"/g, "")}");`);
        }
    
        return sqlQueries;
    }    
    
}
