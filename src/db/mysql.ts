import type { ResultSetHeader, FieldPacket, PoolConnection, Pool } from "mysql2/promise";
import { Database } from "./database";
import { mysqlPermanentErrors } from './permanentErrors/mysql';
import { QueryInput, ColumnDefinition, DatabaseConfig, AlterTableChanges, InsertResult, MetadataHeader, isMetadataHeader, InsertInput } from "../config/types";
import { mysqlConfig } from "./config/mysqlConfig";
import { isValidSingleQuery } from './utils/validateQuery';
import { compareMetaData } from '../helpers/metadata';
import { MySQLTableQueryBuilder } from "./queryBuilders/mysql/tableBuilder";
import { MySQLIndexQueryBuilder } from "./queryBuilders/mysql/indexBuilder";
import { MySQLInsertQueryBuilder } from "./queryBuilders/mysql/insertBuilder";
import { AutoSQLHandler } from "./autosql";
const dialectConfig = mysqlConfig

export class MySQLDatabase extends Database {
    constructor(config: DatabaseConfig) {
        super(config);
        this.autoSQL = new AutoSQLHandler(this);
    }

    async establishDatabaseConnection(): Promise<void> {
        let mysql: any;
        try {
            mysql = require("mysql2/promise");
        } catch (err) {
            throw new Error("Missing required dependency 'mysql2'. Please install it to use MySQLDatabase.");
        }
        this.connection = mysql.createPool({
            host: this.config.host,
            user: this.config.user,
            password: this.config.password,
            database: this.config.database || this.config.schema,
            port: this.config.port || 3306,
            connectionLimit: 5,
            ...(this.config.sshStream ? { stream: this.config.sshStream } : {})
        });
    }

    public getMaxConnections(): number {
        return (this.connection as any)?.config?.connectionLimit ?? 5;
    }

    public getDialectConfig() {
        return dialectConfig;
    }

    protected async getPermanentErrors(): Promise<string[]> {
        return mysqlPermanentErrors;
    }

    async testQuery(queryOrParams: QueryInput): Promise<any> {

        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
    
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
            if(client) await client.query("ROLLBACK;");
            console.error("MySQL testQuery failed:", error);
            throw error;
        } finally {
            if (client) client.release();
        }
    }

    protected async executeQuery(query: string): Promise<any>;
    protected async executeQuery(QueryInput: QueryInput): Promise<any>;
    protected async executeQuery(queryOrParams: QueryInput): Promise<{ rows: any[]; affectedRows: number }> {
        if (!this.connection) {
            await this.establishConnection();
        }
    
        let client: PoolConnection | null = null;
        const query = typeof queryOrParams === "string" ? queryOrParams : queryOrParams.query;
        const params = typeof queryOrParams === "string" ? [] : queryOrParams.params || [];
    
        try {
            client = await (this.connection as Pool).getConnection();
            const [rowsOrResult, maybeHeader] = await client.query(query, params) as [any, ResultSetHeader | FieldPacket[]];
    
            const rows = Array.isArray(rowsOrResult) ? rowsOrResult : [];
            let affectedRows = 0;
    
            if (maybeHeader && typeof maybeHeader === 'object' && 'affectedRows' in maybeHeader) {
                affectedRows = maybeHeader.affectedRows;
            } else if (rows.length > 0) {
                affectedRows = rows.length;
            }
    
            return { rows, affectedRows };
        } catch (error) {
            if (client) await client.query("ROLLBACK;");
            throw error;
        } finally {
            if (client) client.release();
        }
    }    

    getCreateSchemaQuery(schemaName: string): QueryInput {
        return { query: `CREATE SCHEMA IF NOT EXISTS \`${schemaName}\`;` };
    }

    getCheckSchemaQuery(schemaName: string | string[]): QueryInput {
        if (Array.isArray(schemaName)) {
            return { query: `SELECT ${schemaName
                .map(
                    (db) =>
                        `(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${db}') THEN 1 ELSE 0 END) AS '${db}'`
                )
                .join(", ")};`};
        }
        return { query: `SELECT (CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${schemaName}') THEN 1 ELSE 0 END) AS '${schemaName}';`};
    }

    getCreateTableQuery(table: string, headers: MetadataHeader): QueryInput[] {
        return MySQLTableQueryBuilder.getCreateTableQuery(table, headers, this.config);
    }

    async getAlterTableQuery(table: string, alterTableChangesOrOldHeaders: AlterTableChanges | MetadataHeader, newHeaders?: MetadataHeader): Promise<QueryInput[]> {
        let alterTableChanges: AlterTableChanges;
        let updatedMetaData: MetadataHeader
        const alterPrimaryKey = this.config.updatePrimaryKey ?? false;
        if (isMetadataHeader(alterTableChangesOrOldHeaders)) {
            // If old headers are provided in MetadataHeader format, compare them with newHeaders
            if (!newHeaders) {
                throw new Error("Missing new headers for ALTER TABLE query");
            }
            ({ changes: alterTableChanges, updatedMetaData }  = compareMetaData(alterTableChangesOrOldHeaders, newHeaders, this.getDialectConfig()));
            this.updateTableMetadata(table, updatedMetaData, "metaData")
        } else {
            alterTableChanges = alterTableChangesOrOldHeaders as AlterTableChanges;
        }
        const queries: QueryInput[] = [];
        const schemaPrefix = this.config.schema ? `\`${this.config.schema}\`.` : "";
        
        if (alterTableChanges.primaryKeyChanges.length > 0 && alterPrimaryKey) {
            queries.push(this.getDropPrimaryKeyQuery(table));
        }

        let indexesToDrop: string[] = [];
        if (alterTableChanges.noLongerUnique.length > 0) {
            const uniqueIndexesResult = await this.runQuery(this.getUniqueIndexesQuery(table));
        
            if (!uniqueIndexesResult.success || !uniqueIndexesResult.results) {
                throw new Error(`Failed to fetch unique indexes for table ${table}: ${uniqueIndexesResult.error}`);
            }
        
            const uniqueIndexes = uniqueIndexesResult.results as { indexname: string; columns: string }[];
        
            indexesToDrop = uniqueIndexes
                .filter(({ columns }) => columns.split(", ").some(col => alterTableChanges.noLongerUnique.includes(col)))
                .map(({ indexname }) => `DROP INDEX \`${indexname}\``);
        
            if (indexesToDrop.length > 0) {
                queries.push({
                    query: `ALTER TABLE ${schemaPrefix}\`${table}\` ${indexesToDrop.join(", ")};`,
                    params: []
                });
            }
        }
        
        const alterQueries = MySQLTableQueryBuilder.getAlterTableQuery(table, alterTableChanges, this.config.schema, this.getConfig());
        queries.push(...alterQueries);
        if (alterTableChanges.primaryKeyChanges.length > 0 && alterPrimaryKey) {
            queries.push(this.getAddPrimaryKeyQuery(table, alterTableChanges.primaryKeyChanges));
        }
        return queries;
    }

    getDropTableQuery(table: string): QueryInput {
        return MySQLTableQueryBuilder.getDropTableQuery(table, this.config.schema);
    }

    getTableExistsQuery(schema: string, table: string): QueryInput {
        return MySQLTableQueryBuilder.getTableExistsQuery(schema, table);
    }

    getTableMetaDataQuery(schema: string, table: string): QueryInput {
        return MySQLTableQueryBuilder.getTableMetaDataQuery(schema, table);
    }

    getPrimaryKeysQuery(table: string): QueryInput {
        return MySQLIndexQueryBuilder.getPrimaryKeysQuery(table, this.config.schema);
    }

    getForeignKeyConstraintsQuery(table: string): QueryInput {
        return MySQLIndexQueryBuilder.getForeignKeyConstraintsQuery(table, this.config.schema);
    }

    getViewDependenciesQuery(table: string): QueryInput {
        return MySQLIndexQueryBuilder.getViewDependenciesQuery(table, this.config.schema);
    }

    getDropPrimaryKeyQuery(table: string): QueryInput {
        return MySQLIndexQueryBuilder.getDropPrimaryKeyQuery(table, this.config.schema);
    }

    getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput {
        return MySQLIndexQueryBuilder.getAddPrimaryKeyQuery(table, primaryKeys, this.config.schema);
    }

    getUniqueIndexesQuery(table: string, column_name?: string): QueryInput {
        return MySQLIndexQueryBuilder.getUniqueIndexesQuery(table, column_name, this.config.schema);
    }

    getSplitTablesQuery(table: string): QueryInput {
        return MySQLTableQueryBuilder.getSplitTablesQuery(table, this.config.schema);
    }

    getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader): QueryInput {
        return MySQLInsertQueryBuilder.getInsertStatementQuery(tableOrInput, data, metaData, this.getConfig())
    }
}