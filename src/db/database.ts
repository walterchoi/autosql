import { isValidSingleQuery } from './utils/validateQuery';
import { QueryInput, DatabaseConfig, DialectConfig, ColumnDefinition, AlterTableChanges, InsertResult, MetadataHeader, QueryResult, InsertInput } from '../config/types';
import { validateConfig, parseDatabaseMetaData } from '../helpers/utilities';
import { maxQueryAttempts } from "../config/defaults";
import { AutoSQLHandler } from "./autosql";
import { setSSH } from '../helpers/ssh';

// Abstract database class to define common methods.
export abstract class Database {
    protected connection: any | null = null;
    protected config: DatabaseConfig;
    public autoSQL!: AutoSQLHandler;
    startDate : Date = new Date();
    protected abstract getPermanentErrors(): Promise<string[]>;

    constructor(config: DatabaseConfig) {
        this.config = validateConfig(config);
    }

    public getConfig() {
        return this.config;
    }

    public updateTableMetadata(table: string, metaData: MetadataHeader, type: "metaData" | "existingMetaData" = "metaData"): void {
        if (!this.config[type]) {
            this.config[type] = {};
        }

        this.config[type][table] = {
            ...this.config[type][table],
            ...metaData
        };
    }

    public updateSchema(schema: string): void {
        this.config.schema = schema;
    }

    static create(config: DatabaseConfig): Database {
        const DIALECTS: Record<string, new (config: DatabaseConfig) => Database> = {
            mysql: MySQLDatabase,
            pgsql: PostgresDatabase
        };

        const dialect = config.sqlDialect?.toLowerCase();
        if (!DIALECTS[dialect]) {
            throw new Error(`Unsupported SQL dialect: ${dialect}`);
        }

        return new DIALECTS[dialect](config);
    }

    public getDialect() {
        return this.config.sqlDialect
    }

    public abstract getDialectConfig(): DialectConfig;

    public abstract establishDatabaseConnection(): Promise<void>;
    public abstract testQuery(queryOrParams: QueryInput): Promise<any>;
    protected abstract executeQuery(queryOrParams: QueryInput): Promise<any>;

    public async establishConnection(): Promise<void> {
        let attempts = 0;
        const maxAttempts = maxQueryAttempts || 3;

        while (attempts < maxAttempts) {
            try {
                if (this.config.sshConfig) {
                    const { stream, sshClient } = await setSSH(this.config.sshConfig);
                    this.config.sshStream = stream;
                    this.config.sshClient = sshClient;
                }
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

    // Test database connection.
    public async testConnection(): Promise<boolean> {
        try {
            const result = await this.runQuery({ query: "SELECT 1 AS solution;"});
            return !!result;
        } catch (error) {
            console.error("Test connection failed:", error);
            return false;
        }
    }

    // Check if schema(s) exist.
    public async checkSchemaExists(schemaName: string | string[]): Promise<Record<string, boolean>> {
        try {
            const QueryInput = this.getCheckSchemaQuery(schemaName);
            const result = await this.runQuery(QueryInput);
    
            if (!result.success || !result.results) {
                throw new Error(`Failed to check schema existence: ${result.error}`);
            }
    
            const resultsArray = result.results;
    
            if (Array.isArray(schemaName)) {
                return schemaName.reduce((acc, db) => {
                    acc[db] = resultsArray[0]?.[db] === 1;
                    return acc;
                }, {} as Record<string, boolean>);
            }
    
            return { [schemaName]: resultsArray[0]?.[schemaName] === 1 };
        } catch (error) {
            return { [schemaName.toString()]: false };
        }
    }    

    public async createSchema(schemaName: string): Promise<Record<string, boolean>> {
        try {
            const QueryInput = this.getCreateSchemaQuery(schemaName);
            const result = await this.runQuery(QueryInput);
            return { success: true };
        } catch (error) {
            throw new Error(`Failed to create schema: ${error}`);
        }
    }

    public createTableQuery(table: string, headers: MetadataHeader): QueryInput[] {
        if (!table || !headers || Object.keys(headers).length === 0) {
            throw new Error("Invalid table configuration: table name and headers are required.");
        }
    
        return this.getCreateTableQuery(table, headers);
    }

    public async alterTableQuery(table: string, oldHeaders: MetadataHeader, newHeaders: MetadataHeader): Promise<QueryInput[]> {
        if (!table || !oldHeaders || !newHeaders) {
            throw new Error("Invalid table configuration: table name and headers are required.");
        }
    
        return await this.getAlterTableQuery(table, oldHeaders, newHeaders);
    }

    public dropTableQuery(table: string): QueryInput {
        if (!table) {
            throw new Error("Invalid table configuration: table name is required.");
        }
    
        return this.getDropTableQuery(table);
    }
    

    // Begin transaction.
    public async startTransaction(): Promise<void> {
        await this.executeQuery("START TRANSACTION;");
    }

    // Commit transaction.
    public async commit(): Promise<void> {
        await this.executeQuery("COMMIT;");
    }

    // Rollback transaction.
    public async rollback(): Promise<void> {
        await this.executeQuery("ROLLBACK;");
    }

    public async closeConnection(): Promise<{ success: boolean; error?: string }> {
        if (!this.connection)  return { success: true };

        try {
            if ("end" in this.connection) {
                await this.connection.end(); // MySQL & PostgreSQL close method
            }
            if (this.config.sshClient) {
                this.config.sshClient.end();
            }
            return { success: true };
        } catch (error) {
            return { success: false, error: error instanceof Error ? error.message : "Unknown error" };
        } finally {
            this.connection = undefined;        
            this.config.sshClient = undefined;
            this.config.sshStream = undefined;
        }
    }
    
    public async runQuery(queryOrParams: QueryInput): Promise<QueryResult> {
        const results: any[] = [];
        const maxAttempts = maxQueryAttempts || 3;
        let attempts = 0;
        let _error: any;
        const start = new Date();
        let end: Date;
        let affectedRows: number | undefined = undefined;

        const QueryInput: QueryInput = typeof queryOrParams === "string" ? { query: queryOrParams, params: [] } : queryOrParams;

        if (!isValidSingleQuery(QueryInput.query)) {
            return { start, end: new Date(), duration: 0, success: false, error: "Multiple SQL statements detected. Use transactions instead." };
        }

        while (attempts < maxAttempts) {
            try {
                const { rows, affectedRows } = await this.executeQuery(QueryInput);

                end = new Date();
                return {
                    start,
                    end,
                    duration: end.getTime() - start.getTime(),
                    affectedRows,
                    success: true,
                    results: rows
                };

            } catch (error: any) {
                const permanentErrors = await this.getPermanentErrors();
                if (permanentErrors.includes(error.code)) {
                    end = new Date();
                    return { start, end, duration: end.getTime() - start.getTime(), success: false, error: error.message };
                }
    
                attempts++;
                if (attempts < maxAttempts) {
                    await new Promise(res => setTimeout(res, 1000)); // Wait before retry
                } else {
                    _error = error;
                }
            }
        }
    
        end = new Date();
        return { start, end, duration: end.getTime() - start.getTime(), success: false, error: _error?.message };
    }

    public async runTransaction(queriesOrStrings: QueryInput[]): Promise<QueryResult> {
        if (!this.connection) {
            await this.establishConnection();
        }
        let results: any[] = [];
        const maxAttempts = maxQueryAttempts || 3;
        let _error: any;
        const start = new Date();
        let end: Date;
        let totalAffectedRows = 0;
    
        // Convert string queries into QueryInput
        const queries = queriesOrStrings.map(query =>
            typeof query === "string" ? { query, params: [] } : query
        );
    
        try {
            await this.startTransaction();
    
            for (const QueryInput of queries) {
                let attempts = 0;
                while (attempts < maxAttempts) {
                    try {
                        const { rows, affectedRows } = await this.executeQuery(QueryInput);

                        results = results.concat(rows);
                        totalAffectedRows += affectedRows || 0;

                        break; // Exit retry loop on success
                    } catch (error: any) {
                        attempts++;
    
                        const permanentErrors = await this.getPermanentErrors();
                        if (permanentErrors.includes(error.code)) {
                            throw error; // Stop retrying for permanent errors
                        }
    
                        if (attempts >= maxAttempts) {
                            _error = error;
                        } else {
                            await new Promise(res => setTimeout(res, 1000)); // Wait before retry
                        }
                    }
                }
    
                if (_error) {
                    throw _error;
                }
            }
    
            await this.commit();
            end = new Date();
            return {
                start,
                end,
                duration: end.getTime() - start.getTime(),
                affectedRows: totalAffectedRows || results.length,
                success: true,
                results
            };
        } catch (error: any) {
            console.log(error)
            await this.rollback();
            end = new Date();
            return {
                start,
                end,
                duration: end.getTime() - start.getTime(),
                success: false,
                error: error.message
            };
        }
    }

    public async runTransactionsWithConcurrency(queryGroups: QueryInput[][]): Promise<QueryResult[]> {
        if (!this.connection) {
            await this.establishConnection();
        }
        const results: QueryResult[] = [];
        let running: Promise<void>[] = [];
        const poolSize = this.getMaxConnections()
      
        let index = 0;
      
        const runNext = async () => {
          const i = index++;
          if (i >= queryGroups.length) return;
      
          console.log(`🔹 Running transaction for group #${i + 1}`);
          try {
            const result = await this.runTransaction(queryGroups[i]);
            results[i] = result;
          } catch (error: any) {
            results[i] = {
              start: new Date(),
              end: new Date(),
              duration: 0,
              success: false,
              error: error?.message || "Unknown error"
            };
          }
      
          // Recursively trigger the next task when one finishes
          await runNext();
        };
      
        // Start the first N (poolSize) tasks
        for (let i = 0; i < Math.min(poolSize, queryGroups.length); i++) {
          running.push(runNext());
        }
      
        // Wait for all active tasks to complete
        await Promise.all(running);
      
        return results;
    }

    public async getTableMetaData(schema: string, table: string): Promise<MetadataHeader | null> {
        try {
            if (!this.connection) throw new Error("Database connection not established.");
    
            const existsQueryInput = this.getTableExistsQuery(schema, table);
            const exists = await this.runQuery(existsQueryInput);
            if (!exists) return null;
    
            const MetaQueryInput = this.getTableMetaDataQuery(schema, table);
            const result = await this.runQuery(MetaQueryInput);
            if (!result.success || !result.results) {
                throw new Error(`Failed to fetch metadata: ${result.error}`);
            }
    
            const parsedMetadata = parseDatabaseMetaData(result.results, this.getDialectConfig());
    
            // Ensure return type is always MetadataHeader | null
            if (!parsedMetadata) return null;
            if (typeof parsedMetadata === "object" && !Array.isArray(parsedMetadata)) {
                return parsedMetadata as MetadataHeader; // Single table case
            }
    
            throw new Error("Unexpected metadata format: Multiple tables returned for a single-table query.");
        } catch (error) {
            console.error("Error fetching table metadata:", error);
            return null;
        }
    }
    
    public abstract getCreateSchemaQuery(schemaName: string): QueryInput;
    public abstract getCheckSchemaQuery(schemaName: string | string[]): QueryInput;
    public abstract getCreateTableQuery(table: string, headers: MetadataHeader): QueryInput[]
    public abstract getAlterTableQuery(table: string, alterTableChangesOrOldHeaders: AlterTableChanges | MetadataHeader, newHeaders?: MetadataHeader): Promise<QueryInput[]>;
    public abstract getDropTableQuery(table: string): QueryInput;
    public abstract getPrimaryKeysQuery(table: string): QueryInput;
    public abstract getForeignKeyConstraintsQuery(table: string): QueryInput;
    public abstract getViewDependenciesQuery(table: string): QueryInput;
    public abstract getDropPrimaryKeyQuery(table: string): QueryInput;
    public abstract getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput;
    public abstract getUniqueIndexesQuery(table: string, column_name?: string): QueryInput;
    public abstract getTableExistsQuery(schema: string, table: string): QueryInput;
    public abstract getTableMetaDataQuery(schema: string, table: string): QueryInput;
    public abstract getSplitTablesQuery(table: string): QueryInput;
    public abstract getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader): QueryInput; 

    public abstract getMaxConnections(): number;
}

import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";

export { MySQLDatabase, PostgresDatabase };