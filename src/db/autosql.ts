import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { Database } from "./database";
import { InsertResult, MetadataHeader, AlterTableChanges, metaDataInterim, QueryResult } from "../config/types";
import { getMetaData, compareMetaData } from "../helpers/metadata";
import { parseDatabaseMetaData, tableChangesExist, isMetaDataHeader, estimateRowSize, isValidDataFormat } from "../helpers/utilities";
import { ensureTimestamps } from "../helpers/timestamps";

export class AutoSQLHandler {
    private db: Database;

    constructor(dbInstance: MySQLDatabase | PostgresDatabase) {
        this.db = dbInstance;
    }

    async autoCreateTable(table: string, newMetaData: MetadataHeader, tableExists?: boolean): Promise<QueryResult> {
        try {
            // ✅ Skip table existence check if already known
            if (tableExists === undefined) {
                const checkTableExistsQuery = this.db.getTableExistsQuery(this.db.getConfig().schema || this.db.getConfig().database || "", table);
                const checkTableExists = await this.db.runQuery(checkTableExistsQuery);
                if (!checkTableExists.success || !checkTableExists.results) {
                    throw new Error(`Failed to check schema existence: ${checkTableExists.error}`);
                }
                tableExists = Boolean(Number(checkTableExists?.results[0]?.count));
            }
    
            if (tableExists) {
                throw new Error("Table already exists");
            }
    
            // ✅ Create the table
            const createQuery = this.db.getCreateTableQuery(table, newMetaData);
            const createTable = await this.db.runTransaction(createQuery);
    
            return createTable;
    
        } catch (error) {
            throw error
        }
    }    
    
    async autoAlterTable(table: string, tableChanges: AlterTableChanges, tableExists?: boolean): Promise<QueryResult> {
        try {
            // ✅ Skip table existence check if already known
            if (tableExists === undefined) {
                const checkTableExistsQuery = this.db.getTableExistsQuery(this.db.getConfig().schema || this.db.getConfig().database || "", table);
                const checkTableExists = await this.db.runQuery(checkTableExistsQuery);
                if (!checkTableExists.success || !checkTableExists.results) {
                    throw new Error(`Failed to check schema existence: ${checkTableExists.error}`);
                }
                tableExists = Boolean(Number(checkTableExists?.results[0]?.count));
            }
    
            if (!tableExists) {
                throw new Error("Table doesn't exist");
            }
    
            // ✅ Alter the table
            const alterQuery = await this.db.getAlterTableQuery(table, tableChanges);
            const alterTable = await this.db.runTransaction(alterQuery);
    
            return alterTable
    
        } catch (error) {
            throw error
        }
    }    

    async autoConfigureTable(table: string, data?: Record<string, any>[] | null, currentMetaDataOrTableChanges?: MetadataHeader | AlterTableChanges | null, newMetaData?: MetadataHeader | null): Promise<QueryResult> {
        try {
            console.log(`⚡ [autoConfigureTable] Running for table: ${table}`);

            if (!currentMetaDataOrTableChanges && data?.length === 0) {
                // ❌ Cannot configure table '${table}': No existing metadata and no data provided to infer structure.
                throw new Error(`No existing metadata and no data provided to infer structure.`);
            }
            let tableChanges: AlterTableChanges | null = null;
            let updatedMetadata: MetadataHeader | undefined | null = newMetaData;
            let tableExists: boolean | undefined = undefined;
            if(!newMetaData) {
                if(!data || !isValidDataFormat(data)) {throw new Error('Invalid data format: Expected a non-empty array of objects.')}
                newMetaData = await getMetaData(this.db.getConfig(), data);
                updatedMetadata = newMetaData
                this.db.updateTableMetadata(table, newMetaData, "metaData");
            }

            if(!updatedMetadata) { throw new Error('An unexpected error occurred while getting metadata')}

            if(!currentMetaDataOrTableChanges) {
                console.log("🔎 Fetching metadata since no current metadata was provided...");
                const { currentMetaData, tableExists: exists } = await this.fetchTableMetadata(table);
                tableExists = exists;

                if (currentMetaData) {
                    // ✅ Compare metadata if table exists
                    const { changes, updatedMetaData: mergedMetadata } = compareMetaData(
                        currentMetaData,
                        updatedMetadata,
                        this.db.getDialectConfig()
                    );
                    tableChanges = changes;
                    updatedMetadata = mergedMetadata;
                    this.db.updateTableMetadata(table, updatedMetadata, "metaData");
                }
            }
            else if(isMetaDataHeader(currentMetaDataOrTableChanges)) {
                console.log("🔍 Comparing metadata for changes...");
                // ✅ If provided with metadata, compare changes
                const { changes, updatedMetaData: mergedMetadata } = compareMetaData(currentMetaDataOrTableChanges, newMetaData, this.db.getDialectConfig());
                tableChanges = changes;
                updatedMetadata = mergedMetadata;
                tableExists = true;
            } else {
                console.log("🚀 Precomputed table changes detected, using them directly.");
                // ✅ If provided with precomputed table changes, use directly
                tableChanges = currentMetaDataOrTableChanges as AlterTableChanges;
                updatedMetadata = newMetaData; // ✅ No merging needed
                tableExists = true;
            }

            if (!tableExists) {
                console.log(`🔨 Creating table: ${table}`);
                return await this.autoCreateTable(table, updatedMetadata, false);
            }
    
            // ✅ If table exists but no changes, return success
            if (!tableChanges || !tableChangesExist(tableChanges)) {
                console.log(`✅ Table exists, no changes detected. Skipping ALTER TABLE.`);
                const start = this.db.startDate;
                const end = new Date();
                const affectedRows = 0;
                const rows: any[] = []
                return {
                    start,
                    end,
                    duration: end.getTime() - start.getTime(),
                    affectedRows,
                    success: true,
                    results: rows
                };
            }
    
            // ✅ If table exists and changes exist, alter it
            console.log(`✏️ Altering table: ${table} with changes:`, tableChanges);
            return await this.autoAlterTable(table, tableChanges, true);
        } catch (error) {
            const start = this.db.startDate;
            const end = new Date();
            const affectedRows = 0;
            return {
                start,
                end,
                duration: end.getTime() - start.getTime(),
                affectedRows,
                success: false,
                error: error instanceof Error ? error.message : String(error)
            };
        }
    }

    async fetchTableMetadata(table: string): Promise<{ currentMetaData: MetadataHeader | null; tableExists: boolean; }> {
        // ✅ Check if the table exists
        const checkTableExistsQuery = this.db.getTableExistsQuery(
            this.db.getConfig().schema || this.db.getConfig().database || "", 
            table
        );
        const checkTableExists = await this.db.runQuery(checkTableExistsQuery);
        const tableExists = Boolean(Number(checkTableExists?.results?.[0]?.count || 0));
    
        let currentMetaData: MetadataHeader | null = null;
        
        if (tableExists) {
            // ✅ Fetch metadata ONLY if table exists
            const currentMetaDataQuery = this.db.getTableMetaDataQuery(
                this.db.getConfig().schema || this.db.getConfig().database || "",
                table
            );
            const currentMetaDataResults = await this.db.runQuery(currentMetaDataQuery);
    
            if (!currentMetaDataResults || !currentMetaDataResults.success || !currentMetaDataResults.results) {
                throw new Error(`Failed to retrieve existing meta data for table ${table}`);
            }
    
            const parsedMetadata = parseDatabaseMetaData(currentMetaDataResults.results, this.db.getDialectConfig());
    
            if (!parsedMetadata) {
                currentMetaData = null;
            } else if (typeof parsedMetadata === "object" && !Array.isArray(parsedMetadata)) {
                // ✅ Ensure that we only get MetadataHeader, not multiple tables
                currentMetaData = parsedMetadata as MetadataHeader;
            } else {
                throw new Error("Unexpected metadata format: Multiple tables returned for a single-table query.");
            }
    
            if (currentMetaData) {
                this.db.updateTableMetadata(table, currentMetaData, "existingMetaData");
            }
        }
    
        return { currentMetaData, tableExists };
    }    

    async splitTableData(table: string, data: Record<string, any>[], metaData: MetadataHeader): Promise<{table: string, data: Record<string, any>[], metaData: MetadataHeader}[]> {
        try {
            const splitQuery = this.db.getSplitTablesQuery(table);
            const currentSplitResults = await this.db.runQuery(splitQuery);
            if(!currentSplitResults || !currentSplitResults.success || !currentSplitResults.results) { throw new Error(currentSplitResults.error || `Error while retrieving existing split table information for: ${table}`)}
            const currentSplit = currentSplitResults.results
            const normalizedResults = parseDatabaseMetaData(currentSplitResults!.results, this.db.getDialectConfig()) || {}
            const groupedByTable = Object.entries(normalizedResults).reduce((acc, [columnName, columnDef]) => {
                if (!columnDef.tableName) return acc; // Skip if there's no table name
            
                const tableName = columnDef.tableName;
            
                if (!acc[tableName]) acc[tableName] = {}; // Initialize table entry
            
                acc[tableName][columnName] = columnDef; // Add column metadata under the table name
            
                return acc;
            }, {} as Record<string, MetadataHeader>);
            // Split table metadata into multiple:
            // { table: name, data: Record<string, any>[], metaData: MetadataHeader}[]?
            return [{ table, data, metaData}]
        } catch (error) {
            throw error
        }
    }
    
    async autoSQL(table: string, data: Record<string, any>[], schema?: string): Promise<QueryResult> {
        try {
            if(schema) { this.db.updateSchema(schema) }
            const { currentMetaData } = await this.fetchTableMetadata(table)
            // ✅ Generate new metadata based on provided data
            const newMetaData = await getMetaData(this.db.getConfig(), data);
            this.db.updateTableMetadata(table, newMetaData, "metaData");

            let changes: AlterTableChanges | null = null;
            let mergedMetaData: MetadataHeader | null = null;
            if(currentMetaData) {
                ({ changes, updatedMetaData: mergedMetaData, } = compareMetaData(currentMetaData, newMetaData,this.db.getDialectConfig()));
                this.db.updateTableMetadata(table, mergedMetaData, "metaData")
            } else {
                mergedMetaData = newMetaData
            }
            mergedMetaData = ensureTimestamps(this.db.getConfig(), mergedMetaData)

            if(this.db.getConfig().autoSplit) {
                const { rowSize, exceedsLimit } = estimateRowSize(mergedMetaData, this.db.getDialect());
                // Split the table structure
                if(exceedsLimit) {

                }
            }

            const configuredTables = await this.autoConfigureTable(table, data, changes || currentMetaData, mergedMetaData)
            const start = this.db.startDate;
            const affectedRows = 0
            const end = new Date;
            return {
                start,
                end,
                success: true,
                duration: end.getTime() - start.getTime(),
                affectedRows
            };
        } catch (error: any) {
            const start = this.db.startDate;
            const end = new Date();
            const affectedRows = 0;
        
            return {
                start,
                end,
                duration: end.getTime() - start.getTime(),
                affectedRows,
                success: false,
                error: error instanceof Error ? error.message : String(error) // Ensure error is a string
            };
        }
    }
}