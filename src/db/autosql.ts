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
                tableExists = Boolean(Number(checkTableExists?.results[0]?.count));
            }
    
            if (tableExists) {
                throw new Error("Table already exists");
            }
    
            // ✅ Create the table
            const createQuery = this.db.getCreateTableQuery(table, newMetaData);
            const createTable = await this.db.runTransaction(createQuery);
    
            return {
                success: createTable.success,
                results: createTable.results,
                error: createTable.error
            };
    
        } catch (error) {
            return {
                success: false,
                error: `Error in autoCreateTable: ${error}`
            };
        }
    }    
    
    async autoAlterTable(table: string, tableChanges: AlterTableChanges, tableExists?: boolean): Promise<QueryResult> {
        try {
            // ✅ Skip table existence check if already known
            if (tableExists === undefined) {
                const checkTableExistsQuery = this.db.getTableExistsQuery(this.db.getConfig().schema || this.db.getConfig().database || "", table);
                const checkTableExists = await this.db.runQuery(checkTableExistsQuery);
                tableExists = Boolean(Number(checkTableExists?.results[0]?.count));
            }
    
            if (!tableExists) {
                throw new Error("Table doesn't exist");
            }
    
            // ✅ Alter the table
            const alterQuery = await this.db.getAlterTableQuery(table, tableChanges);
            const alterTable = await this.db.runTransaction(alterQuery);
    
            return {
                success: alterTable.success,
                results: alterTable.results,
                error: alterTable.error
            };
    
        } catch (error) {
            return {
                success: false,
                error: `Error in autoAlterTable: ${error}`
            };
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
                return { success: true, results: [] };
            }
    
            // ✅ If table exists and changes exist, alter it
            console.log(`✏️ Altering table: ${table} with changes:`, tableChanges);
            return await this.autoAlterTable(table, tableChanges, true);
        } catch (error) {
            return {
                success: false,
                error: `Error in autoConfigureTable: ${error}`
            };
        }
    }

    async fetchTableMetadata(table: string): Promise<{ currentMetaData: MetadataHeader | null; tableExists: boolean;}> {
        // ✅ Check if the table exists
        const checkTableExistsQuery = this.db.getTableExistsQuery(this.db.getConfig().schema || this.db.getConfig().database || "", table);
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
            currentMetaData = parseDatabaseMetaData(currentMetaDataResults, this.db.getDialectConfig());
    
            if (currentMetaData) {
                this.db.updateTableMetadata(table, currentMetaData, "existingMetaData");
            }
        }
    
        return { currentMetaData, tableExists };
    }
    
    async autoSQL(table: string, data: Record<string, any>[], schema?: string): Promise<InsertResult> {
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

            const { rowSize, exceedsLimit } = estimateRowSize(mergedMetaData, this.db.getDialect());

            if(exceedsLimit && this.db.getConfig().autoSplit) {
                // Split the table structure

            }

            const configuredTables = await this.autoConfigureTable(table, data, changes || currentMetaData, mergedMetaData)
            const start = this.db.startDate;
            const affectedRows = 0
            const end = new Date;
            return {
                start,
                end,
                duration: end.getTime() - start.getTime(),
                affectedRows
            };
        } catch (error) {
            throw error
        } 
    }
}