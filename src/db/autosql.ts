import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { Database } from "./database";
import { InsertResult, MetadataHeader, AlterTableChanges, metaDataInterim, QueryResult } from "../config/types";
import { getMetaData, compareMetaData } from "../helpers/metadata";
import { parseDatabaseMetaData, tableChangesExist, isMetaDataHeader, estimateRowSize } from "../helpers/utilities";
import { ensureTimestamps } from "../helpers/timestamps";

export class AutoSQLHandler {
    private db: Database;

    constructor(dbInstance: MySQLDatabase | PostgresDatabase) {
        this.db = dbInstance;
    }

    async autoConfigureTable(table: string, currentMetaDataOrTableChanges: MetadataHeader | AlterTableChanges | null, newMetaData: MetadataHeader, data: Record<string, any>[]): Promise<QueryResult> {
        try {    
            let tableChanges: AlterTableChanges | null;
            let updatedMetadata: MetadataHeader = newMetaData;
            if(!currentMetaDataOrTableChanges) {
                tableChanges = null
            }
            else if(isMetaDataHeader(currentMetaDataOrTableChanges)) {
                // ✅ If provided with metadata, compare changes
                const { changes, updatedMetaData: mergedMetadata } = compareMetaData(currentMetaDataOrTableChanges, newMetaData,this.db.getDialectConfig());
                tableChanges = changes;
                updatedMetadata = mergedMetadata;
            } else {
                // ✅ If provided with precomputed table changes, use directly
                tableChanges = currentMetaDataOrTableChanges as AlterTableChanges;
                updatedMetadata = newMetaData; // ✅ No merging needed
            }

            if(!tableChanges || !tableChangesExist(tableChanges)) {
                const createQuery = this.db.getCreateTableQuery(table, newMetaData);
                const createTable = await this.db.runTransaction(createQuery);

                return {
                    success: createTable.success,
                    results: createTable.results,
                    error: createTable.error
                };
            }
                
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
                error: `Error in autoConfigureTable: ${error}`
            };
        }
    }

    async fetchTableMetadata(table: string, data: Record<string, any>[]): Promise<{ currentMetaData: MetadataHeader | null; newMetaData: MetadataHeader;}> {
        // ✅ Get current metadata from database
        const currentMetaDataQuery = this.db.getTableMetaDataQuery(
            this.db.getConfig().schema || this.db.getConfig().database || "",
            table
        );
        const currentMetaDataResults = await this.db.runQuery(currentMetaDataQuery);
        const currentMetaData = parseDatabaseMetaData(currentMetaDataResults, this.db.getDialectConfig());
        if(currentMetaData) {
            this.db.updateTableMetadata(table, currentMetaData, "existingMetaData")
        }

        // ✅ Generate new metadata based on provided data
        const newMetaData = await getMetaData(this.db.getConfig(), data);
        this.db.updateTableMetadata(table, newMetaData, "metaData")
        
        return { currentMetaData, newMetaData };
    }
    
    async autoSQL(table: string, data: Record<string, any>[]): Promise<InsertResult> {
        try {
            const { currentMetaData, newMetaData} = await this.fetchTableMetadata(table, data)
            let changes: AlterTableChanges | null = null;
            let mergedMetaData: MetadataHeader | null = null;
            if(currentMetaData) {
                ({ changes, updatedMetaData: mergedMetaData } = compareMetaData(currentMetaData, newMetaData,this.db.getDialectConfig()));
                this.db.updateTableMetadata(table, mergedMetaData, "metaData")
            } else {
                mergedMetaData = newMetaData
            }
            mergedMetaData = ensureTimestamps(this.db.getConfig(), mergedMetaData)

            const { rowSize, exceedsLimit } = estimateRowSize(mergedMetaData, this.db.getDialect());

            const configuredTables = await this.autoConfigureTable(table, changes || currentMetaData, mergedMetaData, data)
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