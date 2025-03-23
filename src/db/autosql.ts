import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { Database } from "./database";
import { InsertResult, InsertInput, MetadataHeader, AlterTableChanges, metaDataInterim, QueryResult, QueryInput } from "../config/types";
import { getMetaData, compareMetaData } from "../helpers/metadata";
import { parseDatabaseMetaData, tableChangesExist, isMetaDataHeader, estimateRowSize, isValidDataFormat, organizeSplitTable, organizeSplitData, splitInsertData, getInsertValues } from "../helpers/utilities";
import { defaults } from "../config/defaults";
import { ensureTimestamps } from "../helpers/timestamps";
import WorkerHelper from "../workers/workerHelper";

export class AutoSQLHandler {
    private db: Database;

    constructor(dbInstance: MySQLDatabase | PostgresDatabase) {
        this.db = dbInstance;
    }

    async autoCreateTable(table: string, newMetaData: MetadataHeader, tableExists?: boolean, runQuery: boolean = true): Promise<QueryResult | QueryInput[]> {
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
            if(!runQuery) {
                return createQuery
            }
            const createTable = await this.db.runTransaction(createQuery);
    
            return createTable;
    
        } catch (error) {
            throw error
        }
    }    
    
    async autoAlterTable(table: string, tableChanges: AlterTableChanges, tableExists?: boolean, runQuery: boolean = true): Promise<QueryResult | QueryInput[]> {
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
            if(!runQuery) {
                return alterQuery
            }
            const alterTable = await this.db.runTransaction(alterQuery);
    
            return alterTable
    
        } catch (error) {
            throw error
        }
    }    

    async autoConfigureTable(inputOrTable: string | InsertInput, inputData?: Record<string, any>[] | null, inputCurrentMetaData?: MetadataHeader | AlterTableChanges | null, inputNewMetaData?: MetadataHeader | null, inputRunQuery: boolean = true): Promise<QueryResult | QueryInput[]> {
        try {
            let table: string;
            let data: Record<string, any>[] | null = null;
            let currentMetaDataOrTableChanges: MetadataHeader | AlterTableChanges | null = null;
            let newMetaData: MetadataHeader | null = null;
            let runQuery: boolean = true

            if (typeof inputOrTable === "object") {
                table = inputOrTable.table;
                data = inputOrTable.data;
                currentMetaDataOrTableChanges = inputOrTable.previousMetaData;
                newMetaData = inputOrTable.metaData;
                runQuery = inputOrTable.runQuery || inputRunQuery || true
            } else {
                // ✅ Handle case where `input` is a `string` (table name)
                table = inputOrTable;
                data = inputData ?? null;
                currentMetaDataOrTableChanges = inputCurrentMetaData ?? null;
                newMetaData = inputNewMetaData ?? null;
                runQuery = inputRunQuery
            }
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
                return await this.autoCreateTable(table, updatedMetadata, false, runQuery);
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
            return await this.autoAlterTable(table, tableChanges, true, runQuery);
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
                error: error instanceof Error ? error.message : String(error),
                table: typeof inputOrTable === "object" ? inputOrTable.table : inputOrTable
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

    async splitTableData(table: string, data: Record<string, any>[], metaData: MetadataHeader): Promise<{table: string, data: Record<string, any>[], metaData: MetadataHeader, previousMetaData: MetadataHeader, mergedMetaData: { changes: AlterTableChanges, updatedMetaData: MetadataHeader }}[]> {
        try {
            const splitQuery = this.db.getSplitTablesQuery(table);
            const currentSplitResults = await this.db.runQuery(splitQuery);
            if(!currentSplitResults || !currentSplitResults.success || !currentSplitResults.results) { throw new Error(currentSplitResults.error || `Error while retrieving existing split table information for: ${table}`)}
            const currentSplit = currentSplitResults.results
            let parsedSplitMetadata = parseDatabaseMetaData(currentSplit as Record<string, any>[], this.db.getDialectConfig());
            if (!parsedSplitMetadata) {
                parsedSplitMetadata = { [table]: {} }; // ✅ Ensure it has a valid structure
            } else if (Object.values(parsedSplitMetadata).some(value => typeof value === "object" && !Array.isArray(value))) {
                parsedSplitMetadata = parsedSplitMetadata as Record<string, MetadataHeader>;
            } else {
                parsedSplitMetadata = { [table]: parsedSplitMetadata as MetadataHeader };
            }
            const newGroupedByTable = organizeSplitTable(table, metaData, parsedSplitMetadata, this.db.getDialectConfig())
            const newGroupedData = organizeSplitData(data, newGroupedByTable)
            const transformedData = await Promise.all(
                Object.keys(newGroupedByTable).map(async (tableName) => {
                    const newMetaData = await getMetaData(this.db.getConfig(), newGroupedData[tableName] || []);
                    const mergedMetaData = compareMetaData(parsedSplitMetadata[tableName], newMetaData, this.db.getDialectConfig());
            
                    return {
                        table: tableName,
                        data: newGroupedData[tableName] || [],
                        metaData: newMetaData,
                        previousMetaData: parsedSplitMetadata[tableName],
                        mergedMetaData: mergedMetaData
                    };
                })
            );
            return transformedData
        } catch (error) {
            throw error
        }
    }

    async insertData(inputOrTable: InsertInput | string, inputData?: Record<string, any>[], inputMetaData?: MetadataHeader, inputPreviousMetaData?: AlterTableChanges | MetadataHeader | null, inputComparedMetaData?: { changes: AlterTableChanges, updatedMetaData: MetadataHeader }, inputRunQuery: boolean = true): Promise<QueryInput[] | QueryResult> {
        let table: string;
        let data: Record<string, any>[] = [];
        let metaData: MetadataHeader;
        let previousMetaData: AlterTableChanges | MetadataHeader | null = null;
        let comparedMetaData: { changes: AlterTableChanges, updatedMetaData: MetadataHeader } | undefined;
        let runQuery: boolean;
      
        // ✅ Support InsertInput object
        if (typeof inputOrTable === "object" && "table" in inputOrTable && "data" in inputOrTable) {
          table = inputOrTable.table;
          data = inputOrTable.data;
          metaData = inputOrTable.metaData;
          previousMetaData = inputOrTable.previousMetaData;
          comparedMetaData = inputOrTable.comparedMetaData;
          runQuery = inputOrTable.runQuery ?? true;
        } else {
          // ✅ Support individual parameters
          table = inputOrTable;
          data = inputData ?? [];
          metaData = inputMetaData!;
          previousMetaData = inputPreviousMetaData ?? null;
          comparedMetaData = inputComparedMetaData;
          runQuery = inputRunQuery ?? true
        }
        if (data.length === 0) {
            throw new Error(`insertData: no data rows provided for table "${table}"`);
        }          

        const splitData: Record<string, any>[][] = splitInsertData(data, this.db.getConfig())
        const effectiveMetaData = comparedMetaData?.updatedMetaData || metaData;
        
        const insertStatements: QueryInput[] = await Promise.all(
            splitData.map((chunk) => {
              const normalisedChunk = chunk.map((row) =>
                getInsertValues(effectiveMetaData, row, this.db.getDialectConfig())
              );
              return this.db.getInsertStatementQuery(table, normalisedChunk, effectiveMetaData);
            })
          );

        if (insertStatements.length > 0 && runQuery) {
            return await this.db.runTransaction(insertStatements);
        }
        
        return insertStatements;
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
            let insertInput: InsertInput[] = []
            let comparedMetaData: { changes: AlterTableChanges, updatedMetaData: MetadataHeader } | undefined
            if(currentMetaData) {
                comparedMetaData = compareMetaData(currentMetaData, newMetaData, this.db.getDialectConfig());
                changes = comparedMetaData.changes;
                mergedMetaData = comparedMetaData.updatedMetaData
                this.db.updateTableMetadata(table, mergedMetaData, "metaData")
            } else {
                mergedMetaData = newMetaData
            }
            mergedMetaData = ensureTimestamps(this.db.getConfig(), mergedMetaData, this.db.startDate)

            if(this.db.getConfig().autoSplit) {
                const { rowSize, exceedsLimit, nearlyExceedsLimit } = estimateRowSize(mergedMetaData, this.db.getDialect());
                // Split the table structure
                if(exceedsLimit) {
                    insertInput = await this.splitTableData(table, data, mergedMetaData)
                }
            }

            if(!insertInput || insertInput.length == 0) {
                if(comparedMetaData === undefined) {
                    comparedMetaData = compareMetaData(currentMetaData || null, newMetaData, this.db.getDialectConfig());
                }
                insertInput = [{
                    table,
                    data,
                    previousMetaData: changes || currentMetaData,
                    metaData: mergedMetaData,
                    comparedMetaData
                }]
            }

            if(!this.db.getConfig().safeMode) {
                // Configure Tables
                let configuredTables: (QueryResult | QueryInput[])[];
                if(this.db.getConfig().useWorkers) {
                    insertInput = insertInput.map((input) => ({
                        ...input,
                        runQuery: false
                    }));
                    configuredTables = await WorkerHelper.run(this.db.getConfig(), "autoConfigureTable", insertInput) as (QueryResult | QueryInput[])[]
                } else {
                    configuredTables = await Promise.all(insertInput.map((input) => this.autoConfigureTable(input))) as (QueryResult | QueryInput[])[]
                }

                const initialResults: QueryResult[] = configuredTables.filter(
                    (result): result is QueryResult => "success" in result
                );
                    
                const queryInputs: QueryInput[][] = configuredTables.filter(
                    (result): result is QueryInput[] => Array.isArray(result)
                );

                let allResults: QueryResult[]
                if(queryInputs.length > 0) {
                    const transactionResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(queryInputs);
                    allResults = [...initialResults, ...transactionResults];
                } else {
                    allResults = [...initialResults];
                }

                const failedResults = allResults.filter((r) => !r.success);

                // ✅ If any table failed, throw an error with details
                if (failedResults.length > 0) {
                    throw new Error(
                    `Table configuration failed for ${failedResults.length} table(s):\n` +
                    failedResults.map((t) => `- ${t.table || "Unknown Table"}: ${t.error || "Unknown Error"}`).join("\n")
                    );
                }
                console.log("✅ All tables configured and executed successfully.");
            }

            let insertQueries: (QueryResult | QueryInput[])[];

            if (insertInput.length === 0) {
                throw new Error("No data found for insert after tables were configured");
            }

            if (insertInput.length === 1) {
                insertQueries = [
                    await this.insertData({ ...insertInput[0], runQuery: false })
                ];
            } else {
                // Defer query execution for now
                insertInput = insertInput.map((input) => ({
                    ...input,
                    runQuery: false
                }));

                if (this.db.getConfig().useWorkers) {
                    insertQueries = await WorkerHelper.run(this.db.getConfig(), "insertData", insertInput) as (QueryResult | QueryInput[])[];
                } else {
                    insertQueries = await Promise.all(
                        insertInput.map((input) => this.insertData({ ...input, runQuery: false }))
                    ) as (QueryResult | QueryInput[])[];
                }
            }

            const insertTransactionInputs: QueryInput[][] = insertQueries as QueryInput[][];

            const allInsertResults: QueryResult[] = await this.db.runTransactionsWithConcurrency(insertTransactionInputs);

            const failedInsertResults = allInsertResults.filter((r) => !r.success);

            if (failedInsertResults.length > 0) {
                throw new Error(
                    `Insert failed for ${failedInsertResults.length} chunk(s):\n` +
                    failedInsertResults.map((t) => `- ${t.table || "Unknown Table"}: ${t.error || "Unknown Error"}`).join("\n")
                );
            }

            const start = this.db.startDate;
            const affectedRows = allInsertResults.reduce((sum, res) => sum + (res.affectedRows || 0), 0);
            const end = new Date;
            return {
                start,
                end,
                success: true,
                duration: end.getTime() - start.getTime(),
                affectedRows,
                results: allInsertResults,
                table
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