import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { Database } from "./database";
import { InsertResult, InsertInput, MetadataHeader, AlterTableChanges, metaDataInterim, QueryResult, QueryInput } from "../config/types";
import { getMetaData, compareMetaData } from "../helpers/metadata";
import { parseDatabaseMetaData, tableChangesExist, isMetaDataHeader, estimateRowSize, isValidDataFormat, organizeSplitTable, organizeSplitData, splitInsertData, getInsertValues, getTempTableName, getTrueTableName, getHistoryTableName, normalizeResultKeys, throwIfFailedResults } from "../helpers/utilities";
import { defaults, MAX_COLUMN_COUNT } from "../config/defaults";
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

            if (!newMetaData && data?.length === 0) {
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

    async autoInsertData(inputOrTable: InsertInput | string, inputData?: Record<string, any>[], inputMetaData?: MetadataHeader, inputPreviousMetaData?: AlterTableChanges | MetadataHeader | null, inputComparedMetaData?: { changes: AlterTableChanges, updatedMetaData: MetadataHeader }, inputRunQuery: boolean = true, inputInsertType?: "UPDATE" | "INSERT"): Promise<QueryInput[] | QueryResult> {
        let table: string;
        let data: Record<string, any>[] = [];
        let metaData: MetadataHeader;
        let previousMetaData: AlterTableChanges | MetadataHeader | null = null;
        let comparedMetaData: { changes: AlterTableChanges, updatedMetaData: MetadataHeader } | undefined;
        let runQuery: boolean;
        let insertType: "UPDATE" | "INSERT"
      
        // ✅ Support InsertInput object
        if (typeof inputOrTable === "object" && "table" in inputOrTable && "data" in inputOrTable) {
          table = inputOrTable.table;
          data = inputOrTable.data;
          metaData = inputOrTable.metaData;
          previousMetaData = inputOrTable.previousMetaData;
          comparedMetaData = inputOrTable.comparedMetaData;
          runQuery = inputOrTable.runQuery ?? true;
          insertType = inputOrTable?.insertType || "UPDATE"
        } else {
          // ✅ Support individual parameters
          table = inputOrTable;
          data = inputData ?? [];
          metaData = inputMetaData!;
          previousMetaData = inputPreviousMetaData ?? null;
          comparedMetaData = inputComparedMetaData;
          runQuery = inputRunQuery ?? true
          insertType = inputInsertType || "UPDATE"
        }
        if (data.length === 0) {
            throw new Error(`insertData: no data rows provided for table "${table}"`);
        }          

        const splitData: Record<string, any>[][] = splitInsertData(data, this.db.getConfig())
        const effectiveMetaData = comparedMetaData?.updatedMetaData || metaData;
        
        const insertStatements: QueryInput[] = await Promise.all(
            splitData.map((chunk) => {
              const normalisedChunk = chunk.map((row) =>
                getInsertValues(effectiveMetaData, row, this.db.getDialectConfig(), this.db.getConfig(), true)
              );
              return this.db.getInsertStatementQuery(table, normalisedChunk, effectiveMetaData, insertType);
            })
          );

        if (insertStatements.length > 0 && runQuery) {
            return await this.db.runTransaction(insertStatements);
        }
        return insertStatements;
    }

    async handleMetadata(table: string, data: Record<string, any>[], primaryKey?: string[]) {
        // Fetch existing metadata
        const { currentMetaData } = await this.fetchTableMetadata(table);
        
        // Generate new metadata from incoming data
        const newMetaData = await getMetaData(this.db.getConfig(), data, primaryKey);
        this.db.updateTableMetadata(table, newMetaData, "metaData");
    
        let initialComparedMetaData : { changes: AlterTableChanges; updatedMetaData: MetadataHeader } | undefined;
        let mergedMetaData: MetadataHeader = newMetaData;
        let changes: AlterTableChanges | null = null;
    
        if (currentMetaData) {
            initialComparedMetaData  = compareMetaData(currentMetaData, newMetaData, this.db.getDialectConfig());
            changes = initialComparedMetaData .changes;
            mergedMetaData = initialComparedMetaData.updatedMetaData;
            this.db.updateTableMetadata(table, mergedMetaData, "metaData");
        }
    
        mergedMetaData = ensureTimestamps(this.db.getConfig(), mergedMetaData, this.db.startDate);
    
        return { currentMetaData, mergedMetaData, initialComparedMetaData, changes, newMetaData };
    }

    private async attemptTableSplit(table: string, data: Record<string, any>[], mergedMetaData: MetadataHeader) {
        if (this.db.getConfig().autoSplit) {
            const { rowSize, exceedsLimit } = estimateRowSize(mergedMetaData, this.db.getDialect());
            const columnCount = Object.keys(mergedMetaData).length;
            const exceedsColumnLimit = columnCount >= MAX_COLUMN_COUNT;
    
            if (exceedsLimit || exceedsColumnLimit) {
                return await this.splitTableData(table, data, mergedMetaData);
            }
        }
        return [];
    }

    private async prepareInsertData(table: string, data: Record<string, any>[], schema?: string, primaryKey?: string[]): Promise<InsertInput[]> {
        // 🔹 Step 1: Handle Metadata
        const { currentMetaData, mergedMetaData, initialComparedMetaData, changes, newMetaData } = await this.handleMetadata(table, data, primaryKey);
    
        // 🔹 Step 2: Attempt Table Split
        let insertInput: InsertInput[] = await this.attemptTableSplit(table, data, mergedMetaData);
    
        // 🔹 Step 3: Handle the case when split is not needed or failed
        if (!insertInput || insertInput.length === 0) {
            // 🔹 Step 3.1: Handle metadata comparison if not split
            let comparedMetaData = initialComparedMetaData;
            if (comparedMetaData === undefined) {
                comparedMetaData = compareMetaData(currentMetaData || null, newMetaData, this.db.getDialectConfig());
            }
    
            insertInput = [{
                table,
                data,
                previousMetaData: changes || currentMetaData,
                metaData: mergedMetaData,
                comparedMetaData
            }];
        }
    
        return insertInput;
    }
    
    private async configureTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
        if (this.db.getConfig().safeMode) return [];
    
        let configuredTables: (QueryResult | QueryInput[])[];
    
        // 🔹 Step 1: Auto-configure tables (with Workers or Directly)
        if (this.db.getConfig().useWorkers) {
            insertInput = insertInput.map((input) => ({ ...input, runQuery: false }));
            
            const workerResults = await WorkerHelper.run(this.db.getConfig(), "autoConfigureTable", insertInput) as { success: boolean; result: QueryResult | QueryInput[], error?: Error }[];
    
            const failed = workerResults.filter(w => !w.success);
            if (failed.length > 0) {
                throw new Error(
                    `Worker execution failed for ${failed.length} task(s):\n` +
                    failed.map((f, i) => `- Task #${i + 1}: ${f?.error?.message || "Unknown Error"}`).join("\n")
                );
            }
    
            configuredTables = workerResults.map(w => w.result);
        } else {
            configuredTables = await Promise.all(insertInput.map((input) => this.autoConfigureTable(input))) as (QueryResult | QueryInput[])[];
        }
    
        // 🔹 Step 2: Split successful results and queries to execute
        const initialResults: QueryResult[] = configuredTables.filter(
            (result): result is QueryResult => "success" in result
        );
    
        const queryInputs: QueryInput[][] = configuredTables.filter(
            (result): result is QueryInput[] => Array.isArray(result)
        );
    
        // 🔹 Step 3: Execute Transactions (if needed)
        let allResults: QueryResult[];
        if (queryInputs.length > 0) {
            const transactionResults: QueryResult[] = await this.db.runTransactionsWithConcurrency(queryInputs);
            allResults = [...initialResults, ...transactionResults];
        } else {
            allResults = [...initialResults];
        }
    
        // 🔹 Step 4: Handle failures
        throwIfFailedResults(allResults, "table configuring queries")
    
        console.log("✅ All tables configured and executed successfully.");
        return allResults;
    }

    private async insertData(insertInput: InsertInput[]): Promise<QueryResult[]> {
        if (insertInput.length === 0) {
            throw new Error("No data found for insert after tables were configured");
        }
    
        let insertQueries: (QueryResult | QueryInput[])[];
    
        // 🔹 Step 1: Handle single insert separately
        if (insertInput.length === 1) {
            insertQueries = [
                await this.autoInsertData({ ...insertInput[0], runQuery: false })
            ];
        } else {
            // 🔹 Step 2: Defer execution & modify inputs
            insertInput = insertInput.map((input) => ({ ...input, runQuery: false }));
    
            if (this.db.getConfig().useWorkers) {
                const workerResults = await WorkerHelper.run(this.db.getConfig(), "autoInsertData", insertInput) as { success: boolean; result: QueryResult | QueryInput[], error?: Error }[];
    
                const failed = workerResults.filter(w => !w.success);
                if (failed.length > 0) {
                    throw new Error(
                        `Worker execution failed for ${failed.length} task(s):\n` +
                        failed.map((f, i) => `- Task #${i + 1}: ${f?.error?.message || "Unknown Error"}`).join("\n")
                    );
                }
    
                insertQueries = workerResults.map(w => w.result);
            } else {
                insertQueries = await Promise.all(
                    insertInput.map((input) => this.autoInsertData({ ...input, runQuery: false }))
                ) as (QueryResult | QueryInput[])[];
            }
        }
    
        // 🔹 Step 3: Execute Insert Transactions
        const insertTransactionInputs: QueryInput[][] = insertQueries as QueryInput[][];
        const allInsertResults: QueryResult[] = await this.db.runTransactionsWithConcurrency(insertTransactionInputs);
    
        // 🔹 Step 4: Handle Failures
        throwIfFailedResults(allInsertResults, "data insert queries")
    
        return allInsertResults;
    }

    private async prepareStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> { 
        const uniqueTables = Array.from(new Set(insertInput.map(input => input.table)));

        const stagingQueries: QueryInput[][] = uniqueTables.map(table => {
            return [this.db.getCreateTempTableQuery(table)];
        });
        const allCreateResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingQueries);

        throwIfFailedResults(allCreateResults, "table create queries")
        return allCreateResults
    }

    private async insertStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
        const stagingInputs: InsertInput[] = insertInput.map(input => ({
            ...input,
            table: getTempTableName(input.table),
            insertType: "INSERT"
        }));
        // Configure staging tables where necessary
        await this.configureTables(stagingInputs)
        return await this.insertData(stagingInputs)
    }

    private async removeStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> { 
        const uniqueTables = Array.from(new Set(insertInput.map(input => input.table)));

        const stagingQueries: QueryInput[][] = uniqueTables.map(table => {
            const tempTableName = getTempTableName(table);
            return [this.db.getDropTableQuery(tempTableName)];
        });
        const allDropResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingQueries);

        throwIfFailedResults(allDropResults, "table drop queries")
        return allDropResults
    }

    private async resolveConflicts(insertInput: InsertInput[]): Promise<void> {
        const uniqueTables = Array.from(new Set(insertInput.map(input => input.table)));
        const uniqueIndexesQuery = uniqueTables.map(table => {
            return [this.db.getUniqueIndexesQuery(table)]
        })
        const primaryKeyQuery = uniqueTables.map(table => {
            return [this.db.getPrimaryKeysQuery(table)]
        })
        const allUniqueKeys : QueryResult[] = await this.db.runTransactionsWithConcurrency(uniqueIndexesQuery);
        const allPrimaryKeys : QueryResult[] = await this.db.runTransactionsWithConcurrency(primaryKeyQuery);
        const tableStructure: Record<string, {
            uniques: Record<string, string[]>,
            primary: string[]
        }> = {};

        for (let i = 0; i < uniqueTables.length; i++) {
            const table = uniqueTables[i];
            const uniqueIndexes = allUniqueKeys[i];
            const primaryColumns = allPrimaryKeys[i]
            if (!uniqueIndexes?.results) continue;
            if (!primaryColumns?.results) continue;
    
            const normalizedUniques = uniqueIndexes.results
                .map(row => normalizeResultKeys(row))
                .filter(row => row.columns);

            const normalizedPrimary = primaryColumns.results
                .map(row => normalizeResultKeys(row))
                .filter(row => row.column_name);

            if (!tableStructure[table]) {
                tableStructure[table] = {
                    uniques: {},
                    primary: []
                };
            }
            normalizedUniques.forEach(result => {
                tableStructure[table].uniques[result.index_name] = (result.columns as string)
                    .split(",")
                    .map(col => col.trim());
            });
        
            normalizedPrimary.forEach(result => {
                tableStructure[table].primary.push(result.column_name);
            });
        }

        const conflictsQuery = Object.keys(tableStructure)
            .filter(table => {
                const structure = tableStructure[table];
                return (structure && Object.keys(structure.uniques || {}).length > 0 && // Must have at least one unique constraint
                Array.isArray(structure.primary) && structure.primary.length > 0 // Must have at least one primary key
                );
            })
            .map(table => {
                return [this.db.getConstraintConflictQuery(table, tableStructure[table])];
            });

        const allConflicts : QueryResult[] = await this.db.runTransactionsWithConcurrency(conflictsQuery);
        const constraintViolations: Record<string, string[]> = {};
        let removeConstraintsQuery : QueryInput[][] = []

        for (let i = 0; i < allConflicts.length; i++) {
          const result = allConflicts[i];
          const table = Object.keys(tableStructure)[i];
          let tableConstraintsQueries : QueryInput[] = []
        
          const row = result?.results?.[0] || {};
          const violatingIndexes: string[] = [];
        
          for (const [indexName, count] of Object.entries(row)) {
            const numericCount = typeof count === "string" ? parseInt(count) : Number(count);
            if (numericCount > 0) {
                violatingIndexes.push(indexName);
                tableConstraintsQueries.push(this.db.getDropUniqueConstraintQuery(table, indexName))
            }
          }
        
          if (violatingIndexes.length) {
            constraintViolations[table] = violatingIndexes;
            removeConstraintsQuery.push(tableConstraintsQueries)
          }
        }

        const removeConstraints : QueryResult[] = await this.db.runTransactionsWithConcurrency(removeConstraintsQuery);
        throwIfFailedResults(removeConstraints, 'unique constraint removal queries')
        return;
    }

    private async insertFromStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
        const stagingInputs: InsertInput[] = insertInput.map(input => ({
            ...input,
            insertType: "UPDATE"
        }));
        const stagingInsertQueries = (stagingInputs).map(stagingInput => {
            return [this.db.getInsertFromStagingQuery(stagingInput)]
        })
        const allInsertResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingInsertQueries);
        throwIfFailedResults(allInsertResults, 'insert from staging table queries') 
        return allInsertResults
    }

    private async insertToHistoryTables(insertInputs: InsertInput[]): Promise<QueryResult[]> {
        const stagingInsertQueries = (insertInputs).map(insertInput => {
            return [this.db.getInsertChangedRowsToHistoryQuery(insertInput)]
        })
        const allInsertResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingInsertQueries);
        throwIfFailedResults(allInsertResults, 'insert from staging table queries') 
        return allInsertResults
    }

    private async insertHistory(insertInput: InsertInput[]): Promise<QueryResult[]> {
        const config = this.db.getConfig();
        if (!config.addHistory || !config.historyTables?.length) return [];
        if(!this.db.getConfig().useStagingInsert) { throw new Error('Cannot add history tables without using staging insert') }

        const uniqueTables = Array.from(new Set(insertInput.map(input => input.table)));
        const eligibleInputs = uniqueTables.filter(table =>
            config.historyTables!.includes(table)
        );
        if(eligibleInputs.length == 0) {return []}
        // Check for current table meta data
        const historyInputs: InsertInput[] = await Promise.all(
            eligibleInputs.map(async (table) => {
              const historyName = getHistoryTableName(table);
          
              // Run both metadata fetches in parallel
              const [currentStatus, historyStatus] = await Promise.all([
                this.fetchTableMetadata(table),
                this.fetchTableMetadata(historyName),
              ]);
          
              const currentMetaData = currentStatus.currentMetaData;
              const currentHistoryMetaData = historyStatus.currentMetaData;
          
              if (!currentMetaData) {
                throw new Error(`Could not find structure of ${table} for history table creation`);
              }
          
              // ✅ Clean up metaData for history table
              const cleanedMeta: MetadataHeader = {};
          
              for (const col in currentMetaData) {
                const def = { ...currentMetaData[col] };
                def.unique = false;
                def.index = false
                cleanedMeta[col] = def;
              }
          
              // ✅ Add as_at column
              cleanedMeta["dwh_as_at"] = {
                type: "datetime",
                allowNull: false,
                primary: true,
              };

              const automatedColumns = ['dwh_created_at', 'dwh_modified_at', 'dwh_loaded_at']

          
              // ✅ Ensure existing PKs are retained
              for (const col in currentMetaData) {
                if (currentMetaData[col].primary) {
                  cleanedMeta[col].primary = true;
                }
                if(automatedColumns.includes(col)) {
                    cleanedMeta[col].calculated = true
                }
              }
          
              return {
                table: historyName,
                data: [],
                metaData: cleanedMeta,
                previousMetaData: currentHistoryMetaData,
              };
            })
        );

        const configuredHistory = await this.configureTables(historyInputs)
        const insertedHistory = await this.insertToHistoryTables(historyInputs)
        return insertedHistory
    }

    private async extractNestedInputs(inputs: InsertInput[]): Promise<InsertInput[]> {
        if(!this.db.getConfig().addNested) { 
            return []
        }
        const nestedInputs: InsertInput[] = [];
        const nestedMap: Record<string, Record<string, any>[]> = {};
        const primaryMap: Record<string, string[]> = {};

        for (const input of inputs) {
          const { table, data, metaData } = input;
          const primaryKeys = Object.keys(metaData).filter(k => metaData[k].primary);
      
          for (const row of data) {
            for (const [key, value] of Object.entries(row)) {
                const nestedTable = `${table}_${key}`;
                // Check if the key is a nested table
                if(!this.db.getConfig().nestedTables?.includes(nestedTable)) { continue; }
              if (value && typeof value === "object") {

                const nestedObjects: Record<string, any>[] = Array.isArray(value)
                    ? value.filter(v => typeof v === "object" && !Array.isArray(v))
                    : [value];
                
                for (const nested of nestedObjects) {
                    const newRow = {
                      ...nested,
                      ...Object.fromEntries(primaryKeys.map(pk => [pk, row[pk]]))
                    };
                
                    // Group by nested table name
                    if (!nestedMap[nestedTable]) {
                        nestedMap[nestedTable] = [];
                    }
                    if(!primaryMap[nestedTable]) {
                        primaryMap[nestedTable] = primaryKeys
                    }
                    nestedMap[nestedTable].push(newRow);
                }
              }
            }
          }
        }
      
        for (const [nestedTable, nestedRows] of Object.entries(nestedMap)) {
            const { currentMetaData, mergedMetaData, initialComparedMetaData, changes, newMetaData } = await this.handleMetadata(nestedTable, nestedRows, primaryMap[nestedTable]);
            let comparedMetaData = initialComparedMetaData;
            if (comparedMetaData === undefined) {
                comparedMetaData = compareMetaData(currentMetaData || null, newMetaData, this.db.getDialectConfig());
            }
            const insertInput : InsertInput = {
                table: nestedTable,
                data: nestedRows,
                previousMetaData: changes || currentMetaData,
                metaData: mergedMetaData,
                comparedMetaData
            };
            nestedInputs.push(insertInput);
        }
        return nestedInputs;
    } 
    
    async autoSQL(table: string, data: Record<string, any>[], schema?: string, primaryKey?: string[]): Promise<QueryResult> {
        try {

            if(schema) { this.db.updateSchema(schema) }

            let affectedRows : number;
            let insertResults : QueryResult[]
            let insertInput = await this.prepareInsertData(table, data, schema, primaryKey);
            let nestedInputs = await this.extractNestedInputs(insertInput)
            insertInput = [...insertInput, ...nestedInputs];
            
            await this.configureTables(insertInput)

            if(this.db.getConfig().useStagingInsert) {
                await this.prepareStagingTables(insertInput);
                await this.insertStagingTables(insertInput);
                await this.resolveConflicts(insertInput);
                await this.insertHistory(insertInput);
                insertResults = await this.insertFromStagingTables(insertInput);
                await this.removeStagingTables(insertInput);
            } else {
                insertResults = await this.insertData(insertInput);
            }

            insertResults = []

            const start = this.db.startDate;
            affectedRows = insertResults.reduce((sum, res) => sum + (res.affectedRows || 0), 0);
            const allResults = insertResults.flatMap(res => res.results || []);
            const end = new Date;
            return {
                start,
                end,
                success: true,
                duration: end.getTime() - start.getTime(),
                affectedRows,
                results: allResults,
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