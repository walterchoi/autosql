import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { SqlServerDatabase } from "./sqlserver";
import { Database } from "./database";
import { InsertResult, InsertInput, MetadataHeader, AlterTableChanges, metaDataInterim, QueryResult, QueryInput, AutoSQLOptions, QueryStats } from "../config/types";
import { getMetaData, compareMetaData, collectDataColumns, schemaCoversColumns, overlaySchema, fillColumnDefaults } from "../helpers/metadata";
import { applySurrogateKey } from "../helpers/keys";
import { parseDatabaseMetaData, tableChangesExist, isMetadataHeader, estimateRowSize, isValidDataFormat, organizeSplitTable, organizeSplitData, splitInsertData, getInsertValues, getTempTableName, getTrueTableName, getHistoryTableName, normalizeResultKeys, throwIfFailedResults, sqlize } from "../helpers/utilities";
import { defaults, MAX_COLUMN_COUNT } from "../config/defaults";
import { ensureTimestamps } from "../helpers/timestamps";
import WorkerHelper from "../workers/workerHelper";
import { buildCompensatingDDL } from "../helpers/compensatingDDL";
import {
    bootstrapSchemaHistoryTable,
    recordMigrationStart,
    recordMigrationSuccess,
    recordMigrationRolledBack,
    recordMigrationFailed,
    detectSchemaDrift as _detectSchemaDrift,
} from '../helpers/schemaHistory';
import {
    generateRunId,
    buildStreamStagingTableName,
    isAutosqlStreamTable,
    buildCreateStreamStagingTableQuery,
    buildInsertIntoStreamStagingQuery,
    buildSelectFromStreamStagingQuery,
    buildDropStreamStagingTableQuery,
    buildOrphanSearchQuery,
    buildMergeFromStreamQuery,
    buildBootstrapRejectedRowsQuery,
    buildInsertRejectedRowsQuery,
} from '../helpers/streamHelpers';

export class AutoSQLHandler {
    private db: Database;
    // Staging tables of streams that are currently open on this instance. Orphan cleanup must
    // never drop these — a concurrent stream to the same table would otherwise destroy a live
    // run's staging data (both share the `${prefix}${table}__` name pattern).
    private activeStreamStagingTables = new Set<string>();

    constructor(dbInstance: MySQLDatabase | PostgresDatabase | SqlServerDatabase) {
        this.db = dbInstance;
    }

    releaseStreamStaging(stagingTable: string): void {
        this.activeStreamStagingTables.delete(stagingTable);
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
        const start = new Date();
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
                runQuery = inputOrTable.runQuery ?? inputRunQuery ?? true
            } else {
                // ✅ Handle case where `input` is a `string` (table name)
                table = inputOrTable;
                data = inputData ?? null;
                currentMetaDataOrTableChanges = inputCurrentMetaData ?? null;
                newMetaData = inputNewMetaData ?? null;
                runQuery = inputRunQuery
            }
            this.db.log(`[autoConfigureTable] Running for table: ${table}`);

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
                this.db.log("Fetching metadata since no current metadata was provided...");
                const { currentMetaData, tableExists: exists } = await this.fetchTableMetadata(table);
                tableExists = exists;

                // Standalone path (metadata inferred above, not supplied by the caller): apply the
                // opt-in surrogate key now that the existing table is known, so it stays sticky.
                updatedMetadata = applySurrogateKey(updatedMetadata, currentMetaData, this.db.getConfig());

                if (currentMetaData) {
                    // ✅ Compare metadata if table exists
                    const { changes, updatedMetaData: mergedMetadata } = compareMetaData(
                        currentMetaData,
                        updatedMetadata,
                        this.db.getDialectConfig(),
                        this.db.getConfig().logger
                    );
                    tableChanges = changes;
                    updatedMetadata = mergedMetadata;
                    this.db.updateTableMetadata(table, updatedMetadata, "metaData");
                }
            }
            else if(isMetadataHeader(currentMetaDataOrTableChanges)) {
                this.db.log("Comparing metadata for changes...");
                // ✅ If provided with metadata, compare changes
                const { changes, updatedMetaData: mergedMetadata } = compareMetaData(currentMetaDataOrTableChanges, newMetaData, this.db.getDialectConfig(), this.db.getConfig().logger);
                tableChanges = changes;
                updatedMetadata = mergedMetadata;
                tableExists = true;
            } else {
                this.db.log("Precomputed table changes detected, using them directly.");
                // ✅ If provided with precomputed table changes, use directly
                tableChanges = currentMetaDataOrTableChanges as AlterTableChanges;
                updatedMetadata = newMetaData; // ✅ No merging needed
                tableExists = true;
            }

            if (!tableExists) {
                this.db.log(`Creating table: ${table}`);
                return await this.autoCreateTable(table, updatedMetadata, false, runQuery);
            }

            // R8 (opt-in): migrate a pre-existing table's text columns to the target charset
            // (utf8mb4) so externally-created 3-byte utf8 columns accept 4-byte characters. Runs on
            // the real table only (staging temp tables are throwaway CTAS copies that already match),
            // before any other ALTER/insert, and even when there are no other schema changes.
            // Best-effort: a CONVERT that fails (e.g. an over-long index) is logged and skipped, not
            // fatal. Convergent: once every text column is utf8mb4 the detect returns nothing.
            const stagingPrefix = this.db.getConfig().stagingPrefix ?? "temp_staging__";
            if (this.db.getConfig().upgradeCharset && !table.startsWith(stagingPrefix)) {
                try {
                    const charsetQueries = await this.db.getCharsetUpgradeQueries(table);
                    if (charsetQueries.length > 0) {
                        await this.db.runTransaction(charsetQueries);
                        this.db.log(`[autoConfigureTable] Upgraded existing charset for '${table}'.`);
                    }
                } catch (charsetErr) {
                    this.db.warn(`[autoConfigureTable] Charset upgrade for '${table}' skipped (continuing): ${charsetErr instanceof Error ? charsetErr.message : String(charsetErr)}`);
                }
            }

            // ✅ If table exists but no changes, return success
            if (!tableChanges || !tableChangesExist(tableChanges)) {
                this.db.log(`Table exists, no changes detected. Skipping ALTER TABLE.`);
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
            this.db.log(`Altering table: ${table} with changes: ${JSON.stringify(tableChanges)}`);
            return await this.autoAlterTable(table, tableChanges, true, runQuery);
        } catch (error) {
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
        // One introspection round-trip, not two: the column-metadata query already tells us whether
        // the table exists (a non-existent table returns 0 rows from INFORMATION_SCHEMA/sys), so we
        // skip the separate exists query. INFORMATION_SCHEMA lookups are slow, so halving them here
        // shaves latency off every non-cached load (the existingSchema fast path skips this entirely).
        const schema = this.db.getConfig().schema || this.db.getConfig().database || "";
        const currentMetaDataQuery = this.db.getTableMetaDataQuery(schema, table);
        const currentMetaDataResults = await this.db.runQuery(currentMetaDataQuery);

        if (!currentMetaDataResults || !currentMetaDataResults.success) {
            throw new Error(`Failed to retrieve existing meta data for table ${table}: ${currentMetaDataResults?.error ?? "unknown error"}`);
        }

        const rows = currentMetaDataResults.results ?? [];
        const tableExists = rows.length > 0;
        let currentMetaData: MetadataHeader | null = null;

        if (tableExists) {
            const parsedMetadata = parseDatabaseMetaData(rows, this.db.getDialectConfig());

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
                    const mergedMetaData = compareMetaData(parsedSplitMetadata[tableName], newMetaData, this.db.getDialectConfig(), this.db.getConfig().logger);
            
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

        // Fall back to the configured insertType (then "UPDATE") when none is set on the input, so
        // the direct-insert path honours `config.insertType` — the staging-population callers pass an
        // explicit insertType, so they are unaffected. (Previously this defaulted straight to
        // "UPDATE", so `config.insertType: "INSERT"` was silently ignored for the bulk batch while
        // the per-row fallback honoured it — an inconsistency.)
        const configInsertType = this.db.getConfig().insertType;

        // ✅ Support InsertInput object
        if (typeof inputOrTable === "object" && "table" in inputOrTable && "data" in inputOrTable) {
          table = inputOrTable.table;
          data = inputOrTable.data;
          metaData = inputOrTable.metaData;
          previousMetaData = inputOrTable.previousMetaData;
          comparedMetaData = inputOrTable.comparedMetaData;
          runQuery = inputOrTable.runQuery ?? true;
          insertType = inputOrTable?.insertType ?? configInsertType ?? "UPDATE"
        } else {
          // ✅ Support individual parameters
          table = inputOrTable;
          data = inputData ?? [];
          if (!inputMetaData) throw new Error(`autoInsertData: metaData is required when called with individual parameters`);
          metaData = inputMetaData;
          previousMetaData = inputPreviousMetaData ?? null;
          comparedMetaData = inputComparedMetaData;
          runQuery = inputRunQuery ?? true
          insertType = inputInsertType ?? configInsertType ?? "UPDATE"
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

    async handleMetadata(table: string, data: Record<string, any>[], primaryKey?: string[], options?: AutoSQLOptions) {
        // Existing table schema. When the caller supplies `existingSchema` (N1 fast path), trust it
        // and skip live introspection of the target table — the main per-run DB round-trip for a
        // stable, no-drift pipeline. It must be a prior run's resolved schema (see AutoSQLOptions),
        // so it already carries the managed dwh_*/surrogate columns and the timestamp step won't
        // re-add them.
        const currentMetaData = options?.existingSchema
            ?? (await this.fetchTableMetadata(table)).currentMetaData;

        // A-4 fast path: when the caller provides the schema (assumeSchema) it is authoritative.
        // If it declares every column present in the data, skip per-value inference entirely; if it
        // only covers some columns, infer the rest and let the provided definitions win (fallback).
        // This is the main compute win for recurring pipelines and side-steps inference footguns
        // (e.g. small integers mis-typed as boolean) for declared columns.
        let newMetaData: MetadataHeader;
        const assumeSchema = options?.assumeSchema;
        if (assumeSchema && Object.keys(assumeSchema).length > 0) {
            const provided = fillColumnDefaults(assumeSchema);
            if (schemaCoversColumns(provided, collectDataColumns(data))) {
                newMetaData = provided; // fully declared → no inference
            } else {
                const inferred = await getMetaData(this.db.getConfig(), data, primaryKey);
                newMetaData = overlaySchema(inferred, provided); // infer undeclared, provided wins
            }
        } else {
            newMetaData = await getMetaData(this.db.getConfig(), data, primaryKey);
        }
        // Apply the opt-in surrogate key, sticky to the existing table so re-ingestion stays
        // idempotent (see applySurrogateKey). No-op unless config.surrogateKey is enabled.
        newMetaData = applySurrogateKey(newMetaData, currentMetaData, this.db.getConfig());
        this.db.updateTableMetadata(table, newMetaData, "metaData");

        let initialComparedMetaData : { changes: AlterTableChanges; updatedMetaData: MetadataHeader } | undefined;
        let mergedMetaData: MetadataHeader = newMetaData;
        let changes: AlterTableChanges | null = null;

        if (currentMetaData) {
            initialComparedMetaData  = compareMetaData(currentMetaData, newMetaData, this.db.getDialectConfig(), this.db.getConfig().logger);
            changes = initialComparedMetaData .changes;
            mergedMetaData = initialComparedMetaData.updatedMetaData;
            this.db.updateTableMetadata(table, mergedMetaData, "metaData");
        }

        // Add the dwh_* timestamp columns to the merged metadata AFTER comparison, so they carry
        // the calculatedDefault that populates them on insert (comparison would otherwise merge in
        // the value-less introspected definition on re-ingest, inserting NULL). For an existing
        // table, a dwh column that is not already on the real table is genuinely new and must be
        // folded into the ALTER (addColumns) — otherwise the real table never gets it and the
        // staging copy fails. Columns already on the table are left untouched (re-adding would
        // duplicate). No-op when addTimestamps is off.
        const beforeTimestamps = new Set(Object.keys(mergedMetaData));
        mergedMetaData = ensureTimestamps(this.db.getConfig(), mergedMetaData, new Date());
        if (currentMetaData && initialComparedMetaData) {
            for (const col of Object.keys(mergedMetaData)) {
                if (!beforeTimestamps.has(col) && !(col in currentMetaData)) {
                    initialComparedMetaData.changes.addColumns[col] = mergedMetaData[col];
                }
            }
            changes = initialComparedMetaData.changes;
            this.db.updateTableMetadata(table, mergedMetaData, "metaData");
        }
    
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

    private async prepareInsertData(table: string, data: Record<string, any>[], schema?: string, primaryKey?: string[], options?: AutoSQLOptions): Promise<InsertInput[]> {
        // 🔹 Step 1: Handle Metadata
        const { currentMetaData, mergedMetaData, initialComparedMetaData, changes, newMetaData } = await this.handleMetadata(table, data, primaryKey, options);
    
        // 🔹 Step 2: Attempt Table Split
        let insertInput: InsertInput[] = await this.attemptTableSplit(table, data, mergedMetaData);
    
        // 🔹 Step 3: Handle the case when split is not needed or failed
        if (!insertInput || insertInput.length === 0) {
            // 🔹 Step 3.1: Handle metadata comparison if not split
            let comparedMetaData = initialComparedMetaData;
            if (comparedMetaData === undefined) {
                comparedMetaData = compareMetaData(currentMetaData || null, newMetaData, this.db.getDialectConfig(), this.db.getConfig().logger);
            }
    
            insertInput = [{
                table,
                data,
                previousMetaData: changes || currentMetaData,
                metaData: mergedMetaData,
                comparedMetaData,
                stagingPrefix: this.db.getConfig().stagingPrefix,
                historyTableSuffix: this.db.getConfig().historyTableSuffix
            }];
        }
    
        return insertInput;
    }
    
    private async configureTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
        if (this.db.getConfig().safeMode) return [];
    
        let configuredTables: (QueryResult | QueryInput[])[];
    
        // 🔹 Step 1: Auto-configure tables (with Workers or Directly). Only fan out to workers when
        // there's more than one table to configure — a single table isn't worth a worker pool (A8).
        if (this.db.getConfig().useWorkers && insertInput.length > 1) {
            insertInput = insertInput.map((input) => ({ ...input, runQuery: false }));
            try {
                const workerResults = await WorkerHelper.run(this.db.getConfig(), "autoConfigureTable", insertInput) as { success: boolean; result: QueryResult | QueryInput[], error?: string | Error, errorCode?: string }[];

                const failed = workerResults.filter(w => !w.success);
                if (failed.length > 0) {
                    const err = new Error(
                        `Worker execution failed for ${failed.length} task(s):\n` +
                        failed.map((f, i) => `- Task #${i + 1}: ${typeof f?.error === "string" ? f.error : (f?.error?.message || "Unknown Error")}`).join("\n")
                    ) as Error & { code?: string };
                    const withCode = failed.find(f => f.errorCode);
                    if (withCode?.errorCode) err.code = withCode.errorCode;
                    throw err;
                }

                configuredTables = workerResults.map(w => w.result);
            } catch (err) {
                if (err instanceof Error && err.message.startsWith("WORKER_UNAVAILABLE:")) {
                    this.db.warn(err.message + " Falling back to direct execution.");
                    configuredTables = await Promise.all(insertInput.map((input) => this.autoConfigureTable(input))) as (QueryResult | QueryInput[])[];
                } else {
                    throw err;
                }
            }
        } else {
            configuredTables = await Promise.all(insertInput.map((input) => this.autoConfigureTable(input))) as (QueryResult | QueryInput[])[];
        }
    
        // 🔹 Step 2: Split immediate results from deferred DDL queries, preserving
        //    index correspondence with insertInput for rollback correlation.
        const initialResults: QueryResult[] = [];
        const pendingDDL: Array<{ queries: QueryInput[]; inputIndex: number }> = [];

        configuredTables.forEach((result, i) => {
            if (Array.isArray(result)) {
                pendingDDL.push({ queries: result, inputIndex: i });
            } else {
                initialResults.push(result as QueryResult);
            }
        });

        // 🔹 Step 3: Execute DDL transactions and attempt compensating rollback on failure
        let allResults: QueryResult[];
        if (pendingDDL.length > 0) {
            const transactionResults: QueryResult[] = await this.db.runTransactionsWithConcurrency(
                pendingDDL.map(d => d.queries)
            );

            // Attempt best-effort compensating DDL for any failed transactions.
            // For PostgreSQL this is a no-op (transactional DDL already rolled back).
            // For MySQL this reverses any partial schema changes as a safety net.
            const failedDDL = transactionResults
                .map((result, j) => (!result.success ? pendingDDL[j] : null))
                .filter((x): x is { queries: QueryInput[]; inputIndex: number } => x !== null);

            if (failedDDL.length > 0) {
                for (const { inputIndex } of failedDDL) {
                    const input = insertInput[inputIndex];
                    if (input?.comparedMetaData) {
                        const { queries: compQueries, warnings } = buildCompensatingDDL(
                            input.table,
                            input.comparedMetaData.changes,
                            input.comparedMetaData.updatedMetaData,
                            this.db.getDialectConfig(),
                            this.db.getConfig().schema
                        );
                        for (const w of warnings) this.db.warn(w);
                        if (compQueries.length > 0) {
                            await this.db.runTransaction(compQueries).catch(e =>
                                this.db.error(`DDL compensation failed for '${input.table}': ${e.message}`)
                            );
                        }
                    }
                }
            }

            allResults = [...initialResults, ...transactionResults];
        } else {
            allResults = [...initialResults];
        }

        // 🔹 Step 4: Handle failures
        throwIfFailedResults(allResults, "table configuring queries")
    
        this.db.log("All tables configured and executed successfully.");
        return allResults;
    }

    private async insertData(insertInput: InsertInput[], options?: { perRowFallback?: boolean }): Promise<QueryResult[]> {
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

            if (this.db.getConfig().useWorkers && insertInput.length > 1) {
                try {
                    const workerResults = await WorkerHelper.run(this.db.getConfig(), "autoInsertData", insertInput) as { success: boolean; result: QueryResult | QueryInput[], error?: string | Error, errorCode?: string }[];

                    const failed = workerResults.filter(w => !w.success);
                    if (failed.length > 0) {
                        const err = new Error(
                            `Worker execution failed for ${failed.length} task(s):\n` +
                            failed.map((f, i) => `- Task #${i + 1}: ${typeof f?.error === "string" ? f.error : (f?.error?.message || "Unknown Error")}`).join("\n")
                        ) as Error & { code?: string };
                        const withCode = failed.find(f => f.errorCode);
                        if (withCode?.errorCode) err.code = withCode.errorCode;
                        throw err;
                    }

                    insertQueries = workerResults.map(w => w.result);
                } catch (err) {
                    if (err instanceof Error && err.message.startsWith("WORKER_UNAVAILABLE:")) {
                        this.db.warn(err.message + " Falling back to direct execution.");
                        insertQueries = await Promise.all(
                            insertInput.map((input) => this.autoInsertData({ ...input, runQuery: false }))
                        ) as (QueryResult | QueryInput[])[];
                    } else {
                        throw err;
                    }
                }
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
        // Graceful degradation is opt-in and only for the non-atomic direct-insert path: the caller
        // passes perRowFallback and the user configured rejectedRowsTable (the same opt-in the
        // streaming path uses). Without it, a failed batch throws — failing loud is the correct
        // default when the caller hasn't said where bad rows should go. The staging population path
        // (insertData without options) is unaffected and stays all-or-nothing.
        const config = this.db.getConfig();
        if (options?.perRowFallback && config.rejectedRowsTable && allInsertResults.some(r => !r?.success)) {
            return await this.applyPerRowFallback(insertInput, allInsertResults);
        }

        throwIfFailedResults(allInsertResults, "data insert queries")

        return allInsertResults;
    }

    /**
     * Graceful-degradation fallback for the direct-insert path: for each input whose bulk insert
     * failed, retry that input's rows one at a time (widening the schema between rounds) and divert
     * any rows still failing to `rejectedRowsTable`. Only reached when the caller opted in via
     * `perRowFallback` AND `rejectedRowsTable` is set (see insertData). The failed batch ran as a
     * transaction and rolled back, so re-inserting every row is safe (no double-insert).
     */
    private async applyPerRowFallback(insertInput: InsertInput[], allInsertResults: QueryResult[]): Promise<QueryResult[]> {
        const config = this.db.getConfig();
        const maxRetries = config.streamMaxRetries ?? defaults.streamMaxRetries;
        for (let i = 0; i < allInsertResults.length; i++) {
            if (allInsertResults[i]?.success) continue;
            const input = insertInput[i];
            if (!input?.data?.length || !input.metaData) {
                // Nothing to retry per-row (no rows/metadata) — surface the original failure.
                throwIfFailedResults([allInsertResults[i]], "data insert queries");
                continue;
            }
            const insertType = (input.insertType ?? config.insertType ?? 'UPDATE') as 'UPDATE' | 'INSERT';
            // Preserve the already-resolved key columns during the widening re-inference so it can't
            // re-infer different keys for the failed rows.
            const primaryKey = Object.keys(input.metaData).filter(col => input.metaData[col]?.primary);
            this.db.warn(`autoSQL: batch insert for '${input.table}' failed (${allInsertResults[i]?.error ?? 'unknown error'}); retrying per-row with schema widening, diverting unrecoverable rows to '${config.rejectedRowsTable}'.`);
            const { inserted } = await this.perRowInsertWithRetry(
                input.table, input.data, input.metaData, insertType, maxRetries,
                primaryKey.length ? primaryKey : undefined
            );
            allInsertResults[i] = { ...allInsertResults[i], success: true, affectedRows: inserted, error: undefined };
        }
        return allInsertResults;
    }

    /**
     * Insert `rows` one at a time as a graceful-degradation fallback after a bulk insert failed:
     * insert each row individually, and between rounds widen the table schema to fit the rows that
     * failed (up to `maxRetries`). Rows still failing after the last round are diverted to
     * `rejectedRowsTable` if configured, otherwise this throws. Returns the number of rows inserted.
     * Shared by the streaming end() flush and the non-streaming direct-insert path.
     */
    async perRowInsertWithRetry(
        table: string,
        rows: Record<string, any>[],
        metaData: MetadataHeader,
        insertType: 'UPDATE' | 'INSERT',
        maxRetries: number,
        primaryKey?: string[],
        label: string = 'autoSQL'
    ): Promise<{ inserted: number; rejected: Record<string, any>[] }> {
        const config = this.db.getConfig();
        let pendingRows = rows;
        let workingMeta = metaData;
        let totalInserted = 0;
        let round = 0;

        while (pendingRows.length > 0 && round < maxRetries) {
            round++;
            const failures: { row: Record<string, any>; error: string }[] = [];

            for (const row of pendingRows) {
                const insertQ = this.db.getInsertStatementQuery(table, [row], workingMeta, insertType);
                // Single attempt: this loop already retries failed rows across rounds, so the internal
                // retry would be redundant — and for insertType "INSERT" (non-idempotent) it could
                // duplicate a row whose ambiguous failure actually applied server-side (A15).
                const result = await this.db.runQuery(insertQ, 1);
                if (result.success) {
                    totalInserted += result.affectedRows ?? 1;
                } else {
                    failures.push({ row, error: result.error ?? 'unknown error' });
                }
            }

            // Remaining work = only the rows that failed this round. Assign BEFORE the break so a
            // fully-successful round leaves nothing pending — otherwise the successfully-inserted
            // rows would still be sitting in pendingRows and get diverted as "rejects" below.
            pendingRows = failures.map(f => f.row);
            if (pendingRows.length === 0) break;

            if (round < maxRetries) {
                // Widen the schema to fit the rows that failed, then loop retries only those rows.
                const failedMeta = await getMetaData(config, pendingRows, primaryKey);
                const { changes, updatedMetaData } = compareMetaData(workingMeta, failedMeta, this.db.getDialectConfig(), config.logger);
                if (tableChangesExist(changes)) {
                    const widenInput = [{
                        table,
                        data: pendingRows,
                        metaData: updatedMetaData,
                        previousMetaData: workingMeta,
                        comparedMetaData: { changes, updatedMetaData },
                        stagingPrefix: config.stagingPrefix,
                        historyTableSuffix: config.historyTableSuffix,
                    }];
                    await this.configureTables(widenInput).catch(e =>
                        this.db.warn(`${label}: schema widening attempt failed: ${e.message}`)
                    );
                    workingMeta = updatedMetaData;
                }
            }
        }

        if (pendingRows.length > 0) {
            if (config.rejectedRowsTable) {
                const bootstrap = await this.db.runTransaction(buildBootstrapRejectedRowsQuery(config));
                const rejQ = buildInsertRejectedRowsQuery(
                    config, table,
                    pendingRows.map(row => ({ row, error: 'failed after max retries' }))
                );
                // Fail loud if the divert itself fails (bootstrap or insert). runTransaction never
                // throws — it returns {success:false} — so an unchecked result would let the rows
                // vanish while the load reported success, the exact loss rejectedRowsTable exists to
                // prevent (A5).
                const divert = bootstrap.success ? await this.db.runTransaction([rejQ]) : bootstrap;
                if (!divert.success) {
                    throw new Error(`${label}: ${pendingRows.length} row(s) failed to insert AND could not be written to rejectedRowsTable '${config.rejectedRowsTable}': ${divert.error ?? 'unknown error'}. No rows were silently dropped — resolve the rejects-table error (e.g. permissions or an incompatible existing table) and retry.`);
                }
                this.db.warn(`${label}: ${pendingRows.length} row(s) could not be inserted and were written to '${config.rejectedRowsTable}'.`);
            } else {
                throw new Error(`${label}: ${pendingRows.length} row(s) failed to insert after ${maxRetries} retry round(s). Configure rejectedRowsTable to capture them instead of throwing.`);
            }
        }

        // `rejected` is the set of rows diverted to rejectedRowsTable (empty when all rows landed).
        // The staging path uses it to compensate row-level history for rows that never merged.
        return { inserted: totalInserted, rejected: pendingRows };
    }

    private async prepareStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
        const stagingPrefix = insertInput[0]?.stagingPrefix;
        const uniqueTables = Array.from(new Set(insertInput.map(input => input.table)));

        const stagingQueries: QueryInput[][] = uniqueTables.map(table => {
            // Drop any leftover staging table BEFORE recreating it. A prior run that crashed before
            // removeStagingTables leaves an orphan; the create uses CREATE TABLE IF NOT EXISTS, which
            // would then reuse that orphan's stale schema and corrupt this load. Dropping first
            // guarantees the temp table always matches the current (just-configured) real table.
            const tempTableName = getTempTableName(table, stagingPrefix);
            return [this.db.getDropTableQuery(tempTableName), this.db.getCreateTempTableQuery(table, stagingPrefix)];
        });
        const allCreateResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingQueries);

        throwIfFailedResults(allCreateResults, "table create queries")
        return allCreateResults
    }

    private async insertStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
        // The staging temp table was just created with CREATE TABLE AS SELECT from the
        // already-configured real table, so its columns already match. Re-applying the real
        // table's ALTER changes (addColumns/modifyColumns) here would try to add columns the
        // CTAS copy already has ("duplicate column" / "already exists"). Clear the changes so
        // staging configuration is a no-op, but keep updatedMetaData so the insert still resolves
        // per-column values (e.g. the dwh_* timestamp defaults) for the temp table.
        const emptyChanges: AlterTableChanges = {
            addColumns: {}, modifyColumns: {}, dropColumns: [], renameColumns: [],
            nullableColumns: [], noLongerUnique: [], primaryKeyChanges: [],
        };
        const stagingInputs: InsertInput[] = insertInput.map(input => ({
            ...input,
            table: getTempTableName(input.table, input.stagingPrefix),
            insertType: "INSERT",
            previousMetaData: emptyChanges,
            comparedMetaData: input.comparedMetaData
                ? { changes: emptyChanges, updatedMetaData: input.comparedMetaData.updatedMetaData }
                : undefined,
        }));
        // Configure staging tables where necessary (no-op schema changes; CTAS already matched)
        await this.configureTables(stagingInputs)
        if (this.db.getConfig().bulkLoad) {
            return await this.bulkLoadStaging(stagingInputs)
        }
        return await this.insertData(stagingInputs)
    }

    /**
     * Populate the staging temp tables with the dialect's bulk-copy mechanism (Postgres COPY /
     * MySQL LOAD DATA LOCAL INFILE) instead of parameterised INSERT — the opt-in `bulkLoad` fast
     * path. The subsequent merge (temp → real) is unchanged, so upsert semantics are preserved. If a
     * table's bulk load fails for any reason (server local_infile off, missing pg-copy-streams, a
     * value the text protocol rejects), it falls back to parameterised INSERT for that table so the
     * load still completes. COPY is all-or-nothing, so the temp table is empty when the fallback runs.
     */
    private async bulkLoadStaging(stagingInputs: InsertInput[]): Promise<QueryResult[]> {
        const config = this.db.getConfig();
        const dialectConfig = this.db.getDialectConfig();
        const excludeAutoIncrement = config.surrogateKey === true;
        const results: QueryResult[] = [];
        for (const input of stagingInputs) {
            const header = input.comparedMetaData?.updatedMetaData || input.metaData;
            // Same column set the INSERT path uses: drop auto-increment (surrogate) columns so the
            // DB assigns them. getInsertValues drops the same columns from each value row, so the
            // columns and value arrays stay aligned.
            const columns = Object.keys(header).filter(col => !(excludeAutoIncrement && header[col].autoIncrement === true));
            const valueRows = input.data.map(row => getInsertValues(header, row, dialectConfig, config, true));
            try {
                results.push(await this.db.bulkLoadRows(input.table, columns, valueRows));
            } catch (err) {
                this.db.warn(`bulkLoad failed for '${input.table}', falling back to INSERT: ${(err as Error).message}`);
                results.push(...await this.insertData([input]));
            }
        }
        return results;
    }

    private async removeStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
        const stagingPrefix = insertInput[0]?.stagingPrefix;
        const uniqueTables = Array.from(new Set(insertInput.map(input => input.table)));

        const stagingQueries: QueryInput[][] = uniqueTables.map(table => {
            const tempTableName = getTempTableName(table, stagingPrefix);
            return [this.db.getDropTableQuery(tempTableName)];
        });
        const allDropResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingQueries);

        throwIfFailedResults(allDropResults, "table drop queries")
        return allDropResults
    }

    private async resolveConflicts(insertInput: InsertInput[]): Promise<void> {
        const stagingPrefix = insertInput[0]?.stagingPrefix;
        const uniqueTables = Array.from(new Set(insertInput.map(input => input.table)));

        // First input seen per table — its resolved metadata drives the metadata-derived path.
        const inputByTable = new Map<string, InsertInput>();
        for (const input of insertInput) {
            if (!inputByTable.has(input.table)) inputByTable.set(input.table, input);
        }

        const tableStructure: Record<string, {
            uniques: Record<string, string[]>,
            primary: string[]
        }> = {};

        // Derive the constraint structure (non-primary unique indexes → columns, plus primary-key
        // columns) from already-known metadata where it is provably identical to the live catalog
        // (see deriveConstraintStructure), and fall back to live introspection otherwise. Deriving
        // skips the unique-index + primary-key introspection round-trip on the common path — an
        // idempotent re-ingest of a stable schema, where the metadata was just read from the DB.
        const tablesToIntrospect: string[] = [];
        for (const table of uniqueTables) {
            const derived = this.deriveConstraintStructure(inputByTable.get(table));
            if (derived) tableStructure[table] = derived;
            else tablesToIntrospect.push(table);
        }

        if (tablesToIntrospect.length > 0) {
            const uniqueIndexesQuery = tablesToIntrospect.map(table => [this.db.getUniqueIndexesQuery(table)]);
            const primaryKeyQuery = tablesToIntrospect.map(table => [this.db.getPrimaryKeysQuery(table)]);
            // Run the unique-index and primary-key introspection as ONE concurrency-governed batch
            // instead of two sequential round-trips. Both are independent reads; a single
            // runTransactionsWithConcurrency call overlaps them under one pool-size cap. Results come
            // back in input order, so the first N groups are unique indexes and the next N are PKs.
            const introspection : QueryResult[] = await this.db.runTransactionsWithConcurrency([
                ...uniqueIndexesQuery,
                ...primaryKeyQuery,
            ]);
            const allUniqueKeys : QueryResult[] = introspection.slice(0, tablesToIntrospect.length);
            const allPrimaryKeys : QueryResult[] = introspection.slice(tablesToIntrospect.length);

            for (let i = 0; i < tablesToIntrospect.length; i++) {
                const table = tablesToIntrospect[i];
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

                const structure = tableStructure[table] ?? { uniques: {}, primary: [] };
                normalizedUniques.forEach(result => {
                    structure.uniques[result.index_name] = (result.columns as string)
                        .split(",")
                        .map(col => col.trim());
                });
                normalizedPrimary.forEach(result => {
                    structure.primary.push(result.column_name);
                });
                tableStructure[table] = structure;
            }
        }

        // Only tables that actually have a unique index AND a primary key need a conflict check.
        // Carry the table name alongside each query so results correlate robustly even when some
        // tables are filtered out here.
        const conflictTables = Object.keys(tableStructure).filter(table => {
            const structure = tableStructure[table];
            return (structure && Object.keys(structure.uniques || {}).length > 0 && // at least one unique constraint
            Array.isArray(structure.primary) && structure.primary.length > 0 // at least one primary key
            );
        });
        const conflictsQuery = conflictTables.map(table => {
            return [this.db.getConstraintConflictQuery(table, tableStructure[table], stagingPrefix)];
        });

        const allConflicts : QueryResult[] = await this.db.runTransactionsWithConcurrency(conflictsQuery);
        let removeConstraintsQuery : QueryInput[][] = []

        for (let i = 0; i < allConflicts.length; i++) {
          const result = allConflicts[i];
          const table = conflictTables[i];
          let tableConstraintsQueries : QueryInput[] = []

          const row = result?.results?.[0] || {};
          const violatingIndexes: string[] = [];
          const dropUniques = this.db.getConfig().dropUniqueConstraints === true;

          for (const [indexName, count] of Object.entries(row)) {
            const numericCount = typeof count === "string" ? parseInt(count) : Number(count);
            if (numericCount > 0) {
                violatingIndexes.push(indexName);
                // Only queue the DROP when opted in (A10). Off (default) → keep the constraint; the
                // merge then fails loud / diverts on the collision.
                if (dropUniques) tableConstraintsQueries.push(this.db.getDropUniqueConstraintQuery(table, indexName))
            }
          }

          if (violatingIndexes.length) {
            if (dropUniques) {
              this.db.warn(`resolveConflicts: dropping UNIQUE constraint(s) [${violatingIndexes.join(", ")}] on '${table}' — staged data violates them and dropUniqueConstraints is on.`);
              removeConstraintsQuery.push(tableConstraintsQueries)
            } else {
              this.db.warn(`resolveConflicts: staged data for '${table}' violates UNIQUE constraint(s) [${violatingIndexes.join(", ")}], but dropUniqueConstraints is off — the constraint(s) are KEPT and the merge will fail (or divert to rejectedRowsTable if configured) on the colliding rows. Set dropUniqueConstraints: true to auto-drop them instead.`);
            }
          }
        }

        const removeConstraints : QueryResult[] = await this.db.runTransactionsWithConcurrency(removeConstraintsQuery);
        throwIfFailedResults(removeConstraints, 'unique constraint removal queries')
        return;
    }

    /**
     * Derive a table's drop-target constraint structure (non-primary unique indexes → their
     * columns, plus the primary-key columns) from the resolved metadata, WITHOUT a catalog
     * round-trip — but only when it is provably identical to the live catalog. Returns null (→ the
     * caller introspects live) whenever the run changed the constraint structure or any unique lacks
     * a real, introspected name:
     *   • no compared/resolved metadata to reason about;
     *   • a unique was dropped this run (`noLongerUnique`) — `updatedMetaData` still shows it unique;
     *   • the primary key changed this run (`primaryKeyChanges`) — the conflict-count join keys on it;
     *   • a newly-added column is unique — its index name isn't introspected yet;
     *   • any unique column lacks a `uniqueName` — inferred/just-created uniques (incl. every first
     *     load) and MySQL's column-named uniques can't be reproduced, so the DROP target is unknown;
     *   • any column belongs to MORE THAN ONE non-primary unique index (`uniqueName` is a
     *     comma-joined list) — the per-column single-name model can't unambiguously reconstruct each
     *     composite index's full column set, so grouping could mis-scope (and thus over-drop) a
     *     constraint. autosql only ever creates single-column uniques, so this is external-table-only.
     * These are exactly the cases where deriving could drop the wrong (or an already-gone) constraint
     * — the silent over-drop the introspection fallback exists to avoid.
     */
    private deriveConstraintStructure(input?: InsertInput): { uniques: Record<string, string[]>, primary: string[] } | null {
        if (!input) return null;
        const changes = input.comparedMetaData?.changes;
        const meta = input.comparedMetaData?.updatedMetaData || input.metaData;
        if (!changes || !meta) return null;
        if (changes.noLongerUnique?.length) return null;
        if (changes.primaryKeyChanges?.length) return null;
        if (changes.addColumns && Object.values(changes.addColumns).some(col => col?.unique)) return null;

        const uniques: Record<string, string[]> = {};
        const primary: string[] = [];
        for (const [columnName, def] of Object.entries(meta)) {
            if (!def) continue;
            if (def.primary) primary.push(columnName);
            // A column marked unique but without a real DB index name means we don't know the DROP
            // target (a just-created or inferred unique) — bail to live introspection.
            if (def.unique && !def.uniqueName) return null;
            if (def.uniqueName) {
                // A comma means the column is in >1 non-primary unique index — the single-name model
                // can't group composite indexes unambiguously, so fall back rather than mis-scope.
                if (def.uniqueName.includes(',')) return null;
                (uniques[def.uniqueName] ??= []).push(columnName);
            }
        }
        return { uniques, primary };
    }

    private async insertFromStagingTables(insertInput: InsertInput[], options?: { perRowFallback?: boolean }): Promise<QueryResult[]> {
        const stagingInputs: InsertInput[] = insertInput.map(input => ({
            ...input,
            insertType: "UPDATE"
        }));
        const stagingInsertQueries = (stagingInputs).map(stagingInput => {
            return [this.db.getInsertFromStagingQuery(stagingInput)]
        })
        const allInsertResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingInsertQueries);

        // Opt-in graceful degradation WITHOUT row-level history: when perRowFallback is passed AND
        // rejectedRowsTable is configured AND the atomic merge failed for a table, retry that table's
        // rows one at a time and divert unrecoverable rows. Without the opt-in this stays
        // all-or-nothing (the current default). The addHistory case uses the atomic path instead
        // (insertFromStagingTablesAtomic), so history and data stay transactionally consistent.
        const config = this.db.getConfig();
        if (options?.perRowFallback && config.rejectedRowsTable && allInsertResults.some(r => !r?.success)) {
            return await this.applyStagingPerRowFallback(insertInput, allInsertResults);
        }

        throwIfFailedResults(allInsertResults, 'insert from staging table queries')
        return allInsertResults
    }

    /**
     * Graceful-degradation fallback for the STAGING path WITHOUT row-level history (opt-in, see
     * insertFromStagingTables): the atomic merge rolled back, so nothing from a failed table landed.
     * Re-run that table's rows one at a time as an upsert into the real table (matching the merge's
     * UPDATE semantics), diverting unrecoverable rows to `rejectedRowsTable`.
     */
    private async applyStagingPerRowFallback(insertInput: InsertInput[], allInsertResults: QueryResult[]): Promise<QueryResult[]> {
        const config = this.db.getConfig();
        const maxRetries = config.streamMaxRetries ?? defaults.streamMaxRetries;
        for (let i = 0; i < allInsertResults.length; i++) {
            if (allInsertResults[i]?.success) continue;
            const input = insertInput[i];
            if (!input?.data?.length || !input.metaData) {
                // Nothing to retry per-row — surface the original failure.
                throwIfFailedResults([allInsertResults[i]], 'insert from staging table queries');
                continue;
            }
            const primaryKey = Object.keys(input.metaData).filter(col => input.metaData[col]?.primary);
            this.db.warn(`autoSQL: staged merge for '${input.table}' failed (${allInsertResults[i]?.error ?? 'unknown error'}); retrying per-row, diverting unrecoverable rows to '${config.rejectedRowsTable}'.`);
            const { inserted } = await this.perRowInsertWithRetry(
                input.table, input.data, input.metaData, 'UPDATE', maxRetries,
                primaryKey.length ? primaryKey : undefined
            );
            allInsertResults[i] = { ...allInsertResults[i], success: true, affectedRows: inserted, error: undefined };
        }
        return allInsertResults;
    }

    /**
     * Zero-window staging merge WITH row-level history (case 3: rejectedRowsTable + addHistory). The
     * before-image capture and the merge run in ONE transaction, so history and data commit — or roll
     * back — together, with no crash window between them. Per table: attempt the whole-table
     * [before-image, merge] transaction; if it fails, fall back to a per-PK loop where each PK's
     * [before-image, single-PK merge] is its own transaction — a PK whose merge violates a constraint
     * rolls back (no history, no data) and is diverted to `rejectedRowsTable`. `historyByTable` maps a
     * real table to its (already-created) history input; a table not in `historyTables` merges with no
     * before-image.
     */
    private async insertFromStagingTablesAtomic(insertInput: InsertInput[], historyInputs: InsertInput[]): Promise<QueryResult[]> {
        const historyByTable = new Map<string, InsertInput>();
        for (const h of historyInputs) {
            historyByTable.set(getTrueTableName(h.table, h.stagingPrefix, h.historyTableSuffix), h);
        }
        const stagingInputs: InsertInput[] = insertInput.map(input => ({ ...input, insertType: "UPDATE" }));

        // Whole-table attempt: [before-image (if history-eligible), merge] as one transaction per table.
        const groups: QueryInput[][] = stagingInputs.map((stagingInput, i) => {
            const group: QueryInput[] = [];
            const historyInput = historyByTable.get(insertInput[i].table);
            if (historyInput) group.push(this.db.getInsertChangedRowsToHistoryQuery(historyInput));
            group.push(this.db.getInsertFromStagingQuery(stagingInput));
            return group;
        });
        const allResults: QueryResult[] = await this.db.runTransactionsWithConcurrency(groups);

        for (let i = 0; i < allResults.length; i++) {
            if (allResults[i]?.success) continue;
            allResults[i] = await this.perPkAtomicStagingMerge(insertInput[i], stagingInputs[i], historyByTable.get(insertInput[i].table));
        }
        return allResults;
    }

    /**
     * Per-PK fallback for the atomic history path: for each row, run [before-image for that PK,
     * single-PK merge] in ONE transaction. A PK whose merge violates a constraint rolls the whole
     * transaction back (no history, no data) and is diverted to `rejectedRowsTable`. No schema
     * widening is attempted here (unlike the shared `perRowInsertWithRetry`): `configureTables`
     * already fitted the schema to every row before the merge, so a failure here is a data/constraint
     * issue a re-inference could not fix.
     */
    private async perPkAtomicStagingMerge(input: InsertInput, stagingInput: InsertInput, historyInput?: InsertInput): Promise<QueryResult> {
        const config = this.db.getConfig();
        const dialectConfig = this.db.getDialectConfig();
        const header = input.comparedMetaData?.updatedMetaData || input.metaData;
        const pkCols = Object.keys(header).filter(col => header[col]?.primary);
        if (!pkCols.length) {
            throw new Error(`autoSQL: staged merge for '${input.table}' failed and cannot degrade per-row (no primary key to divert on).`);
        }
        this.db.warn(`autoSQL: staged merge for '${input.table}' failed; retrying per-row atomically (before-image + merge in one transaction), diverting unrecoverable rows to '${config.rejectedRowsTable}'.`);

        let inserted = 0;
        const rejected: Record<string, any>[] = [];
        for (const row of input.data) {
            const pkFilter: Record<string, any> = {};
            for (const pk of pkCols) pkFilter[pk] = sqlize(row[pk], header[pk].type, dialectConfig, config);
            const group: QueryInput[] = [];
            if (historyInput) group.push(this.db.getInsertChangedRowsToHistoryQuery(historyInput, undefined, pkFilter));
            group.push(this.db.getInsertFromStagingQuery(stagingInput, undefined, undefined, pkFilter));
            const result = await this.db.runTransaction(group);
            if (result.success) inserted += 1; // one data row per PK landed (history rows aren't counted)
            else rejected.push(row);
        }

        if (rejected.length) {
            if (!config.rejectedRowsTable) {
                throw new Error(`autoSQL: ${rejected.length} row(s) failed to merge into '${input.table}' and rejectedRowsTable is not configured.`);
            }
            const bootstrap = await this.db.runTransaction(buildBootstrapRejectedRowsQuery(config));
            const divert = bootstrap.success
                ? await this.db.runTransaction([
                    buildInsertRejectedRowsQuery(config, input.table, rejected.map(row => ({ row, error: 'failed to merge after per-row retry' })))
                  ])
                : bootstrap;
            // Fail loud if the divert itself fails — otherwise the rows vanish while the load reports
            // success (A5). runTransaction returns {success:false} rather than throwing, so this must be
            // checked explicitly.
            if (!divert.success) {
                throw new Error(`autoSQL: ${rejected.length} row(s) failed to merge into '${input.table}' AND could not be written to rejectedRowsTable '${config.rejectedRowsTable}': ${divert.error ?? 'unknown error'}. No rows were silently dropped — resolve the rejects-table error (e.g. permissions or an incompatible existing table) and retry.`);
            }
            this.db.warn(`autoSQL: ${rejected.length} row(s) could not be merged into '${input.table}' and were written to '${config.rejectedRowsTable}'.`);
        }
        const now = new Date();
        return { start: now, end: now, duration: 0, success: true, affectedRows: inserted };
    }

    private async insertToHistoryTables(insertInputs: InsertInput[]): Promise<QueryResult[]> {
        const stagingInsertQueries = (insertInputs).map(insertInput => {
            return [this.db.getInsertChangedRowsToHistoryQuery(insertInput)]
        })
        const allInsertResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingInsertQueries);
        throwIfFailedResults(allInsertResults, 'insert from staging table queries')
        return allInsertResults
    }

    /**
     * Build the per-table history inputs (cleaned metadata + `dwh_as_at` PK) for the tables in
     * `historyTables`. Shared by `insertHistory` (non-atomic path) and `configureHistoryTables`
     * (atomic path). Returns [] when history is off or no eligible table is present.
     */
    private async buildHistoryInputs(insertInput: InsertInput[]): Promise<InsertInput[]> {
        const config = this.db.getConfig();
        if (!config.addHistory || !config.historyTables?.length) return [];
        if (!config.useStagingInsert) { throw new Error('Cannot add history tables without using staging insert'); }

        const uniqueTables = Array.from(new Set(insertInput.map(input => input.table)));
        const eligibleInputs = uniqueTables.filter(table => config.historyTables!.includes(table));
        if (eligibleInputs.length == 0) { return []; }

        return await Promise.all(
            eligibleInputs.map(async (table) => {
              const matchingInput = insertInput.find(i => i.table === table);
              const historyName = getHistoryTableName(table, matchingInput?.historyTableSuffix);

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
              cleanedMeta["dwh_as_at"] = { type: "datetime", allowNull: false, primary: true };

              const automatedColumns = ['dwh_created_at', 'dwh_modified_at', 'dwh_loaded_at']
              // ✅ Ensure existing PKs are retained
              for (const col in currentMetaData) {
                if (currentMetaData[col].primary) { cleanedMeta[col].primary = true; }
                if (automatedColumns.includes(col)) { cleanedMeta[col].calculated = true }
              }

              return {
                table: historyName,
                data: [],
                metaData: cleanedMeta,
                previousMetaData: currentHistoryMetaData,
                // Carry the prefix/suffix so the history/temp/real table names resolve correctly (incl.
                // custom prefixes) both in the before-image query and in the atomic path's table map.
                stagingPrefix: matchingInput?.stagingPrefix,
                historyTableSuffix: matchingInput?.historyTableSuffix,
              };
            })
        );
    }

    private async insertHistory(insertInput: InsertInput[]): Promise<QueryResult[]> {
        const historyInputs = await this.buildHistoryInputs(insertInput);
        if (!historyInputs.length) return [];
        await this.configureTables(historyInputs)
        return await this.insertToHistoryTables(historyInputs)
    }

    /**
     * Create the history tables (DDL only) and return their inputs, for the ATOMIC history path — the
     * before-image INSERT itself is deferred into the merge transaction (see
     * `insertFromStagingTablesAtomic`). Errors up-front on SQL Server, whose row-level history is
     * unverified (D-F), so the opt-in combo fails before any partial work.
     */
    private async configureHistoryTables(insertInput: InsertInput[]): Promise<InsertInput[]> {
        if (this.db.getConfig().sqlDialect === 'sqlserver') {
            throw new Error('Staging-path per-row degradation (rejectedRowsTable) with addHistory is not supported for SQL Server.');
        }
        const historyInputs = await this.buildHistoryInputs(insertInput);
        if (!historyInputs.length) return [];
        await this.configureTables(historyInputs);
        return historyInputs;
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
                comparedMetaData = compareMetaData(currentMetaData || null, newMetaData, this.db.getDialectConfig(), this.db.getConfig().logger);
            }
            const insertInput : InsertInput = {
                table: nestedTable,
                data: nestedRows,
                previousMetaData: changes || currentMetaData,
                metaData: mergedMetaData,
                comparedMetaData,
                stagingPrefix: this.db.getConfig().stagingPrefix,
                historyTableSuffix: this.db.getConfig().historyTableSuffix,
            };
            nestedInputs.push(insertInput);
        }
        return nestedInputs;
    } 
    
    async autoSQL(table: string, data: Record<string, any>[], schema?: string, primaryKey?: string[], options?: AutoSQLOptions): Promise<QueryResult> {
      return this.db.runWithSchema(schema, async () => {
        const start = new Date();
        const config = this.db.getConfig();
        const useSchemaLock = config.useSchemaLock;
        const lockTimeout = config.schemaLockTimeout ?? defaults.schemaLockTimeout;
        const useHistory = config.schemaHistory;

        try {
            let affectedRows: number;
            let insertResults: QueryResult[];
            let insertInput: InsertInput[];

            // Per-run instrumentation (QueryStats): wall-clock per phase. `perf` is monotonic and
            // sub-millisecond, independent of the Date-based start/end used for the result envelope.
            const perf = () => (typeof performance !== "undefined" ? performance.now() : Date.now());
            const phases: { prepare?: number; configure?: number; load?: number } = {};

            if (useSchemaLock) await this.db.acquireSchemaLock(table, lockTimeout);
            let historyId: number | undefined;
            try {
                const tPrepare = perf();
                insertInput = await this.prepareInsertData(table, data, schema, primaryKey, options);
                let nestedInputs = await this.extractNestedInputs(insertInput);
                insertInput = [...insertInput, ...nestedInputs];
                phases.prepare = perf() - tPrepare;

                // Schema history: drift detection + record start
                if (useHistory) {
                    if (config.detectDrift ?? true) {
                        await _detectSchemaDrift(this.db, table).catch(e => { throw e; });
                    }
                    await bootstrapSchemaHistoryTable(this.db);
                    const primary = insertInput[0];
                    if (primary?.comparedMetaData && tableChangesExist(primary.comparedMetaData.changes)) {
                        historyId = await recordMigrationStart(
                            this.db, table,
                            (primary.previousMetaData && !Array.isArray(primary.previousMetaData) && 'addColumns' in primary.previousMetaData ? {} : primary.previousMetaData) as any || {},
                            primary.comparedMetaData.changes
                        );
                    }
                }

                try {
                    const tConfigure = perf();
                    await this.configureTables(insertInput);
                    phases.configure = perf() - tConfigure;
                    if (historyId !== undefined) {
                        const updatedMeta = insertInput[0]?.comparedMetaData?.updatedMetaData;
                        if (updatedMeta) {
                            // Store the drift baseline as the checksum of the RE-INTROSPECTED table, not
                            // the inferred `updatedMeta` — drift detection next run reads the introspected
                            // schema, and inferred-vs-introspected would false-positive on legitimate type
                            // round-trips (A6). `new_schema` stays `updatedMeta` (inferred) for
                            // point-in-time reconstruction; a null introspection stores a null checksum
                            // (treated as "no baseline"), which also covers the A19 couldn't-read case.
                            const liveMeta = await this.db.getTableMetaData(config.schema || config.database || "", table);
                            await recordMigrationSuccess(this.db, historyId, updatedMeta, liveMeta);
                        }
                        else await recordMigrationRolledBack(this.db, historyId);
                    }
                } catch (ddlErr) {
                    if (historyId !== undefined) {
                        await recordMigrationFailed(this.db, historyId).catch(() => {});
                    }
                    throw ddlErr;
                }
            } finally {
                if (useSchemaLock) await this.db.releaseSchemaLock(table);
            }

            const tLoad = perf();
            if (config.useStagingInsert) {
                // Case 3 — opt-in per-row degradation (rejectedRowsTable) WITH row-level history — uses
                // the zero-window atomic path (before-image + merge in one transaction). Every other
                // staging load keeps the existing insertHistory-then-merge flow unchanged.
                const historyDegradation = !!(config.rejectedRowsTable && config.addHistory && config.historyTables?.length);
                try {
                    await this.prepareStagingTables(insertInput);
                    await this.insertStagingTables(insertInput);
                    await this.resolveConflicts(insertInput);
                    if (historyDegradation) {
                        const historyInputs = await this.configureHistoryTables(insertInput);
                        insertResults = await this.insertFromStagingTablesAtomic(insertInput, historyInputs);
                    } else {
                        await this.insertHistory(insertInput);
                        insertResults = await this.insertFromStagingTables(insertInput, { perRowFallback: !!config.rejectedRowsTable });
                    }
                } finally {
                    // Always drop the staging table, even if a step above threw, so a failed load
                    // doesn't leave an orphaned temp table behind (mirrors the streaming end() path).
                    await this.removeStagingTables(insertInput).catch(e => this.db.error(`autoSQL: failed to drop staging table(s) for '${table}': ${e instanceof Error ? e.message : String(e)}`));
                }
            } else {
                insertResults = await this.insertData(insertInput, { perRowFallback: true });
            }
            phases.load = perf() - tLoad;

            affectedRows = insertResults.reduce((sum, res) => sum + (res.affectedRows || 0), 0);
            const allResults = insertResults.flatMap(res => res.results || []);
            const end = new Date();
            // Return the resolved schema (incl. managed dwh_*/surrogate columns) so callers can
            // cache it and skip re-introspection next load — the `existingSchema` fast path.
            const resolvedMetaData = insertInput[0]?.comparedMetaData?.updatedMetaData || insertInput[0]?.metaData;

            // Per-run metrics: attach to the result (so the caller can store them) and emit to the
            // structured stats sink if one is configured.
            const durationMs = end.getTime() - start.getTime();
            const stats: QueryStats = {
                table,
                rows: data.length,
                affectedRows,
                durationMs,
                rowsPerSecond: durationMs > 0 ? Math.round((data.length / durationMs) * 1000) : 0,
                phases: {
                    prepare: phases.prepare !== undefined ? Math.round(phases.prepare) : undefined,
                    configure: phases.configure !== undefined ? Math.round(phases.configure) : undefined,
                    load: phases.load !== undefined ? Math.round(phases.load) : undefined,
                },
                staged: !!config.useStagingInsert,
                bulkLoad: !!config.bulkLoad,
            };
            try { config.logger?.stats?.(stats); } catch { /* a metrics sink must never break a load */ }

            return { start, end, success: true, duration: durationMs, affectedRows, results: allResults, table, metaData: resolvedMetaData, stats };
        } catch (error: any) {
            const end = new Date();
            return { start, end, duration: end.getTime() - start.getTime(), affectedRows: 0, success: false, error: error instanceof Error ? error.message : String(error), errorCode: (error as any)?.code != null ? String((error as any).code) : undefined };
        }
      });
    }

    /**
     * Insert data from an async iterable of chunks into a table.
     *
     * The first chunk drives full schema inference and table configuration.
     * Subsequent chunks reuse the schema established by the first chunk,
     * bypassing inference and DDL entirely — making them significantly faster.
     *
     * When `useSchemaLock: true`, the advisory lock is held only during the
     * first-chunk schema inference + DDL phase, then released before inserts begin.
     *
     * @param table     Target table name
     * @param chunks    Async iterable of row arrays (each array is one chunk)
     * @param schema    Optional schema/database override for this call
     * @param primaryKey Optional primary key hint passed to schema inference
     */
    async autoSQLChunked(
        table: string,
        chunks: AsyncIterable<Record<string, any>[]>,
        schema?: string,
        primaryKey?: string[]
    ): Promise<QueryResult> {
      return this.db.runWithSchema(schema, async () => {
        const start = new Date();
        const config = this.db.getConfig();
        const useSchemaLock = config.useSchemaLock;
        const lockTimeout = config.schemaLockTimeout ?? defaults.schemaLockTimeout;

        let totalAffectedRows = 0;
        let lockedInsertInput: InsertInput[] | null = null;

        // Per-run instrumentation (QueryStats), aggregated across chunks: prepare/configure are the
        // first-chunk schema inference + DDL; load accumulates every chunk's insert time.
        const perf = () => (typeof performance !== "undefined" ? performance.now() : Date.now());
        const phases: { prepare?: number; configure?: number; load?: number } = {};
        let totalRows = 0;

        try {
            for await (const chunk of chunks) {
                if (chunk.length === 0) continue;
                totalRows += chunk.length;

                let chunkInsertInput: InsertInput[];

                if (lockedInsertInput === null) {
                    // First chunk: full schema inference + table configuration (the table is new).
                    if (useSchemaLock) await this.db.acquireSchemaLock(table, lockTimeout);
                    try {
                        const tPrepare = perf();
                        chunkInsertInput = await this.prepareInsertData(table, chunk, schema, primaryKey);
                        const nestedInputs = await this.extractNestedInputs(chunkInsertInput);
                        chunkInsertInput = [...chunkInsertInput, ...nestedInputs];
                        phases.prepare = perf() - tPrepare;
                        const tConfigure = perf();
                        await this.configureTables(chunkInsertInput);
                        phases.configure = perf() - tConfigure;
                    } finally {
                        if (useSchemaLock) await this.db.releaseSchemaLock(table);
                    }
                    lockedInsertInput = chunkInsertInput;
                } else {
                    // Subsequent chunks: reuse the locked schema, but GUARD AGAINST DRIFT. Locking the
                    // first chunk's inferred types means a later chunk can carry a value the locked
                    // column can't hold (e.g. ids 1..100 -> tinyint, then id 128 overflows). Re-infer
                    // this chunk (cheap) and compare ONLY the chunk's data columns against the locked
                    // schema — the managed columns (dwh_* timestamps, a surrogate) aren't in the chunk
                    // and must not be seen as "to drop". Widen (ALTER + update the lock) only when a
                    // column actually needs it; a no-drift chunk skips the DDL entirely.
                    const lockedMeta = lockedInsertInput[0].metaData as MetadataHeader;
                    const tPrepare = perf();
                    const inferred = await getMetaData(config, chunk, primaryKey);
                    phases.prepare = (phases.prepare ?? 0) + (perf() - tPrepare);

                    const lockedDataOnly: MetadataHeader = {};
                    for (const col of Object.keys(inferred)) if (lockedMeta[col]) lockedDataOnly[col] = lockedMeta[col];
                    const { changes } = compareMetaData(lockedDataOnly, inferred, this.db.getDialectConfig(), config.logger);

                    // Drift for a chunked load means only "the locked column can't hold this chunk's
                    // data": a widened/nullable-relaxed column (modifyColumns) or a brand-new column
                    // (addColumns). Keys are locked from the first chunk — a later chunk's per-chunk
                    // uniqueness/primary re-evaluation must NOT drive DDL.
                    const drift = Object.keys(changes.modifyColumns).length > 0 || Object.keys(changes.addColumns).length > 0;
                    if (drift) {
                        this.db.warn(`autoSQLChunked: schema drift in a later chunk for '${table}' — widening the table to fit.`);
                        // Overlay the widened / newly-added data columns onto the full locked schema so
                        // managed columns are preserved; autoConfigureTable re-diffs and ALTERs cleanly.
                        const widened: MetadataHeader = { ...lockedMeta, ...changes.modifyColumns, ...changes.addColumns };
                        const driftInput: InsertInput = { ...lockedInsertInput[0], data: chunk, previousMetaData: lockedMeta, metaData: widened };
                        if (useSchemaLock) await this.db.acquireSchemaLock(table, lockTimeout);
                        try {
                            const tConfigure = perf();
                            await this.configureTables([driftInput]);
                            phases.configure = (phases.configure ?? 0) + (perf() - tConfigure);
                        } finally {
                            if (useSchemaLock) await this.db.releaseSchemaLock(table);
                        }
                        lockedInsertInput = lockedInsertInput.map((inp, i) => i === 0 ? { ...inp, metaData: widened } : inp);
                    }

                    chunkInsertInput = lockedInsertInput.map(input => ({ ...input, data: chunk }));
                }

                let insertResults: QueryResult[];
                const tLoad = perf();
                if (this.db.getConfig().useStagingInsert) {
                    // See the non-chunked path: case 3 (rejectedRowsTable + addHistory) uses the atomic path.
                    const historyDegradation = !!(config.rejectedRowsTable && config.addHistory && config.historyTables?.length);
                    try {
                        await this.prepareStagingTables(chunkInsertInput);
                        await this.insertStagingTables(chunkInsertInput);
                        await this.resolveConflicts(chunkInsertInput);
                        if (historyDegradation) {
                            const historyInputs = await this.configureHistoryTables(chunkInsertInput);
                            insertResults = await this.insertFromStagingTablesAtomic(chunkInsertInput, historyInputs);
                        } else {
                            await this.insertHistory(chunkInsertInput);
                            insertResults = await this.insertFromStagingTables(chunkInsertInput, { perRowFallback: !!config.rejectedRowsTable });
                        }
                    } finally {
                        // Drop staging even if a step above threw, so a failed chunk can't orphan a temp table.
                        await this.removeStagingTables(chunkInsertInput).catch(e => this.db.error(`autoSQLChunked: failed to drop staging table(s) for '${table}': ${e instanceof Error ? e.message : String(e)}`));
                    }
                } else {
                    insertResults = await this.insertData(chunkInsertInput, { perRowFallback: true });
                }
                phases.load = (phases.load ?? 0) + (perf() - tLoad);

                totalAffectedRows += insertResults.reduce((s, r) => s + (r.affectedRows || 0), 0);
            }

            const end = new Date();
            const durationMs = end.getTime() - start.getTime();
            const stats: QueryStats = {
                table,
                rows: totalRows,
                affectedRows: totalAffectedRows,
                durationMs,
                rowsPerSecond: durationMs > 0 ? Math.round((totalRows / durationMs) * 1000) : 0,
                phases: {
                    prepare: phases.prepare !== undefined ? Math.round(phases.prepare) : undefined,
                    configure: phases.configure !== undefined ? Math.round(phases.configure) : undefined,
                    load: phases.load !== undefined ? Math.round(phases.load) : undefined,
                },
                staged: !!this.db.getConfig().useStagingInsert,
                bulkLoad: !!this.db.getConfig().bulkLoad,
            };
            try { this.db.getConfig().logger?.stats?.(stats); } catch { /* a metrics sink must never break a load */ }
            return {
                start,
                end,
                success: true,
                duration: durationMs,
                affectedRows: totalAffectedRows,
                table,
                stats
            };
        } catch (error: any) {
            const end = new Date();
            return {
                start,
                end,
                duration: end.getTime() - start.getTime(),
                affectedRows: 0,
                success: false,
                error: error instanceof Error ? error.message : String(error)
            };
        }
      });
    }

    /**
     * Open a streaming session and return a handle for incremental writes.
     *
     * Contract: this returns a promise that **rejects** on failure (e.g. the initial
     * connectivity check fails) and must be awaited. The returned handle's methods
     * (`write`/`end`/`abort`) follow the same reject-on-failure, must-await contract —
     * they are not fire-and-forget. See {@link AutoSQLStreamHandle.write}.
     */
    async openStream(
        table: string,
        schema?: string,
        primaryKey?: string[]
    ): Promise<AutoSQLStreamHandle> {
      return this.db.runWithSchema(schema, async () => {
        // Connectivity check — surfaces auth/connection errors before first write
        const ping = await this.db.runQuery({ query: 'SELECT 1', params: [] });
        if (!ping.success) {
            throw new Error(`openStream: cannot connect to database — ${ping.error}`);
        }

        const config = this.db.getConfig();
        const prefix = config.streamingStagingPrefix ?? defaults.streamingStagingPrefix;

        // Clean up orphaned staging tables unless configured to keep them
        if (!config.keepOrphanedStagingTables) {
            await this._cleanupOrphanedStreamTables(table, prefix);
        }

        const runId = generateRunId();
        const stagingTable = buildStreamStagingTableName(table, prefix, runId);
        this.activeStreamStagingTables.add(stagingTable);

        // The handle re-establishes this schema context for its own write()/end()/abort()
        // calls, which run later in separate async invocations.
        return new AutoSQLStreamHandle(this, this.db, table, stagingTable, schema, primaryKey);
      });
    }

    async _cleanupOrphanedStreamTables(table: string, prefix: string): Promise<void> {
        const config = this.db.getConfig();
        const dialect = config.sqlDialect;
        const schema = config.schema || config.database || '';
        const pattern = `${prefix}${table}__%`;
        const q = dialect === 'mysql'
            ? { query: `SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE ?`, params: [schema, pattern] }
            : { query: `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name LIKE $2`, params: [schema, pattern] };

        const result = await this.db.runQuery(q);
        if (!result.success || !result.results?.length) return;

        for (const row of result.results) {
            const name: string = row.table_name || row.TABLE_NAME;
            if (!isAutosqlStreamTable(name, table, prefix)) continue;
            if (this.activeStreamStagingTables.has(name)) continue; // live stream on this instance — not an orphan
            this.db.warn(`autoSQLStream: dropping orphaned stream staging table '${name}' from a previous crashed run.`);
            const dropQ = buildDropStreamStagingTableQuery(name, config);
            await this.db.runTransaction([dropQ]).catch(e =>
                this.db.error(`Failed to drop orphaned stream staging table '${name}': ${e.message}`)
            );
        }
    }
}

export class AutoSQLStreamHandle {
    private handler: AutoSQLHandler;
    private db: Database;
    private table: string;
    private stagingTable: string;
    private schema: string | undefined;
    private primaryKey: string[] | undefined;
    private columns: string[] | null = null;
    private stagingCreated = false;
    private ended = false;

    constructor(
        handler: AutoSQLHandler,
        db: Database,
        table: string,
        stagingTable: string,
        schema: string | undefined,
        primaryKey: string[] | undefined
    ) {
        this.handler = handler;
        this.db = db;
        this.table = table;
        this.stagingTable = stagingTable;
        this.schema = schema;
        this.primaryKey = primaryKey;
    }

    /**
     * Append a chunk of rows to this run's staging table.
     *
     * Contract: returns a promise that **rejects** on failure and MUST be awaited (or
     * `.catch`ed). It is NOT fire-and-forget — an un-awaited write() that fails becomes an
     * unhandled promise rejection and its error is lost.
     *
     * A rejected write() leaves the staging table in an indeterminate state (the chunk may be
     * partly applied or absent). On rejection, either **retry the same chunk** (write() is
     * append-only, so re-sending after a transient failure is safe) or call {@link abort} to
     * discard the run. Do NOT call {@link end} after a failed/un-awaited write() expecting the
     * gap to be ignored: end() merges whatever is staged, so a lost chunk becomes missing rows.
     */
    async write(chunk: Record<string, any>[]): Promise<void> {
        if (this.ended) throw new Error(`autoSQLStream: write() called after end()/abort()`);
        if (chunk.length === 0) return;
        return this.db.runWithSchema(this.schema, async () => {
            const config = this.db.getConfig();

            if (!this.stagingCreated) {
                // Derive columns from first row
                this.columns = Object.keys(chunk[0]);
                const createQ = buildCreateStreamStagingTableQuery(this.stagingTable, this.columns, config);
                const createResult = await this.db.runTransaction([createQ]);
                if (!createResult.success) {
                    throw new Error(`autoSQLStream: failed to create stream staging table '${this.stagingTable}': ${createResult.error}`);
                }
                this.stagingCreated = true;
            }

            const insertQ = buildInsertIntoStreamStagingQuery(this.stagingTable, this.columns!, chunk, config);
            const insertResult = await this.db.runTransaction([insertQ]);
            if (!insertResult.success) {
                throw new Error(`autoSQLStream: failed to write chunk to staging table '${this.stagingTable}': ${insertResult.error}`);
            }
        });
    }

    /**
     * Merge all staged rows into the target table (infer schema, apply DDL, bulk INSERT…SELECT
     * with a per-row retry fallback), then drop the staging table.
     *
     * Contract: returns a promise that **rejects** on failure and must be awaited. end() merges
     * whatever is currently staged — it does not know about {@link write} calls that failed or
     * were never awaited, so ensure every chunk resolved (or was retried) before calling this, or
     * a lost chunk will silently become missing rows. To discard instead of merge, use {@link abort}.
     */
    async end(): Promise<QueryResult> {
      return this.db.runWithSchema(this.schema, async () => {
        const start = new Date();
        this.ended = true;
        const config = this.db.getConfig();
        const insertType = config.insertType ?? 'UPDATE';
        const maxRetries = config.streamMaxRetries ?? defaults.streamMaxRetries;
        // Per-run instrumentation (QueryStats). For a stream these cover the `end()` FLUSH — read
        // staging + infer (prepare) → DDL (configure) → merge (load) — not the incremental write()s.
        const perf = () => (typeof performance !== "undefined" ? performance.now() : Date.now());
        const phases: { prepare?: number; configure?: number; load?: number } = {};

        try {
            if (!this.stagingCreated) {
                // Nothing was written
                return { start, end: new Date(), success: true, duration: 0, affectedRows: 0, table: this.table };
            }

            // Read all staging rows
            const selectQ = buildSelectFromStreamStagingQuery(this.stagingTable, config);
            const selectResult = await this.db.runQuery(selectQ);
            if (!selectResult.success || !selectResult.results) {
                throw new Error(`autoSQLStream: failed to read staging data: ${selectResult.error}`);
            }
            const stagingRows: Record<string, any>[] = selectResult.results;
            if (stagingRows.length === 0) {
                return { start, end: new Date(), success: true, duration: 0, affectedRows: 0, table: this.table };
            }

            // Infer schema from staging data
            const tPrepare = perf();
            const inferredMeta = await getMetaData(config, stagingRows, this.primaryKey);
            const { currentMetaData } = await this.handler.fetchTableMetadata(this.table);
            const { changes, updatedMetaData } = compareMetaData(currentMetaData, inferredMeta, this.db.getDialectConfig(), config.logger);
            phases.prepare = perf() - tPrepare;

            // Configure main table (with schema lock + history if enabled)
            const insertInput = [{
                table: this.table,
                data: stagingRows,
                metaData: updatedMetaData,
                previousMetaData: currentMetaData,
                comparedMetaData: { changes, updatedMetaData },
                stagingPrefix: config.stagingPrefix,
                historyTableSuffix: config.historyTableSuffix,
            }];

            const useSchemaLock = config.useSchemaLock;
            const lockTimeout = config.schemaLockTimeout ?? defaults.schemaLockTimeout;
            const useHistory = config.schemaHistory;
            let historyId: number | undefined;

            if (useSchemaLock) await this.db.acquireSchemaLock(this.table, lockTimeout);
            try {
                if (useHistory) {
                    await bootstrapSchemaHistoryTable(this.db);
                    if (tableChangesExist(changes)) {
                        historyId = await recordMigrationStart(this.db, this.table, currentMetaData || {}, changes);
                    }
                }
                try {
                    const tConfigure = perf();
                    await this.handler['configureTables'](insertInput);
                    phases.configure = perf() - tConfigure;
                    if (historyId !== undefined) await recordMigrationSuccess(this.db, historyId, updatedMetaData);
                } catch (ddlErr) {
                    if (historyId !== undefined) await recordMigrationFailed(this.db, historyId).catch(() => {});
                    throw ddlErr;
                }
            } finally {
                if (useSchemaLock) await this.db.releaseSchemaLock(this.table);
            }

            // Attempt bulk merge with casts
            const tLoad = perf();
            const mergeQ = buildMergeFromStreamQuery(this.table, this.stagingTable, updatedMetaData, insertType as 'UPDATE' | 'INSERT', config);
            const mergeResult = await this.db.runTransaction([mergeQ]);

            let affectedRows = mergeResult.affectedRows ?? 0;

            if (!mergeResult.success) {
                // Fallback: per-row retry with schema widening
                affectedRows = await this._perRowMerge(stagingRows, updatedMetaData, insertType as 'UPDATE' | 'INSERT', maxRetries);
            }
            phases.load = perf() - tLoad;

            const end = new Date();
            const durationMs = end.getTime() - start.getTime();
            const stats: QueryStats = {
                table: this.table,
                rows: stagingRows.length,
                affectedRows,
                durationMs,
                rowsPerSecond: durationMs > 0 ? Math.round((stagingRows.length / durationMs) * 1000) : 0,
                phases: {
                    prepare: phases.prepare !== undefined ? Math.round(phases.prepare) : undefined,
                    configure: phases.configure !== undefined ? Math.round(phases.configure) : undefined,
                    load: phases.load !== undefined ? Math.round(phases.load) : undefined,
                },
                staged: true,
                bulkLoad: !!config.bulkLoad,
            };
            try { config.logger?.stats?.(stats); } catch { /* a metrics sink must never break a load */ }
            return { start, end, success: true, duration: durationMs, affectedRows, table: this.table, stats };
        } catch (error: any) {
            const end = new Date();
            return { start, end, duration: end.getTime() - start.getTime(), affectedRows: 0, success: false, error: error instanceof Error ? error.message : String(error), errorCode: (error as any)?.code != null ? String((error as any).code) : undefined };
        } finally {
            // Always drop staging table
            if (this.stagingCreated) {
                const dropQ = buildDropStreamStagingTableQuery(this.stagingTable, this.db.getConfig());
                await this.db.runTransaction([dropQ]).catch(e =>
                    this.db.error(`autoSQLStream: failed to drop staging table '${this.stagingTable}': ${e.message}`)
                );
            }
            this.handler.releaseStreamStaging(this.stagingTable);
        }
      });
    }

    private async _perRowMerge(
        rows: Record<string, any>[],
        metaData: MetadataHeader,
        insertType: 'UPDATE' | 'INSERT',
        maxRetries: number
    ): Promise<number> {
        // Shared with the non-streaming direct-insert path — see AutoSQLHandler.perRowInsertWithRetry.
        const { inserted } = await this.handler.perRowInsertWithRetry(
            this.table, rows, metaData, insertType, maxRetries, this.primaryKey, 'autoSQLStream'
        );
        return inserted;
    }

    /**
     * Discard the run: drop the staging table without merging. Safe to call even if {@link write}
     * was never called, and the correct way to bail out after a failed write(). Returns a promise
     * that should be awaited.
     */
    async abort(): Promise<void> {
        this.ended = true;
        return this.db.runWithSchema(this.schema, async () => {
            if (this.stagingCreated) {
                const dropQ = buildDropStreamStagingTableQuery(this.stagingTable, this.db.getConfig());
                await this.db.runTransaction([dropQ]).catch(e =>
                    this.db.error(`autoSQLStream: failed to drop staging table during abort: ${e.message}`)
                );
            }
            this.handler.releaseStreamStaging(this.stagingTable);
        });
    }
}