import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { SqlServerDatabase } from "./sqlserver";
import { Database } from "./database";
import { InsertResult, InsertInput, MetadataHeader, AlterTableChanges, metaDataInterim, QueryResult, QueryInput, AutoSQLOptions, QueryStats, AutoSQLPreview, TablePreview, DatabaseConfig } from "../config/types";
import { getMetaData, compareMetaData, collectDataColumns, schemaCoversColumns, overlaySchema, fillColumnDefaults } from "../helpers/metadata";
import { applySurrogateKey } from "../helpers/keys";
import { resolveDatasetSeparators } from "../helpers/numberFormat";
import { HistoryCoordinator } from "./historyCoordinator";
import { DegradationPolicy } from "./degradationPolicy";
import { StagingPipeline } from "./stagingPipeline";
import { RowStoreLoadStrategy } from "./loadStrategy";
import { parseDatabaseMetaData, tableChangesExist, isMetadataHeader, estimateRowSize, isValidDataFormat, organizeSplitTable, organizeSplitData, splitInsertData, getInsertValues, throwIfFailedResults, sqlize } from "../helpers/utilities";
import { defaults, MAX_COLUMN_COUNT } from "../config/defaults";
import { ensureTimestamps } from "../helpers/timestamps";
import WorkerHelper from "../workers/workerHelper";
import { buildCompensatingDDL } from "../helpers/compensatingDDL";
import {
    generateRunId,
    buildStreamStagingTableName,
    isAutosqlStreamTable,
    buildDropStreamStagingTableQuery,
    buildOrphanSearchQuery,
} from '../helpers/streamHelpers';
// AutoSQLStreamHandle was extracted to its own module (R1 Slice 1). Imported here so `openStream`
// can construct it, and re-exported below to keep the public `index.ts` export path stable.
import { AutoSQLStreamHandle } from "./autoSQLStreamHandle";
export { AutoSQLStreamHandle };

export class AutoSQLHandler {
    private db: Database;
    // R1 Slice 2 collaborators (behaviour-preserving extraction; each holds a back-ref to this handler).
    private history: HistoryCoordinator;
    private degradation: DegradationPolicy;
    private staging: StagingPipeline;
    private strategy: RowStoreLoadStrategy;
    // Staging tables of streams that are currently open on this instance. Orphan cleanup must
    // never drop these — a concurrent stream to the same table would otherwise destroy a live
    // run's staging data (both share the `${prefix}${table}__` name pattern).
    private activeStreamStagingTables = new Set<string>();

    constructor(dbInstance: MySQLDatabase | PostgresDatabase | SqlServerDatabase) {
        this.db = dbInstance;
        this.history = new HistoryCoordinator(this, dbInstance);
        this.degradation = new DegradationPolicy(this, dbInstance);
        this.staging = new StagingPipeline(this, dbInstance);
        this.strategy = new RowStoreLoadStrategy(this, dbInstance, this.staging, this.history);
    }

    /**
     * Delegator kept on the handler so AutoSQLStreamHandle (which calls handler.perRowInsertWithRetry)
     * and any other caller reach the shared per-row degradation engine unchanged (R1 Slice 2, PR 2c).
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
        return this.degradation.perRowInsertWithRetry(table, rows, metaData, insertType, maxRetries, primaryKey, label);
    }

    releaseStreamStaging(stagingTable: string): void {
        this.activeStreamStagingTables.delete(stagingTable);
    }

    /**
     * Dataset-level number-format consensus (self-contained — reads only `data`, no DB queries).
     * When the caller supplied neither explicit separators nor `numberFormat`, infer ONE
     * {thousands, decimal} pair from structural evidence pooled across all columns of the batch, so
     * a decisive column resolves ambiguous siblings. The returned pair is meant to be run under
     * `db.runWithSeparators(...)`, which overlays it on `getConfig()` for BOTH inference and
     * load-time sqlize (and worker dispatch) for the duration of the call.
     *
     * Returns undefined (no override — load uses the assume-decimal default + the A24c per-column
     * warning) when separators are already configured, there is no data, there is no structural
     * evidence, or the evidence is genuinely contradictory (warns loudly in that last case).
     */
    private resolveSeparatorConsensus(data: Record<string, any>[]): { thousands: string; decimal: string } | undefined {
        const config = this.db.getConfig();
        // Explicit separators / numberFormat (both resolve to thousandsSeparator in validateConfig) win.
        if (config.thousandsSeparator !== undefined || config.decimalSeparator !== undefined) return undefined;
        if (!Array.isArray(data) || data.length === 0) return undefined;

        const decision = resolveDatasetSeparators(data, config.numberFormatMinEvidence ?? 1);
        if (decision === null) return undefined; // no evidence → default; A24c warns per ambiguous column
        if ("conflict" in decision) {
            this.db.warn(`autosql: the data contains conflicting number formats (some values look US "1,234,567", others EU "1.234.567"). Not guessing — using the default (a lone separator is treated as a decimal). Set numberFormat or thousandsSeparator/decimalSeparator to disambiguate.`);
            return undefined;
        }
        this.db.log(`autosql: detected ${decision.thousands === "," ? "US/IN" : "EU"} number format from the data (thousands "${decision.thousands}", decimal "${decision.decimal}").`);
        return decision;
    }

    /**
     * Dry run: compute what an `autoSQL(table, data, …)` load WOULD do — the inferred schema, the
     * create/alter decision, the exact DDL, and any changes that would be blocked without opting in —
     * **without writing anything**. Only reads the current schema (to diff against); nothing is
     * created, altered, or inserted. Mirrors the `autoSQL` signature so you can preview a call before
     * committing it (e.g. to show a UI confirmation). Contrast with `safeMode`, which runs a load but
     * skips DDL; `preview` runs no load and executes no DDL.
     */
    async preview(table: string, data: Record<string, any>[], schema?: string, primaryKey?: string[], options?: AutoSQLOptions): Promise<AutoSQLPreview> {
        const separators = this.resolveSeparatorConsensus(data);
        return this.db.runWithSeparators(separators, () => this.db.runWithSchema(schema, async () => {
            const insertInput = await this.prepareInsertData(table, data, schema, primaryKey, options);
            const config = this.db.getConfig();
            const numberFormat = (config.thousandsSeparator !== undefined && config.decimalSeparator !== undefined)
                ? { thousands: config.thousandsSeparator, decimal: config.decimalSeparator }
                : undefined;

            // blockedChanges surfaces the destructive-change gates (derived cleanly from `changes` +
            // config). TODO: also surface the per-column A24c ambiguity warning here — it is emitted to
            // logger.warn during inference; capturing it would need a log-capture seam (an ALS channel,
            // like schemaContext), deferred to keep v1 free of that infra.
            const tables: TablePreview[] = [];
            for (const input of insertInput) {
                const { currentMetaData, tableExists } = await this.fetchTableMetadata(input.table);
                const changes = input.comparedMetaData?.changes ?? null;
                const hasChanges = changes ? tableChangesExist(changes) : false;
                const action: TablePreview["action"] = !tableExists ? "create" : (hasChanges ? "alter" : "noop");

                // Same call configureTables makes per table (autoConfigureTable), but runQuery:false so
                // it BUILDS the CREATE/ALTER without executing — read-only.
                let ddl: string[] = [];
                try {
                    const ddlResult = await this.autoConfigureTable({ ...input, runQuery: false }) as QueryInput[];
                    if (Array.isArray(ddlResult)) ddl = ddlResult.map(q => typeof q === "string" ? q : q.query);
                } catch { /* a noop table may produce no DDL; leave ddl empty */ }

                tables.push({
                    table: input.table,
                    action,
                    inferredSchema: input.metaData,
                    currentSchema: currentMetaData,
                    changes: action === "create" ? null : changes,
                    ddl,
                    blockedChanges: this.deriveBlockedChanges(changes, config),
                });
            }
            return { tables, numberFormat, rowCount: Array.isArray(data) ? data.length : 0 };
        }));
    }

    // Changes autosql would refuse to apply without an explicit opt-in — the same gates
    // warnBlockedSchemaChanges enforces at load time, surfaced here so a preview can flag them.
    private deriveBlockedChanges(changes: AlterTableChanges | null, config: DatabaseConfig): string[] {
        if (!changes) return [];
        const blocked: string[] = [];
        if (changes.dropColumns?.length && !config.deleteColumns) {
            blocked.push(`Would DROP column(s) ${changes.dropColumns.join(", ")} — blocked; set deleteColumns: true to allow.`);
        }
        if (changes.primaryKeyChanges?.length && !config.updatePrimaryKey) {
            blocked.push(`Would change the primary key (${changes.primaryKeyChanges.join(", ")}) — blocked; set updatePrimaryKey: true to allow.`);
        }
        if (changes.noLongerUnique?.length && !config.dropUniqueConstraints) {
            blocked.push(`Would DROP the unique constraint on ${changes.noLongerUnique.join(", ")} — blocked; set dropUniqueConstraints: true to allow.`);
        }
        return blocked;
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
            return await this.degradation.applyPerRowFallback(insertInput, allInsertResults);
        }

        throwIfFailedResults(allInsertResults, "data insert queries")

        return allInsertResults;
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
      // Resolve dataset-level number format ONCE, before inference, and run the whole load under it
      // so inference, staging, direct insert, and workers all sqlize with the same separators.
      const separators = this.resolveSeparatorConsensus(data);
      return this.db.runWithSeparators(separators, () => this.db.runWithSchema(schema, async () => {
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
                        await this.history.detectDrift(table);
                    }
                    await this.history.bootstrap();
                    const primary = insertInput[0];
                    if (primary?.comparedMetaData && tableChangesExist(primary.comparedMetaData.changes)) {
                        historyId = await this.history.recordStart(
                            table,
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
                            await this.history.recordSuccess(historyId, updatedMeta, liveMeta);
                        }
                        else await this.history.recordRolledBack(historyId);
                    }
                } catch (ddlErr) {
                    if (historyId !== undefined) {
                        await this.history.recordFailed(historyId).catch(() => {});
                    }
                    throw ddlErr;
                }
            } finally {
                if (useSchemaLock) await this.db.releaseSchemaLock(table);
            }

            const tLoad = perf();
            insertResults = await this.strategy.load({ insertInput, table, label: 'autoSQL' });
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
      }));
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

        // Dataset-level number format is resolved ONCE from the first non-empty chunk and LOCKED for
        // the whole load (across chunks only column lengths grow, never the format). Peek that chunk,
        // resolve, then process it and the rest under the resolved separators.
        const iterator = chunks[Symbol.asyncIterator]();
        let pending = await iterator.next();
        while (!pending.done && pending.value.length === 0) pending = await iterator.next();
        const firstValue: Record<string, any>[] | null = pending.done ? null : pending.value;
        const separators = firstValue ? this.resolveSeparatorConsensus(firstValue) : undefined;
        const remaining: AsyncIterable<Record<string, any>[]> = {
            async *[Symbol.asyncIterator]() {
                try {
                    if (firstValue !== null) yield firstValue;
                    let r; while (!(r = await iterator.next()).done) yield r.value;
                } finally {
                    // Forward early termination (an error mid-load, or a break/return in the consuming
                    // loop) to the SOURCE iterator so a cursor/stream-backed `chunks` runs its own
                    // cleanup. We took over manual iteration for the first-chunk peek, so the for-await's
                    // automatic `.return()` reaches this wrapper, not the underlying source iterator.
                    await iterator.return?.();
                }
            }
        };

        return this.db.runWithSeparators(separators, async () => {
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
            for await (const chunk of remaining) {
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
                insertResults = await this.strategy.load({ insertInput: chunkInsertInput, table, label: 'autoSQLChunked' });
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
        // Streaming is deferred on SQL Server (D-F): the stream staging/merge/cleanup builders emit
        // Postgres placeholders/DDL, so limping in would fail with a confusing mid-stream error (and
        // never clean up its staging table). Fail loud up front instead (A20).
        if (this.db.getConfig().sqlDialect === 'sqlserver') {
            throw new Error("openStream is not yet supported on SQL Server (streaming parity is deferred — see roadmap D-F). Use autoSQL/autoSQLChunked instead.");
        }
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
