import type { AutoSQLHandler } from "./autosql";
import type { Database } from "./database";
import { InsertInput, MetadataHeader, QueryResult, AlterTableChanges } from "../config/types";
import { getHistoryTableName, throwIfFailedResults } from "../helpers/utilities";
import {
    bootstrapSchemaHistoryTable,
    recordMigrationStart,
    recordMigrationSuccess,
    recordMigrationRolledBack,
    recordMigrationFailed,
    detectSchemaDrift,
} from "../helpers/schemaHistory";

/**
 * Row-level history collaborator (R1 Slice 2, PR 2a). Builds the per-table history inputs and
 * populates the history tables. Behaviour-preserving extraction from `AutoSQLHandler`; holds a
 * back-ref to the handler (like `AutoSQLStreamHandle`) because table configuration is
 * worker-dispatched and `configureTables`/`fetchTableMetadata` stay on the handler. `AutoSQLHandler`
 * imported type-only to avoid a runtime import cycle.
 */
export class HistoryCoordinator {
    private handler: AutoSQLHandler;
    private db: Database;

    constructor(handler: AutoSQLHandler, db: Database) {
        this.handler = handler;
        this.db = db;
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
                this.handler.fetchTableMetadata(table),
                this.handler.fetchTableMetadata(historyName),
              ]);

              const currentMetaData = currentStatus.currentMetaData;
              const currentHistoryMetaData = historyStatus.currentMetaData;

              if (!currentMetaData) {
                throw new Error(`Could not find structure of ${table} for history table creation`);
              }

              // Clean up metaData for history table
              const cleanedMeta: MetadataHeader = {};
              for (const col in currentMetaData) {
                const def = { ...currentMetaData[col] };
                def.unique = false;
                def.index = false
                cleanedMeta[col] = def;
              }

              // Add as_at column
              cleanedMeta["dwh_as_at"] = { type: "datetime", allowNull: false, primary: true };

              const automatedColumns = ['dwh_created_at', 'dwh_modified_at', 'dwh_loaded_at']
              // Ensure existing PKs are retained
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

    async insertHistory(insertInput: InsertInput[]): Promise<QueryResult[]> {
        const historyInputs = await this.buildHistoryInputs(insertInput);
        if (!historyInputs.length) return [];
        await this.handler['configureTables'](historyInputs)
        return await this.insertToHistoryTables(historyInputs)
    }

    /**
     * Create the history tables (DDL only) and return their inputs, for the ATOMIC history path — the
     * before-image INSERT itself is deferred into the merge transaction (see
     * `insertFromStagingTablesAtomic`). Works on all three dialects (SQL Server's atomic pkFilter path
     * landed in spec-4 §3.8; the former SQL Server guard is gone).
     */
    async configureHistoryTables(insertInput: InsertInput[]): Promise<InsertInput[]> {
        const historyInputs = await this.buildHistoryInputs(insertInput);
        if (!historyInputs.length) return [];
        await this.handler['configureTables'](historyInputs);
        return historyInputs;
    }

    // --- Schema-history / drift (thin wrappers over helpers/schemaHistory) ---
    // Behaviour-preserving: the `if (config.schemaHistory)` / `tableChangesExist` gates stay at each
    // entry point, so which entry points record (autoSQL full, stream partial, chunked none) is
    // unchanged — only the helper calls are centralized here.

    async detectDrift(table: string): Promise<void> {
        await detectSchemaDrift(this.db, table);
    }

    async bootstrap(): Promise<void> {
        await bootstrapSchemaHistoryTable(this.db);
    }

    async recordStart(table: string, previousSchema: MetadataHeader, changes: AlterTableChanges): Promise<number | undefined> {
        return recordMigrationStart(this.db, table, previousSchema, changes);
    }

    async recordSuccess(historyId: number, newSchema: MetadataHeader, checksumSchema?: MetadataHeader | null): Promise<void> {
        await recordMigrationSuccess(this.db, historyId, newSchema, checksumSchema);
    }

    async recordRolledBack(historyId: number): Promise<void> {
        await recordMigrationRolledBack(this.db, historyId);
    }

    async recordFailed(historyId: number): Promise<void> {
        await recordMigrationFailed(this.db, historyId);
    }
}
