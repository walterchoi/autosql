import type { AutoSQLHandler } from "./autosql";
import type { Database } from "./database";
import type { StagingPipeline } from "./stagingPipeline";
import type { HistoryCoordinator } from "./historyCoordinator";
import type { InsertInput, QueryResult } from "../config/types";

/**
 * Prepared inputs handed to a LoadStrategy: metadata resolved, split applied, nested appended, and
 * tables already configured. `table`/`label` are only for the staging-cleanup error message.
 */
export interface LoadContext {
    insertInput: InsertInput[];
    table: string;
    label: string;
}

export interface LoadStrategy {
    /**
     * Load already-configured inputs and return one QueryResult per input. Implementations own
     * staging-vs-direct dispatch, conflict resolution, row-level history, and graceful degradation.
     * Runs inside the caller's runWithSchema/runWithSeparators scope.
     */
    load(ctx: LoadContext): Promise<QueryResult[]>;
}

/**
 * The default row-store load strategy. The LoadStrategy seam exists so a future columnar
 * `StageCopyLoad` can implement the same contract (R1 Slice 2).
 */
export class RowStoreLoadStrategy implements LoadStrategy {
    constructor(
        private handler: AutoSQLHandler,
        private db: Database,
        private staging: StagingPipeline,
        private history: HistoryCoordinator,
    ) {}

    async load(ctx: LoadContext): Promise<QueryResult[]> {
        const config = this.db.getConfig();
        const input = ctx.insertInput;
        if (config.useStagingInsert) {
            // Row-level history takes the atomic path (before-image + merge in ONE transaction) so
            // history and data commit or roll back together, leaving no crash window between them.
            // rejectedRowsTable forces this branch too, since it needs addHistory for before-images;
            // only the merge-failure handling differs (see perRowFallback below).
            const useAtomicHistory = !!(config.addHistory && config.historyTables?.length);
            try {
                await this.staging.prepareStagingTables(input);
                await this.staging.insertStagingTables(input);
                await this.staging.resolveConflicts(input);
                if (useAtomicHistory) {
                    const historyInputs = await this.history.configureHistoryTables(input);
                    return await this.staging.insertFromStagingTablesAtomic(input, historyInputs, { perRowFallback: !!config.rejectedRowsTable });
                } else {
                    await this.history.insertHistory(input);
                    return await this.staging.insertFromStagingTables(input, { perRowFallback: !!config.rejectedRowsTable });
                }
            } finally {
                // Drop staging even if a step above threw, so a failed load leaves no orphaned temp
                // table (mirrors the streaming end() path).
                await this.staging.removeStagingTables(input).catch((e: unknown) => { this.db.error(`${ctx.label}: failed to drop staging table(s) for '${ctx.table}': ${e instanceof Error ? e.message : String(e)}`); });
            }
        } else {
            return this.handler['insertData'](input, { perRowFallback: true });
        }
    }
}
