import type { AutoSQLHandler } from "./autosql";
import type { Database } from "./database";
import type { StagingPipeline } from "./stagingPipeline";
import type { HistoryCoordinator } from "./historyCoordinator";
import { InsertInput, QueryResult } from "../config/types";

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
     * Load already-configured inputs into their target tables and return one QueryResult per input.
     * Implementations own staging-vs-direct dispatch, conflict resolution, row-level history, and
     * graceful degradation. Runs inside the caller's runWithSchema/runWithSeparators scope.
     */
    load(ctx: LoadContext): Promise<QueryResult[]>;
}

/**
 * The default row-store load strategy (R1 Slice 2, PR 2e): the staging-vs-direct load routine that
 * `autoSQL` and `autoSQLChunked` previously duplicated inline. Extracting it behind the LoadStrategy
 * seam lets a future columnar `StageCopyLoad` implement the same contract. Behaviour-preserving — the
 * block is verbatim, parameterized only by the LoadContext (input, and the cleanup error table/label).
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
            // Row-level history uses the zero-window atomic path (before-image + merge in ONE
            // transaction) so history and data commit — or roll back — together, with no crash window
            // between them. This covers BOTH plain addHistory and the opt-in rejectedRowsTable
            // degradation combo (rejectedRowsTable forces this branch since it requires addHistory to
            // capture before-images... it's the merge-failure handling that differs, see the
            // perRowFallback flag below). SQL Server keeps the non-atomic insertHistory-then-merge path:
            // its row-level history is unverified (decisions.md D-F) and configureHistoryTables guards
            // it off, so we don't extend the atomic guarantee to it (spec-1 §5.b: keep that guard).
            const useAtomicHistory = !!(config.addHistory && config.historyTables?.length && config.sqlDialect !== 'sqlserver');
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
                // Always drop the staging table, even if a step above threw, so a failed load
                // doesn't leave an orphaned temp table behind (mirrors the streaming end() path).
                await this.staging.removeStagingTables(input).catch(e => this.db.error(`${ctx.label}: failed to drop staging table(s) for '${ctx.table}': ${e instanceof Error ? e.message : String(e)}`));
            }
        } else {
            return await this.handler['insertData'](input, { perRowFallback: true });
        }
    }
}
