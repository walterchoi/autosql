import type { Database } from "./database";
import type { AutoSQLHandler } from "./autosql";
import type { MetadataHeader, QueryResult, QueryStats } from "../config/types";
import { getMetaData, compareMetaData, restrictInferredUniques } from "../helpers/metadata";
import { writeRunAudit } from "../helpers/runAudit";
import { tableChangesExist } from "../helpers/utilities";
import { DEFAULTS } from "../config/defaults";
import {
    buildCreateStreamStagingTableQuery,
    buildInsertIntoStreamStagingQuery,
    buildSelectFromStreamStagingQuery,
    buildDropStreamStagingTableQuery,
    buildMergeFromStreamQuery,
    buildAddStreamStagingColumnsQuery,
    buildStagingStateQuery,
    isStagingMetaColumn,
    STAGING_COLUMN_PREFIX,
    STAGING_SEQ_COLUMN,
} from '../helpers/streamHelpers';

/**
 * Collapse rows sharing a primary key to the one staged last, ordered by `_autosql_seq`.
 * JSON-encodes the key tuple so a composite key can't be made ambiguous by a separator (A24).
 */
function dedupByKeyKeepingLast(rows: Record<string, any>[], primaryKey: string[]): Record<string, any>[] {
    const bySeq = new Map<string, Record<string, any>>();
    for (const row of rows) {
        const key = JSON.stringify(primaryKey.map(k => row[k] ?? null));
        const held = bySeq.get(key);
        if (!held || Number(row[STAGING_SEQ_COLUMN] ?? 0) >= Number(held[STAGING_SEQ_COLUMN] ?? 0)) {
            bySeq.set(key, row);
        }
    }
    return [...bySeq.values()];
}

export class AutoSQLStreamHandle {
    private handler: AutoSQLHandler;
    private db: Database;
    private table: string;
    private stagingTable: string;
    private schema: string | undefined;
    private primaryKey: string[] | undefined;
    private columns: string[] | null = null;
    private columnSet: Set<string> | null = null;
    private stagingCreated = false;
    private ended = false;
    /** Opened with a `sessionKey`, so staging may outlive this run. */
    public readonly resumable: boolean;
    /**
     * Attached to rows an earlier run staged.
     *
     * ⚠ A caller with its own resume point must read this: trusting a stored high-water mark against
     * a non-adopted stream silently drops every row before that mark.
     */
    public readonly adopted: boolean;
    /** Columns added to an adopted table mid-load. Reporting only; the load is correct either way. */
    public readonly widenedColumns: string[] = [];
    /** Cursors this handle staged, for reporting. `stagingState()` reads the table for the truth. */
    private cursorsWritten = new Set<string>();

    constructor(
        handler: AutoSQLHandler,
        db: Database,
        table: string,
        stagingTable: string,
        schema: string | undefined,
        primaryKey: string[] | undefined,
        resume?: { resumable: boolean; adoptedColumns: string[] | null }
    ) {
        this.handler = handler;
        this.db = db;
        this.table = table;
        this.stagingTable = stagingTable;
        this.schema = schema;
        this.primaryKey = primaryKey;
        this.resumable = resume?.resumable ?? false;

        // Pre-set so write() doesn't try to create a table that's already there.
        const adoptedColumns = resume?.adoptedColumns ?? null;
        if (adoptedColumns && adoptedColumns.length > 0) {
            this.columns = adoptedColumns;
            this.columnSet = new Set(adoptedColumns);
            this.stagingCreated = true;
            this.adopted = true;
        } else {
            this.adopted = false;
        }
    }

    /**
     * Append a chunk of rows to this run's staging table.
     *
     * Rejects on failure and MUST be awaited: an un-awaited failing write() becomes an unhandled
     * rejection and its error is lost. A rejected write() leaves staging indeterminate, so either
     * retry the same chunk (write() is append-only, so re-sending is safe) or {@link abort}. Do NOT
     * call {@link end} expecting the gap to be ignored; end() merges whatever is staged, so a lost
     * chunk becomes missing rows.
     */
    async write(chunk: Record<string, any>[], opts?: { cursor?: string }): Promise<void> {
        if (this.ended) throw new Error(`autoSQLStream: write() called after end()/abort()`);
        if (chunk.length === 0) return;
        const cursor = opts?.cursor;
        if (cursor !== undefined) {
            if (!this.resumable) {
                throw new Error(`autoSQLStream: a cursor is only meaningful on a stream opened with a sessionKey — a per-run staging table is dropped at the end of the run, so nothing could read the cursor back.`);
            }
            if (!this.primaryKey?.length) {
                throw new Error(`autoSQLStream: a cursor requires a primaryKey — resuming re-sends the page that failed, and without a key those duplicate rows cannot be deduplicated at merge.`);
            }
            this.cursorsWritten.add(cursor);
        }
        return this.db.runWithSchema(this.schema, async () => {
            const config = this.db.getConfig();

            if (!this.stagingCreated) {
                // Columns from the UNION of the first chunk's rows — not just chunk[0], which would
                // silently drop a key first appearing in a later row of the same chunk (A18).
                const cols = new Set<string>();
                for (const row of chunk) for (const k of Object.keys(row)) cols.add(k);
                // Reserved, or the injected column collides and the CREATE fails unhelpfully.
                const reserved = [...cols].filter(isStagingMetaColumn);
                if (reserved.length > 0) {
                    throw new Error(`autoSQLStream: column(s) ${reserved.join(', ')} use the reserved '${STAGING_COLUMN_PREFIX}' prefix, which autosql uses for its own staging columns. Rename them.`);
                }
                this.columns = [...cols];
                this.columnSet = cols;
                const createQ = buildCreateStreamStagingTableQuery(this.stagingTable, this.columns, config);
                const createResult = await this.db.runTransaction([createQ]);
                if (!createResult.success) {
                    throw new Error(`autoSQLStream: failed to create stream staging table '${this.stagingTable}': ${createResult.error}`);
                }
                this.stagingCreated = true;
            }

            // A key first appearing in a LATER row/chunk has no column to land in, and dropping it
            // silently is data loss (A18).
            const unknown: string[] = [];
            for (const row of chunk) {
                for (const k of Object.keys(row)) {
                    if (!this.columnSet!.has(k) && !unknown.includes(k)) unknown.push(k);
                }
            }
            if (unknown.length > 0) {
                // Widen rather than refuse: absorbing a drifted source is what autosql does
                // everywhere else, and refusing would strand a resume mid-load. `safeMode` means no
                // DDL, so there the only non-lossy option left is to refuse. A keyless stream keeps
                // the established throw — its columns came from its own first chunk moments earlier,
                // so an unknown key is caller inconsistency, not drift between runs.
                if (this.resumable && !config.safeMode) {
                    const alterQ = buildAddStreamStagingColumnsQuery(this.stagingTable, unknown, config);
                    const alterResult = await this.db.runTransaction([alterQ]);
                    if (!alterResult.success) {
                        throw new Error(`autoSQLStream: failed to widen staging table '${this.stagingTable}' with new column(s) ${unknown.join(', ')}: ${alterResult.error}`);
                    }
                    for (const k of unknown) { this.columns!.push(k); this.columnSet!.add(k); }
                    this.widenedColumns.push(...unknown);
                    this.db.warn(`autoSQLStream: staging table '${this.stagingTable}' widened with new column(s) ${unknown.join(', ')} that appeared after it was created.`);
                } else {
                    const why = this.resumable
                        ? `safeMode is set, so the staging table cannot be altered to fit it`
                        : `a stream requires a stable column set`;
                    throw new Error(`autoSQLStream: row has column '${unknown[0]}' that was not present when the stream started (columns: ${this.columns!.filter(c => !unknown.includes(c)).join(', ')}). ${why} — include '${unknown[0]}' in the first written rows, or use a separate stream/load for it.`);
                }
            }

            // SQL Server caps a request at 2,100 bound parameters, so split the chunk into sub-batches of
            // ≤ floor(2000 / colCount) rows (one request each). Other dialects send the whole chunk in one.
            // A bound cursor is one more parameter per row, so it counts toward the cap.
            const boundPerRow = this.columns!.length + (cursor !== undefined ? 1 : 0);
            const perStatement = config.sqlDialect === 'sqlserver'
                ? Math.max(1, Math.floor(2000 / boundPerRow))
                : chunk.length;
            for (let i = 0; i < chunk.length; i += perStatement) {
                const sub = chunk.slice(i, i + perStatement);
                const insertQ = buildInsertIntoStreamStagingQuery(this.stagingTable, this.columns!, sub, config, cursor);
                const insertResult = await this.db.runTransaction([insertQ]);
                if (!insertResult.success) {
                    throw new Error(`autoSQLStream: failed to write chunk to staging table '${this.stagingTable}': ${insertResult.error}`);
                }
            }
        });
    }

    /**
     * Merge all staged rows into the target (infer schema, apply DDL, bulk INSERT…SELECT with a
     * per-row retry fallback), then drop staging.
     *
     * Rejects on failure and must be awaited. end() merges whatever is staged and knows nothing of
     * write() calls that failed or were never awaited, so a lost chunk silently becomes missing rows.
     */
    async end(): Promise<QueryResult> {
      return this.db.runWithSchema(this.schema, async () => {
        const start = new Date();
        this.ended = true;
        const config = this.db.getConfig();
        const insertType = config.insertType ?? 'UPDATE';
        const maxRetries = config.streamMaxRetries ?? DEFAULTS.streamMaxRetries;
        // Per-run instrumentation (QueryStats). For a stream these cover the `end()` FLUSH — read
        // staging + infer (prepare) → DDL (configure) → merge (load) — not the incremental write()s.
        const perf = () => (typeof performance !== "undefined" ? performance.now() : Date.now());
        const phases: { prepare?: number; configure?: number; load?: number } = {};
        let promoted = false; // gates the drop in `finally`

        try {
            if (!this.stagingCreated) {
                // Nothing was written
                return { start, end: new Date(), success: true, duration: 0, affectedRows: 0, table: this.table };
            }

            const selectQ = buildSelectFromStreamStagingQuery(this.stagingTable, config);
            const selectResult = await this.db.runQuery(selectQ);
            if (!selectResult.success || !selectResult.results) {
                throw new Error(`autoSQLStream: failed to read staging data: ${selectResult.error}`);
            }
            // Dedup BEFORE inference, not just at merge. A resumed run re-sends the page it died on,
            // so a key legitimately appears twice in staging — and inference reading that decides the
            // key is not unique and WIDENS it (an `id` re-send produced a composite `(id, name)` PK).
            // Keep the last-staged row per key, matching the merge's own dedup.
            const rawRows: Record<string, any>[] = this.resumable && this.primaryKey?.length
                ? dedupByKeyKeepingLast(selectResult.results, this.primaryKey)
                : selectResult.results;
            // Strip after: `_autosql_*` describes the load, not the data.
            const stagingRows: Record<string, any>[] = rawRows.map((r: Record<string, any>) => {
                const out: Record<string, any> = {};
                for (const k of Object.keys(r)) if (!isStagingMetaColumn(k)) out[k] = r[k];
                return out;
            });
            if (stagingRows.length === 0) {
                // Nothing to lose by dropping an empty table, so don't leak it to the sweeper.
                promoted = true;
                return { start, end: new Date(), success: true, duration: 0, affectedRows: 0, table: this.table };
            }

            // Overlay the resolved separators on inference AND the per-row fallback: the bulk merge
            // casts via the DB, so a grouped value like "1,234" is rejected there and lands per-row,
            // where sqlize normalizes under these separators. numberFormat needs no overlay, being
            // already on this.config.
            const separators = this.handler['resolveSeparatorConsensus'](stagingRows);
            const flushConfig = separators
                ? { ...config, thousandsSeparator: separators.thousands, decimalSeparator: separators.decimal }
                : config;

            const tPrepare = perf();
            const { currentMetaData } = await this.handler.fetchTableMetadata(this.table);
            const inferredMeta = restrictInferredUniques(await getMetaData(flushConfig, stagingRows, this.primaryKey), currentMetaData, config);
            const { changes, updatedMetaData } = compareMetaData(currentMetaData, inferredMeta, this.db.getDialectConfig(), config.logger);
            phases.prepare = perf() - tPrepare;

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
            const lockTimeout = config.schemaLockTimeout ?? DEFAULTS.schemaLockTimeout;
            const useHistory = config.schemaHistory;
            let historyId: number | undefined;

            if (useSchemaLock) await this.db.acquireSchemaLock(this.table, lockTimeout);
            try {
                if (useHistory) {
                    await this.handler['history'].bootstrap();
                    if (tableChangesExist(changes)) {
                        historyId = await this.handler['history'].recordStart(this.table, currentMetaData || {}, changes);
                    }
                }
                try {
                    const tConfigure = perf();
                    await this.handler['configureTables'](insertInput);
                    phases.configure = perf() - tConfigure;
                    if (historyId !== undefined) await this.handler['history'].recordSuccess(historyId, updatedMetaData);
                } catch (ddlErr) {
                    if (historyId !== undefined) await this.handler['history'].recordFailed(historyId).catch(() => {});
                    throw ddlErr;
                }
            } finally {
                if (useSchemaLock) await this.db.releaseSchemaLock(this.table);
            }

            const tLoad = perf();
            // Any resumable stream, not just cursored ones: a resume can re-send staged rows.
            const mergeQ = buildMergeFromStreamQuery(this.table, this.stagingTable, updatedMetaData, insertType, config, this.resumable);
            const mergeResult = await this.db.runTransaction([mergeQ]);

            let affectedRows = mergeResult.affectedRows ?? 0;

            if (!mergeResult.success) {
                // Fallback: per-row retry with schema widening, under the resolved separators so the
                // per-row sqlize (via getConfig()) normalizes values with the detected format.
                affectedRows = await this.db.runWithSeparators(separators, () =>
                    this._perRowMerge(stagingRows, updatedMetaData, insertType, maxRetries));
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
            await writeRunAudit(this.db, { table: this.table, success: true, rows: stagingRows.length, affectedRows, durationMs, rowsPerSecond: stats.rowsPerSecond, phases: stats.phases, staged: true, bulkLoad: !!config.bulkLoad });
            promoted = true;
            return { start, end, success: true, duration: durationMs, affectedRows, table: this.table, stats };
        } catch (error: any) {
            const end = new Date();
            const durationMs = end.getTime() - start.getTime();
            // rows/affected unknown here (the failure may precede the staging read) — record 0 with the error.
            await writeRunAudit(this.db, { table: this.table, success: false, rows: 0, affectedRows: 0, durationMs, rowsPerSecond: 0, staged: true, bulkLoad: !!this.db.getConfig().bulkLoad, error: error instanceof Error ? error.message : String(error), errorCode: error?.code != null ? String(error.code) : undefined });
            return { start, end, duration: durationMs, affectedRows: 0, success: false, error: error instanceof Error ? error.message : String(error), errorCode: error?.code != null ? String(error.code) : undefined };
        } finally {
            // A resumable stream keeps its table when the rows didn't land: the failure may be
            // transient, and dropping here would discard rows accumulated over earlier runs, the
            // exact loss this feature exists to prevent. Per-run tables are useless after the call.
            const keepForResume = this.resumable && !promoted;
            if (this.stagingCreated && !keepForResume) {
                const dropQ = buildDropStreamStagingTableQuery(this.stagingTable, this.db.getConfig());
                await this.db.runTransaction([dropQ]).catch(e => {
                    this.db.error(`autoSQLStream: failed to drop staging table '${this.stagingTable}': ${e.message}`);
                });
            } else if (keepForResume) {
                this.db.warn(`autoSQLStream: end() did not promote staged rows for '${this.table}'; keeping resumable staging table '${this.stagingTable}' so the run can be retried. Call abort() to discard it.`);
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
     * What is currently staged, for a caller that lost its run. Reads the table, not this handle, so
     * it is valid when another process staged the rows. See README.
     */
    async stagingState(): Promise<{
        stagedRows: number;
        cursors: string[];
        lastCursor: string | null;
        firstStagedAt: Date | null;
        lastStagedAt: Date | null;
    }> {
        const empty = { stagedRows: 0, cursors: [], lastCursor: null, firstStagedAt: null, lastStagedAt: null };
        if (!this.stagingCreated) return empty;
        return this.db.runWithSchema(this.schema, async () => {
            const result = await this.db.runQuery(buildStagingStateQuery(this.stagingTable, this.db.getConfig()));
            if (!result.success || !result.results?.length) return empty;
            const pick = (r: any, k: string) => r[k] ?? r[k.toUpperCase()];
            const rows = result.results;
            let stagedRows = 0;
            const cursors: string[] = [];
            const stamps: Date[] = [];
            for (const r of rows) {
                stagedRows += Number(pick(r, 'row_count')) || 0;
                const c = pick(r, 'cursor_value');
                if (c !== null && c !== undefined) cursors.push(String(c));
                for (const k of ['first_staged_at', 'last_staged_at']) {
                    const v = pick(r, k);
                    if (v) stamps.push(v instanceof Date ? v : new Date(v));
                }
            }
            stamps.sort((a, b) => a.getTime() - b.getTime());
            return {
                stagedRows,
                cursors,
                lastCursor: cursors.length ? cursors[cursors.length - 1] : null,
                firstStagedAt: stamps.length ? stamps[0] : null,
                lastStagedAt: stamps.length ? stamps[stamps.length - 1] : null,
            };
        });
    }

    /**
     * Stop without promoting and without discarding, so a later run can adopt the staging table.
     *
     * Throws on a non-resumable stream: its table name is per-invocation, so suspending would leak a
     * table nothing can ever find again, and both that and silently dropping the rows are worse than
     * a loud refusal.
     */
    suspend(): Promise<void> {
        if (!this.resumable) {
            return Promise.reject(new Error(
                `autoSQLStream: suspend() requires a stream opened with a sessionKey — a per-run staging table cannot be resumed, ` +
                `so suspending it would leave an unreachable table behind. Use end() to merge or abort() to discard.`
            ));
        }
        this.ended = true;
        this.handler.releaseStreamStaging(this.stagingTable);
        return Promise.resolve();
    }

    /**
     * Discard the run: drop the staging table without merging. Safe to call even if {@link write}
     * was never called, and the correct way to bail out after a failed write(). On a resumable
     * stream this discards work accumulated across earlier runs too — use {@link suspend} to keep it.
     * Returns a promise that should be awaited.
     */
    async abort(): Promise<void> {
        this.ended = true;
        return this.db.runWithSchema(this.schema, async () => {
            if (this.stagingCreated) {
                const dropQ = buildDropStreamStagingTableQuery(this.stagingTable, this.db.getConfig());
                await this.db.runTransaction([dropQ]).catch(e => {
                    this.db.error(`autoSQLStream: failed to drop staging table during abort: ${e.message}`);
                });
            }
            this.handler.releaseStreamStaging(this.stagingTable);
        });
    }
}
