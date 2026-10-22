import type { Database } from "./database";
import type { AutoSQLHandler } from "./autosql";
import { MetadataHeader, QueryResult, QueryStats } from "../config/types";
import { getMetaData, compareMetaData, restrictInferredUniques } from "../helpers/metadata";
import { writeRunAudit } from "../helpers/runAudit";
import { tableChangesExist } from "../helpers/utilities";
import { defaults } from "../config/defaults";
import {
    buildCreateStreamStagingTableQuery,
    buildInsertIntoStreamStagingQuery,
    buildSelectFromStreamStagingQuery,
    buildDropStreamStagingTableQuery,
    buildMergeFromStreamQuery,
} from '../helpers/streamHelpers';

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
    /** True when opened with a `sessionKey` — staging may outlive this run. */
    public readonly resumable: boolean;
    /**
     * Whether this stream attached to staging rows left by an earlier run.
     *
     * ⚠ **Callers that track their own resume point MUST read this.** `false` means the staging table
     * is empty — first run for this session key, or an earlier attempt's table is gone — so nothing
     * was carried over and you must re-send from the beginning. Trusting a stored high-water mark
     * against a non-adopted stream silently drops every row before that mark. Always `false` for a
     * non-resumable (no `sessionKey`) stream.
     */
    public readonly adopted: boolean;

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

        // Adopting: the table already exists, so its committed columns become this stream's shape and
        // write() must NOT try to create it again.
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
     * Contract: returns a promise that **rejects** on failure and MUST be awaited (or `.catch`ed) —
     * NOT fire-and-forget; an un-awaited failing write() becomes an unhandled rejection and its error
     * is lost. A rejected write() leaves staging indeterminate (chunk partly applied or absent); on
     * rejection either **retry the same chunk** (write() is append-only, so re-sending after a
     * transient failure is safe) or call {@link abort} to discard the run. Do NOT call {@link end}
     * after a failed/un-awaited write() expecting the gap to be ignored: end() merges whatever is
     * staged, so a lost chunk becomes missing rows.
     */
    async write(chunk: Record<string, any>[]): Promise<void> {
        if (this.ended) throw new Error(`autoSQLStream: write() called after end()/abort()`);
        if (chunk.length === 0) return;
        return this.db.runWithSchema(this.schema, async () => {
            const config = this.db.getConfig();

            if (!this.stagingCreated) {
                // Columns from the UNION of the first chunk's rows — not just chunk[0], which would
                // silently drop a key first appearing in a later row of the same chunk (A18).
                const cols = new Set<string>();
                for (const row of chunk) for (const k of Object.keys(row)) cols.add(k);
                this.columns = [...cols];
                this.columnSet = cols;
                const createQ = buildCreateStreamStagingTableQuery(this.stagingTable, this.columns, config);
                const createResult = await this.db.runTransaction([createQ]);
                if (!createResult.success) {
                    throw new Error(`autoSQLStream: failed to create stream staging table '${this.stagingTable}': ${createResult.error}`);
                }
                this.stagingCreated = true;
            }

            // Staging columns are fixed at creation. A key first appearing in a LATER row/chunk has no
            // column to land in and would be silently dropped (data loss) — fail loud so the caller
            // keeps a stable column set or starts a separate load (A18).
            for (const row of chunk) {
                for (const k of Object.keys(row)) {
                    if (!this.columnSet!.has(k)) {
                        throw new Error(`autoSQLStream: row has column '${k}' that was not present when the stream started (columns: ${this.columns!.join(', ')}). A stream requires a stable column set — include '${k}' in the first written rows, or use a separate stream/load for it.`);
                    }
                }
            }

            // SQL Server caps a request at 2,100 bound parameters, so split the chunk into sub-batches of
            // ≤ floor(2000 / colCount) rows (one request each). Other dialects send the whole chunk in one.
            const perStatement = config.sqlDialect === 'sqlserver'
                ? Math.max(1, Math.floor(2000 / this.columns!.length))
                : chunk.length;
            for (let i = 0; i < chunk.length; i += perStatement) {
                const sub = chunk.slice(i, i + perStatement);
                const insertQ = buildInsertIntoStreamStagingQuery(this.stagingTable, this.columns!, sub, config);
                const insertResult = await this.db.runTransaction([insertQ]);
                if (!insertResult.success) {
                    throw new Error(`autoSQLStream: failed to write chunk to staging table '${this.stagingTable}': ${insertResult.error}`);
                }
            }
        });
    }

    /**
     * Merge all staged rows into the target table (infer schema, apply DDL, bulk INSERT…SELECT with a
     * per-row retry fallback), then drop the staging table.
     *
     * Contract: returns a promise that **rejects** on failure and must be awaited. end() merges
     * whatever is currently staged — it doesn't know about {@link write} calls that failed or were
     * never awaited, so ensure every chunk resolved (or was retried) first, or a lost chunk silently
     * becomes missing rows. To discard instead of merge, use {@link abort}.
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
        // Whether the staged rows reached the target. Gates the drop in `finally` for a resumable
        // stream, so a failed merge does not destroy work accumulated across earlier runs.
        let promoted = false;

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
                // An empty staging table is a completed (if empty) session — nothing to lose by
                // dropping it, so let `finally` clean up rather than leaking it to the sweeper.
                promoted = true;
                return { start, end: new Date(), success: true, duration: 0, affectedRows: 0, table: this.table };
            }

            // Dataset-level number-format consensus from the staged rows. Overlay the resolved
            // separators on inference (flushConfig) AND the per-row fallback below: the bulk merge
            // casts via the DB (so a grouped value like "1,234" is rejected there and lands per-row),
            // and the per-row fallback now sqlizes, normalizing under these separators. numberFormat
            // needs no overlay — already on this.config, so resolveSeparatorConsensus returns undefined
            // and inference/per-row read it directly.
            const separators = this.handler['resolveSeparatorConsensus'](stagingRows);
            const flushConfig = separators
                ? { ...config, thousandsSeparator: separators.thousands, decimalSeparator: separators.decimal }
                : config;

            // Infer schema from staging data
            const tPrepare = perf();
            const { currentMetaData } = await this.handler.fetchTableMetadata(this.table);
            const inferredMeta = restrictInferredUniques(await getMetaData(flushConfig, stagingRows, this.primaryKey), currentMetaData, config);
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

            // Attempt bulk merge with casts
            const tLoad = perf();
            const mergeQ = buildMergeFromStreamQuery(this.table, this.stagingTable, updatedMetaData, insertType as 'UPDATE' | 'INSERT', config);
            const mergeResult = await this.db.runTransaction([mergeQ]);

            let affectedRows = mergeResult.affectedRows ?? 0;

            if (!mergeResult.success) {
                // Fallback: per-row retry with schema widening, under the resolved separators so the
                // per-row sqlize (via getConfig()) normalizes values with the detected format.
                affectedRows = await this.db.runWithSeparators(separators, () =>
                    this._perRowMerge(stagingRows, updatedMetaData, insertType as 'UPDATE' | 'INSERT', maxRetries));
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
            await writeRunAudit(this.db, { table: this.table, success: false, rows: 0, affectedRows: 0, durationMs, rowsPerSecond: 0, staged: true, bulkLoad: !!this.db.getConfig().bulkLoad, error: error instanceof Error ? error.message : String(error), errorCode: (error as any)?.code != null ? String((error as any).code) : undefined });
            return { start, end, duration: durationMs, affectedRows: 0, success: false, error: error instanceof Error ? error.message : String(error), errorCode: (error as any)?.code != null ? String((error as any).code) : undefined };
        } finally {
            // Drop staging once the rows are safely in the target. A per-run stream drops either way
            // (its table is useless after this call). A RESUMABLE stream whose merge failed keeps its
            // table: the failure may be transient (lock timeout, connection blip), and dropping here
            // would throw away rows accumulated over earlier runs — the exact loss this feature
            // exists to prevent. The caller can retry end(), or abort() to discard deliberately; if
            // it never comes back, the table's lifetime belongs to the session registry/sweeper.
            const keepForResume = this.resumable && !promoted;
            if (this.stagingCreated && !keepForResume) {
                const dropQ = buildDropStreamStagingTableQuery(this.stagingTable, this.db.getConfig());
                await this.db.runTransaction([dropQ]).catch(e =>
                    this.db.error(`autoSQLStream: failed to drop staging table '${this.stagingTable}': ${e.message}`)
                );
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
     * Stop without promoting and without discarding: leave the staging table exactly as it is so a
     * later `openStream` with the same `sessionKey` can adopt it and keep appending.
     *
     * This is the third state, distinct from both of the existing ones — {@link end} merges and
     * {@link abort} destroys, and neither expresses "I got through part of the data, I'll be back".
     * Call it when a run ends early for a reason that is worth retrying (page fetch failed, token
     * expired, Lambda timed out) rather than a reason that invalidates the data.
     *
     * Only meaningful on a resumable stream: without a `sessionKey` the table name is per-invocation,
     * so nothing could ever find it again — suspending would just leak it. That case throws, since
     * silently leaking a table (or silently dropping the rows) are both worse than a loud refusal.
     *
     * After this call the handle is spent; open a new stream with the same key to continue.
     */
    async suspend(): Promise<void> {
        if (!this.resumable) {
            throw new Error(
                `autoSQLStream: suspend() requires a stream opened with a sessionKey — a per-run staging table cannot be resumed, ` +
                `so suspending it would leave an unreachable table behind. Use end() to merge or abort() to discard.`
            );
        }
        this.ended = true;
        this.handler.releaseStreamStaging(this.stagingTable);
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
                await this.db.runTransaction([dropQ]).catch(e =>
                    this.db.error(`autoSQLStream: failed to drop staging table during abort: ${e.message}`)
                );
            }
            this.handler.releaseStreamStaging(this.stagingTable);
        });
    }
}
