import type { Database } from "./database";
import type { AutoSQLHandler } from "./autosql";
import { MetadataHeader, QueryResult, QueryStats } from "../config/types";
import { getMetaData, compareMetaData } from "../helpers/metadata";
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
            const inferredMeta = await getMetaData(flushConfig, stagingRows, this.primaryKey);
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
