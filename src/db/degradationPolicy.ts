import type { AutoSQLHandler } from "./autosql";
import type { Database } from "./database";
import { InsertInput, QueryResult, MetadataHeader, QueryInput } from "../config/types";
import { getInsertValues, tableChangesExist, throwIfFailedResults, sqlize } from "../helpers/utilities";
import { getMetaData, compareMetaData } from "../helpers/metadata";
import { buildBootstrapRejectedRowsQuery, buildInsertRejectedRowsQuery } from "../helpers/streamHelpers";
import { defaults } from "../config/defaults";

/**
 * Graceful-degradation collaborator (R1 Slice 2, PR 2c). Owns the four fallback variants: the shared
 * per-row retry engine plus the direct-insert, staging non-atomic, and staging atomic-per-PK adapters.
 * Behaviour-preserving move out of AutoSQLHandler; holds a back-ref because schema widening is
 * worker-dispatched (configureTables). `perRowInsertWithRetry` stays reachable on the handler via a
 * thin delegator so AutoSQLStreamHandle is untouched. AutoSQLHandler imported type-only.
 */
export class DegradationPolicy {
    private handler: AutoSQLHandler;
    private db: Database;

    constructor(handler: AutoSQLHandler, db: Database) {
        this.handler = handler;
        this.db = db;
    }

    /**
     * Graceful-degradation fallback for the direct-insert path: for each input whose bulk insert
     * failed, retry its rows one at a time (widening schema between rounds) and divert still-failing
     * rows to `rejectedRowsTable`. Only reached when `perRowFallback` AND `rejectedRowsTable` are set
     * (see insertData). The failed batch ran as a transaction and rolled back, so re-inserting every
     * row is safe (no double-insert).
     */
    async applyPerRowFallback(insertInput: InsertInput[], allInsertResults: QueryResult[]): Promise<QueryResult[]> {
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
            // Preserve already-resolved key columns during widening re-inference so it can't re-infer
            // different keys for the failed rows.
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
                // Pre-sqlize the row like the bulk direct path (getInsertValues, sqlizeValues=true) so
                // this fallback normalizes values (number separators, decimal rounding,
                // datetime/timezone, boolean canonicalization) instead of binding raw. Without it the
                // fallback would store different values (e.g. resolved "1,234" -> 1234 rejected as raw),
                // and locale-formatted numbers could never land via degradation/streaming.
                const normalisedRow = getInsertValues(workingMeta, row, this.db.getDialectConfig(), this.db.getConfig(), true);
                const insertQ = this.db.getInsertStatementQuery(table, [normalisedRow], workingMeta, insertType);
                // Single attempt: this loop already retries across rounds, so the internal retry is
                // redundant — and for "INSERT" (non-idempotent) could duplicate a row whose ambiguous
                // failure actually applied server-side (A15).
                const result = await this.db.runQuery(insertQ, 1);
                if (result.success) {
                    totalInserted += result.affectedRows ?? 1;
                } else {
                    failures.push({ row, error: result.error ?? 'unknown error' });
                }
            }

            // Remaining work = only rows that failed this round. Assign BEFORE the break so a fully
            // successful round leaves nothing pending — otherwise inserted rows would stay in
            // pendingRows and get diverted as "rejects" below.
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
                    await this.handler['configureTables'](widenInput).catch(e =>
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
                // Fail loud if the divert itself fails (bootstrap or insert). runTransaction returns
                // {success:false} rather than throwing, so an unchecked result would let rows vanish
                // while the load reported success — the exact loss rejectedRowsTable prevents (A5).
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

    /**
     * Graceful-degradation fallback for the STAGING path WITHOUT row-level history (opt-in, see
     * insertFromStagingTables): the atomic merge rolled back, so nothing from a failed table landed.
     * Re-run that table's rows one at a time as an upsert into the real table (matching the merge's
     * UPDATE semantics), diverting unrecoverable rows to `rejectedRowsTable`.
     */
    async applyStagingPerRowFallback(insertInput: InsertInput[], allInsertResults: QueryResult[]): Promise<QueryResult[]> {
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
     * Per-PK fallback for the atomic history path: per row, run [before-image for that PK, single-PK
     * merge] in ONE transaction. A PK whose merge violates a constraint rolls back (no history, no
     * data) and diverts to `rejectedRowsTable`. No schema widening here (unlike `perRowInsertWithRetry`):
     * `configureTables` already fitted the schema to every row, so a failure is a data/constraint issue
     * re-inference can't fix.
     */
    async perPkAtomicStagingMerge(input: InsertInput, stagingInput: InsertInput, historyInput?: InsertInput): Promise<QueryResult> {
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
            // Fail loud if the divert itself fails — otherwise rows vanish while the load reports
            // success (A5). runTransaction returns {success:false} rather than throwing, so check it.
            if (!divert.success) {
                throw new Error(`autoSQL: ${rejected.length} row(s) failed to merge into '${input.table}' AND could not be written to rejectedRowsTable '${config.rejectedRowsTable}': ${divert.error ?? 'unknown error'}. No rows were silently dropped — resolve the rejects-table error (e.g. permissions or an incompatible existing table) and retry.`);
            }
            this.db.warn(`autoSQL: ${rejected.length} row(s) could not be merged into '${input.table}' and were written to '${config.rejectedRowsTable}'.`);
        }
        const now = new Date();
        return { start: now, end: now, duration: 0, success: true, affectedRows: inserted };
    }
}
