import type { AutoSQLHandler } from "./autosql";
import type { Database } from "./database";
import { InsertInput, QueryResult, QueryInput, AlterTableChanges } from "../config/types";
import { getTempTableName, getInsertValues, throwIfFailedResults, normalizeResultKeys, getTrueTableName } from "../helpers/utilities";
import { defaults } from "../config/defaults";

/**
 * Staging pipeline collaborator (R1 Slice 2, PR 2d): create -> populate -> resolve-conflicts ->
 * merge -> cleanup. Behaviour-preserving move out of AutoSQLHandler; holds a back-ref because table
 * configuration and direct insert are worker-dispatched and per-row degradation lives on the handler.
 * AutoSQLHandler imported type-only to avoid a runtime import cycle.
 */
export class StagingPipeline {
    private handler: AutoSQLHandler;
    private db: Database;

    constructor(handler: AutoSQLHandler, db: Database) {
        this.handler = handler;
        this.db = db;
    }

    async prepareStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
        const stagingPrefix = insertInput[0]?.stagingPrefix;
        const uniqueTables = Array.from(new Set(insertInput.map(input => input.table)));

        const stagingQueries: QueryInput[][] = uniqueTables.map(table => {
            // Drop any leftover staging table BEFORE recreating it: a run that crashed before
            // removeStagingTables leaves an orphan, and CREATE TABLE IF NOT EXISTS would reuse its
            // stale schema. Dropping first guarantees the temp table matches the current real table.
            const tempTableName = getTempTableName(table, stagingPrefix);
            return [this.db.getDropTableQuery(tempTableName), this.db.getCreateTempTableQuery(table, stagingPrefix)];
        });
        const allCreateResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingQueries);

        throwIfFailedResults(allCreateResults, "table create queries")
        return allCreateResults
    }

    async insertStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
        // The staging temp table was created via CREATE TABLE AS SELECT from the already-configured
        // real table, so its columns match. Re-applying the real table's ALTER changes here would try
        // to add columns the CTAS copy already has ("duplicate column"). Clear the changes so staging
        // config is a no-op, but keep updatedMetaData so the insert still resolves per-column values
        // (e.g. the dwh_* timestamp defaults) for the temp table.
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
        await this.handler['configureTables'](stagingInputs)
        if (this.db.getConfig().bulkLoad) {
            return await this.bulkLoadStaging(stagingInputs)
        }
        return await this.handler['insertData'](stagingInputs)
    }

    /**
     * Populate the staging temp tables with the dialect's bulk-copy mechanism (Postgres COPY / MySQL
     * LOAD DATA LOCAL INFILE) instead of parameterised INSERT — the opt-in `bulkLoad` fast path. The
     * temp → real merge is unchanged, so upsert semantics are preserved. On any bulk-load failure
     * (server local_infile off, missing pg-copy-streams, a value the text protocol rejects) it falls
     * back to INSERT for that table. COPY is all-or-nothing, so the temp table is empty on fallback.
     */
    private async bulkLoadStaging(stagingInputs: InsertInput[]): Promise<QueryResult[]> {
        const config = this.db.getConfig();
        const dialectConfig = this.db.getDialectConfig();
        const excludeAutoIncrement = config.surrogateKey === true;
        const results: QueryResult[] = [];
        for (const input of stagingInputs) {
            const header = input.comparedMetaData?.updatedMetaData || input.metaData;
            // Same column set as the INSERT path: drop auto-increment (surrogate) columns so the DB
            // assigns them. getInsertValues drops the same columns per row, keeping columns/values aligned.
            const columns = Object.keys(header).filter(col => !(excludeAutoIncrement && header[col].autoIncrement === true));
            const valueRows = input.data.map(row => getInsertValues(header, row, dialectConfig, config, true));
            try {
                results.push(await this.db.bulkLoadRows(input.table, columns, valueRows, header));
            } catch (err) {
                this.db.warn(`bulkLoad failed for '${input.table}', falling back to INSERT: ${(err as Error).message}`);
                results.push(...await this.handler['insertData']([input]));
            }
        }
        return results;
    }

    async removeStagingTables(insertInput: InsertInput[]): Promise<QueryResult[]> {
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

    async resolveConflicts(insertInput: InsertInput[]): Promise<void> {
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

        // Derive the constraint structure (non-primary unique indexes → columns + primary-key columns)
        // from already-known metadata where it is provably identical to the live catalog (see
        // deriveConstraintStructure), else fall back to live introspection. Deriving skips the
        // unique-index + PK introspection round-trip on the common path (idempotent re-ingest of a
        // stable schema, metadata just read from the DB).
        const tablesToIntrospect: string[] = [];
        for (const table of uniqueTables) {
            const derived = this.deriveConstraintStructure(inputByTable.get(table));
            if (derived) tableStructure[table] = derived;
            else tablesToIntrospect.push(table);
        }

        if (tablesToIntrospect.length > 0) {
            const uniqueIndexesQuery = tablesToIntrospect.map(table => [this.db.getUniqueIndexesQuery(table)]);
            const primaryKeyQuery = tablesToIntrospect.map(table => [this.db.getPrimaryKeysQuery(table)]);
            // Run unique-index and primary-key introspection as ONE concurrency-governed batch (both
            // are independent reads) rather than two sequential round-trips. Results come back in input
            // order, so the first N groups are unique indexes and the next N are PKs.
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
        const dropsByTable: { table: string; queries: QueryInput[] }[] = [];

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
              dropsByTable.push({ table, queries: tableConstraintsQueries });
            } else {
              this.db.warn(`resolveConflicts: staged data for '${table}' violates UNIQUE constraint(s) [${violatingIndexes.join(", ")}], but dropUniqueConstraints is off — the constraint(s) are KEPT and the merge will fail (or divert to rejectedRowsTable if configured) on the colliding rows. Set dropUniqueConstraints: true to auto-drop them instead.`);
            }
          }
        }

        // Re-acquire each table's schema lock around its unique-constraint DROP when locking is on
        // (R1 Slice 2, PR 2f). The entry point releases the load's advisory lock before inserts (held
        // only through inference + DDL), so without this two concurrent same-constraint DROPs race —
        // unserialized DDL on a live table. Lock off → drops run as one concurrent batch (unchanged).
        // Residual TOCTOU (acceptable): the conflict-count check above ran BEFORE this lock, so two
        // loads can both decide to drop; the loser then DROPs an already-gone constraint and — builders
        // don't emit IF EXISTS — fails loud via throwIfFailedResults. Strictly better than the prior
        // unserialized race (which could corrupt the DDL), just surfaced as an error.
        const config = this.db.getConfig();
        const useSchemaLock = config.useSchemaLock;
        const lockTimeout = config.schemaLockTimeout ?? defaults.schemaLockTimeout;
        const removeConstraints: QueryResult[] = [];
        if (useSchemaLock) {
            for (const { table, queries } of dropsByTable) {
                await this.db.acquireSchemaLock(table, lockTimeout);
                try {
                    removeConstraints.push(...await this.db.runTransactionsWithConcurrency([queries]));
                } finally {
                    await this.db.releaseSchemaLock(table);
                }
            }
        } else {
            removeConstraints.push(...await this.db.runTransactionsWithConcurrency(dropsByTable.map(d => d.queries)));
        }
        throwIfFailedResults(removeConstraints, 'unique constraint removal queries')
        return;
    }

    /**
     * Derive a table's drop-target constraint structure (non-primary unique indexes → columns + PK
     * columns) from resolved metadata WITHOUT a catalog round-trip, but only when provably identical
     * to the live catalog. Returns null (→ caller introspects live) whenever the run changed the
     * constraint structure or any unique lacks a real introspected name:
     *   • no compared/resolved metadata to reason about;
     *   • a unique was dropped this run (`noLongerUnique`) — `updatedMetaData` still shows it unique;
     *   • the PK changed this run (`primaryKeyChanges`) — the conflict-count join keys on it;
     *   • a newly-added column is unique — its index name isn't introspected yet;
     *   • any unique column lacks a `uniqueName` — inferred/just-created uniques (incl. every first
     *     load) and MySQL's column-named uniques can't be reproduced, so the DROP target is unknown;
     *   • any column belongs to >1 non-primary unique index (`uniqueName` is a comma-joined list) —
     *     the per-column single-name model can't unambiguously reconstruct each composite index's
     *     full column set, so grouping could mis-scope/over-drop. autosql only creates single-column
     *     uniques, so this is external-table-only.
     * These are exactly the cases where deriving could drop the wrong (or already-gone) constraint —
     * the silent over-drop the introspection fallback exists to avoid.
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
            // Unique but no real DB index name → DROP target unknown (just-created/inferred unique);
            // bail to live introspection.
            if (def.unique && !def.uniqueName) return null;
            if (def.uniqueName) {
                // Comma → column is in >1 non-primary unique index; the single-name model can't group
                // composite indexes unambiguously, so fall back rather than mis-scope.
                if (def.uniqueName.includes(',')) return null;
                (uniques[def.uniqueName] ??= []).push(columnName);
            }
        }
        return { uniques, primary };
    }

    async insertFromStagingTables(insertInput: InsertInput[], options?: { perRowFallback?: boolean }): Promise<QueryResult[]> {
        const stagingInputs: InsertInput[] = insertInput.map(input => ({
            ...input,
            insertType: "UPDATE"
        }));
        const stagingInsertQueries = (stagingInputs).map(stagingInput => {
            return [this.db.getInsertFromStagingQuery(stagingInput)]
        })
        const allInsertResults : QueryResult[] = await this.db.runTransactionsWithConcurrency(stagingInsertQueries);

        // Opt-in graceful degradation WITHOUT row-level history: when perRowFallback AND
        // rejectedRowsTable are set AND a table's atomic merge failed, retry that table's rows one at a
        // time and divert unrecoverable rows. Without the opt-in this stays all-or-nothing (default).
        // The addHistory case uses insertFromStagingTablesAtomic so history/data stay consistent.
        const config = this.db.getConfig();
        if (options?.perRowFallback && config.rejectedRowsTable && allInsertResults.some(r => !r?.success)) {
            return await this.handler['degradation'].applyStagingPerRowFallback(insertInput, allInsertResults);
        }

        throwIfFailedResults(allInsertResults, 'insert from staging table queries')
        return allInsertResults
    }

    /**
     * Zero-window staging merge WITH row-level history: before-image capture and merge run in ONE
     * transaction, so history and data commit/roll-back together with no crash window. Per table,
     * attempt the whole-table [before-image, merge] transaction; merge-failure handling depends on
     * `perRowFallback`:
     *   - plain atomic history (no rejectedRowsTable): the failed table's transaction already rolled
     *     back — surface all-or-nothing (throwIfFailedResults), matching the non-atomic path replaced;
     *   - degradation combo (rejectedRowsTable + addHistory): fall back to a per-PK loop where each
     *     PK's [before-image, single-PK merge] is its own transaction — a PK whose merge violates a
     *     constraint rolls back (no history, no data) and diverts to `rejectedRowsTable`.
     * `historyByTable` maps a real table to its (already-created) history input; a table not in
     * `historyTables` merges with no before-image.
     */
    async insertFromStagingTablesAtomic(insertInput: InsertInput[], historyInputs: InsertInput[], options?: { perRowFallback?: boolean }): Promise<QueryResult[]> {
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

        // Without the rejectedRowsTable opt-in there is nothing to divert to: the failed table's
        // [before-image, merge] transaction already rolled back atomically — surface all-or-nothing
        // (plain-addHistory atomicity path, spec-1 §5.b / PR 2g).
        if (!(options?.perRowFallback && this.db.getConfig().rejectedRowsTable)) {
            throwIfFailedResults(allResults, 'insert from staging table queries');
            return allResults;
        }

        for (let i = 0; i < allResults.length; i++) {
            if (allResults[i]?.success) continue;
            allResults[i] = await this.handler['degradation'].perPkAtomicStagingMerge(insertInput[i], stagingInputs[i], historyByTable.get(insertInput[i].table));
        }
        return allResults;
    }
}
