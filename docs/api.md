# API Reference

The `Database` class is the primary entry point into AutoSQL's workflow. It handles connection
management and exposes high-level `autoSQL` methods for automated insertions, table creation, and
metadata handling. See the [README](../README.md) for a quick start.

```ts
import { Database } from 'autosql';

const db = Database.create(config);
await db.establishConnection();

await db.autoConfigureTable(
  'target_table', // table name
  sampleData,     // raw input data
  null,           // optional existing metadata
  initialMeta     // optional manually defined metadata
);
```

## `Database` class

### Static method

- **`Database.create(config)`** – returns a `MySQLDatabase`, `PostgresDatabase`, or `SqlServerDatabase` instance based on `config.sqlDialect`.

### Core methods

- **`getConfig()`** – the full `DatabaseConfig` used to initialise this instance.
- **`updateSchema(schema: string)`** – sets the instance's default schema (mutates config). For a **per-operation** schema that won't interfere with concurrent operations, prefer passing `schema` to `autoSQL`/`autoSQLChunked`/`openStream`, or use `runWithSchema` below.
- **`runWithSchema(schema: string, fn: () => T)`** – runs `fn` with `schema` as the effective schema for the duration, **without mutating instance config** — concurrent operations with different schemas stay isolated. (What the per-call `schema` argument uses internally.)
- **`getDialect()`** – the SQL dialect (`mysql` | `pgsql` | `sqlserver`).
- **`establishConnection()`** – creates and stores a live database connection.
- **`testConnection()`** – attempts to connect, returns success as a boolean.
- **`runQuery(queryOrParams: QueryInput | QueryInput[])`** – executes a query or list of queries.
- **`runTransaction(queries: QueryInput[])`** – runs the queries **atomically on a single pinned connection** (`BEGIN` → … → `COMMIT`, automatic `ROLLBACK` on failure, transient-error retry). Use this for transactional work.
- **`startTransaction(client)` / `commit(client)` / `rollback(client)`** – low-level transaction control; each **requires a pinned connection** (managed internally by `runTransaction`). Prefer `runTransaction()`.
- **`runTransactionsWithConcurrency(queryGroups: QueryInput[][])`** – runs multiple query batches in parallel.
- **`closeConnection()`** – safely closes the active connection.

### Table and schema methods

- **`checkSchemaExists(schemaName: string)`** – whether the given schema exists.
- **`createSchema(schemaName: string)`** – creates the schema if it doesn't exist. Resolves `{ [schemaName]: true }` (plus a deprecated `success: true`). Rejects if the `CREATE` fails, with the driver code on `error.code` (e.g. Postgres `42501`, MySQL `ER_DBACCESS_DENIED_ERROR`).
- **`createTableQuery(table: string, headers: MetadataHeader)`** – returns `QueryInput[]` to create a table.
- **`alterTableQuery(table: string, oldHeaders: MetadataHeader, newHeaders: MetadataHeader)`** – returns `QueryInput[]` to alter an existing table.
- **`dropTableQuery(table: string)`** – returns a `QueryInput` to drop a table.
- **`getTableMetaData(schema: string, table: string)`** – fetches current metadata from the DB for a table.

### AutoSQL methods (exposed on `db`)

- **`autoSQL(table, data, schema?, primaryKey?, options?)`** – the simplest way to handle everything: inference, schema changes, batching, inserting, history, workers, nested structures, in one call. Pass `options.assumeSchema` when you already know the schema to skip type inference (declared columns are authoritative, which also avoids inference footguns like small integers reading as boolean); undeclared columns still infer. Returns a `QueryResult` with `affectedRows`, the resolved `metaData` (cache it, pass back as `options.existingSchema` next time to skip introspection), and per-run `stats` (`QueryStats`).
- **`preview(table, data, schema?, primaryKey?, options?)`** – dry run; see [Preview / dry run](#preview--dry-run) below.
- **`autoInsertData(inputOrTable, inputData?, inputMetaData?, inputPreviousMetaData?, inputComparedMetaData?, inputRunQuery = true, inputInsertType?)`** – executes a full insert via the dialect-aware batching engine. `inputRunQuery: true` runs the queries via `runTransactionsWithConcurrency()`; `false` returns the `QueryInput[]` without running them.
- **`autoConfigureTable(inputOrTable, data?, currentMeta?, newMeta?, runQuery = true)`** – determines create vs. alter from a metadata comparison. `runQuery: true` applies immediately; `false` returns the queries for inspection.
- **`autoCreateTable(table, newMetaData, tableExists?, runQuery = true)`** – creates a new table. `runQuery: false` returns the `CREATE TABLE` queries without executing.
- **`autoAlterTable(table, tableChanges, tableExists?, runQuery = true)`** – alters a table from a computed diff. Same `runQuery` semantics.
- **`fetchTableMetadata(table)`** – looks up a table's metadata, returns `{ currentMetaData, tableExists }`.
- **`splitTableData(table, data, metaData)`** – when `autoSplit` is enabled, splits a wide dataset across multiple tables. Returns `InsertInput[]` for multi-table execution.
- **`handleMetadata(table, data, primaryKey?)`** – combines inference + comparison into one call. Returns `{ currentMetaData, newMetaData, mergedMetaData, initialComparedMetaData, changes }`.
- **`getMetaData(config, data, primaryKey?)`** – analyses sample data; returns a metadata map with type, length, nullability, uniqueness, key suggestions.
- **`compareMetaData(oldMeta, newMeta)`** – returns `{ changes: AlterTableChanges, updatedMetaData }`.
- **`autoSQLChunked(table, iterable, schema?, primaryKey?)`** *(v1.0.5+)* – streaming-friendly variant of `autoSQL` for an `AsyncIterable` of row chunks. Schema inference and DDL run once on the first non-empty chunk; later chunks skip straight to insert. Compatible with `useSchemaLock` and `useStagingInsert`. See [Streaming & large datasets](streaming.md#large-dataset-support-autosqlchunked).
- **`openStream(table, schema?, primaryKey?, opts?)`** *(v1.1.0+; `opts.sessionKey` for staging that survives between runs)* – opens a streaming session, returns an `AutoSQLStreamHandle`. See [Streaming & large datasets](streaming.md).

Each method works with the same `Database` instance.

## Preview / dry run

`db.preview(table, data, schema?, primaryKey?, options?)` computes exactly what an `autoSQL(table, data, …)` call **would** do — the inferred schema, whether the table is created or altered, the exact DDL, and any changes that would be blocked — **without writing anything**. It reads the current schema to compute the diff; nothing is created, altered, or inserted. Ideal for a "here's how your table will change" confirmation step in a UI, or a CI check.

```ts
const plan = await db.preview('orders', [
  { order_id: 1, amount: '1.234,56', country: 'US' },
]);

plan.tables[0].action;          // 'create' | 'alter' | 'noop'
plan.tables[0].ddl;             // ['ALTER TABLE `orders` ADD COLUMN …', …]  — NOT executed
plan.tables[0].changes;         // the AlterTableChanges diff (null on 'create')
plan.tables[0].inferredSchema;  // the MetadataHeader autosql inferred (incl. managed dwh_* columns)
plan.tables[0].currentSchema;   // the live table's schema, or null if it doesn't exist yet
plan.tables[0].blockedChanges;  // ["Would DROP column 'obsolete' — blocked; set deleteColumns: true", …]
plan.numberFormat;              // { thousands: '.', decimal: ',' } when a format was detected/set
plan.rowCount;                  // number of input rows
```

Returns an `AutoSQLPreview`: a `tables` array (one entry per target table — more than one when `autoSplit` or nested extraction apply), the effective `numberFormat`, and the input `rowCount`. It runs the same inference/diff pipeline as `autoSQL` (surrogate keys, timestamps, number-format detection, splits), so the plan matches what a real load would do.

**`preview` vs `safeMode`:** `safeMode` runs a load but **skips DDL** (inserts into whatever the table already supports); `preview` runs **no** load and executes **no** DDL — it only reports what would happen.

## Convenience utilities

AutoSQL exposes the utilities that power `autoSQL`, usable independently: metadata analysis, SQL formatting, batching, config validation, and more.

### Type inference & normalisation

- **`predictType(value)`** – predicts a SQL-compatible type (`varchar`, `datetime`, `int`, etc.) from a single value.
- **`collateTypes(typeSetOrArray)`** – collapses a `Set`/`Array` of types to one compatible SQL type.
- **`normalizeNumber(input, thousands, decimal)`** – standardises a numeric value to SQL-safe form with optional locale indicators.
- **`calculateColumnLength(column, value, sqlLookup)`** – updates column length/decimal precision from input data.
- **`shuffleArray(array)`** – randomly reorders an array (used for sampling).
- **`isObject(val)`** – type-safe non-null-object check.

### Config & metadata tools

- **`validateConfig(config)`** – validates and merges `DatabaseConfig` with defaults.
- **`mergeColumnLengths(lengthA, lengthB)`** – the greater of two metadata column length states.
- **`setToArray(set)`** – converts a `Set` to an array.
- **`normalizeKeysArray(keys)`** – flattens/sanitizes arrays of key strings (e.g. primary keys).
- **`isValidDataFormat(data)`** – checks the input is a valid array of plain objects suitable for insert.

### Metadata inference & preparation

- **`initializeMetaData(headers)`** – a default metadata object from column headers (default flags, null types).
- **`getDataHeaders(data, config)`** – scans sample data to derive column names and infer initial metadata.
- **`predictIndexes(metaData, maxKeyLength?, primaryKey?, sampleData?)`** – suggests primary keys, unique constraints, indexes.
- **`updateColumnType(existingMeta, newValue)`** – adjusts a column's type/attributes from a new sample value.

### Insert planning & execution

- **`splitInsertData(data, config)`** – splits a dataset into batches meeting size/row-count constraints.
- **`getInsertValues(metaData, row, dialectConfig)`** – a row's values as a SQL-safe array, dialect-formatted.
- **`organizeSplitData(data, splitMetaData)`** – partitions a dataset by metadata groups for multi-table insert.
- **`organizeSplitTable(table, newMetaData, currentMetaData, dialectConfig)`** – generates split-metadata configs from structural divergence.
- **`estimateRowSize(metaData, dialect)`** – estimates a row's byte size, flags potential overflows.
- **`parseDatabaseMetaData(rows, dialectConfig?)`** – transforms SQL column descriptions into autosql metadata.
- **`tableChangesExist(alterTableChanges)`** – whether a proposed diff requires any schema change.
- **`isMetaDataHeader(obj)`** – type guard for a metadata header shape.
