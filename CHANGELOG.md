## [1.3.1] - 2026-07-05
### 🐛 Bug Fixes (concurrency)
- **Schema-history record id.** `recordMigrationStart` returned a wrong/zero id, leaving the history row stuck at `pending` and drift detection without a baseline. MySQL read the id from a separate `SELECT LAST_INSERT_ID()` that ran on a different pooled connection (LAST_INSERT_ID is connection-scoped) — it now runs in one connection-pinned transaction. PostgreSQL failed outright with `"inconsistent types deduced for parameter $1"` (schema-history recording was entirely broken on PG) — fixed with an explicit `$1::varchar` cast.
- **Stream orphan cleanup no longer drops a live stream's staging table.** Concurrent streams to the same table share the `${prefix}${table}__` name pattern the cleanup scans; staging tables of streams open on the same instance are now excluded. (Cross-process concurrency still needs a DB-side liveness marker.)
- **PostgreSQL advisory-lock key widened to 64-bit** (two int4 keys via sha256, `pg_advisory_lock(int4, int4)`) so distinct table names no longer collide onto the same lock and serialize. Also release any stale lock connection before overwriting the registry entry (prevents a pooled-connection leak) on both dialects.

---

## [1.3.0] - 2026-07-05
### 🐛 Bug Fixes (transaction atomicity)
- **`runTransaction` is now atomic.** Previously `START TRANSACTION`, the statements, and `COMMIT`/`ROLLBACK` each ran through a freshly acquired/released pool connection, so a transaction's statements scattered across different connections in autocommit mode — atomicity held only by luck (sequential reuse of the same idle connection) and broke under `runTransactionsWithConcurrency`, with rollback running on an unrelated connection. Transactions now acquire **one** connection and run all statements plus BEGIN/COMMIT/ROLLBACK on it. **PostgreSQL transactional DDL rollback now actually works.**
- **Transaction-level retry.** Transient errors (deadlock/serialization) now retry the whole transaction rather than a single statement (which can't succeed once the transaction is aborted). DDL-containing batches run exactly once, since MySQL implicitly commits before DDL and re-running would double-apply non-idempotent DML in mixed batches.

### ⚠️ Behavior change
- `startTransaction` / `commit` / `rollback` now require a pinned connection argument and throw otherwise (previously they were silent no-ops against throwaway connections). Use `runTransaction` for atomic multi-statement work.

---

## [1.2.0] - 2026-07-05
### 🔒 Security
- **SQL identifier escaping.** All interpolated identifiers (table/column/schema/index/constraint names, which originate from arbitrary JSON keys and `metaData`) are now quote-escaped at generation time, and type tokens/lengths are validated. Closes DDL/DML injection via crafted column names. Output is byte-identical for well-formed identifiers.
- **DEFAULT value validation.** Column defaults are validated (`assertSafeDefaultExpression`) to reject statement separators, comment introducers, commas, and unbalanced quotes before they reach the statement.
- **No value double-escaping.** `getInsertValues(..., sqlize=true)` no longer applies quote/backslash escaping to values that are then parameter-bound, so `O'Brien` is stored correctly instead of `O''Brien`.

### 🐛 Bug Fixes (type inference — fidelity-first)
- Scientific-notation columns now emit the `exponent` type (→ `DOUBLE`/`NUMERIC`) instead of the invalid `exponential`, which produced invalid DDL.
- Leading-zero digit strings (`"007"`, `"07030"`, phone numbers) stay `varchar` identifiers instead of being coerced to integers (which dropped the leading zeros).
- Plain `0/1` digit strings (`"10"`, `"100"`) type as numbers, not `BINARY` (which right-pads with `\0`).
- The date regex is fully anchored, so partial matches like `"2021-05-05 is my birthday"` are no longer coerced to `date`.
- TEXT/MEDIUMTEXT/LONGTEXT promotion and max-key-length checks are now byte-aware, preventing silent truncation of multibyte (CJK/emoji) data. `VARCHAR(n)` sizing stays char-based.
- An explicitly requested primary key on a long/text/decimal column is honored instead of being silently dropped by the auto-index length guard.
- The id-like primary-key preference is anchored to `^id$`/`_id$`, so ordinary words ending in `id` (`paid`, `void`, `grid`) are no longer mistaken for identifier columns.

### ✨ What's New
- **`thousandsSeparator` / `decimalSeparator` config** — locale-aware number parsing. Set both to disambiguate values like `"1.000"` (1 vs 1000); omit both to use the existing auto-heuristic.

---

## [1.1.0] - 2026-03-25
### ✨ What's New

#### Streaming inserts
- **`openStream(table, schema?, primaryKey?)`** — new streaming API for writing large or incremental datasets without holding everything in memory.
  - Returns an `AutoSQLStreamHandle` with `write(chunk)`, `end()`, and `abort()` methods.
  - **Connectivity check on open** — a `SELECT 1` is issued immediately when the stream is opened so that bad credentials or unreachable hosts surface before any data is written.
  - **Staging-table isolation** — each stream run gets its own staging table (e.g. `autosql_stream__users__a3f9b2c1`). All staging columns are untyped (`LONGTEXT` / `TEXT`) to accept arbitrary raw values. The actual target schema is inferred at merge time.
  - **Lazy staging table creation** — the staging table is not created until the first `write()` call, when the column names of the incoming chunk are known.
  - **Atomic merge on `end()`** — at close time, autosql reads the staging data, runs the full `getMetaData` → `compareMetaData` → `configureTables` pipeline, then issues a bulk `INSERT … SELECT` with dialect-specific type casts (`CAST(col AS …)` for MySQL, `col::type` for PostgreSQL).
  - **Per-row fallback** — if the bulk merge fails, autosql retries each row individually. Failed rows trigger a schema widening pass (`compareMetaData`) before each retry round. Up to `streamMaxRetries` (default `3`) rounds are attempted.
  - **Rejected-rows table (opt-in)** — if `rejectedRowsTable` is configured, rows that cannot be merged after all retries are written to that table instead of throwing. Without this option, unrecoverable rows throw.
  - **`abort()`** — drops the staging table without merging. Safe to call even if no data has been written yet (no-op if staging table was never created).
  - **`keepOrphanedStagingTables`** (default `false`) — on `openStream`, autosql scans for and drops leftover staging tables from previous runs that were never cleanly ended. Set to `true` to preserve them (useful for debugging).
  - Works with `useSchemaLock: true` — the advisory lock is held only during the merge's DDL phase, then released before inserts begin.
  - Works with `schemaHistory: true` — a history record is written for any DDL applied during the merge.
  - New config options: `streamingStagingPrefix` (default `"autosql_stream__"`), `streamMaxRetries` (default `3`), `rejectedRowsTable`, `rejectedRowsSchema`, `keepOrphanedStagingTables` (default `false`).

#### Schema history
- **`schemaHistory: true`** — opt-in audit log of every DDL operation applied to a table.
  - A `autosql_schema_history` table (configurable via `schemaHistoryTable` / `schemaHistorySchema`) is created automatically the first time a DDL event occurs.
  - Each migration writes a `pending` record, then updates to `applied`, `failed`, or `rolled_back` as the operation completes. Includes the full before/after schema snapshot and a sha256 checksum.
  - Version numbers are assigned atomically using `INSERT … SELECT MAX(version)+1` with a UNIQUE constraint, preventing duplicate version numbers under concurrent writers.
  - MySQL schema: `BIGINT AUTO_INCREMENT`, `DATETIME`, `JSON` columns. PostgreSQL: `BIGSERIAL`, `TIMESTAMPTZ`, `JSONB`.
- **Drift detection** — on every `autoSQL` call, autosql computes a sha256 checksum of the current live schema and compares it to the last `applied` history record.
  - Enabled by default when `schemaHistory: true`. Disable with `detectDrift: false`.
  - If drift is detected: warns by default. Set `strictDriftDetection: true` to throw `SchemaDriftError` instead.
  - Checksums are computed over key-sorted JSON (`stableStringify`) so column insertion order never affects the result.
- **`detectSchemaDrift(db, table)`** — exported standalone function for detecting schema drift outside of `autoSQL`.
- **`getSchemaAt(db, table, at)`** — exported function for point-in-time schema reconstruction. Returns the `MetadataHeader` from the last `applied` history record before the given timestamp.
- **`computeChecksum(schema)`** — exported function that returns the 64-char hex sha256 used internally for drift detection.
- New config options: `schemaHistory` (default `false`), `schemaHistoryTable` (default `"autosql_schema_history"`), `schemaHistorySchema`, `detectDrift` (default `true`), `strictDriftDetection` (default `false`).

### 🔧 Internal
- `src/helpers/schemaHistory.ts` added — schema history bootstrap, migration record helpers, drift detection, and `getSchemaAt`.
- `src/helpers/streamHelpers.ts` added — staging table creation, staging insert, merge-from-staging (with type casts), orphan search, and rejected-rows query builders.
- `SchemaDriftError` added to `src/errors.ts` and exported from the package root.
- `AutoSQLStreamHandle` exported as a type from the package root.
- `getSchemaAt`, `detectSchemaDrift`, and `computeChecksum` added to the stable public API (`src/index.ts`).

---

## [1.0.5] - 2026-03-25
### ✨ What's New

#### Type inference
- **`forceStringColumns`** — column names in this list are always stored as `varchar`, bypassing numeric inference. Use for phone numbers, zip codes, padded codes, account numbers, and any other string-encoded identifier that happens to look numeric.

#### Table naming
- **`stagingPrefix` / `historyTableSuffix`** — staging and history table names are now fully configurable. Defaults remain `temp_staging__` and `__history`. Per-call overrides flow through `InsertInput` to all query builders.

#### DDL safety
- **DDL rollback on failure** — if `configureTables` fails after partially applying an `ALTER TABLE`, autosql now attempts a compensating pass to restore the previous schema.
  - PostgreSQL: DDL is transactional — `runTransaction` already issued a `ROLLBACK`. No compensating queries are needed; a warning is still emitted for any dropped columns since data is unrecoverable.
  - MySQL: best-effort compensating `ALTER TABLE` is emitted — added columns are dropped (`IF EXISTS`), modified columns are restored to their `previousType`, renamed columns are renamed back. Dropped columns emit a warning (data cannot be recovered regardless).

#### Workers
- **Worker fallback** — if the compiled `worker.js` is not present (e.g. running via `ts-node` without building first), workers are disabled automatically with a warning instead of throwing an unhandled error. Execution continues on the main thread.

#### Large-dataset support
- **`autoSQLChunked`** — new method for datasets too large to hold in memory. Accepts an `AsyncIterable<Record<string,any>[]>` of chunks.
  - The **first chunk** runs the full pipeline: schema inference (`getMetaData`), comparison (`compareMetaData`), and table configuration (`configureTables` / `CREATE` or `ALTER TABLE`).
  - **Subsequent chunks** reuse the schema established by the first chunk and skip directly to insert — no repeated inference, no repeated DDL.
  - Compatible with both the direct insert path and the staging insert path (`useStagingInsert: true`).
  - Works with `useSchemaLock: true` — the advisory lock is held only during the first-chunk DDL phase, then released before any inserts begin.

#### Multi-writer safety
- **Advisory locks (`useSchemaLock` / `schemaLockTimeout`)** — opt-in per-table advisory locks prevent race conditions when two processes call `autoSQL` on the same table simultaneously.
  - MySQL: `GET_LOCK('autosql_schema__<table>', timeout)` on a dedicated pool connection held for the duration of schema inference + DDL.
  - PostgreSQL: `pg_try_advisory_lock(djb2_hash(<table>))` polled every 500 ms on a dedicated pool client.
  - If the lock cannot be acquired within `schemaLockTimeout` seconds (default `30`), throws `SchemaLockTimeoutError` (exported from the package root). The lock is released before inserts begin — concurrent inserts are never blocked.
  - Disabled by default (`useSchemaLock: false`) — no overhead for single-writer deployments.

### 🐛 Bug Fixes
- Fixed varchar→text promotion gap: long values that appear only in the non-sampled portion of the dataset (`remainingData`) now correctly trigger text-type promotion (`varchar` → `text` → `mediumtext` → `longtext`).
- Fixed rename detection: replaced O(n²) column fingerprint comparison with O(n) approach. Ambiguous renames (multiple columns with identical type + length + nullability) are now correctly left as drop + add rather than incorrectly matched.
- Fixed `validateConfig` missing defaults: `categorical`, `maxVarcharLength`, `insertStack`, `insertType`, `safeMode`, `deleteColumns`, `stagingPrefix`, `historyTableSuffix`, `useSchemaLock`, and `schemaLockTimeout` are now fully populated so `getConfig()` always returns a complete config.
- Fixed `insertHistory` ignoring the configurable `historyTableSuffix` — history table names now respect the configured suffix end-to-end.
- Fixed `extractNestedInputs` not propagating `stagingPrefix`/`historyTableSuffix` to nested `InsertInput` objects.
- Fixed SQL injection in PostgreSQL `getViewDependenciesQuery` — replaced string interpolation with parameterized `$1`/`$2`.
- Fixed `autoInsertData` non-null assertion (`!`) on `metaData`; replaced with a runtime guard that throws a clear descriptive error.
- Consolidated duplicate `isMetadataHeader` type guard — removed the weaker version from `types.ts`, kept the stronger one in `utilities.ts`.
- Fixed `validateConfig` not rejecting `schemaLockTimeout <= 0`.

### 🔧 Internal
- Worker error handler added to `WorkerPool` — crashed workers now resolve their pending task with an error rather than hanging the queue indefinitely.
- Worker message handler wrapped in try/catch — errors thrown inside a worker method are now caught and returned as `{ success: false }` rather than producing unhandled rejections.
- `src/errors.ts` added — `SchemaLockTimeoutError` lives here; additional typed error classes will be added as new features are introduced.
- `src/internals.ts` added for internal utility exports (not part of the stable public API).
- Public API surface narrowed in `src/index.ts`; `AlterTableChanges` and `SchemaLockTimeoutError` added to stable exports.

---

## [1.0.4] - 2025-08-11
### 🐛 Bug Fixes
- Updated length calculation when converting from a decimal type to a non-decimal type.  
  - In `1.0.3`, we added `+1` to account for the decimal point.  
  - In `1.0.4`, we now add `trueMaxDecimal` as well, as we found that in cases where the decimal was rounded due to exceeding `databaseConfig.decimalMaxLength`, the resulting length on conversion to `varchar` could still mismatch. This ensures the length matches the literal string being stored after rounding.

## [1.0.3] - 2025-08-09
### 🐛 Bug Fixes
- Fixed issue where going from decimal column to varchar column would result in length that was insufficient by 1. This was due to not counting the decimal point (since length was just set to length + decimal)
- Added a step before inserting data into staging table to alter the staging tables to reflect any other alterations that were made to the primary table.

## [1.0.2] - 2025-07-31
### 🐛 Bug Fixes
- Fixed issue where `package.json` dependencies were accidentally set to `^latest` instead of specific versions. This broke clean installs and has now been corrected.
- Corrected sampling behavior: when `sampling = 0`, the engine mistakenly applied the `samplingMinimum` instead of returning the full dataset. This caused inaccurate index prediction on large datasets.

## [1.0.1] - 2025-07-30
### ✨ What's New
- Added `excludeBlankColumns` feature to ignore completely empty columns across all rows.
- Updated test suite and README to document the new configuration flag.
- Upgraded all dependencies to their latest stable versions.

## [1.0.0] - 2025-04-02
### 🚨 Breaking Changes
- This is a complete rewrite of the library.
- All previous APIs have been replaced with a new architecture and new function names.

### ✨ What's New
- Entirely new class-based design
- Better error handling, logging, and modularity
- Improved performance and flexibility

### 💥 Upgrade Instructions
If you're not ready to upgrade, you can lock your version to `^0.7.6` in `package.json`.