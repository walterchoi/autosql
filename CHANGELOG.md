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