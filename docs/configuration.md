# Configuration

Full reference for `DatabaseConfig`, the object passed to `Database.create(config)`. Every field
below is optional except `sqlDialect`. See the [README](../README.md) for a quick start.

```ts
import { Database } from 'autosql';

const db = Database.create({
  sqlDialect: 'mysql',
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'mysql',
  port: 3306,
});
```

## Connection

- `sqlDialect: 'mysql' | 'pgsql' | 'sqlserver'` — **required.**
- `host?`, `user?`, `password?`, `database?`, `port?: number`
- `connectionLimit?: number` — max pooled connections, governing insert/introspection concurrency. Defaults to `5`.
- `schema?: string` — target schema/database namespace. Everything autosql creates (target table, staging tables, history tables, audit tables) is qualified under this.
- `table?: string` — default target table, if you don't pass one per call.

## Metadata control

- `metaData?: { [tableName: string]: MetadataHeader }` / `existingMetaData?: { [tableName: string]: MetadataHeader }` — pre-supplied schema for one or more tables. See [Metadata Format](#metadata-format) below.
- `updatePrimaryKey?: boolean` — allow autosql to change a table's primary key. Defaults to `false`.
- `primaryKey?: string[]` — explicit primary key columns, overriding auto-detection.

## Table creation & charset

- `engine?: string`, `charset?: string`, `collate?: string`, `encoding?: string`

  These also pin the **connection** encoding: MySQL connects with `charset` (default the dialect's `utf8mb4`) and Postgres with `client_encoding` from `encoding` (default `UTF8`), so 4-byte characters (emoji, some CJK) transfer intact instead of failing with a MySQL `Incorrect string value` error against an otherwise-`utf8mb4` table.

- `upgradeCharset?: boolean` — MySQL only, off by default. Converts a **pre-existing** table's text columns to `charset`, so externally-created 3-byte `utf8`/`utf8mb3` columns accept 4-byte characters. Connection pinning and new-table defaults don't fix an existing column; this does. Best-effort (a failed `CONVERT` is logged, not fatal). No-op on Postgres.

## Type inference

- `pseudoUnique?: number` — % of values that must be unique to be considered pseudo-unique. Defaults to `0.9` (90%).
- `categorical?: number` — % of values that must repeat to be considered categorical. Defaults to `0.2` (20%).
- `autoIndexing?: boolean` — automatically add indexes on create/alter. Defaults to `true`.
- `inferUnique?: boolean` — infer `UNIQUE` constraints from 100%-distinct columns at `CREATE TABLE`. Defaults to `true`. Turning it off doesn't affect primary-key, index, or pseudo-unique prediction.
- `inferAdditionalUniques?: boolean` — also infer uniques beside an explicit `primaryKey`, and on an existing table. Defaults to `true`. Set `false` for composite keys such as `(tenant, id)`, where a stray `UNIQUE(id)` would let MySQL's upsert overwrite another tenant's row (uniques already on a table are kept either way). `inferUnique: false` wins over this.
- `decimalMaxLength?: number` — cap on inferred decimal scale. No hard default: a decimal keeps the full scale the data needs, up to the dialect's limit (MySQL 30, SQL Server 38, Postgres 16383), so precision is never silently lost. Values beyond a configured cap are rounded **with a warning** (or see `decimalToVarchar`).
- `decimalToVarchar?: boolean` — when a decimal exceeds the scale cap, store the column as `varchar` (exact text) instead of rounding. Defaults to `false`.
- `maxKeyLength?: number` — excludes columns longer than this from indexes/primary keys. Defaults to `255`.
- `maxCompositeKeyColumns?: number` — caps the auto-detected composite primary key search, which is otherwise O(2^N) over candidate columns — a hang risk on a wide table with no natural key. Defaults to `4`; raise only for a genuine 5+ column natural key.
- `maxVarcharLength?: number` — `varchar` columns past this length auto-convert to `text`. Defaults to `1024`.

### Overriding inferred types

- `forceStringColumns?: string[]` — always store these columns as `varchar`, regardless of content. Use for string-encoded identifiers that would otherwise infer as numeric (phone numbers, zip codes, padded codes like `"007"`, account numbers):

  ```ts
  forceStringColumns: ['phone', 'zip_code', 'account_number', 'product_code']
  ```

  Without this, `"14155550100"` would infer as `bigint`. A value with a **leading zero** (`"01234"`, `"007"`) is already kept as text automatically — a leading zero marks an identifier, not a number — so this is only needed for numeric-looking IDs with no leading zero.

- `booleanColumns?: string[]` — type these columns as `boolean`. By default a bare `0`/`1` infers as an **integer** (boolean is only inferred from real `true`/`false`), so keys/counts/coded categories aren't mis-typed. Use this for flags genuinely stored as `0`/`1`. An out-of-domain value (`2`, `"yes"`) in a hinted column **throws** rather than being silently coerced.

### Number formats

- `thousandsSeparator?: string`, `decimalSeparator?: string` — explicit separators, set together.
- `numberFormat?: 'US' | 'EU' | 'IN'` — regional preset. `'US'`/`'IN'`: `,` thousands / `.` decimal (Indian lakh/crore grouping accepted). `'EU'`: `.` / `,`.
- `numberFormatMinEvidence?: number` — how many structural values a layout needs before auto-detection trusts it. Defaults to `1`.

autosql reads locale-formatted numbers (`"1,234.56"`, `"1.234,56"`, Indian `"12,34,567"`, Swiss `"1'234.50"`). The format is resolved with this precedence:

1. **Explicit separators** — set both together to force it, e.g. `thousandsSeparator: '.'`, `decimalSeparator: ','` parses `"1.000"` as `1000`.
2. **`numberFormat` preset** — the shortcut above.
3. **Automatic detection (zero-config)** — with none of the above set, autosql infers **one** format for the whole dataset from structural evidence pooled across all columns: a value that can only be one layout (e.g. `"1,234,567"`, comma must be thousands) resolves ambiguous siblings. When a lone-separator value like `"1,234"` is genuinely ambiguous and nothing resolves it, autosql assumes decimal and emits a one-per-column warning via `logger.warn`.

```ts
// European-formatted input — explicit, or via the preset
thousandsSeparator: '.', decimalSeparator: ','
// numberFormat: 'EU'
```

Applies to `autoSQL`, `autoSQLChunked`, and `openStream`.

### Time zones

- `sourceTimeZone?: string` — IANA zone (e.g. `"America/New_York"`) that **zoneless** datetime inputs are interpreted as. A value with no offset (e.g. `"2024-01-15 12:00:00"`) is treated as local time in that zone and stored as the corresponding UTC instant (DST-aware). Inputs that already carry a zone (`"…Z"` / `"+05:00"`) are unaffected, and `date`/`time` columns are never shifted. Omit it (default) to store zoneless values exactly as given. Never inferred from the host process; an invalid zone name is rejected up front. This normalises the stored *instant* — it does not by itself make a `timestamptz` column round-trip a source offset.

  ```ts
  sourceTimeZone: 'America/New_York' // treat incoming naive timestamps as US Eastern; store UTC
  ```

## Keys

- `surrogateKey?: boolean` — add an auto-increment surrogate key (`BIGINT AUTO_INCREMENT` / `BIGSERIAL`) when the dataset has no natural one, so the table can be created and Postgres upserts have a conflict target. Defaults to `false`.
  - **A natural key always wins** — the surrogate is only a fallback.
  - **Sticky / idempotent** — anchored to the existing table: re-ingestion never thrashes the key, and an existing table without a surrogate never gains one.
  - **Database-generated** — omitted from generated `INSERT` column lists.
  - **Append semantics** — unique per physical insert, so every ingest appends; `insertType: "UPDATE"` never matches an existing row. Provide a natural `primaryKey` if you need upserts.
  - Not compatible with `addHistory`, `addNested`, or `autoSplit` (config validation throws).
- `surrogateKeyColumn?: string` — column name for the surrogate. Defaults to `"autosql_id"`.

## Insert behavior

- `insertType?: 'UPDATE' | 'INSERT'` — `'UPDATE'` upserts (replaces non-primary-key values on a duplicate key); `'INSERT'` appends and errors on a duplicate. Defaults to `'UPDATE'`.
- `insertStack?: number` — max rows per insert query. Defaults to `100`.
- `safeMode?: boolean` — prevent any table alteration at runtime. Defaults to `false`.
- `deleteColumns?: boolean` — allow dropping existing columns when altering. Defaults to `false`.
- `dropUniqueConstraints?: boolean` — allow autosql to drop a `UNIQUE` constraint that incoming data would violate. Default `false` keeps the constraint and fails the load loud, rather than permanently removing a uniqueness guarantee on the strength of one batch. Mirrors `deleteColumns`.
- `bulkLoad?: boolean` — populate staging with the dialect's native bulk-copy (Postgres `COPY FROM STDIN` via the optional `pg-copy-streams`, MySQL `LOAD DATA LOCAL INFILE`, SQL Server's `mssql` bulk API) instead of parameterised `INSERT`. Much faster for large loads; merge/upsert semantics are unchanged. Falls back to `INSERT` with a warning on failure. Defaults to `false`.
- `autoSplit?: boolean` — automatically split a dataset across multiple tables when row size or column count exceeds dialect limits. Defaults to `false`.
- `addNested?: boolean` / `nestedTables?: string[]` — extract nested JSON objects/arrays into separate tables with composite primary keys. Defaults to `false`.

  `nestedTables` entries follow `"<tableName>_<columnName>"`: when the dataset includes `<tableName>` with a `<columnName>` holding a JSON object or array of objects, autosql extracts it into a new table named `<tableName>_<columnName>`, carrying the parent row's primary key (e.g. `row1_id`) to preserve the relationship. An array flattens — each item becomes a row in the nested table.

- `excludeBlankColumns?: boolean` — exclude columns from generated insert queries when all their values are null/undefined across the batch. Defaults to `true`.
- `sanitizeInvalidChars?: boolean` — strip characters a SQL text column can't store before insert: NUL bytes (`U+0000`) removed, unpaired UTF-16 surrogates replaced with `U+FFFD`. These otherwise hard-fail Postgres (`invalid byte sequence for encoding UTF8`) and can corrupt MySQL; well-formed text (including emoji, 日本語, café, Привет) is untouched. A separate concern from connection charset: emoji/CJK failing with a MySQL `Incorrect string value` error is fixed by `charset`/`encoding`, not by this. Off by default because it mutates data.
- `rejectedRowsTable?: string` / `rejectedRowsSchema?: string` — opt-in graceful degradation: a row the database rejects no longer sinks the whole batch. See [Streaming & large datasets](streaming.md#rejected-rows) for the full behavior.

### Schema fast paths

5th argument to `autoSQL` / `autoSQLChunked` (`options`), for repeated loads that want to skip inference and/or introspection:

- `assumeSchema: MetadataHeader` — skip per-value type inference for the covered columns (uncovered columns are still inferred).
- `existingSchema: MetadataHeader` — skip the introspection round-trip. `autoSQL` returns its resolved `metaData` on the result — cache that and pass it back as `existingSchema` next time.

```ts
const first = await db.autoSQL('events', batch1);
await db.autoSQL('events', batch2, undefined, undefined, { existingSchema: first.metaData });
```

## Timestamp columns

- `addTimestamps?: boolean` — adds and manages `dwh_created_at`, `dwh_modified_at`, `dwh_loaded_at`. Also checks for existing equivalents (e.g. `modified_at`, `modify_at`, `modified_date`, `update_date`) and skips adding a duplicate. Defaults to `true`.

## Staging & history

- `useStagingInsert?: boolean` — load via a staging temp table: empty clone, populate, merge, drop. Atomic all-or-nothing merge; required for `bulkLoad` and `addHistory`. `false` inserts straight to the target (upsert still works) but skips the round-trip. Defaults to `true`.
- `addHistory?: boolean` / `historyTables?: string[]` — before overwriting a row (`'UPDATE'` mode), write its previous version to a history table. Requires `useStagingInsert`. Works on all three dialects. Defaults to `false`.

## DDL safety

AutoSQL automatically attempts to compensate for a failed `ALTER TABLE`:

- **PostgreSQL** — DDL is fully transactional; a failed `ALTER TABLE` rolls back automatically. No action needed.
- **MySQL** — DDL is non-transactional. On failure, autosql runs a best-effort compensating pass: newly added columns are dropped (`DROP COLUMN IF EXISTS`), modified columns are restored to their previous type, renamed columns are renamed back. Dropped columns **cannot be recovered** — a warning is logged and no compensation is attempted.

Warnings about irrecoverable changes (dropped columns, nullable changes) are always emitted via `logger`.

## Scaling & workers

- `useWorkers?: boolean` — parallel worker threads for inserting batches. Defaults to `true`. Requires a compiled `worker.js`; running via `ts-node`/from source, autosql detects the missing file and falls back to direct execution with a warning.
- `maxWorkers?: number` — max concurrent workers. Defaults to `8`.
- `workerTaskTimeout?: number` — seconds before a wedged worker task fails instead of hanging the load (`0` disables). A **dead** worker is always caught by the pool regardless; this only guards an alive-but-wedged one. Defaults to `0`.

## Table naming

Change these only if your schema already has real tables using the default prefix/suffix — autosql identifies its own throwaway staging tables by `stagingPrefix` (they skip primary-key reconciliation and are dropped after each run), so don't name a real target table with it.

- `stagingPrefix?: string` — prefix for auto-created staging tables. Defaults to `"temp_staging__"`.
- `historyTableSuffix?: string` — suffix for auto-created history tables. Defaults to `"__history"`.

## Logging

- `logger?: { log?, warn?, error?, stats? }` — omit to suppress all output, pass `console` to restore default behaviour, or supply a structured logger. `stats` receives per-run metrics (`QueryStats`: `rows`, `affectedRows`, `durationMs`, `rowsPerSecond`, per-phase timings) — the same object returned on `QueryResult.stats`.

## Multi-writer safety, schema history & run audit

Config fields are listed here; full behavior is in [Schema history & multi-writer safety](schema-history.md).

- `useSchemaLock?: boolean` / `schemaLockTimeout?: number` — per-table advisory lock during schema inference + DDL. Defaults `false` / `30` seconds.
- `schemaHistory?: boolean`, `schemaHistoryTable?: string`, `schemaHistorySchema?: string`, `detectDrift?: boolean`, `strictDriftDetection?: boolean` — audit trail of DDL + drift detection.
- `runAudit?: boolean`, `runAuditTable?: string`, `runAuditSchema?: string` — persist one row per load with counts/duration/success to a managed table.

## Streaming

Config fields are listed here; full behavior is in [Streaming & large datasets](streaming.md).

- `streamingStagingPrefix?: string` — prefix for per-run stream staging tables. Defaults to `"autosql_stream__"`.
- `streamMaxRetries?: number` — max per-row widening retry rounds after a bulk merge failure. Defaults to `3`.
- `keepOrphanedStagingTables?: boolean` — skip orphaned stream staging table cleanup on `openStream`. Defaults to `false`.

## TLS & SSH

Config fields are listed here; full behavior (including the least-privilege/BYOD guarantees) is in [Security & safe operation](security.md).

- `ssl?: boolean | { ca?, cert?, key?, rejectUnauthorized?, servername? }` — TLS for the driver connection.
- `sshConfig?: SSHKeys`, `sshStream?: ClientChannel | null`, `sshClient?: SSHClient` — SSH tunnel.

## Metadata Format

AutoSQL can infer metadata from your data, or you can specify it manually as a `MetadataHeader`:

```ts
const metaData: MetadataHeader = {
  created_at: {
    type: 'datetime',
    length: 0,
    allowNull: true,
    default: 'CURRENT_TIMESTAMP',
    index: true,
  },
  name: {
    type: 'varchar',
    length: 50,
    allowNull: false,
    unique: true,
    primary: true,
  },
};
```
