## [Unreleased]

### ✨ New — SQL Server feature parity (spec-2 slices 1–4)
- **IDENTITY introspection.** SQL Server table introspection now reports `IDENTITY` columns as
  auto-increment (via `COLUMNPROPERTY(... 'IsIdentity')`), matching MySQL `AUTO_INCREMENT` / Postgres
  `nextval`. Fixes two latent issues on re-load: a re-introspected identity column no longer shows
  spurious drift, and `surrogateKey` stays sticky to its existing identity surrogate (detected by the
  auto-increment flag) instead of thrashing the primary key on run 2.
- **`schemaHistory` on SQL Server.** The schema-history bootstrap + record/detect queries now emit T-SQL
  (`BIGINT IDENTITY`, `NVARCHAR(MAX)`, `DATETIME2`, `TOP`, `OUTPUT INSERTED.id`) instead of Postgres-only
  DDL; `schemaHistory: true` is now supported on SQL Server (drift detection included). The former
  fail-loud guard is removed.
- **`rejectedRowsTable` (dead-letter) on SQL Server.** The rejected-rows table + insert builders now emit
  T-SQL; opt-in per-row degradation diverts bad rows to the dead-letter table on SQL Server (direct and
  staging paths). The former fail-loud guard is removed.
- **Row-level history (`addHistory`) and split tables (`autoSplit`) verified on SQL Server** — already
  implemented, now covered by live tests.

### 🐛 Bug Fixes
- **SQL Server `surrogateKey` re-ingest now appends (spec-4 §3.7).** A surrogate-key load re-ingested on
  SQL Server was updating in place (row count stayed flat) instead of appending like MySQL/Postgres. The
  insert-from-staging `MERGE` matched on the surrogate primary key, which is excluded from the insert and
  regenerated in the staging clone, so it self-matched. It now merges only on primary keys that are
  actually written; a surrogate (DB-generated) key falls through to a plain `INSERT`, so the real table
  assigns fresh keys and rows append — matching the other dialects.

### 🔒 Guardrails
- **`rejectedRowsTable` + `addHistory` together still fail loud on SQL Server.** That combination uses the
  zero-window atomic (before-image + merge in one transaction) path on MySQL/Postgres; the SQL Server
  atomic path is not yet ported, so the combo is rejected at construction rather than silently running a
  weaker, non-atomic version. Use the two features separately on SQL Server for now.

## [2.4.0] - 2026-08-16

### ✨ New
- **`db.preview(table, data, …)` — dry run.** Computes exactly what an `autoSQL(table, data, …)` call
  **would** do — the inferred schema, the create/alter/noop decision, the exact `CREATE`/`ALTER` DDL,
  and any changes that would be **blocked** without an opt-in (dropping a column, changing the primary
  key, dropping a unique constraint) — **without writing anything**. It reads the current schema to
  compute the diff; nothing is created, altered, or inserted (verified by live integrity tests: a new
  table still doesn't exist and an existing table is byte-identical after a preview). Mirrors the
  `autoSQL` signature and runs the same pipeline (surrogate keys, timestamps, number-format detection,
  splits), so the plan matches a real load. Returns an `AutoSQLPreview` (a per-table `tables` array,
  the effective `numberFormat`, and `rowCount`); `AutoSQLPreview` / `TablePreview` are exported.
  Distinct from `safeMode` (which runs a load but skips DDL) — `preview` runs no load and executes no
  DDL.

## [2.3.1] - 2026-08-16

### 📝 Documentation
- **README brought current with 2.1–2.3.** Corrected the number-separator docs (they described the
  pre-2.3.0 "single separator = decimal" behaviour, which automatic detection now supersedes); added a
  **Number formats** section covering `numberFormat` / `numberFormatMinEvidence`, zero-config
  consensus, and the A24c ambiguity warning; documented `logger.stats` + `QueryResult.stats`
  (`QueryStats` run metrics), SSH **`hostFingerprint`** host-key verification, automatic leading-zero
  identifier preservation, and the `existingSchema` option / DDL-preview (`runQuery: false`).

### 🐛 Bug Fixes
- **`autoSQLChunked` forwards early termination to the source `chunks` iterator.** The first-chunk peek
  (added in 2.3.0) took over manual iteration, so when a load errored mid-stream the for-await's
  automatic `.return()` reached the internal wrapper, not the source — a DB-cursor/stream-backed
  `chunks` would not run its cleanup. It now forwards `.return()` to the source in a `finally`.

## [2.3.0] - 2026-08-16

> **Locale-aware number ingestion.** Zero-config detection of the dataset's number format, an explicit
> `numberFormat` preset, and a warning for the genuinely-ambiguous `"1,234"` shape — plus a fix so the
> per-row degradation fallback (and therefore `openStream`) normalizes values like the bulk path. All
> backward-compatible. Note: number-format **consensus is automatic** — a dataset that pairs an
> ambiguous `"1,234"` with a decisive `"1,234,567"` now reads the ambiguous value as `1234` (previously
> `1.234`); pass `numberFormat`/separators to override, or rely on the once-per-run detection log.

### ✨ New
- **`DatabaseConfig.numberFormat` — regional number-format preset (`"US"` / `"EU"` / `"IN"`).** Sugar
  over `thousandsSeparator`/`decimalSeparator`: it resolves to those fields in `validateConfig`, so a
  known source locale disambiguates lone-separator values through the **same** path that type inference
  and value storage already use (no new plumbing). `"US"`/`"IN"` = thousands `,`, decimal `.` (Indian
  lakh/crore grouping is accepted automatically — it shares US separators); `"EU"` = thousands `.`,
  decimal `,`. Explicit `thousandsSeparator`/`decimalSeparator` still take precedence. Omit it to use
  the auto-detection heuristic.
- **Automatic dataset-level number-format detection (zero-config).** When neither explicit separators
  nor `numberFormat` are given, autosql now infers the format from **structural evidence in the data
  itself** — a value that can only be one layout (e.g. `"1,234,567"` → comma is thousands, or `"12,5"`
  → comma is decimal) votes, and the ambiguous `"1,234"` shape abstains. Evidence is pooled across
  **all columns** (one format per dataset, since a single source doesn't mix US and EU), so a decisive
  column resolves an ambiguous sibling: `amt: "1,234"` next to `total: "1,234,567"` stores `1234`, no
  config. It's **self-contained** (reads only the batch — no extra DB queries), resolved once per load
  and **locked** (across chunks only column lengths grow, never the format), logs the detected format
  via `logger.log`, and falls back to assume-decimal + the A24c warning when evidence is absent or
  genuinely contradictory. `numberFormatMinEvidence` (default 1) raises the vote floor. Applies to
  `autoSQL`, `autoSQLChunked` **and `openStream`** — separators now flow through every load path,
  including the per-row degradation fallback (see the Bug Fixes entry below).

### 🛡️ Robustness
- **Ambiguous single-separator numbers now warn once per column (A24c).** A lone `,` or `.` followed by
  exactly three digits (e.g. `"1,234"`) is genuinely ambiguous between thousands-grouping (`1234`) and a
  decimal (`1.234`) — and only three trailing digits are ambiguous, since a trailing thousands group is
  always three digits in both Western and Indian formats. autosql still assumes decimal, but now logs a
  one-per-run warning (via `logger.warn`) naming the affected **numeric** column(s), so you can set
  `numberFormat` / separators when the guess is wrong. Suppressed when separators are supplied; silent
  for text columns that merely contain such a value.

### 🐛 Bug Fixes
- **The per-row degradation fallback now normalizes values like the bulk path (`sqlize`).**
  `perRowInsertWithRetry` built its INSERT from raw row objects (no `sqlize`), while the bulk direct
  path normalizes them — so when a load *degraded to per-row*, values were stored differently (or
  rejected): a locale number like `"1,234"`/`"1.234"`, a decimal needing half-up rounding, a
  timezone-normalized datetime, a canonicalized boolean. It now pre-sqlizes each row exactly as the
  bulk path does, so degradation stores identical values. This also makes **`openStream` honour
  `numberFormat` and consensus** — a stream's bulk merge is a DB `CAST` that rejects grouped numbers
  and always degrades to per-row, which previously bound them raw.

## [2.2.0] - 2026-08-16

> **Audit-remediation release.** Two independent code audits of 2.1.0 (Opus + Fable) produced 25
> prioritised findings; this release fixes all of them (or defers a few with documented rationale),
> plus a requested `sourceTimeZone` override for going global. The headline is **silent
> data-corruption fixes** (timezone shifting, decimal truncation, history over-capture, introspected
> defaults bound as values) and **fail-loud hardening** (no more silently-dropped rejected rows,
> false-positive drift, or worker crashes). Also: SSH host-key verification, an opt-in unique-drop
> gate, cross-dialect upsert/DDL consistency, and internal test/refactor guardrails (a non-UTC test
> zone, a DB preflight health check, a cross-dialect conformance matrix, and unified insert builders).
> Backwards-compatible with 2.1.0 except for the documented behaviour changes below — each is a
> correctness fix or a new default-safe gate; the pre-existing corrupt/silent behaviour is the only
> thing that changes.

### ✨ New
- **`DatabaseConfig.sourceTimeZone` — opt-in timezone override for zoneless datetimes.** Set an IANA
  zone (e.g. `"America/New_York"`, `"Australia/Sydney"`, `"UTC"`) and a datetime **without** an offset
  (e.g. `"2024-01-15 12:00:00"`) is interpreted as local time in that zone and stored as the
  corresponding **UTC instant** (DST-correct, both hemispheres). Inputs that already carry a zone
  (`…Z` / `+05:00`) are unaffected (already absolute), and `date`/`time` columns are never shifted.
  Omit it (the default) to store zoneless values exactly as given. No new dependency (uses `Intl`);
  an invalid zone is rejected up front by `validateConfig`. Note: this normalises the stored *instant*
  for plain `datetime`/`timestamp`; full `timestamptz` offset round-tripping remains a separate item.

### ⚠️ Behavior change
- **Timezone-naive datetimes are no longer shifted by the host's UTC offset (A1).** `sqlize` parsed a
  zoneless datetime string through `new Date()`, which reads it in the **Node process's local zone**,
  then re-emitted UTC — so the same input was stored differently depending on where the process ran
  (invisible on a UTC host/CI). Zoneless values are now normalised **textually** (wall-clock preserved,
  no `Date` involved); only zone-qualified inputs are converted to UTC (they carry an absolute
  instant); `date`/`time` columns keep the wall-clock regardless of any zone. Result is host-timezone
  independent. Offset-bearing (`+02:00`) and fractional-second inputs, previously mangled by the
  cleaning step, now normalise correctly.
- **Decimal values now round half-up instead of truncating downward (A4).** With a `decimalMaxLength`
  cap, a value with more fractional digits than the cap was **always truncated** (`2.675` → `2.67`) —
  a systematic downward bias (notably on currency) — because the rounding step operated on
  already-truncated digits. Rounding is now exact half-up (away from zero, matching MySQL/Postgres),
  computed with string/digit arithmetic so it is correct at any magnitude. (This supersedes the
  `precision > 15` truncation fallback from 2.0.0's D-G, which existed only to avoid float error.)
- **Unique constraints are no longer auto-dropped without opt-in (A10).** When incoming data collided
  with a `UNIQUE` constraint (staged data hitting an existing unique, or a batch with duplicates in a
  previously-unique column), autosql **silently and permanently dropped the constraint** — including a
  user-defined one — to force the load through. This is now gated behind **`dropUniqueConstraints`
  (default `false`)**, mirroring `deleteColumns`/`updatePrimaryKey`: by default the constraint is kept
  and a warning is logged naming it (the load then fails loud on Postgres, or upserts on the secondary
  unique on MySQL); set `dropUniqueConstraints: true` to restore the auto-drop (still warned).

### 🐛 Bug Fixes
- **Row-level history no longer records unchanged rows on incremental loads (A2).** The before-image
  capture `LEFT JOIN`ed the full target table against the staged batch, so on an incremental load
  every row **not** in the batch was written to the history table on every run (history growth
  proportional to table size, not batch size). It now `INNER JOIN`s, capturing a before-image only for
  the rows the merge actually changes. Fixed on MySQL, Postgres, and SQL Server.
- **Introspected column defaults are no longer stored as literal values (A3).** When loading into a
  pre-existing table (Postgres/SQL Server) whose column has a DDL `DEFAULT`, a row with a NULL/omitted
  value for that column could store the introspected **default expression string** (e.g.
  `'active'::character varying`, `CURRENT_TIMESTAMP`) as the value. Introspected defaults are now kept
  separate from the value-substitution path; a missing value becomes `NULL` (or the DB's own default),
  never the expression. (MySQL was unaffected — it does not introspect the default.)
- **Failed graceful-degradation diverts now fail loud instead of silently dropping rows (A5).** When a
  row failed the load and was diverted to `rejectedRowsTable`, the divert writes were unchecked — so a
  broken/incompatible rejects table (missing privilege, wrong shape) swallowed the rows while the load
  still reported `success: true`. Both divert paths now check the result and **throw** if the divert
  itself fails, so no row is ever lost silently. `rejectedRowsTable` is also now rejected up front on
  SQL Server (its builder emits Postgres-only DDL — previously a guaranteed silent loss; parity deferred).
- **Schema-drift detection no longer false-positives on every run (A6).** With `schemaHistory`, the
  recorded baseline checksum was taken over the *inferred* schema while the drift check compared the
  *introspected* schema — different shapes for the same table — so under `strictDriftDetection` it threw
  and **blocked every load after the first** (and warned falsely otherwise). The baseline is now recorded
  from a post-migration re-introspection, so both sides are introspection-derived and match for an
  unchanged table; genuine out-of-band changes are still detected.
- **The schema-history subsystem no longer fails silently (A19).** A failed history-table bootstrap now
  throws (opt-in feature that can't work fails loud); an unrecordable migration-start now warns;
  `detectSchemaDrift` distinguishes "no baseline / first run" from "couldn't read the live table" (the
  latter now warns instead of silently reporting no drift); a dead table-existence check is fixed; and
  `schemaHistory` is rejected up front on SQL Server (Postgres-only bootstrap; parity deferred).
- **Ambiguous single-row insert failures are no longer retried into duplicates (A15).** On the per-row
  degradation path, `runQuery`'s internal retry could re-execute a non-idempotent `INSERT` whose failure
  was ambiguous (connection dropped after the server applied it), duplicating the row. That path (which
  already owns an outer retry loop) now runs each insert once. `runQuery` gained an optional per-call
  attempts override; the default retry behavior is unchanged.
- **Postgres primary-key changes are now atomic (A9).** The adapter injected literal `COMMIT;`/`BEGIN;`
  around a PK change, splitting the migration into three transactions — a failure partway through could
  leave the table with no primary key while the call reported failure. The drop/alter/add now run in one
  transaction (Postgres DDL is transactional).
- **Postgres type-conversion `ALTER … USING` casts now name real Postgres types (A12).** Conversions like
  `→ double` / `→ tinyint` / `→ datetime` emitted `::double` / `::tinyint` / `::datetime` (local inference
  tokens that don't exist in Postgres), so the ALTER failed at execution while MySQL/SQL Server succeeded.
  The cast target is now translated to the server type.
- **Cross-dialect upsert consistency (A11).** With no updatable columns, a duplicate key now skips on all
  three dialects (MySQL previously errored on a bare INSERT; it now emits a no-op self-update). The SQL
  Server `MERGE` now takes `WITH (HOLDLOCK)` to prevent a concurrent-upsert double-insert race.
- **`schemaHistory` table references are now escaped consistently (A13).** The history module built its
  table references by splitting on `.` instead of routing through `escapeIdentifier`, which corrupted any
  schema/table name containing a dot and bypassed the identifier guards; it now escapes schema and table
  the same way the rest of the engine does.
- **Sparse columns are inferred as nullable, not spuriously NOT-NULL/unique (A17).** A column that first
  appeared partway through the data had its earlier absences uncounted, so it looked NOT-NULL and unique
  — a spurious primary-key candidate the same batch then failed to insert. Rows before a column's first
  value now count as nulls, so it infers nullable.
- **Streaming (`openStream`) no longer silently drops columns or mangles objects (A18).** A key that first
  appeared in a later row/chunk was silently dropped (its column didn't exist in the staging table); the
  first chunk's columns are now the union of its rows, and a genuinely new later column fails loud instead
  of vanishing. Object/array values are JSON-serialised (were becoming `"[object Object]"`), matching the
  `autoSQL` batch path.

### 🛡️ Robustness
- **`runTransaction` never rejects, even on connection-pool failure (A16).** The pool acquire was
  awaited outside the retry guard, so pool exhaustion / an acquire timeout could escape as an unhandled
  rejection and crash a caller relying on the "always resolves to a result" contract. It now returns
  `{ success: false }` like every other failure.
- **The worker path no longer crashes with a configured `logger`, and cleans up its connections (A8/A14).**
  With `useWorkers` (the default), passing a `logger` (or using an SSH tunnel) threw `DataCloneError`
  when the config was cloned to the worker, crashing the load; the non-cloneable fields are now stripped
  before spawning. `maxWorkers` is now honoured, the pool is capped at the number of tasks, and a
  single-table load skips workers entirely (no thread/connection-pool overhead). Workers are now shut
  down gracefully — each closes its database connections before the thread is terminated — instead of
  being killed abruptly and leaking server-side connections on every worker-backed load.
- **Assorted low-severity hardening.** `openStream` fails fast on SQL Server (A20) instead of emitting
  Postgres-shaped SQL that failed mid-stream; SQL Server unrecoverable connect errors (login failed /
  cannot open database) are classified and no longer retried (A21); the single-statement guard
  (`isValidSingleQuery`) is now a proper tokenizer (A23) that no longer mis-parses comments straddling
  string literals, doubled quotes, or dollar-quoting; a connection whose `ROLLBACK` failed is discarded
  rather than returned to the pool (A24); composite-key uniqueness checks are unambiguous (A24); and
  `estimateRowSize` counts varchar bytes per dialect (A25) so table auto-splitting doesn't under-trigger
  on multibyte columns.

### 🔒 Security
- **SSH tunnels now verify the host key (A7).** The `ssh2` tunnel performed **no** host-key
  verification — it accepted whatever key the server presented, so the tunnel that protects the DB
  credentials in transit was silently MITM-able. New `SSHKeys.hostFingerprint` (OpenSSH `SHA256:…`
  form, from `ssh-keyscan <host> | ssh-keygen -lf -`) pins the bastion's key and refuses a mismatch;
  when it's omitted the tunnel still connects but logs a loud warning that its identity is unverified
  (parity with the TLS `rejectUnauthorized: false` warning). Also: `setSSH` no longer mutates the
  caller's `sshKeys` object, and SSH debug output routes through the configured logger, not `console`.

### 🔧 Maintenance
- **Tests run in a non-UTC timezone by default** (`TZ=Australia/Sydney`, overridable) so
  timezone-sensitive bugs like A1 can't hide behind a UTC-only CI host; the date tests assert a
  non-UTC offset so a misconfigured harness fails loud rather than passing vacuously.
- **DB preflight health check for the live suite.** If a test database is unreachable, the full run
  now fails fast with one clear, actionable message (`npm run db:up`) instead of dozens of tests
  failing with a cryptic, empty-message `ECONNREFUSED`. Docker Compose gained per-service healthchecks
  and `db:up` waits for healthy; the unit run excludes all `*-live` tests by convention so it stays
  database-free.

## [2.1.0] - 2026-08-15

> Adds encrypted-connection support (`ssl`, all three dialects) and BYOD/least-privilege hardening:
> structured `errorCode`, up-front identifier-length validation, and documented non-destructive
> operating guarantees. Backwards-compatible with 2.0.0 (the identifier check only affects names that
> previously failed mid-load or were silently truncated by Postgres).

### ✨ New
- **TLS/`ssl` passthrough for the connection (`DatabaseConfig.ssl`).** Previously only an SSH tunnel (`sshConfig`) could encrypt the connection; there was no way to connect over direct TLS (e.g. a managed Postgres / RDS that requires SSL, or a customer MySQL host). `ssl` accepts `true` (enable TLS with default verification) or an object (`ca`, `cert`, `key`, `rejectUnauthorized`, `servername`). **Works on all three dialects:** MySQL (`mysql2`) and Postgres (`pg`) receive it as the driver `ssl` object (mysql2 rejects `ssl: true`, so it's normalised to `{}`); **SQL Server maps it automatically** onto the `mssql` driver's `encrypt` / `trustServerCertificate` / `cryptoCredentialsDetails` (custom CA, mutual TLS, SNI) options — so the same `{ ca, rejectUnauthorized }` config works everywhere. `rejectUnauthorized: false` logs a warning that verification is off. Omitted **or `false`** → byte-for-byte the previous plaintext behaviour. Verified live against all three databases (a real encrypted session + rejection of an untrusted self-signed cert).
- **SQL Server now honors `DatabaseConfig.ssl`** — it is mapped automatically onto the `mssql` driver's TLS options (`encrypt` / `trustServerCertificate` / `cryptoCredentialsDetails`), so the same `{ ca, rejectUnauthorized }` config works on SQL Server as on MySQL/Postgres (see the `ssl` entry above).
- **`QueryResult.errorCode`** now carries the driver's structured error code on a failed query — mysql2's `code` (e.g. `ER_TABLEACCESS_DENIED_ERROR`), Postgres's SQLSTATE (e.g. `42501`), or SQL Server's error number — so a caller can branch on the exact failure (e.g. surface which `GRANT` is missing) without string-matching the message. It is populated on direct `runQuery`/`runTransaction` results **and** threaded through to a failing `autoSQL()`/`autoSQLChunked()` top-level result (worker and direct execution paths alike). The human-readable `error` message is unchanged.

### ⚠️ Behavior change
- **Over-long SQL identifiers now fail loudly instead of being emitted or silently truncated.** A table/column name (source-derived, or an autosql-derived staging/history name built from it) that exceeds the dialect's identifier limit now throws a clear error at generation time. Previously MySQL/SQL Server let the database reject it mid-load, and **Postgres silently truncated** it, which could collide two distinct names on the same table. The limit is measured the way each dialect counts it — Postgres by UTF-8 **bytes** (63), MySQL by Unicode **code points** (64), SQL Server by UTF-16 code units (128) — so a legal international/astral-character name is not false-rejected. Generated constraint/index names are byte-safe, so they never produce a name the check then rejects. Well-formed identifiers within the limit are unaffected.

### 🔧 Maintenance
- **Documented the least-privilege / BYOD operating guarantees** (verified, no behaviour change): autosql only ever issues DDL/DML inside `config.schema`; it never emits `DROP DATABASE`/`DROP SCHEMA` and never drops the target table (with `deleteColumns:false` + `updatePrimaryKey:false`, the only drops are its own `temp_staging__*` tables); `autoSQL()` does not implicitly create the schema (it assumes it exists, so a least-privilege user without `CREATE SCHEMA` works); the default load path is parameterised `INSERT` (no `LOAD DATA LOCAL INFILE`/`FILE` privilege unless `bulkLoad` is opt-in); and `closeConnection()` fully drains the pool.

## [2.0.0] - 2026-08-01

> **Major bump:** this release contains breaking changes — `startTransaction`/`commit`/`rollback` now
> require a pinned-connection argument (previously silent no-ops), decimals preserve full precision by
> default instead of rounding to 6, and a bare `0`/`1` now infers as an integer instead of a boolean.
> See the ⚠️ Behavior change entries below. Also adds the SQL Server / Azure SQL adapter, native
> bulk-copy (`bulkLoad`), opt-in graceful degradation (`rejectedRowsTable`) across all load paths with
> transactional row-level history, and a ~8× faster inference path.

### ⚠️ Behavior change
- **High-precision decimals are preserved by default instead of silently rounded.** `decimalMaxLength` previously defaulted to **6**, so any value with more than 6 fractional digits was silently rounded on insert (e.g. `3.14159265358979323846` → `3.141592` on both MySQL and Postgres). It now has **no hard default**: a decimal is stored at the full scale the data needs, up to the dialect's numeric limit (`maxDecimalScale` — MySQL 30, SQL Server 38, Postgres 16383). Set `decimalMaxLength` to deliberately cap scale (e.g. `2` for currency). When a value's scale exceeds the cap it is rounded **with a warning** (no longer silent), or — with the new opt-in **`decimalToVarchar: true`** — the column is stored as text (`varchar`) to preserve the exact value. `forceStringColumns` remains the per-column exact-text option. (Design: decisions.md D-G.)

### 🐛 Bug Fixes (robustness)
- **The direct-insert path now honours `config.insertType`.** On the non-streaming direct path (`useStagingInsert: false`), the bulk batch insert defaulted straight to `UPDATE` (upsert) and ignored a configured `insertType: "INSERT"` — so a duplicate primary key was silently upserted instead of erroring, even though the per-row degradation fallback *did* honour `insertType` (an inconsistency). `autoInsertData` now falls back to `config.insertType` (then `UPDATE`) when the input carries none; the staging-population callers pass an explicit `insertType`, so they are unaffected, and the default (no `insertType` set) is unchanged.
- **Decimal precision is no longer under-counted (mixed-scale overflow fix).** Precision was computed as `max(row's integer digits + running scale)` per value, so when the widest-integer value and the widest-scale value were *different* rows the total precision was too small and the load overflowed on insert — e.g. `[10.5, 20.0, 5.25]` inferred `decimal(3,2)`, which cannot store `10.5` (`numeric field overflow` on Postgres, out-of-range on MySQL). Precision is now `maxIntegerDigits + maxScale` taken as independent running maxes, so `[10.5, 20.0, 5.25]` → `decimal(4,2)`. Affected all dialects.

### ⚡ Performance
- **Inference is ~8× faster (first-load / one-off throughput).** `predictType` ran `JSON.parse` inside a try/catch on **every** value — and a non-JSON string (the common case: names, codes, dates, free text) triggered a thrown exception each time, which is very slow in V8. The `JSON.parse` is now gated on the value actually looking like JSON (starts with `[`, `{`, or `"`), which is behaviour-identical (the parse result only ever affected arrays/objects/quoted-strings; numbers/booleans/dates are matched by the regex chain first). `predictType` went from ~107k to ~2.35M values/s, and end-to-end `getMetaData` from ~5k to ~40k rows/s in the benchmark. (Recurring loads already skip inference via `assumeSchema`/`existingSchema`; this is the win for first loads and one-offs.)
- **Type-aware inference fast path.** `predictType` now short-circuits native JS values before the string path: a `boolean` returns `boolean` directly, and a finite `number` is typed without the per-value `JSON.parse` + regex chain + `normalizeNumber` (exponential form is checked first so a huge integer still types as `exponent`, not `int`). Results are identical to the string path — verified across boundary widths, decimals, exponentials and booleans. (JSON sources deliver most values as native numbers/booleans; strings still take the full path.)
- **Opt-in benchmark suite — `npm run bench`.** A performance-regression harness kept out of the normal test run (matches `*.bench.ts`, not `*.test.ts`). The inference benchmark asserts a **scaling invariant** — 10× the rows must stay well under ~20× the time — which catches an accidental `O(n²)` regression independent of machine/DB speed; the live benchmark reports per-dialect rows/s with a generous catastrophic-slowdown ceiling. Run it before merging a change to the inference or load hot paths.
- **Bounded composite-primary-key search (P4).** When no single unique key exists, `predictIndexes` searches column combinations for a unique composite key — an unbounded `O(2^N)` subset search (each combination scanned over the full dataset), a real hang risk on a wide, key-less table. The search is now capped at `maxCompositeKeyColumns` (config, default `4`); real composite keys are well within that, and the cap turns the worst case into a bounded search. Raise it only to auto-detect a genuine 5+ column natural key.
- **`resolveConflicts` skips a catalog round-trip on the common staging load.** The unique-index + primary-key introspection (`getUniqueIndexesQuery`/`getPrimaryKeysQuery`) that `resolveConflicts` ran on every staged load with a unique constraint is now **derived from the metadata already introspected during table configuration** — `getTableMetaDataQuery` additionally returns each column's non-primary unique-index name(s) (`ColumnDefinition.uniqueName`), sourced from the same catalog view as the drop path so the constraint is dropped by its real database name. Deriving is gated to the case where it is provably identical to the live catalog — a stable-schema load where every unique carries a real introspected name; any structural change this run (a dropped unique, a primary-key change, a newly-added unique), an inferred/just-created unique with no real name yet, or a column in more than one unique index falls back to live introspection. Net: one fewer round-trip on the overwhelmingly common idempotent re-ingest, with no change in which constraints are (or aren't) dropped. All three dialects (MySQL/Postgres/SQL Server); verified live end-to-end (introspection skipped, correct constraint dropped by real name). (Design: decisions.md D-J / D-M.)

### ✨ New
- **Opt-in per-row graceful degradation on the default (staging) load path, with zero-window row-level history.** Previously only the direct (`useStagingInsert:false`) and streaming paths degraded gracefully; the default atomic staging merge (`INSERT…SELECT … ON CONFLICT`) was all-or-nothing. Now, when `rejectedRowsTable` is set, a staging merge that fails rolls back (as before) and then retries the failed table **row-by-row** — landing the good rows and diverting unrecoverable ones to `rejectedRowsTable` — instead of failing the whole load. **Without `rejectedRowsTable` the path stays atomic/fail-loud (unchanged default).** When `addHistory` is also on, the retry captures each row's before-image and merges it **in the same transaction**, so row-level history and the data change commit — or roll back — together: a diverted row leaves neither a data change nor a before-image, with no window in between. Every non-degraded history load is unchanged (server-clock `NOW()`/`CURRENT_TIMESTAMP`, byte-for-byte). MySQL + Postgres; the `addHistory` + `rejectedRowsTable` combination errors up-front on SQL Server (row-level history there is unverified — D-F). "Zero-window" means history↔data atomicity specifically; it does not change the pre-existing resolveConflicts↔merge ordering. (Design: decisions.md D-K/D-L/D-N/D-O.)
- **`upgradeCharset` (opt-in, default `false`, MySQL only) — migrate a pre-existing table's columns to utf8mb4 (R8).** Pinning the connection charset and defaulting new tables to `utf8mb4` does not fix a **pre-existing 3-byte `utf8`/`utf8mb3` column** on a table AutoSQL didn't create — it still rejects 4-byte characters (emoji, some CJK) with `Incorrect string value`. With `upgradeCharset: true`, configuring an existing table first detects text columns whose charset differs from the target (`charset`, default `utf8mb4`) and, if any, runs one `ALTER TABLE ... CONVERT TO CHARACTER SET utf8mb4 COLLATE …` before insert. Convergent (once every text column matches, it's a no-op — no repeat ALTER on re-ingest) and best-effort (a `CONVERT` that fails, e.g. an over-long index where 4 bytes/char exceeds the key-length limit, is logged and skipped, not fatal). Real tables only (not staging temp tables). No-op on Postgres (`UTF8` already stores 4-byte characters).
- **Blocked schema changes are now surfaced (R9).** `deleteColumns` and `updatePrimaryKey` default to `false` (the safe default), so a column drop or primary-key change can be *computed* from the incoming data and then silently not applied. AutoSQL now logs a warning (via the configured `logger`) naming the affected columns and the flag to set — so "the schema changed and AutoSQL didn't apply it" is observable instead of silent. Fires once, on the real table only (not staging temp tables), and only on a genuine computed change (no spurious warning on an unchanged re-ingest).

### 🔧 Maintenance
- **Confirmed: credentials are never logged (R7).** Audited both dialect connection paths — the library logs schema/identifier/error-message detail but never the config object or the `password`. A driver's own error string may include the host/user (its convention), never the secret. Added a regression test that drives a failed (wrong-password) connection and asserts the password never appears in any `log`/`warn`/`error` output. (Secrets-at-rest / vault-KMS is a product-layer concern, out of the engine's scope.)
- **`npm audit` is clean (0 vulnerabilities).** Production was always unaffected — the package ships no regular `dependencies`, only optional `mysql2`/`pg`/`pg-copy-streams`/`ssh2`. The remaining dev-only test-tooling advisories (handlebars, js-yaml, brace-expansion, @babel/core) are resolved via `overrides` pinned to each advisory's actual fixed version, plus a lockfile regeneration that made the pre-existing `handlebars@4.7.9` override effective (the committed lock had a stale `4.7.8`). No consumer-facing change.

### 🐛 Bug Fixes (robustness)
- **Adding a new column to a populated table no longer fails on Postgres.** A column added to an existing table is now emitted nullable — pre-existing rows have no value for it, so forcing `NOT NULL` either errored on Postgres (`column "x" contains null values`) or silently back-filled `0`/`''` on MySQL. A column that can back-fill (a calculated timestamp, or one with an explicit default) keeps `NOT NULL`.
- **A new column that arrives with no data (all null) is deferred, not mis-typed.** Such a column has no inferable type; it previously errored on MySQL (`varchar` with no length) or would have been guessed as `varchar` (locking it, so later int/date data collates back to strings). It is now deferred — created with the correct type when a later batch first carries data. (Made unconditional; `excludeBlankColumns: false` no longer forces a guessed-type placeholder. Existing blank columns are unaffected.)
- **Postgres: an integer column added or retyped via `ALTER` no longer emits a display width.** The ADD/MODIFY builders rendered `smallint(3)` / `int(11)` (invalid Postgres — `syntax error at or near "("`); they now match `CREATE` and emit a bare integer type.
- **MySQL small-integer columns no longer round-trip as boolean (data-loss fix).** A small integer (≤255) was inferred as `tinyint` and, for single-digit values, rendered as `tinyint(1)` — MySQL's boolean convention — so introspection read it back as `boolean`. On the next load that produced a `boolean→int` conversion (`SET x = CASE WHEN x THEN 1 ELSE 0 END`) that collapsed values to `0/1` and duplicated keys. Fixed on both sides: MySQL never emits a display width for `tinyint` (boolean is always `TINYINT(1)`, applied consistently across CREATE/ADD/MODIFY), and introspection reads `COLUMN_TYPE` so only `tinyint(1)` maps to boolean.
- **Postgres widening an existing column on re-ingest no longer fails.** The modify path comma-joined multiple sub-actions under one `ALTER COLUMN` (`... SET DATA TYPE x, SET DEFAULT y`), which Postgres rejects at the bare second action (`syntax error at or near "DEFAULT"`). Each action is now its own `ALTER COLUMN`, and a degenerate `SET DEFAULT NULL` (an introspection artefact) is no longer emitted.
- **Orphaned staging tables no longer corrupt the next load.** A run that crashed before cleanup left a `temp_staging__*` table that `CREATE TABLE IF NOT EXISTS` then reused with its stale schema. The staging create now drops any leftover first, so the temp table always matches the current real table.
- **Boolean values are normalized to `0`/`1` before insert.** A string flag (`"true"`/`"false"`, `"TRUE"`, `"t"`/`"f"`, `"yes"`/`"no"` — the form CSV and most text sources deliver) reached the driver unchanged: MySQL rejected it against a `TINYINT(1)` (`Incorrect integer value: 'true'`) and the raw distinct strings also inflated sampled cardinality into a spurious `UNIQUE`. `sqlize` now canonicalizes any boolean-domain value to `"1"`/`"0"` (accepted by both MySQL `tinyint` and Postgres `boolean`); an out-of-domain value is left unchanged so the DB surfaces it. Affects both plain inference of literal `true`/`false` and the new `booleanColumns` hint.
- **Postgres streaming: a small-integer column no longer fails to merge.** The stream-merge cast mapped `int`/`smallint`/`bigint` but not `tinyint`/`mediumint`, so a `tinyint` column fell through uncast and the text→smallint `INSERT ... SELECT` from staging was rejected (`column "x" is of type smallint but expression is of type text`). `tinyint`→`smallint` and `mediumint`→`integer` are now cast (matching the CREATE-path translation). This was previously masked because a bare `0`/`1` inferred as boolean; the R3 change (0/1 → integer) surfaced it.

### 🐛 Bug Fixes (encoding)
- **Connection charset is now pinned.** The MySQL pool set no `charset`, so mysql2 negotiated its default (historically 3-byte `utf8_general_ci`) and a 4-byte character (emoji, some CJK) threw `Incorrect string value: '\xF0\x9F...'` on insert **even against a utf8mb4 table** — the bytes couldn't cross the wire. The pool now pins `charset` (defaults to the dialect's `utf8mb4`, overridable via `config.charset`). The Postgres pool now pins `client_encoding: 'UTF8'` (overridable via `config.encoding`) as defense-in-depth against locale-derived client defaults.
- **Sample-derived `UNIQUE` is re-validated against the full dataset.** With `sampling` enabled, the `unique`/`pseudounique` flags were computed from the sample only, so a column unique across the sample but duplicated in the unsampled remainder could get a `UNIQUE` constraint (and be chosen as the primary key), then fail on insert of the full data. The few columns still flagged `unique` are now re-checked against the whole dataset and demoted (to `pseudounique` when still highly distinct) if duplicates exist. No effect when sampling is off (flags already reflect the full dataset).
- **Postgres `bigint` auto-increment now maps to `BIGSERIAL`.** It was mapped to plain `SERIAL` (int4), silently capping the sequence at ~2.1 billion — a real ceiling for large tables. `int` still maps to `SERIAL`.
- **Timestamps on existing tables that lack them are now created.** With `addTimestamps` + staging on, ingesting into a table without the `dwh_*` columns failed (`column "dwh_created_at" ... does not exist`): the columns were added to the insert metadata after schema comparison, so they never entered the `ALTER`. They are now folded into `addColumns`, and the `NOT NULL` `dwh_created_at` is added with `DEFAULT CURRENT_TIMESTAMP` so pre-existing rows backfill (their true creation time is unknown, so alter-time is used). Re-ingestion still populates the per-row value.
- **Staging no longer double-applies schema changes.** The staging temp table (`CREATE TABLE AS SELECT` from the already-configured real table) was being re-`ALTER`ed with the real table's `addColumns`, hitting "duplicate column" whenever a column was added in the same run. Staging configuration is now a no-op (the CTAS copy already matches).

### ⚠️ Behavior change
- **A bare `0`/`1` now types as an integer, not a boolean (R3).** AutoSQL previously inferred any value in `{0, 1}` as boolean (`TINYINT(1)` on MySQL, `boolean` on Postgres) — a data-modelling error for the common case of small integer keys, counts, and coded categories (it also broke integer comparisons: `WHERE id = 1` on Postgres → `operator does not exist: boolean = integer`). Boolean is now inferred **only** from the literals `true`/`false`. Columns that store a real flag as `0`/`1` opt in via the new **`booleanColumns`** config hint. **Upgrade note:** a column previously created as boolean purely from `0`/`1` data will convert to integer on its next ingest — this is **lossless** (`true`→1, `false`→0, `null`→null) and **convergent** (it happens once; a later `true`/`false` batch does not flip it back). The R10 destructive path stays closed: post-R10 introspection maps only `tinyint(1)`→boolean and never emits a tinyint display width, so an integer column can no longer be misread as boolean. (Edge case, document-only: a legacy MySQL `tinyint(1)` physically holding values >1 would collapse those to 1 under the boolean→int `CASE WHEN` — a pre-R10 artifact, not created by this change.)

### ✨ New
- **Per-run performance metrics — `QueryResult.stats` + `logger.stats`.** Every load — `autoSQL`, `autoSQLChunked`, and the streaming `end()` — now reports its throughput and per-phase timings: `stats = { table, rows, affectedRows, durationMs, rowsPerSecond, phases: { prepare, configure, load }, staged, bulkLoad }`. The same object is passed to an optional structured `logger.stats(stats)` sink. A pipeline can capture these to persist a load-stats history (throughput trends, phase breakdown, batch sizing) without parsing log strings. `prepare` = input prep + schema inference/resolution, `configure` = CREATE/ALTER DDL, `load` = staging populate + merge (or direct insert). For a chunked load the phases aggregate across chunks; for a stream they cover `end()` (the flush). A throwing stats sink can never break a load.
- **SQL Server / Azure SQL support (new destination, core ETL path).** A `SqlServerDatabase` adapter (driver: `mssql`) adds `sqlDialect: "sqlserver"` — the first destination beyond MySQL/Postgres. It's a Class-A row-store, so it reuses the existing inference + staging/upsert model. Supported and live-tested: create + insert, multilingual/emoji round-trip (text stored as `NVARCHAR`), idempotent re-ingest (introspection via `sys.*`/`INFORMATION_SCHEMA`), **MERGE**-based upsert, and add-column schema evolution. T-SQL specifics handled: `[bracket]` identifiers, `IDENTITY(1,1)`, `@p`-style parameters, `sp_getapplock` schema locks, and the mssql transaction API. A docker `sqlserver` service (`mcr.microsoft.com/azure-sql-edge`) is included for local testing. Not yet implemented for SQL Server (falls back or is unsupported): bulk-copy (`bulkLoad` → INSERT fallback), streaming, schema history, split tables, and retyping an *indexed* column — see `decisions.md` D-F.
- **`booleanColumns` (opt-in).** Column names to always store as a boolean flag. By default only `true`/`false` infer boolean (see the behavior change above); use this hint for columns that encode a flag as `0`/`1` (or `true`/`false`) so they are created as boolean. Mirrors `forceStringColumns`. A value outside the boolean domain (`0`, `1`, `true`, `false`, case-insensitive, plus null/blank) is **rejected with an error** rather than silently coerced — forcing a column to boolean is lossy (unlike forcing to string), so an unexpected value like `2` or `"yes"` is surfaced, not hidden.
- **`sanitizeInvalidChars` (opt-in, default `false`).** Strips characters a SQL text column cannot store from string values before insert — NUL bytes (which hard-fail Postgres with `invalid byte sequence for encoding UTF8` / `unsupported Unicode escape sequence`) and unpaired UTF-16 surrogates (replaced with U+FFFD). Well-formed text, including emoji and non-ASCII scripts, is untouched. Enable when ingesting free-text that may contain pasted/garbage bytes; connection charset pinning does not address these.
- **Configurable connection-pool size — `connectionLimit`.** The driver pool size was hardcoded to 5 (MySQL `connectionLimit` / Postgres `max`), bottlenecking parallel/worker loads. It is now configurable via `connectionLimit` (default 5). (Also fixes `getMaxConnections()` on MySQL, which read the wrong path and always reported 5.)
- **`autoSQL` returns its resolved schema.** The `QueryResult` now includes `metaData` — the final `MetadataHeader` used for the load, including managed columns (`dwh_*` timestamps, an auto-increment surrogate). Callers can cache this and pass it back to skip re-introspection on the next load.
- **Skip-introspection fast path — `autoSQL(..., { existingSchema })`.** Pass a prior run's resolved `metaData` back as `existingSchema` and AutoSQL skips live introspection of the target table (the main per-run DB round-trip for a stable pipeline). Use in steady state only; drop it on a load error or detected drift so AutoSQL re-introspects.
- **`buildColumnProfile(metaData, otherTables?)`.** Derives a physical "column profile" from a resolved schema — per column: type, key role, cardinality, an FK-candidate (by name), and a semantic *hint* (identifier / flag / time / dimension / measure / attribute). It describes the data, not its business meaning (no DB access); it's the input a semantic-layer generator can build on.
- **Provided-schema fast path — `autoSQL(table, data, schema?, primaryKey?, { assumeSchema })`.** When the caller already knows the schema (e.g. a mapped-in column spec), pass it as `assumeSchema`: if it declares every column in the data, per-value type inference is skipped entirely; if it declares only some, the rest are inferred and the declared columns win. Sparse column definitions are filled with sensible defaults. Besides the compute saving on recurring loads, declared columns are authoritative — so this side-steps inference footguns (e.g. small integers being mis-typed as boolean) for the columns it covers.
- **Bulk-load fast path — `bulkLoad` (opt-in, default `false`).** When enabled, staging inserts populate the temp table via the driver-native bulk primitive instead of parameterised multi-row `INSERT`: **Postgres `COPY … FROM STDIN`** (requires the optional `pg-copy-streams` dependency) and **MySQL `LOAD DATA LOCAL INFILE`** (needs server-side `local_infile=1`). Typically far faster on large loads (no per-row parse/plan). Values are serialized to a shared tab-delimited format (`\N` NULL, backslash-escaped tab/newline/carriage-return/backslash) that round-trips multilingual text, emoji, and embedded control characters. Upsert semantics are unchanged — only the staging populate is bulk; the merge step still does the `ON CONFLICT`/`ON DUPLICATE`. If the bulk path fails (e.g. `local_infile` disabled, or `pg-copy-streams` not installed) it **logs a warning and falls back to the normal `INSERT` path**, so the load still succeeds. Staging-only (no effect when `useStagingInsert: false`).
- **`surrogateKey` (opt-in, default `false`).** When a dataset has no natural primary key, adds an auto-increment surrogate (`BIGINT AUTO_INCREMENT` / `BIGSERIAL`, column `autosql_id`, overridable via `surrogateKeyColumn`) so the table can still be created and Postgres upserts have a conflict target. A natural key always takes precedence. The surrogate is **sticky** to the existing table, so re-ingestion is idempotent (a coincidentally-unique later batch cannot introduce a competing key, and an existing table without a surrogate never gains one). Auto-increment columns are excluded from generated `INSERT` column lists so the database assigns them. Note: a surrogate is unique per physical insert, so every ingest **appends** — upsert (`insertType: "UPDATE"`) never matches an existing row. Not compatible with `addHistory`, `addNested`, or `autoSplit` (rejected by config validation).

---

## [1.4.2] - 2026-07-05
### 🔒 Security
- **Streaming SQL injection fixed.** The streaming builders (`openStream` → `write()`/`end()`) interpolated caller JSON keys (column names) and table/schema identifiers with bare quotes and no quote-doubling — an identifier break-out injection that the rest of the library already escaped. All streaming identifiers now route through the central escape module, and cast lengths are validated.
- **Compensating-DDL injection fixed.** The MySQL best-effort DDL-rollback builder (reached when a DDL transaction fails) had the same raw-identifier flaw with inferred column names, plus unvalidated type/length. Now escaped and validated. A full sweep also hardened the remaining raw identifier sites in the dialect adapters (alter-path table/index names, schema-existence helpers). Only config-derived schema-history identifiers remain raw (not attacker-reachable).

### 🐛 Bug Fixes (re-audit)
- **Staging + `updatePrimaryKey` fixed.** `useStagingInsert` defaults to `true`, so setting `updatePrimaryKey: true` hard-failed `autoSQL` on the first fresh table — the staging temp table (`CREATE TABLE AS SELECT`, no keys) was PK-reconciled and emitted `DROP`/`ADD PRIMARY KEY` on a keyless table. Primary-key reconciliation is now skipped for staging tables (the real target is untouched); PG `DROP CONSTRAINT` is now `IF EXISTS`.
- **Per-call `schema` override now applies to every statement.** ~15 DDL/staging/index builder wrappers still read the shared instance schema after the AsyncLocalStorage refactor, so `autoSQL(table, data, otherSchema)` created the table in one schema and inserted into another. All wrappers now use the effective (context) schema.
- **Schema-lock timeout no longer double-releases** a pooled connection (both dialects). **Postgres serialization failures (`40001`) now retry** via the whole-transaction retry (they were misclassified as permanent). **`recordMigrationStart` returns `undefined`** (not `0`) on failure so callers don't run `UPDATE … WHERE id = 0`.
- **Postgres:** each `RENAME COLUMN` is emitted as its own `ALTER TABLE` (Postgres rejects renames combined with other actions).
- **Type inference:** `UNIQUE` is confirmed for dense unique columns (was mislabeled `pseudounique`); sampling re-evaluates the type on non-sampled rows (wide values no longer overflow); out-of-range `DD/MM`/`MM/DD` dates are rejected; index/key checks gate on byte length for multibyte data; `datetimetz` is preserved when collating with `date`; negative leading-zero strings (`-007`) stay text; `exponent` length handling corrected.

---

## [1.4.1] - 2026-07-05
### 🐛 Bug Fixes (schema history)
- **Version-race resilience.** `recordMigrationStart` computes `version = MAX(version)+1` against a `UNIQUE(table_name, version)` constraint; without a schema lock, concurrent migrations could collide. It now retries (recomputing the version) on a unique-constraint violation instead of failing the migration.
- **Stale `pending` sweep.** Orphaned `pending` starts (from crashed runs) older than 1h are best-effort marked `failed` so they don't linger.

### 🔒 Security / tooling
- Pin the transitive dev dependency `handlebars` to `4.7.9` via `overrides` (high-severity advisory; not shipped in the package). Raise Jest `testTimeout` to 30s for DB integration tests. Add a `test:unit` script (`jest.unit.config.js`) for fast no-DB runs.

---

## [1.4.0] - 2026-07-05
### 🐛 Bug Fixes (concurrency)
- **Per-call schema is now isolated per operation.** Passing a `schema` to `autoSQL`/`autoSQLChunked`/`openStream` previously mutated the shared `Database` config schema and restored it in a `finally`. Concurrent calls with different schemas on one instance raced on that shared field (queries against the wrong schema; the restore could leave the config permanently wrong), and `openStream` held the mutation for the whole stream lifetime. Each schema-scoped operation now runs inside an `AsyncLocalStorage` context and `getConfig()` resolves the schema from it without mutating the instance, so concurrent operations stay isolated.

### ✨ New
- `Database.runWithSchema(schema, fn)` — run an operation with an effective schema override for its async duration without mutating instance config.

---

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