import type { Client as SSHClient, ClientChannel } from "ssh2";

export interface ColumnDefinition {
  type: string | null;
  length?: number;
  allowNull?: boolean;
  unique?: boolean;
  /**
   * The real database name of a NON-PRIMARY unique index this column participates in, captured
   * during introspection (`getTableMetaDataQuery`). Composite unique members share one name;
   * `undefined` for inferred (not-yet-created) uniques and for columns in no unique index. Lets
   * `resolveConflicts` derive the drop-target constraint structure from already-known metadata
   * instead of re-querying the catalog — but ONLY when every unique carries a real name (see the
   * derive-with-fallback gate in autosql.ts). Never reconstructed/guessed: MySQL auto-names a
   * unique after its column, so a synthesised name would not match `DROP INDEX`.
   */
  uniqueName?: string;
  index?: boolean;
  pseudounique?: boolean;
  categorical?: boolean;
  singleValue?: boolean;
  primary?: boolean;
  autoIncrement?: boolean;
  default?: any;
  /**
   * DDL default EXPRESSION introspected from the live catalog (e.g. Postgres `'active'::character
   * varying`, `CURRENT_TIMESTAMP`, `nextval('…'::regclass)`). Kept SEPARATE from `default` because
   * `default` doubles as the literal value `getInsertValues` substitutes for a missing cell — binding
   * an introspected DDL expression as a row value would store the expression string verbatim (A3).
   * This field is informational: DDL builders may read it, but the insert path never binds it.
   */
  ddlDefault?: any;
  decimal?: number;
  calculated?: boolean;
  updatedCalculated?: boolean;
  calculatedDefault?: any;
  previousType?: string;
  tableName?: string[];
}

export type MetadataHeader = Record<string, ColumnDefinition>;

/**
 * Per-call options for `autoSQL`.
 *
 * - `assumeSchema` (A-4): the caller already knows the schema (e.g. a SproutSpec `columns` block
 *   mapped to a `MetadataHeader`). AutoSQL skips per-value type inference for declared columns
 *   (which also side-steps inference footguns such as small integers being mis-typed as boolean);
 *   any data column not declared is still inferred as a fallback.
 *
 * - `existingSchema` (N1 / v1b): the caller already knows the CURRENT table's schema and passes it
 *   so AutoSQL skips live introspection (`getTableMetaData`) of the target table. It must be
 *   AutoSQL's own **last resolved schema** — i.e. a previous run's `QueryResult.metaData`, which
 *   already includes managed columns (`dwh_*` timestamps, an auto-increment surrogate) — NOT a bare
 *   spec: a baseline missing the managed columns would make the timestamp step re-`ADD` them. Only
 *   pass it in steady state (the table exists and hasn't drifted); on a load error or a detected
 *   drift, drop it so AutoSQL re-introspects.
 */
export interface AutoSQLOptions {
  assumeSchema?: MetadataHeader;
  existingSchema?: MetadataHeader;
}


export type supportedDialects = "mysql" | "pgsql" | "sqlserver";

export interface AlterTableChanges {
  addColumns: MetadataHeader;
  modifyColumns: MetadataHeader;
  dropColumns: string[];
  renameColumns: { oldName: string; newName: string }[];
  nullableColumns: string[];
  noLongerUnique: string[];
  primaryKeyChanges: string[];
}
  
export interface DatabaseConfig {
      sqlDialect: supportedDialects;
      host?: string;
      user?: string;
      password?: string;
      database?: string;
      port?: number;
      schema?: string;
      table?: string;
      /**
       * Max connections in the driver pool (MySQL `connectionLimit` / Postgres `max`). Defaults to 5.
       * Raise it for parallel/worker loads so pool acquisition doesn't serialise; keep it under the
       * server's own connection limit and size it against `maxWorkers`.
       */
      connectionLimit?: number;

      metaData?: {
        [tableName: string]: MetadataHeader;
      };
      existingMetaData?: {
        [tableName: string]: MetadataHeader;
      };
      updatePrimaryKey?: boolean;
      primaryKey?: string[];

      /**
       * When a dataset has no natural primary key, add an auto-increment surrogate key
       * (`BIGINT AUTO_INCREMENT` / `BIGSERIAL`) so the table can still be created and Postgres
       * upserts have a conflict target. A natural key always takes precedence; the surrogate is
       * only a fallback. It is sticky to the existing table (idempotent on re-ingestion).
       * Note: because the surrogate is unique per physical insert, every ingest is an append —
       * upsert (`insertType: "UPDATE"`) never matches an existing row. Off by default.
       * Not compatible with `addHistory`, `addNested`, or `autoSplit`.
       */
      surrogateKey?: boolean;
      /** Column name for the surrogate key (see `surrogateKey`). Defaults to `"autosql_id"`. */
      surrogateKeyColumn?: string;
      engine?: string;
      charset?: string;
      collate?: string;
      encoding?: string;

      /**
       * Opt-in (default `false`, MySQL only): when configuring a **pre-existing** table, convert its
       * text columns to the target charset (`charset`, default `utf8mb4`) so externally-created 3-byte
       * `utf8`/`utf8mb3` columns accept 4-byte characters (emoji, some CJK). Connection-charset pinning
       * and defaulting new tables to utf8mb4 do NOT fix an already-existing utf8 column — this does, via
       * a one-time `ALTER TABLE ... CONVERT TO CHARACTER SET`. Detect-and-convert is convergent (once
       * every text column matches, it is a no-op) and best-effort — a `CONVERT` that fails (e.g. an
       * over-long index: 4 bytes/char can exceed the key-length limit) is logged and skipped, not fatal.
       * No-op on Postgres (its `UTF8` already stores 4-byte characters).
       */
      upgradeCharset?: boolean;

      pseudoUnique?: number;
      categorical?: number;
      autoIndexing?: boolean;
      /**
       * Maximum fractional-digit scale for inferred `decimal` columns. When unset (the default), a
       * decimal is stored at the full scale the data needs, up to the dialect's numeric limit (MySQL
       * 30, SQL Server 38, Postgres 16383) — so a standard user never silently loses precision. Set
       * a lower value to deliberately cap scale (e.g. `2` for currency). When a value's true scale
       * exceeds the cap it is ROUNDED and a warning is logged — unless `decimalToVarchar` is on.
       */
      decimalMaxLength?: number;
      /**
       * When a decimal value's true scale exceeds the cap (`decimalMaxLength` / the dialect limit),
       * store the whole column as text (`varchar`) to preserve the exact value instead of rounding it.
       * Off by default (the column stays numeric and is rounded, with a warning). Turn on when exact
       * high-precision values matter more than being able to do SQL arithmetic on the column.
       */
      decimalToVarchar?: boolean;
      maxKeyLength?: number;
      /**
       * Cap the auto-detected composite primary key at this many columns (default 4). The key search
       * tries combinations of pseudo-unique columns and scans the data for uniqueness; without a cap
       * it is O(2^N) in the number of candidate columns — a real hang risk on a wide table with no
       * natural key. 4 covers effectively every real composite key; raise it only to auto-detect a
       * genuine 5+ column natural key.
       */
      maxCompositeKeyColumns?: number;
      maxVarcharLength?: number,

      sampling?: number;
      samplingMinimum?: number;

      insertType?: "UPDATE" | "INSERT";
      insertStack?: number;
      /**
       * Load rows with the dialect's bulk-copy mechanism (Postgres `COPY` / MySQL
       * `LOAD DATA LOCAL INFILE`) instead of parameterised multi-row `INSERT` — much faster and
       * cheaper for large loads. Applies to the staging-table population (so upsert semantics are
       * preserved by the unchanged merge step) and requires `useStagingInsert`. On a bulk-load error
       * the batch falls back to parameterised `INSERT` so a single bad row can still surface a clear
       * error. Off by default. Postgres `COPY` needs the optional `pg-copy-streams` dependency.
       */
      bulkLoad?: boolean;

      safeMode?: boolean;
      deleteColumns?: boolean;

      /**
       * Allow autosql to DROP a UNIQUE constraint when incoming data would violate it — either staged
       * data colliding with an existing unique (resolveConflicts) or a batch containing duplicates in
       * a previously-unique column. Default false (safe): the constraint is KEPT and the load fails
       * loud (or diverts to `rejectedRowsTable`) on the colliding rows, rather than silently and
       * permanently removing a uniqueness guarantee — including a user-defined one — based on one
       * batch's data. Set true to auto-drop (a warning naming the constraint is logged either way).
       * Mirrors `deleteColumns` / `updatePrimaryKey`.
       */
      dropUniqueConstraints?: boolean;

      autoSplit?: boolean;

      addTimestamps?: boolean;
      /**
       * Load via a staging temp table (default `true`): CTAS an empty clone → populate it → merge
       * into the real table (upsert via `ON CONFLICT`/`ON DUPLICATE`/`MERGE`) → drop it. This gives an
       * atomic all-or-nothing merge, is the target for `bulkLoad`, and runs conflict resolution.
       *
       * Set to `false` for a **direct load** — rows go straight to the target with
       * `INSERT … ON CONFLICT/ON DUPLICATE` (upsert still works), skipping the temp-table create /
       * populate / merge / drop round-trips and the extra write. Faster and cheaper for append-only or
       * small/frequent loads where the staging atomicity isn't needed. Required (`true`) for
       * `addHistory` (history diffs the staging table against the target).
       */
      useStagingInsert?: boolean;
      addHistory?: boolean;
      historyTables?: string[];
      addNested?: boolean;
      nestedTables?: string[];
      excludeBlankColumns?: boolean;

      /**
       * Strip characters that the target database cannot store from string values before
       * insert: NUL bytes (``) and unpaired UTF-16 surrogates (replaced with U+FFFD).
       * These otherwise hard-fail Postgres (`invalid byte sequence for encoding UTF8`,
       * `unsupported Unicode escape sequence`) and can corrupt MySQL. Off by default because
       * it mutates data; enable it when ingesting free-text that may contain pasted/garbage
       * bytes. Connection charset pinning does NOT address these — this does.
       */
      sanitizeInvalidChars?: boolean;

      useWorkers?: boolean;
      maxWorkers?: number;

      /**
       * Per-worker-task timeout in SECONDS. `0` (the default) disables it.
       *
       * A worker that dies mid-task (terminate/OOM/native crash) is always caught by
       * the pool's exit/error handlers and surfaced as a failed task — this timeout is
       * NOT required for that. It only guards an alive-but-wedged worker (e.g. a DB call
       * that never returns). Off by default so a legitimately long-running batch is not
       * spuriously failed; set it when you need a hard per-task upper bound.
       */
      workerTaskTimeout?: number;

      /**
       * Column names that should always be stored as varchar regardless of their
       * content. Use this for string-encoded identifiers (phone numbers, zip codes,
       * padded codes) that would otherwise be inferred as numeric types.
       */
      forceStringColumns?: string[];

      /**
       * Column names that should always be stored as a boolean flag. By default AutoSQL
       * only infers boolean from the literals `true`/`false` — a bare `0`/`1` is treated
       * as an integer. Use this hint for columns that encode a real flag as `0`/`1` (or
       * `true`/`false`) so they are created as a boolean column. Values outside the
       * boolean domain (`0`, `1`, `true`, `false`, case-insensitive, plus null/blank) are
       * rejected with an error rather than silently coerced — forcing a column to boolean
       * is lossy, so an unexpected value is surfaced, not hidden.
       */
      booleanColumns?: string[];

      /**
       * Explicit number-format separators for locale-aware ingestion. Set BOTH to
       * disambiguate single-separator values (e.g. with `thousandsSeparator: "."` and
       * `decimalSeparator: ","`, "1.000" is parsed as 1000, not 1). Omit both to use the
       * auto-detection heuristic (a lone separator is treated as decimal).
       */
      thousandsSeparator?: string;
      decimalSeparator?: string;

      /**
       * IANA time zone (e.g. "America/New_York", "Australia/Sydney", "UTC") that ZONELESS datetime
       * inputs should be interpreted as. When set, a value with no offset (e.g. "2024-01-15 12:00:00")
       * is treated as local time in this zone and converted to a UTC instant before storage. Inputs
       * that ALREADY carry a zone ("…Z" / "+05:00") are unaffected — they are already absolute — and
       * `date`/`time` columns are never shifted. Omit (the default) to store zoneless values exactly
       * as given (wall-clock preserved, no zone assumed). autosql NEVER infers this from the host
       * process timezone. Note: this normalises the stored INSTANT; it does not by itself make a
       * `datetimetz`/`timestamptz` column round-trip a source offset (a separate concern).
       */
      sourceTimeZone?: string;

      /**
       * Acquire a per-table advisory lock before running schema inference and
       * ALTER TABLE.  Set to `true` when the same table may be written by multiple
       * concurrent processes to prevent race conditions in compareMetaData.
       * Defaults to `false`.
       */
      useSchemaLock?: boolean;
      /**
       * How long (in seconds) to wait for the advisory lock before throwing
       * `SchemaLockTimeoutError`.  Defaults to 30.
       */
      schemaLockTimeout?: number;

      // --- Schema history ---
      /** Record every DDL event to an audit table. Default: false. */
      schemaHistory?: boolean;
      /** Name of the history table. Default: "autosql_schema_history". */
      schemaHistoryTable?: string;
      /** Schema containing the history table (same DB, different schema). Default: same as config.schema. */
      schemaHistorySchema?: string;
      /** Throw SchemaDriftError when drift is detected. Default: false (warn only). */
      strictDriftDetection?: boolean;
      /** Run drift detection on every autoSQL call when schemaHistory: true. Default: true. */
      detectDrift?: boolean;

      // --- Streaming ---
      /** Prefix for per-run stream staging tables. Default: "autosql_stream__". */
      streamingStagingPrefix?: string;
      /**
       * Max per-row widening retry rounds after a bulk insert/merge fails. Default: 3. Applies to the
       * streaming flush AND the non-streaming direct-insert path (`useStagingInsert: false`).
       */
      streamMaxRetries?: number;
      /**
       * Opt-in rejected-rows table name. When set, rows that still fail after per-row retries are
       * written here instead of throwing — enabling graceful degradation on the streaming flush and
       * the non-streaming direct-insert path (`useStagingInsert: false`). Has no effect on the default
       * staging path, which stays atomic (all-or-nothing) by design.
       */
      rejectedRowsTable?: string;
      /** Schema for the rejected rows table. Default: same as config.schema. */
      rejectedRowsSchema?: string;
      /** When true, orphaned stream staging tables are preserved instead of dropped. Default: false. */
      keepOrphanedStagingTables?: boolean;

      /**
       * Prefix for auto-created staging tables (default: "temp_staging__").
       * Change this if your schema already has tables with that prefix.
       */
      stagingPrefix?: string;
      /**
       * Suffix for auto-created history tables (default: "__history").
       * Change this if your schema already has tables with that suffix.
       */
      historyTableSuffix?: string;

      /**
       * Optional logger. When omitted, the library writes nothing to stdout/stderr.
       * Pass `console` to restore the old behaviour, or supply your own structured logger.
       */
      logger?: {
          log?: (msg: string) => void;
          warn?: (msg: string) => void;
          error?: (msg: string) => void;
          /**
           * Structured per-run metrics sink (see `QueryStats`). Called once per `autoSQL` load with
           * the phase timings and throughput, so a pipeline can forward them to its observability /
           * stats store without parsing log strings. Optional; omit to not collect metrics.
           */
          stats?: (stats: QueryStats) => void;
      };

      /**
       * TLS for the driver connection. Omit **or `false`** (default) → no change: plaintext / driver
       * default, so existing configs are byte-for-byte unaffected. `true` → enable TLS with default
       * verification. An object configures TLS:
       *   - `ca`: PEM CA bundle to verify the server certificate against (e.g. the AWS RDS bundle, or
       *     a BYOD host's CA).
       *   - `rejectUnauthorized`: verify the certificate chain. Defaults to the driver's default (true)
       *     when a `ca` is supplied; set `false` ONLY for dev/self-signed (verification is then off).
       *   - `cert` / `key`: client certificate + key for mutual TLS (optional).
       *   - `servername`: SNI override when the host differs from the certificate CN/SAN.
       * Works on **all three dialects**: MySQL/Postgres receive it as the driver `ssl` object; SQL
       * Server maps it onto the mssql driver's `encrypt` / `trustServerCertificate` /
       * `cryptoCredentialsDetails` options automatically. Not composed with `sshConfig` — pick one path
       * (SSH tunnel OR direct TLS).
       */
      ssl?: boolean | {
        ca?: string;
        cert?: string;
        key?: string;
        rejectUnauthorized?: boolean;
        servername?: string;
      };

      sshConfig?: SSHKeys;
      sshStream?: ClientChannel | null;
      sshClient?: SSHClient;
}

export interface SSHKeys {
  host: string;
  port: number;
  username: string;
  password?: string;
  private_key_path?: string;
  private_key?: string;
  /**
   * Expected OpenSSH host-key fingerprint of the SSH server, e.g. "SHA256:abc123…" (obtain via
   * `ssh-keyscan -t ed25519 <host> | ssh-keygen -lf -`). When set, the tunnel VERIFIES the bastion's
   * host key against it and refuses a mismatch — closing the MITM window (ssh2 does NO verification by
   * default). When omitted, the tunnel still connects but logs a loud warning that its identity is
   * unverified. The "SHA256:" prefix and trailing base64 padding are optional.
   */
  hostFingerprint?: string;
  timeout?: number;
  debug?: boolean;
  source_address?: string;
  source_port?: number;
  destination_address: string;
  destination_port: number;
}

export type QueryInput = string | QueryWithParams;
export type QueryWithParams = { query: string; params?: any[] };

export interface TranslateMap {
    serverToLocal: Record<string, string>;
    localToServer: Record<string, string>;
  }
  
export interface DialectConfig {
    dialect: supportedDialects;
    requireLength: string[];
    optionalLength: string[];
    noLength: string[];
    decimals: string[];
    translate: TranslateMap;
    defaultTranslation: Record<string, string>;
    sqlize: SqlizeRule[]
    engine: string;
    charset: string;
    collate: string;
    encoding: string;
    maxIndexCount?: number;
    /** Maximum fractional-digit scale the dialect's decimal/numeric type can hold (MySQL 30,
     *  SQL Server 38, Postgres 16383). Used as the default decimal scale ceiling so a decimal is
     *  stored at full precision up to what the DB supports, rather than an arbitrary low cap. */
    maxDecimalScale: number;
}

export interface InsertResult { 
  start: Date; 
  end: Date; 
  duration: number; 
  affectedRows: number 
}

export interface InsertInput {
  table: string,
  data: Record<string, any>[],
  metaData: MetadataHeader,
  previousMetaData: AlterTableChanges | MetadataHeader | null,
  comparedMetaData?: { changes: AlterTableChanges, updatedMetaData: MetadataHeader },
  runQuery?: boolean,
  insertType?: "UPDATE" | "INSERT",
  stagingPrefix?: string,
  historyTableSuffix?: string
}

export interface QueryResult {
    start: Date; 
    end: Date; 
    duration: number;
    affectedRows?: number 
    success: boolean;
    results?: any[];
    error?: string;
    /**
     * The driver's structured error code, when the failure came from the database driver — mysql2's
     * `code` (e.g. `"ER_TABLEACCESS_DENIED_ERROR"`), Postgres's SQLSTATE (e.g. `"42501"`), or SQL
     * Server's error number. Lets a caller branch on the exact failure (e.g. tell a user which GRANT
     * is missing) without string-matching `error`. Omitted for non-driver errors.
     */
    errorCode?: string;
    table?: string;
    /**
     * The resolved schema AutoSQL used for the load — the final `MetadataHeader` including any
     * managed columns (`dwh_*` timestamps, an auto-increment surrogate). Callers can cache this and
     * pass it back to skip re-introspection on the next load (see the `existingSchema` fast path).
     */
    metaData?: MetadataHeader;
    /** Per-run performance metrics (phase timings + throughput). Populated on a successful `autoSQL`
     *  load; the same object is passed to `logger.stats`. See `QueryStats`. */
    stats?: QueryStats;
}

/**
 * Per-run performance metrics for one `autoSQL` load — for production observability and for building
 * a stats history (e.g. to size batches, spot drift in load times, or bill by throughput). Durations
 * are milliseconds of wall-clock time for that phase.
 */
export interface QueryStats {
    table: string;
    /** Number of input rows passed to `autoSQL`. */
    rows: number;
    /** Rows the database reported affected by the load (insert + update). */
    affectedRows: number;
    /** Total wall-clock time for the whole `autoSQL` call. */
    durationMs: number;
    /** `rows / (durationMs / 1000)` — input-row throughput. */
    rowsPerSecond: number;
    phases: {
        /** Input prep + schema inference/resolution (`predictType`/introspection/compare). */
        prepare?: number;
        /** DDL: CREATE / ALTER to make the table match the resolved schema. */
        configure?: number;
        /** Data load: staging populate + merge into the target, or a direct insert. */
        load?: number;
    };
    /** Whether the staging-table path was used (vs a direct insert). */
    staged: boolean;
    /** Whether the bulk-copy path (`COPY` / `LOAD DATA`) was used to populate. */
    bulkLoad: boolean;
}

export interface metaDataInterim {
  [key: string]: {
    uniqueSet: Set<any>;
    uniqueSaturated: boolean;
    valueCount: number;
    nullCount: number;
    types: Set<string>;
    collated_type?: string;
    length: number;
    byteLength: number;
    decimal: number;
    trueMaxDecimal: number;
    // Running max of integer-part digits for a numeric column. Precision must be
    // maxIntegerDigits + maxScale; tracking the integer part separately avoids under-counting
    // when the widest-integer value and the widest-scale value are different rows.
    intLen?: number;
  }
}

export interface SqlizeRule {
  regex: string;
  replace: string;
  type: true | string[]; // `true` = apply to all types
}