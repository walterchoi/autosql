import type { Client as SSHClient, ClientChannel } from "ssh2";

export interface ColumnDefinition {
  type: string | null;
  length?: number;
  allowNull?: boolean;
  unique?: boolean;
  /**
   * Real DB name of a NON-PRIMARY unique index this column participates in, from introspection
   * (`getTableMetaDataQuery`). Composite members share one name; `undefined` for inferred uniques
   * and columns in no unique index. Lets `resolveConflicts` derive the drop-target constraint from
   * known metadata instead of re-querying — but ONLY when every unique carries a real name (see the
   * derive-with-fallback gate in autosql.ts). Never guessed: MySQL auto-names a unique after its
   * column, so a synthesised name wouldn't match `DROP INDEX`.
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
   * DDL default EXPRESSION introspected from the catalog (e.g. `CURRENT_TIMESTAMP`,
   * `nextval('…'::regclass)`). Kept SEPARATE from `default` because `default` doubles as the literal
   * `getInsertValues` substitutes for a missing cell — binding a DDL expression as a row value would
   * store the expression string verbatim (A3). Informational: DDL builders may read it, insert never binds it.
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
 * - `assumeSchema` (A-4): caller-known schema (e.g. a SproutSpec `columns` block as a
 *   `MetadataHeader`). Skips per-value inference for declared columns (also side-steps footguns like
 *   small integers mis-typed as boolean); undeclared data columns are still inferred as a fallback.
 *
 * - `existingSchema` (N1 / v1b): caller-known CURRENT table schema, so AutoSQL skips live
 *   introspection (`getTableMetaData`). Must be AutoSQL's own **last resolved schema** (a previous
 *   run's `QueryResult.metaData`, which includes managed columns like `dwh_*` timestamps + surrogate),
 *   NOT a bare spec — a baseline missing managed columns would make the timestamp step re-`ADD` them.
 *   Only pass in steady state; on load error or detected drift, drop it so AutoSQL re-introspects.
 */
export interface AutoSQLOptions {
  assumeSchema?: MetadataHeader;
  existingSchema?: MetadataHeader;
}

/** One target table's slice of an `autoSQL` preview (see `Database.preview`). */
export interface TablePreview {
  table: string;
  /** What a real load would do to this table. */
  action: 'create' | 'alter' | 'noop';
  /** Schema inferred from the data, with managed columns applied (dwh_* timestamps, surrogate key). */
  inferredSchema: MetadataHeader;
  /** The live table's current schema, or `null` if it does not exist yet. */
  currentSchema: MetadataHeader | null;
  /** The diff that would be applied (`null` on `create`). */
  changes: AlterTableChanges | null;
  /** The exact CREATE/ALTER statement(s) a real load would execute — preview runs none of them. */
  ddl: string[];
  /** Changes autosql would NOT apply without opting in (e.g. dropping a column needs `deleteColumns`). */
  blockedChanges: string[];
}

/**
 * Result of `Database.preview(...)` — what an `autoSQL(table, data, …)` load WOULD do, computed
 * without writing anything (schema is read to compute the diff; nothing is created, altered, or
 * inserted). Contrast with `safeMode`, which runs a load but skips DDL.
 */
export interface AutoSQLPreview {
  /** One entry per target table (usually one; more when `autoSplit` / nested extraction apply). */
  tables: TablePreview[];
  /** Effective thousands/decimal separators (from `numberFormat`, explicit config, or auto-detection); omitted when the default heuristic applies. */
  numberFormat?: { thousands: string; decimal: string };
  /** Number of input rows. */
  rowCount: number;
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
       * Max connections in the driver pool (MySQL `connectionLimit` / Postgres `max`). Default 5.
       * Raise for parallel/worker loads so pool acquisition doesn't serialise; keep under the server's
       * own connection limit and size against `maxWorkers`.
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
       * (`BIGINT AUTO_INCREMENT` / `BIGSERIAL`) so the table can be created and Postgres upserts have a
       * conflict target. A natural key takes precedence; surrogate is a fallback, sticky to the existing
       * table (idempotent on re-ingestion). Because it's unique per physical insert, every ingest is an
       * append — upsert (`insertType: "UPDATE"`) never matches an existing row. Off by default.
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
       * Opt-in (default `false`, MySQL only): on a **pre-existing** table, convert text columns to the
       * target charset (`charset`, default `utf8mb4`) so externally-created 3-byte `utf8`/`utf8mb3`
       * columns accept 4-byte characters (emoji, some CJK). Connection-charset pinning and defaulting
       * new tables to utf8mb4 do NOT fix an existing utf8 column — this does, via a one-time
       * `ALTER TABLE ... CONVERT TO CHARACTER SET`. Convergent (a no-op once every column matches) and
       * best-effort — a failed `CONVERT` (e.g. over-long index: 4 bytes/char can exceed key-length
       * limit) is logged and skipped, not fatal. No-op on Postgres (`UTF8` already stores 4-byte chars).
       */
      upgradeCharset?: boolean;

      pseudoUnique?: number;
      categorical?: number;
      autoIndexing?: boolean;
      /**
       * Max fractional-digit scale for inferred `decimal` columns. Unset (default): stored at the full
       * scale the data needs, up to the dialect limit (MySQL 30, SQL Server 38, Postgres 16383) — no
       * silent precision loss. Set lower to cap scale (e.g. `2` for currency). A value whose true scale
       * exceeds the cap is ROUNDED with a warning — unless `decimalToVarchar` is on.
       */
      decimalMaxLength?: number;
      /**
       * When a decimal value's true scale exceeds the cap (`decimalMaxLength` / dialect limit), store
       * the column as text (`varchar`) to preserve the exact value instead of rounding. Off by default
       * (column stays numeric, rounded with a warning). On when exact high-precision values matter more
       * than SQL arithmetic on the column.
       */
      decimalToVarchar?: boolean;
      maxKeyLength?: number;
      /**
       * Cap the auto-detected composite primary key at this many columns (default 4). The search tries
       * combinations of pseudo-unique columns; without a cap it is O(2^N) in candidate columns — a hang
       * risk on a wide table with no natural key. 4 covers effectively every real composite key; raise
       * only to auto-detect a genuine 5+ column natural key.
       */
      maxCompositeKeyColumns?: number;
      maxVarcharLength?: number,

      sampling?: number;
      samplingMinimum?: number;

      insertType?: "UPDATE" | "INSERT";
      insertStack?: number;
      /**
       * Load rows with the dialect's bulk-copy mechanism (Postgres `COPY` / MySQL
       * `LOAD DATA LOCAL INFILE`) instead of parameterised multi-row `INSERT` — faster/cheaper for large
       * loads. Applies to staging-table population (upsert preserved by the unchanged merge step) and
       * requires `useStagingInsert`. On a bulk-load error the batch falls back to parameterised `INSERT`
       * so a bad row surfaces a clear error. Off by default. Postgres `COPY` needs optional
       * `pg-copy-streams`.
       */
      bulkLoad?: boolean;

      safeMode?: boolean;
      deleteColumns?: boolean;

      /**
       * Allow autosql to DROP a UNIQUE constraint when incoming data would violate it — staged data
       * colliding with an existing unique (resolveConflicts) or a batch with duplicates in a
       * previously-unique column. Default false (safe): the constraint is KEPT and the load fails loud
       * (or diverts to `rejectedRowsTable`) on colliding rows, rather than permanently removing a
       * uniqueness guarantee — including a user-defined one — based on one batch's data. Set true to
       * auto-drop (a warning naming the constraint is logged either way). Mirrors `deleteColumns` /
       * `updatePrimaryKey`.
       */
      dropUniqueConstraints?: boolean;

      autoSplit?: boolean;

      addTimestamps?: boolean;
      /**
       * Load via a staging temp table (default `true`): CTAS empty clone → populate → merge into the
       * real table (upsert via `ON CONFLICT`/`ON DUPLICATE`/`MERGE`) → drop. Gives an atomic
       * all-or-nothing merge, is the target for `bulkLoad`, and runs conflict resolution.
       *
       * `false` = **direct load**: rows go straight to the target with `INSERT … ON CONFLICT/ON
       * DUPLICATE` (upsert still works), skipping the temp-table round-trips and extra write —
       * faster/cheaper for append-only or small/frequent loads not needing staging atomicity. Required
       * (`true`) for `addHistory` (history diffs the staging table against the target).
       */
      useStagingInsert?: boolean;
      addHistory?: boolean;
      historyTables?: string[];
      addNested?: boolean;
      nestedTables?: string[];
      excludeBlankColumns?: boolean;

      /**
       * Strip unstorable characters from string values before insert: NUL bytes (``) and unpaired
       * UTF-16 surrogates (replaced with U+FFFD). These otherwise hard-fail Postgres (`invalid byte
       * sequence for encoding UTF8`, `unsupported Unicode escape sequence`) and can corrupt MySQL. Off
       * by default (it mutates data); enable when ingesting free-text with pasted/garbage bytes.
       * Connection charset pinning does NOT address these — this does.
       */
      sanitizeInvalidChars?: boolean;

      useWorkers?: boolean;
      maxWorkers?: number;

      /**
       * Per-worker-task timeout in SECONDS. `0` (default) disables it. A worker that dies mid-task
       * (terminate/OOM/native crash) is always caught by the pool's exit/error handlers — this timeout
       * is NOT required for that; it only guards an alive-but-wedged worker (e.g. a DB call that never
       * returns). Off by default so a legitimately long batch isn't spuriously failed; set for a hard
       * per-task upper bound.
       */
      workerTaskTimeout?: number;

      /**
       * Column names always stored as varchar regardless of content. For string-encoded identifiers
       * (phone numbers, zip codes, padded codes) that would otherwise be inferred as numeric.
       */
      forceStringColumns?: string[];

      /**
       * Column names always stored as a boolean flag. By default AutoSQL infers boolean only from the
       * literals `true`/`false` — a bare `0`/`1` is an integer. Use this hint for columns encoding a
       * flag as `0`/`1` or `true`/`false`. Values outside the boolean domain (`0`, `1`, `true`, `false`,
       * case-insensitive, plus null/blank) are rejected with an error, not silently coerced — forcing to
       * boolean is lossy, so an unexpected value is surfaced.
       */
      booleanColumns?: string[];

      /**
       * Explicit number-format separators for locale-aware ingestion. Set BOTH to disambiguate
       * single-separator values (e.g. `thousandsSeparator: "."` + `decimalSeparator: ","` parses
       * "1.000" as 1000, not 1). Omit both to use the auto-detection heuristic (lone separator = decimal).
       */
      thousandsSeparator?: string;
      decimalSeparator?: string;

      /**
       * Regional number-format preset — sugar over `thousandsSeparator`/`decimalSeparator`, resolved to
       * those fields in `validateConfig` so it flows to BOTH inference and storage. For a known source
       * locale wanting lone-separator values disambiguated (`"US"` reads "1,234" as 1234; `"EU"` reads
       * "1.234" as 1234).
       *
       *  - `"US"` / `"IN"` — thousands `","`, decimal `"."` (Indian lakh/crore grouping accepted
       *    automatically; shares US separators).
       *  - `"EU"` — thousands `"."`, decimal `","`.
       *
       * Explicit `thousandsSeparator`/`decimalSeparator` take precedence when both supplied. Omit for
       * the auto-detection heuristic (lone separator = decimal, with a one-per-run warning for the
       * ambiguous "1,234" case).
       */
      numberFormat?: "US" | "EU" | "IN";

      /**
       * When neither explicit separators nor `numberFormat` are given, autosql infers the format from
       * **structural evidence in the data** (dataset-level: one layout for the whole load). A value that
       * can only be one layout — `"1,234,567"` (comma = thousands) or `"12.5"` (dot = decimal) — is a
       * vote; the ambiguous `"1,234"` shape abstains. If both layouts appear (contradictory, ~never for
       * real data) it falls back to assume-decimal + a warning.
       *
       * `numberFormatMinEvidence` = votes a layout needs before it's trusted (and before an opposing
       * minority counts as a real conflict). Default **1** — one structural value is certainty, not a
       * guess. Raise to tolerate a few stray/mis-parsed values.
       */
      numberFormatMinEvidence?: number;

      /**
       * IANA time zone (e.g. "America/New_York", "UTC") that ZONELESS datetime inputs are interpreted
       * as. When set, a value with no offset (e.g. "2024-01-15 12:00:00") is treated as local time in
       * this zone and converted to a UTC instant before storage. Inputs that ALREADY carry a zone ("…Z"
       * / "+05:00") are unaffected (already absolute); `date`/`time` columns are never shifted. Omit
       * (default) to store zoneless values as given (wall-clock preserved, no zone assumed). autosql
       * NEVER infers this from the host process timezone. Normalises the stored INSTANT; does not by
       * itself make a `datetimetz`/`timestamptz` column round-trip a source offset (separate concern).
       */
      sourceTimeZone?: string;

      /**
       * Acquire a per-table advisory lock before schema inference and ALTER TABLE. Set `true` when the
       * same table may be written by concurrent processes, to prevent races in compareMetaData.
       * Default `false`.
       */
      useSchemaLock?: boolean;
      /** Seconds to wait for the advisory lock before throwing `SchemaLockTimeoutError`. Default 30. */
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
       * Opt-in rejected-rows table name. Rows still failing after per-row retries are written here
       * instead of throwing — graceful degradation on the streaming flush and the non-streaming
       * direct-insert path (`useStagingInsert: false`). No effect on the default staging path, which
       * stays atomic (all-or-nothing) by design.
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
           * phase timings + throughput, so a pipeline can forward them to observability without parsing
           * log strings. Optional; omit to not collect metrics.
           */
          stats?: (stats: QueryStats) => void;
      };

      /**
       * TLS for the driver connection. Omit **or `false`** (default) → no change: plaintext / driver
       * default (existing configs byte-for-byte unaffected). `true` → TLS with default verification. An
       * object configures TLS:
       *   - `ca`: PEM CA bundle to verify the server cert against (e.g. AWS RDS bundle, or a BYOD CA).
       *   - `rejectUnauthorized`: verify the cert chain. Defaults to driver default (true) when a `ca`
       *     is supplied; set `false` ONLY for dev/self-signed (verification then off).
       *   - `cert` / `key`: client certificate + key for mutual TLS (optional).
       *   - `servername`: SNI override when the host differs from the cert CN/SAN.
       * Works on **all three dialects**: MySQL/Postgres get it as the driver `ssl` object; SQL Server
       * maps it onto mssql's `encrypt` / `trustServerCertificate` / `cryptoCredentialsDetails`
       * automatically. Not composed with `sshConfig` — pick one path (SSH tunnel OR direct TLS).
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
   * Expected OpenSSH host-key fingerprint, e.g. "SHA256:abc123…" (obtain via `ssh-keyscan -t ed25519
   * <host> | ssh-keygen -lf -`). When set, the tunnel VERIFIES the bastion's host key and refuses a
   * mismatch — closing the MITM window (ssh2 does NO verification by default). Omitted: still connects
   * but logs a loud warning that identity is unverified. "SHA256:" prefix and base64 padding optional.
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
    /** Max fractional-digit scale the dialect's decimal type holds (MySQL 30, SQL Server 38, Postgres
     *  16383). Default decimal scale ceiling — stored at full precision up to the DB limit, not an
     *  arbitrary low cap. */
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
     * The driver's structured error code, when the failure came from the DB driver — mysql2's `code`
     * (e.g. `"ER_TABLEACCESS_DENIED_ERROR"`), Postgres SQLSTATE (e.g. `"42501"`), or SQL Server error
     * number. Lets a caller branch on the exact failure without string-matching `error`. Omitted for
     * non-driver errors.
     */
    errorCode?: string;
    table?: string;
    /**
     * The resolved schema AutoSQL used for the load — final `MetadataHeader` including managed columns
     * (`dwh_*` timestamps, an auto-increment surrogate). Cache and pass back to skip re-introspection
     * on the next load (see the `existingSchema` fast path).
     */
    metaData?: MetadataHeader;
    /** Per-run performance metrics (phase timings + throughput). Populated on a successful `autoSQL`
     *  load; the same object is passed to `logger.stats`. See `QueryStats`. */
    stats?: QueryStats;
}

/**
 * Per-run performance metrics for one `autoSQL` load — for observability and a stats history (size
 * batches, spot load-time drift, bill by throughput). Durations are wall-clock milliseconds per phase.
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
    // Running max of integer-part digits. Precision = maxIntegerDigits + maxScale; tracking the
    // integer part separately avoids under-counting when the widest-integer and widest-scale values
    // are different rows.
    intLen?: number;
  }
}

export interface SqlizeRule {
  regex: string;
  replace: string;
  type: true | string[]; // `true` = apply to all types
}