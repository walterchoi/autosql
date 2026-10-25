import type { Client as SSHClient, ClientChannel } from "ssh2";

export interface ColumnDefinition {
  type: string | null;
  length?: number;
  allowNull?: boolean;
  unique?: boolean;
  /**
   * Real DB name of a NON-PRIMARY unique index this column participates in, from introspection.
   * Composite members share one name; `undefined` for inferred uniques. Never guessed: MySQL
   * auto-names a unique after its column, so a synthesised name would not match `DROP INDEX`.
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
   * DDL default EXPRESSION from the catalog. Kept separate from `default`, which doubles as the
   * literal `getInsertValues` substitutes for a missing cell: binding a DDL expression as a row
   * value would store the expression text verbatim (A3).
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
 * `assumeSchema` (A-4) skips per-value inference for declared columns. `existingSchema` (N1) skips
 * live introspection, and must be autosql's own last resolved schema rather than a bare spec: a
 * baseline missing managed columns (`dwh_*`, surrogate) would make autosql re-ADD them. Pass it only
 * in steady state; drop it on load error or detected drift.
 */
export interface AutoSQLOptions {
  assumeSchema?: MetadataHeader;
  existingSchema?: MetadataHeader;
}

/** Options for `openStream` (see `AutoSQLHandler.openStream`). */
export interface OpenStreamOptions {
  /**
   * Opt in to staging that survives between runs. Identifies the load SESSION, not one run of it:
   * a run or attempt id would hand every retry a fresh table, so resuming would silently become
   * restarting. ⚠ One writer per key — autosql does not serialise them.
   * See README, "Resuming a stream across runs".
   */
  sessionKey?: string;
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
 * What an `autoSQL` load WOULD do, computed without writing anything. Contrast with `safeMode`,
 * which runs the load but skips DDL.
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
       * Max connections in the driver pool. Default 5. Raise for parallel/worker loads so pool
       * acquisition does not serialise; keep under the server's own limit and size against `maxWorkers`.
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
       * Add an auto-increment surrogate key when the dataset has no natural one, so the table can be
       * created and Postgres upserts have a conflict target. Off by default. The key is unique per
       * physical insert, so every ingest APPENDS: `insertType: "UPDATE"` never matches an existing
       * row. Not compatible with `addHistory`, `addNested`, or `autoSplit`.
       */
      surrogateKey?: boolean;
      /** Column name for the surrogate key (see `surrogateKey`). Defaults to `"autosql_id"`. */
      surrogateKeyColumn?: string;
      engine?: string;
      charset?: string;
      collate?: string;
      encoding?: string;

      /**
       * MySQL only, off by default. Converts text columns on a PRE-EXISTING table to `charset`, so
       * externally-created 3-byte `utf8` columns accept 4-byte characters. Connection-charset pinning
       * and new-table defaults do not fix an existing column; this does. Best-effort: a failed
       * CONVERT (e.g. an over-long index) is logged and skipped. No-op on Postgres.
       */
      upgradeCharset?: boolean;

      pseudoUnique?: number;
      categorical?: number;
      autoIndexing?: boolean;
      /**
       * Infer `UNIQUE` constraints from the data (default `true`). Set `false` when one batch's
       * distinctness says nothing about the real key. Wins over `inferAdditionalUniques`; primary-key,
       * index and pseudounique prediction are unaffected.
       */
      inferUnique?: boolean;
      /**
       * Infer uniques beyond a new keyless table (default `true`): beside an explicit `primaryKey`,
       * and on an existing table. Set `false` for composite keys, because on MySQL a stray
       * `UNIQUE(id)` beside `PRIMARY KEY (conn, id)` makes `ON DUPLICATE KEY UPDATE` overwrite
       * another key's row. `inferUnique: false` wins over this.
       */
      inferAdditionalUniques?: boolean;
      /**
       * Cap on inferred `decimal` scale. Unset (default) stores at the full scale the data needs, up
       * to the dialect limit, so there is no silent precision loss. A value past the cap is ROUNDED
       * with a warning, unless `decimalToVarchar` is on.
       */
      decimalMaxLength?: number;
      /**
       * Store a decimal as text when its true scale exceeds the cap, keeping the exact value instead
       * of rounding. Off by default. Use when exact values matter more than SQL arithmetic.
       */
      decimalToVarchar?: boolean;
      maxKeyLength?: number;
      /**
       * Cap the auto-detected composite primary key (default 4). The search is O(2^N) in candidate
       * columns without a cap, which can hang on a wide table with no natural key. Raise only to
       * auto-detect a genuine 5+ column key.
       */
      maxCompositeKeyColumns?: number;
      maxVarcharLength?: number,

      sampling?: number;
      samplingMinimum?: number;

      insertType?: "UPDATE" | "INSERT";
      insertStack?: number;
      /**
       * Populate via the dialect's bulk-copy mechanism instead of parameterised INSERT (off by
       * default, requires `useStagingInsert`). Upsert semantics are unaffected — only the populate
       * step changes. Falls back to INSERT on error. Postgres needs `pg-copy-streams`.
       */
      bulkLoad?: boolean;

      safeMode?: boolean;
      deleteColumns?: boolean;

      /**
       * Allow autosql to DROP a UNIQUE constraint that incoming data would violate. Default `false`:
       * the constraint is kept and the load fails loud, rather than permanently removing a uniqueness
       * guarantee (possibly a user-defined one) on the strength of one batch. Mirrors `deleteColumns`.
       */
      dropUniqueConstraints?: boolean;

      autoSplit?: boolean;

      addTimestamps?: boolean;
      /**
       * Load via a staging temp table (default `true`): empty clone, populate, merge, drop —
       * atomic all-or-nothing, required by `bulkLoad` and `addHistory` (which diffs staging
       * against the target). `false` inserts straight to the target (upsert still works) but
       * skips that round-trip.
       */
      useStagingInsert?: boolean;
      addHistory?: boolean;
      historyTables?: string[];
      addNested?: boolean;
      nestedTables?: string[];
      excludeBlankColumns?: boolean;

      /**
       * Strip unstorable characters (NUL bytes, unpaired UTF-16 surrogates) before insert. These
       * hard-fail Postgres and can corrupt MySQL, and connection-charset pinning does not address
       * them. Off by default because it mutates data.
       */
      sanitizeInvalidChars?: boolean;

      useWorkers?: boolean;
      maxWorkers?: number;

      /**
       * Per-worker-task timeout in SECONDS, `0` (default) disables. A worker that DIES is already
       * caught by the pool's exit/error handlers; this only guards an alive-but-wedged worker. Off by
       * default so a legitimately long batch is not spuriously failed.
       */
      workerTaskTimeout?: number;

      /**
       * Column names always stored as varchar, for string-encoded identifiers (phone numbers, zip
       * codes, padded codes) that would otherwise infer as numeric.
       */
      forceStringColumns?: string[];

      /**
       * Column names always stored as a boolean flag. autosql infers boolean only from
       * `true`/`false`, since a bare `0`/`1` is more often an integer. Values outside the boolean
       * domain are rejected rather than coerced, because forcing to boolean is lossy.
       */
      booleanColumns?: string[];

      /**
       * Explicit number-format separators. Set BOTH to disambiguate single-separator values: e.g.
       * thousands `"."` with decimal `","` parses "1.000" as 1000. Omit both for auto-detection.
       */
      thousandsSeparator?: string;
      decimalSeparator?: string;

      /**
       * Regional preset, sugar over `thousandsSeparator`/`decimalSeparator` (explicit separators
       * win when both are supplied):
       *  - `"US"` / `"IN"`: thousands `","`, decimal `"."` (Indian grouping accepted automatically).
       *  - `"EU"`: thousands `"."`, decimal `","`.
       */
      numberFormat?: "US" | "EU" | "IN";

      /**
       * With no explicit separators/`numberFormat`, the number layout is inferred from structural
       * evidence in the data (an unambiguous value votes; the ambiguous `"1,234"` shape abstains;
       * conflicts fall back to decimal + a warning). This sets how many votes are needed to trust a
       * layout — default 1. Raise it to tolerate stray values.
       */
      numberFormatMinEvidence?: number;

      /**
       * IANA time zone that zoneless datetime inputs are interpreted as, then converted to UTC for
       * storage. Zoned inputs and `date`/`time` columns are untouched; omit to store zoneless
       * values as given. Never inferred from the host process timezone — this normalises the
       * stored instant, it doesn't make a column round-trip a source offset on read.
       */
      sourceTimeZone?: string;

      /**
       * Acquire a per-table advisory lock before schema inference and ALTER TABLE. Default `false`;
       * set `true` when concurrent processes may write the same table, to prevent compareMetaData races.
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

      // --- Run audit ---
      /** Persist one row per top-level load (autoSQL/autoSQLChunked/stream end) to a managed run-audit
       *  table — run counts, durations, row counts, success/error — with no caller wiring. Best-effort:
       *  a failed audit write never fails the load. Default: false. */
      runAudit?: boolean;
      /** Name of the run-audit table. Default: "autosql_runs". */
      runAuditTable?: string;
      /** Schema containing the run-audit table. Default: same as config.schema. */
      runAuditSchema?: string;

      // --- Streaming ---
      /** Prefix for per-run stream staging tables. Default: "autosql_stream__". */
      streamingStagingPrefix?: string;
      /**
       * Max per-row widening retry rounds after a bulk insert/merge fails. Default: 3. Applies to the
       * streaming flush AND the non-streaming direct-insert path (`useStagingInsert: false`).
       */
      streamMaxRetries?: number;
      /**
       * Opt-in rejected-rows table. Rows still failing after per-row retries are written here instead
       * of throwing. Applies to the streaming flush and the direct-insert path; no effect on the
       * default staging path, which stays all-or-nothing by design.
       */
      rejectedRowsTable?: string;
      /** Schema for the rejected rows table. Default: same as config.schema. */
      rejectedRowsSchema?: string;
      /** When true, orphaned stream staging tables are preserved instead of dropped. Default: false. */
      keepOrphanedStagingTables?: boolean;

      /** Prefix for auto-created staging tables. Default `"temp_staging__"`. */
      stagingPrefix?: string;
      /** Suffix for auto-created history tables. Default `"__history"`. */
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
       * TLS for the driver connection. Omit or `false` (default): no change. `true`: TLS with
       * default verification. An object:
       *   - `ca`: PEM CA bundle to verify the server cert against.
       *   - `rejectUnauthorized`: verify the cert chain (default `true` when `ca` is set). Set
       *     `false` only for dev/self-signed.
       *   - `cert` / `key`: client cert + key for mutual TLS.
       *   - `servername`: SNI override when the host differs from the cert CN/SAN.
       * Works on all three dialects (mapped onto each driver's own TLS options). Not composed
       * with `sshConfig` — pick one path.
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
   * Expected OpenSSH host-key fingerprint, e.g. "SHA256:abc123…" (get it with `ssh-keyscan -t
   * ed25519 <host> | ssh-keygen -lf -`). When set, the tunnel VERIFIES the bastion's host key and
   * refuses a mismatch, closing the MITM window that ssh2 leaves open by doing no verification at
   * all. Omitted: still connects, with a loud warning that identity is unverified.
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
     * The driver's structured error code (mysql2 `code`, Postgres SQLSTATE, SQL Server error
     * number), so a caller can branch on the exact failure without string-matching `error`. Omitted
     * for non-driver errors.
     */
    errorCode?: string;
    table?: string;
    /**
     * The resolved schema autosql used, including managed columns (`dwh_*` timestamps, surrogate).
     * Cache and pass back as `existingSchema` to skip re-introspection on the next load.
     */
    metaData?: MetadataHeader;
    /** Per-run performance metrics (phase timings + throughput). Populated on a successful `autoSQL`
     *  load; the same object is passed to `logger.stats`. See `QueryStats`. */
    stats?: QueryStats;
}

/**
 * Per-run performance metrics for one `autoSQL` load. Durations are wall-clock milliseconds per phase.
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
    // Tracked separately from `decimal` (precision = intLen + decimal) to avoid under-counting
    // when the widest-integer and widest-scale values are different rows.
    intLen?: number;
  }
}

export interface SqlizeRule {
  regex: string;
  replace: string;
  type: true | string[]; // `true` = apply to all types
}