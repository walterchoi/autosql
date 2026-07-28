import type { Client as SSHClient, ClientChannel } from "ssh2";

export interface ColumnDefinition {
  type: string | null;
  length?: number;
  allowNull?: boolean;
  unique?: boolean;
  index?: boolean;
  pseudounique?: boolean;
  categorical?: boolean;
  singleValue?: boolean;
  primary?: boolean;
  autoIncrement?: boolean;
  default?: any;
  decimal?: number;
  calculated?: boolean;
  updatedCalculated?: boolean;
  calculatedDefault?: any;
  previousType?: string;
  tableName?: string[];
}

export type MetadataHeader = Record<string, ColumnDefinition>;

/**
 * Per-call options for `autoSQL`. Currently the provided-schema fast path (A-4): when the caller
 * already knows the schema (e.g. a SproutSpec `columns` block mapped to a `MetadataHeader`), passing
 * it as `assumeSchema` lets AutoSQL skip per-value type inference. Declared columns are authoritative
 * (which also side-steps inference footguns such as small integers being mis-typed as boolean); any
 * data column not declared is still inferred as a fallback.
 */
export interface AutoSQLOptions {
  assumeSchema?: MetadataHeader;
}


export type supportedDialects = "mysql" | "pgsql";

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

      pseudoUnique?: number;
      categorical?: number;
      autoIndexing?: boolean;
      decimalMaxLength?: number;
      maxKeyLength?: number;
      maxVarcharLength?: number,

      sampling?: number;
      samplingMinimum?: number;

      insertType?: "UPDATE" | "INSERT";
      insertStack?: number;

      safeMode?: boolean;
      deleteColumns?: boolean;

      autoSplit?: boolean;

      addTimestamps?: boolean;
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
       * Column names that should always be stored as varchar regardless of their
       * content. Use this for string-encoded identifiers (phone numbers, zip codes,
       * padded codes) that would otherwise be inferred as numeric types.
       */
      forceStringColumns?: string[];

      /**
       * Explicit number-format separators for locale-aware ingestion. Set BOTH to
       * disambiguate single-separator values (e.g. with `thousandsSeparator: "."` and
       * `decimalSeparator: ","`, "1.000" is parsed as 1000, not 1). Omit both to use the
       * auto-detection heuristic (a lone separator is treated as decimal).
       */
      thousandsSeparator?: string;
      decimalSeparator?: string;

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
      /** Max widening retry rounds during stream merge. Default: 3. */
      streamMaxRetries?: number;
      /** Opt-in rejected rows table name. When set, unresolvable rows are written here instead of throwing. */
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
    table?: string;
    /**
     * The resolved schema AutoSQL used for the load — the final `MetadataHeader` including any
     * managed columns (`dwh_*` timestamps, an auto-increment surrogate). Callers can cache this and
     * pass it back to skip re-introspection on the next load (see the `existingSchema` fast path).
     */
    metaData?: MetadataHeader;
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
  }
}

export interface SqlizeRule {
  regex: string;
  replace: string;
  type: true | string[]; // `true` = apply to all types
}