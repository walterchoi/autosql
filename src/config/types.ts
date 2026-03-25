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

      metaData?: {
        [tableName: string]: MetadataHeader;
      };
      existingMetaData?: {
        [tableName: string]: MetadataHeader;
      };
      updatePrimaryKey?: boolean;
      primaryKey?: string[];
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

      useWorkers?: boolean;
      maxWorkers?: number;

      /**
       * Column names that should always be stored as varchar regardless of their
       * content. Use this for string-encoded identifiers (phone numbers, zip codes,
       * padded codes) that would otherwise be inferred as numeric types.
       */
      forceStringColumns?: string[];

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
    decimal: number;
    trueMaxDecimal: number;
  }
}

export interface SqlizeRule {
  regex: string;
  replace: string;
  type: true | string[]; // `true` = apply to all types
}