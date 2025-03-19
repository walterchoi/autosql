export interface ColumnDefinition {
  type: string | null;
  length?: number;
  allowNull?: boolean;
  unique?: boolean;
  index?: boolean;
  pseudounique?: boolean;
  primary?: boolean;
  autoIncrement?: boolean;
  default?: any;
  decimal?: number;
  calculated?: boolean;
  updatedCalculated?: boolean;
  previousType?: string;
  tableName?: string[];
}

export type MetadataHeader = Record<string, ColumnDefinition>;

export function isMetadataHeader(obj: any): obj is MetadataHeader {
  return (
      obj !== null &&
      typeof obj === "object" &&
      !Array.isArray(obj) &&
      Object.values(obj).every(
          (col) =>
              typeof col === "object" &&
              col !== null &&
              "type" in col &&
              typeof col.type === "string"
      )
  );
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
      addTimestamps?: boolean;

      minimumUnique?: number;
      maximumUniqueLength?: number;
      maxNonTextLength?: number;
      pseudoUnique?: number;
      autoIndexing?: boolean;
      decimalMaxLength?: number;
      maxKeyLength?: number;

      sampling?: number;
      samplingMinimum?: number;

      insertType?: "REPLACE" | "INSERT";
      maxInsert?: number;
      insertStack?: number;
      maxInsertSize?: number;
      safeMode?: boolean;
      deleteColumns?: boolean;
      waitForApproval?: boolean;

      autoSplit?: boolean;

      useWorkers?: boolean;
      
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
    sqlize: Array<{
      find: string;
      replace: string;
      regex: string;
      type: boolean | string[];
    }>;
    engine: string;
    charset: string;
    collate: string;
    encoding: string;
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
  metaData: MetadataHeader
}

export interface QueryResult {
    start: Date; 
    end: Date; 
    duration: number;
    affectedRows?: number 
    success: boolean;
    results?: any[];
    error?: string;
}

export interface metaDataInterim {
  [key: string]: {
    uniqueSet: Set<any>;
    valueCount: number;
    nullCount: number;
    types: Set<string>;
    length: number;
    decimal: number;
  }
}