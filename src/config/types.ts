export type MetadataHeader = Record<string, ColumnDefinition>;

export interface AlterTableChanges {
    addColumns: { [column: string]: ColumnDefinition }[];
    modifyColumns: { [column: string]: ColumnDefinition }[];
    dropColumns: string[];
    renameColumns: { oldName: string; newName: string }[];
  }
  
export interface DatabaseConfig {
      sql_dialect: string;
      host?: string;
      user?: string;
      password?: string;
      database?: string;
      port?: number;
      table?: string;
      headers?: ColumnDefinition[];
      updatePrimaryKey?: boolean;
      primaryKey?: string[];
}

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
}

export type QueryInput = string | QueryWithParams;
export type QueryWithParams = { query: string; params?: any[] };

export interface TranslateMap {
    server_to_local: Record<string, string>;
    local_to_server: Record<string, string>;
  }
  
export interface DialectConfig {
    require_length: string[];
    optional_length: string[];
    no_length: string[];
    decimals: string[];
    translate: TranslateMap;
    default_translation: Record<string, string>;
    sqlize: Array<{
      find: string;
      replace: string;
      regex: string;
      type: boolean | string[];
    }>;
}