export type MetadataHeader = Record<string, ColumnDefinition>;

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