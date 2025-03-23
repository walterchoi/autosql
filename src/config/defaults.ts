export const defaults = {
    minimumUnique: 10,
    maximumUniqueLength: 64,
    maxNonTextLength: 256,
    pseudoUnique: 0.90,
    sampling: 0,
    samplingMinimum: 100,
    maxKeyLength: 255,
    autoIndexing: true,
    insertType: "UPDATE",
    insertStack: 100,
    safeMode: false,
    deleteColumns: false,
    decimalMaxLength: 10,
    autoSplit: false,
    useWorkers: true,
    maxWorkers: 8
}

export const maxQueryAttempts = 3;

export const MYSQL_MAX_ROW_SIZE = 16 * 1024; // 16KB
export const POSTGRES_MAX_ROW_SIZE = 8 * 1024; // 8KB
export const DEFAULT_LENGTHS = {
    varchar: 255,
    text: 2, 
    mediumtext: 3,
    longtext: 4,
    json: 4, 
    decimal: 10, 
};

export const CREATED_TIMESTAMP_NAMES = [
    "dwh_created_at", "created_at", "create_at", "created_date",
    "create_date", "creation_time", "createdon", "record_created",
    "inserted_at", "creation_date", "createdtimestamp", "created_ts"
];
  
export const MODIFIED_TIMESTAMP_NAMES = [
    "dwh_modified_at", "modified_at", "modify_at", "modified_date",
    "update_date", "updatedon", "last_modified", "last_update",
    "record_updated", "changed_at", "updated_timestamp", "modified_ts"
];

export const DWH_LOADED_TIMESTAMP_NAMES = [
    "dwh_loaded_at", "dwh_loaded_date", "data_warehouse_loaded_at",
    "etl_loaded_at", "data_loaded_at", "extract_timestamp",
    "ingestion_time", "etl_timestamp", "dw_timestamp"
];