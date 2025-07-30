export const defaults = {
    pseudoUnique: 0.90,
    categorical: 0.20,
    sampling: 0,
    samplingMinimum: 100,
    maxKeyLength: 255,
    maxVarcharLength: 1024,
    autoIndexing: true,
    insertType: "UPDATE",
    insertStack: 100,
    safeMode: false,
    deleteColumns: false,
    decimalMaxLength: 6,
    autoSplit: false,
    useWorkers: true,
    maxWorkers: 8,
    addTimestamps: true,
    addHistory: false,
    useStagingInsert: true,
    addNested: false
}

export const maxQueryAttempts = 3;

export const MAX_COLUMN_COUNT = 100;
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
    "inserted_at", "creation_date", "createdtimestamp", "created_ts",
    "createddatetime", "createdutc", "creationtimestamp",
    "insertedutc", "inserteddate", "insertedtime", "createddateutc",
    "createdondate", "row_created_at", "inserted_timestamp",
    "created_iso", "created_time_utc"
  ];
  
  export const MODIFIED_TIMESTAMP_NAMES = [
    "dwh_modified_at", "modified_at", "modify_at", "modified_date",
    "update_date", "updatedon", "last_modified", "last_update",
    "record_updated", "changed_at", "updated_timestamp", "modified_ts",
    "updateddatetime", "lastmodified", "modifiedutc", "modifieddateutc",
    "updateddateutc", "lastupdateutc", "updatedondate", "row_modified_at",
    "updatetime", "last_updated_at", "modified_time_utc"
  ];
  
  export const DWH_LOADED_TIMESTAMP_NAMES = [
    "dwh_loaded_at", "dwh_loaded_date", "data_warehouse_loaded_at",
    "etl_loaded_at", "data_loaded_at", "extract_timestamp",
    "ingestion_time", "etl_timestamp", "dw_timestamp",
    "loadedat", "load_timestamp", "ingested_at", "load_date",
    "dwloadtimestamp", "sync_timestamp", "imported_at", "import_timestamp",
    "staging_loaded_at", "data_loaded_time", "record_loaded_at"
  ];
  

export const nonCategoricalTypes = ["boolean", "tinyint", "smallint", "binary", "datetimetz", "datetime"]