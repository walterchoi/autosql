export const defaults = {
    minimumUnique: 10,
    maximumUniqueLength: 64,
    maxNonTextLength: 256,
    pseudoUnique: 0.90,
    sampling: 0,
    samplingMinimum: 100,
    maxKeyLength: 255,
    autoIndexing: true,
    insert_type: "REPLACE",
    maxInsert: 5000,
    insertStack: 100,
    maxInsertSize: 1048576,
    safeMode: true,
    deleteColumns: false,
    waitForApproval: false,
    decimalMaxLength: 10
}

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