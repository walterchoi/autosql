
import { predictType, collateTypes } from "./helpers/columnTypes";
import { getMetaData, compareMetaData, getDataHeaders, initializeMetaData } from "./helpers/metadata";
import { predictIndexes } from "./helpers/keys";
import { isObject, sqlize, shuffleArray, validateConfig, calculateColumnLength, 
    normalizeNumber, mergeColumnLengths, setToArray, parseDatabaseLength, parseDatabaseMetaData, 
    generateCombinations, isCombinationUnique, tableChangesExist, isMetaDataHeader, estimateRowSize, 
    isValidDataFormat, normalizeKeysArray, organizeSplitTable, organizeSplitData, splitInsertData, getInsertValues, getNextTableName } from "./helpers/utilities";

export {
    predictType, collateTypes,
    getMetaData, compareMetaData, getDataHeaders, initializeMetaData,
    predictIndexes,
    isObject, sqlize, shuffleArray, validateConfig, calculateColumnLength, 
    normalizeNumber, mergeColumnLengths, setToArray, parseDatabaseLength, parseDatabaseMetaData, 
    generateCombinations, isCombinationUnique, tableChangesExist, isMetaDataHeader, estimateRowSize, 
    isValidDataFormat, normalizeKeysArray, organizeSplitTable, organizeSplitData, splitInsertData, getInsertValues, getNextTableName    
};

export { Database } from "./db/database";

export type {
    DatabaseConfig,
    DialectConfig,
    ColumnDefinition,
    MetadataHeader,
    InsertInput,
    QueryInput,
    QueryWithParams,
    QueryResult,
    InsertResult,
    SSHKeys
  } from "./config/types";