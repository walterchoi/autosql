/**
 * Internal utility exports — not part of the stable public API.
 * These may change without a semver bump.
 */
export { predictType, collateTypes } from "./helpers/columnTypes";
export { predictIndexes } from "./helpers/keys";
export {
    isObject, sqlize, shuffleArray, calculateColumnLength,
    normalizeNumber, mergeColumnLengths, setToArray, parseDatabaseLength, parseDatabaseMetaData,
    generateCombinations, isCombinationUnique, tableChangesExist, isMetaDataHeader, estimateRowSize,
    isValidDataFormat, normalizeKeysArray, organizeSplitTable, organizeSplitData, splitInsertData,
    getInsertValues, getNextTableName, normalizeResultKeys, getTempTableName, getTrueTableName, getHistoryTableName
} from "./helpers/utilities";
