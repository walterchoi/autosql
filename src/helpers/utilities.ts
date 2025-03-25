import { defaults, DEFAULT_LENGTHS, MYSQL_MAX_ROW_SIZE, POSTGRES_MAX_ROW_SIZE, MAX_COLUMN_COUNT } from "../config/defaults";
import { DatabaseConfig, MetadataHeader, DialectConfig, AlterTableChanges, supportedDialects, SqlizeRule } from "../config/types";
import { groupings } from "../config/groupings";
import crypto from 'crypto';
export function isObject(val: any): boolean {
    return val !== null && typeof val === "object";
}

export function shuffleArray<T>(array: T[]): T[] {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

export function validateConfig(config: DatabaseConfig): DatabaseConfig {
    try {
        if (!config.sqlDialect) {
            throw new Error("Please provide a sqlDialect (such as pgsql, mysql) as part of the configuration object.");
        }

        // Define default values
        const defaultConfig: DatabaseConfig = {
            sqlDialect: config.sqlDialect, // Keep required field
            pseudoUnique: defaults.pseudoUnique,
            autoIndexing: defaults.autoIndexing,
            sampling: defaults.sampling,
            samplingMinimum: defaults.samplingMinimum,
            metaData: config.metaData || {}, // Ensuring headers remain intact
            maxKeyLength: defaults.maxKeyLength,
            autoSplit: defaults.autoSplit,
            useWorkers: defaults.useWorkers,
            maxWorkers: defaults.maxWorkers
        };

        // Merge provided config with defaults
        return { ...defaultConfig, ...config };
    } catch (error) {
        throw error;
    }
}

export function calculateColumnLength(column: any, dataPoint: string, sqlLookupTable: any) {
    if (sqlLookupTable.decimals.includes(column.type)) {
        column.decimal = column.decimal ?? 0;

        const decimalLen = dataPoint.includes(".") ? dataPoint.split(".")[1].length + 1 : 0;
        column.decimal = Math.max(column.decimal, decimalLen);
        column.decimal = Math.min(column.decimal, sqlLookupTable.decimals_max_length || 10);

        const integerLen = dataPoint.split(".")[0].length;
        column.length = Math.max(column.length, integerLen + column.decimal + 3);
    } else {
        column.length = Math.max(column.length, dataPoint.length);
    }
}

export function normalizeNumber(input: any, thousandsIndicatorOverride?: string, decimalIndicatorOverride?: string): string | null {
    if ((thousandsIndicatorOverride && !decimalIndicatorOverride) || (!thousandsIndicatorOverride && decimalIndicatorOverride)) {
        throw new Error("Both 'thousandsIndicatorOverride' and 'decimalIndicatorOverride' must be provided together.");
    }
    let inputStr = String(input)
    let overridden: Boolean = false
    if(thousandsIndicatorOverride && decimalIndicatorOverride) {
        const THOUSANDS_INDICATORS = [",", "#*#*", "%*%*"];
        const DECIMAL_INDICATORS = [".", "%*%*", "#*#*"];
        const usedThousands = thousandsIndicatorOverride;
        const usedDecimal = decimalIndicatorOverride;
        const unusedThousands = THOUSANDS_INDICATORS.filter(ind => ind !== usedThousands && ind !== usedDecimal)[0];
        const unusedDecimal = DECIMAL_INDICATORS.filter(ind => ind !== usedThousands && ind !== usedDecimal)[0];
        overridden = true
        // Temporarily replace thousands and decimal indicators with placeholders
        let tempinputStr = inputStr.replaceAll(usedThousands, unusedThousands);
        tempinputStr = tempinputStr.replaceAll(usedDecimal, unusedDecimal);

        // Replace placeholders with final characters (comma for thousands, dot for decimal)
        tempinputStr = tempinputStr.replaceAll(unusedThousands, ",").replaceAll(unusedDecimal, ".");

        inputStr = tempinputStr;
    }

    // 🚨 Ensure `-` appears only at the start
    if (inputStr.includes("-") && inputStr.indexOf("-") !== 0) return null;

    const isNegative = inputStr.startsWith("-");
    if (isNegative) inputStr = inputStr.slice(1); // Remove `-` temporarily for processing

    if (!inputStr || /[^0-9., `']/.test(inputStr)) return null; // Reject if non-numeric characters exist. Allowing ` and ' as part of the Swiss number format

    const dotCount = (inputStr.match(/\./g) || []).length;
    let commaCount = (inputStr.match(/,/g) || []).length;

    // 🔍 Detect and normalize Swiss format if no commas are present but apostrophes exist
    if (commaCount === 0 && inputStr.includes("'")) {
        inputStr = inputStr.replace(/'/g, ","); // ✅ Convert apostrophes to commas
        commaCount = (inputStr.match(/,/g) || []).length;
    }
    if (commaCount === 0 && inputStr.includes("`")) {
        inputStr = inputStr.replace(/`/g, ","); 
        commaCount = (inputStr.match(/,/g) || []).length;
    }

    inputStr = inputStr.replace(/ /g, "");

    // 🚨 Reject cases
    if (
        !/\d/.test(inputStr) || // No digits present
        (dotCount > 1 && commaCount > 1) || // Too many of both
        inputStr.includes(".,") || inputStr.includes(",.") || // Misplaced combinations
        /\d[.,]{2,}\d/.test(inputStr) // Double separators like "1..234"
    ) {
        return null;
    }

    // 🚨 Check incorrect ordering of separators
    const firstComma = inputStr.indexOf(",");
    const lastComma = inputStr.lastIndexOf(",")
    const firstDot = inputStr.indexOf(".");
    const lastDot = inputStr.lastIndexOf(".")

    if (firstComma !== -1 && firstDot !== -1 && // Both exist
        (
            (firstComma < firstDot && dotCount > 1) || // Comma first, but multiple dots
            (firstDot < firstComma && commaCount > 1) || // Dot first, but multiple commas
            (firstComma < firstDot && firstDot < lastComma) || // Comma first, but comma after first dot
            (firstDot < firstComma && firstComma < lastDot) // Dot first, but dot after first comma
        )
    ) 
    {
        return null;
    }

    // Determine thousands and decimal indicators
    let thousandsIndicator = "";
    let decimalIndicator = "";

    if(overridden) {
        thousandsIndicator = ","
        decimalIndicator = "."
    } else if (dotCount === 1 && commaCount === 1) {
        thousandsIndicator = firstComma < firstDot ? "," : ".";
        decimalIndicator = thousandsIndicator === "," ? "." : ",";
    } else if (dotCount > 1) {
        thousandsIndicator = ".";
        decimalIndicator = ",";
    } else if (commaCount > 1) {
        thousandsIndicator = ",";
        decimalIndicator = ".";
    } else {
        // Only one separator exists, assume it is the decimal separator
        thousandsIndicator = "";
        decimalIndicator = dotCount === 1 ? "." : ",";
    }

    const decimalSplit = inputStr.split(decimalIndicator);
    
    if (decimalSplit.length > 2) return null; // More than one decimal, invalid

    let preDecimal = decimalSplit[0];
    let postDecimal = decimalSplit[1] || ""; // Optional decimal part

    // Validate thousands separator formatting
    if (thousandsIndicator) {
        const thousandsSplit = preDecimal.split(thousandsIndicator);
    
        if(thousandsSplit.length == 1) {
            const part = thousandsSplit[0];
            if(part.length > 3) {
                return null;
            }
        } else {
            // 🔍 Detect if the format is Indian-style or Western-style
            const isWesternFormat = thousandsSplit.length > 1 && thousandsSplit.every((part, i) =>
                (i === 0 ? part.length <= 3 : part.length === 3)
            );
        
            const isIndianFormat = thousandsSplit.length > 1 && thousandsSplit.every((part, i) =>
                (i === 0 ? part.length <= 2 : i === thousandsSplit.length - 1 ? part.length === 3 : part.length === 2)
            );
        
            if (!isWesternFormat && !isIndianFormat) return null; // ❌ Reject if it fits neither format
        
            // ✅ If valid, remove thousands separators
        }
        preDecimal = thousandsSplit.join("");
    }

    const normalized = `${isNegative ? "-" : ""}${preDecimal}${postDecimal ? "." + postDecimal : ""}`;
    return normalized;
}

export function mergeColumnLengths(lengthA?: string, lengthB?: string): string | undefined {
    if (!lengthA && !lengthB) return undefined;

    const parseLength = (length: string) => {
        const parts = length.split(",").map(Number);
        return parts.length === 2 ? parts : [parts[0], 0]; // Ensure decimal part exists
    };

    const [lenA, decA] = lengthA ? parseLength(lengthA) : [0, 0];
    const [lenB, decB] = lengthB ? parseLength(lengthB) : [0, 0];

    return `${Math.max(lenA, lenB)},${Math.max(decA, decB)}`;
}

export function setToArray<T>(inputSet: Set<T>): T[] {
    return [...inputSet]; // Spread operator converts Set to an array
}

export function parseDatabaseLength(lengthStr?: string): { length?: number; decimal?: number } {
    if (!lengthStr) return {};
    
    const parts = lengthStr.split(",").map(Number);
    const length = isNaN(parts[0]) ? undefined : parts[0];
    const decimal = parts.length === 2 && !isNaN(parts[1]) ? parts[1] : undefined;

    return { length, decimal };
}

export function parseDatabaseMetaData(rows: any[], dialectConfig?: DialectConfig ): MetadataHeader | Record<string, MetadataHeader> | null {
    if (!rows || rows.length === 0) return null; // Return null if no data

    const hasTableName = rows.some(row => "table_name" in row || "TABLE_NAME" in row);
    const hasNoTableName = rows.some(row => !("table_name" in row) && !("TABLE_NAME" in row));

    if (hasTableName && hasNoTableName) {
        throw new Error("Inconsistent data: Some rows contain 'table_name' while others do not.");
    }

    const metadata: Record<string, MetadataHeader> = {};

    rows.forEach((row) => {
        const normalizedRow = Object.keys(row).reduce((acc, key) => {
            acc[key.toLowerCase()] = row[key];
            return acc;
        }, {} as Record<string, any>);

        if (!normalizedRow.column_name) return; // Skip invalid rows

        const lengthInfo = parseDatabaseLength(String(normalizedRow["length"]));
        const dataType =
            dialectConfig?.translate?.serverToLocal[normalizedRow.data_type.toLowerCase()] ||
            normalizedRow["data_type"].toLowerCase();
        const columnKey = (normalizedRow["column_key"] || "").toUpperCase();

        let normalizedLength: number | undefined = lengthInfo.length;
        if (dialectConfig?.noLength.includes(dataType)) {
            normalizedLength = undefined;
        } else if (dialectConfig?.optionalLength.includes(dataType) && lengthInfo.length === undefined) {
            normalizedLength = undefined;
        }

        const autoIncrement =
            String(normalizedRow["extra"] || "").includes("auto_increment") ||
            String(normalizedRow["column_default"] || "").includes("nextval");

        const tableName = normalizedRow.table_name || "noTableName"; // Default for single-table case

        if (!metadata[tableName]) {
            metadata[tableName] = {};
        }

        metadata[tableName][normalizedRow.column_name] = { 
            type: dataType,
            length: normalizedLength,
            allowNull: normalizedRow["is_nullable"] === "YES",
            unique: columnKey === "UNIQUE",
            primary: columnKey === "PRIMARY",
            index: columnKey === "INDEX",
            autoIncrement: autoIncrement,
            decimal: lengthInfo.decimal ?? undefined,
            default: normalizedRow["column_default"],
        };
    });

    return hasTableName ? metadata : metadata["noTableName"] || null;
}

export function generateCombinations<T>(array: T[], length: number): T[][] {
    if (length === 1) return array.map(el => [el]);
    const combinations: T[][] = [];

    for (let i = 0; i < array.length; i++) {
        const smallerCombinations = generateCombinations(array.slice(i + 1), length - 1);
        for (const smaller of smallerCombinations) {
            combinations.push([array[i], ...smaller]);
        }
    }

    return combinations;
}

export function isCombinationUnique(data: Record<string, any>[], columns: string[]): boolean {
    const seenValues = new Set<string>();

    for (const row of data) {
        const key = columns.map(col => row[col]).join("|");
        if (seenValues.has(key)) return false;
        seenValues.add(key);
    }

    return true;
}

export function tableChangesExist(alterTableChanges: AlterTableChanges): boolean {
    if (
        Object.keys(alterTableChanges.addColumns).length > 0 ||
        Object.keys(alterTableChanges.modifyColumns).length > 0 ||
        alterTableChanges.dropColumns.length > 0 ||
        alterTableChanges.renameColumns.length > 0 ||
        alterTableChanges.nullableColumns.length > 0 ||
        alterTableChanges.noLongerUnique.length > 0 ||
        alterTableChanges.primaryKeyChanges.length > 0
    ) {
        return true
    } else {
        return false
    }
}

export function isMetaDataHeader(input: any): input is MetadataHeader {
    if (typeof input !== "object" || input === null || Array.isArray(input)) {
        return false; // ❌ Must be a non-null object
    }

    for (const key in input) {
        if (typeof key !== "string") return false; // ❌ Keys must be strings

        const column = input[key];

        if (
            typeof column !== "object" || column === null ||
            (!("type" in column) || typeof column.type !== "string") // ✅ "type" is required and must be a string
        ) {
            return false;
        }

        // ✅ Optional fields must match expected types
        if (
            ("length" in column && typeof column.length !== "number") ||
            ("allowNull" in column && typeof column.allowNull !== "boolean") ||
            ("unique" in column && typeof column.unique !== "boolean") ||
            ("index" in column && typeof column.index !== "boolean") ||
            ("pseudounique" in column && typeof column.pseudounique !== "boolean") ||
            ("primary" in column && typeof column.primary !== "boolean") ||
            ("autoIncrement" in column && typeof column.autoIncrement !== "boolean") ||
            ("decimal" in column && typeof column.decimal !== "number") ||
            ("default" in column && column.default === undefined) // `default` can be anything except `undefined`
        ) {
            return false;
        }
    }

    return true; // ✅ Passed all checks
}

export function estimateRowSize(mergedMetaData: MetadataHeader, dbType: supportedDialects): { rowSize: number; exceedsLimit: boolean, nearlyExceedsLimit: boolean } {
    let totalSize = 0;
  
    for (const columnName in mergedMetaData) {
      const column = mergedMetaData[columnName];
      const type = column.type?.toLowerCase() || "varchar";
  
      let columnSize = 0;
  
      if (["boolean", "binary", "tinyint"].includes(type)) {
        columnSize = 1;
      } else if (["smallint"].includes(type)) {
        columnSize = 2;
      } else if (["int", "numeric"].includes(type)) {
        columnSize = 4;
      } else if (["bigint"].includes(type)) {
        columnSize = 8;
      } else if (["decimal", "double", "exponent"].includes(type)) {
        columnSize = column.decimal ? Math.ceil(column.decimal / 2) + 1 : DEFAULT_LENGTHS.decimal;
      } else if (["varchar"].includes(type)) {
        columnSize = column.length ?? DEFAULT_LENGTHS.varchar;
      } else if (["text", "mediumtext", "longtext", "json"].includes(type)) {
        columnSize = DEFAULT_LENGTHS[type as keyof typeof DEFAULT_LENGTHS] ?? 4; // Only store pointer size
      } else if (["date"].includes(type)) {
        columnSize = 3;
      } else if (["time"].includes(type)) {
        columnSize = 3;
      } else if (["datetime", "datetimetz"].includes(type)) {
        columnSize = 8;
      }
  
      if (column.allowNull) {
        columnSize += 1; // Add 1 byte for NULL flag
      }
  
      if (column.primary || column.unique || column.index) {
        columnSize += 8; // Approximate index storage
      }
  
      totalSize += columnSize;
    }
  
    // Add row overhead (~20 bytes for metadata, depends on storage engine)
    const rowOverhead = 20;
    totalSize += rowOverhead;
  
    let maxRowSize
    if(dbType === 'mysql') { maxRowSize = MYSQL_MAX_ROW_SIZE }
    else if (dbType === 'pgsql') { maxRowSize = POSTGRES_MAX_ROW_SIZE }
    else { maxRowSize = POSTGRES_MAX_ROW_SIZE }

    return { rowSize: totalSize, exceedsLimit: totalSize > maxRowSize, nearlyExceedsLimit: totalSize > maxRowSize * 0.8 };
}

export function isValidDataFormat(data: Record<string, any>[] | any): boolean {
    return Array.isArray(data) && data.length > 0 && typeof data[0] === "object" && data[0] !== null && !Array.isArray(data[0]);
}

export const normalizeKeysArray = (data: Record<string, any>[]): Record<string, any>[] => {
    return data.map(obj =>
        Object.keys(obj).reduce((acc, key) => {
            acc[key.toLowerCase()] = obj[key];
            return acc;
        }, {} as Record<string, any>)
    );
};

export function organizeSplitTable(table: string, newMetaData: MetadataHeader, currentMetaData: Record<string, any>[] | MetadataHeader | Record<string, MetadataHeader>, dialectConfig: DialectConfig) : Record<string, MetadataHeader> {
    let normalizedMetaData: Record<string, MetadataHeader>;

    // ✅ Check if currentMetaData is already in structured format
    if (typeof currentMetaData === "object" && !Array.isArray(currentMetaData)) {
        if (Object.values(currentMetaData).some(value => typeof value === "object" && !Array.isArray(value))) {
            // ✅ Already `Record<string, MetadataHeader>`, use it directly
            normalizedMetaData = currentMetaData as Record<string, MetadataHeader>;
        } else {
            // ✅ If it's `MetadataHeader`, wrap it in `{ table: MetadataHeader }`
            normalizedMetaData = { [table]: currentMetaData as MetadataHeader };
        }
    } else {
        // ✅ Otherwise, assume it's raw DB results and parse
        const parsedMetadata = parseDatabaseMetaData(currentMetaData as Record<string, any>[], dialectConfig);
        if (!parsedMetadata) {
            normalizedMetaData = { [table]: {} }; // ✅ Ensure it has a valid structure
        } else if (Object.values(parsedMetadata).some(value => typeof value === "object" && !Array.isArray(value))) {
            normalizedMetaData = parsedMetadata as Record<string, MetadataHeader>; // ✅ Multiple tables
        } else {
            normalizedMetaData = { [table]: parsedMetadata as MetadataHeader }; // ✅ Single table
        }
    }

    const primaryKeys: MetadataHeader = {};
    const newColumns: MetadataHeader = {};
    const allTablesEmpty = Object.values(normalizedMetaData).every(table => Object.keys(table).length === 0);
    const newGroupedByTable = Object.entries(newMetaData).reduce((acc, [columnName, columnDef]) => {
        if (allTablesEmpty) {
            if (columnDef.primary) {
                primaryKeys[columnName] = columnDef;
            } else {
                newColumns[columnName] = columnDef;
            }
            return acc;
        }

        const matchingTables = Object.keys(normalizedMetaData).filter(table =>
            Object.prototype.hasOwnProperty.call(normalizedMetaData[table], columnName)
        );

        if (matchingTables.length > 0) {
            matchingTables.forEach(tableName => {
            if (!acc[tableName]) acc[tableName] = {};
            acc[tableName][columnName] = columnDef;
            });

            if (columnDef.primary) {
            primaryKeys[columnName] = columnDef;
            }
        } else {
            newColumns[columnName] = columnDef;
        }

        return acc;
    }, {} as Record<string, MetadataHeader>);

    let tableName = Object.keys(newGroupedByTable).pop() || getNextTableName(Object.keys(newGroupedByTable).pop() || table);
    const unallocatedColumns = { ...newColumns };

    while (Object.keys(unallocatedColumns).length > 0) {
        // ✅ Check the row size before adding new columns
        for (var i = 0; i < Object.keys(unallocatedColumns).length; i++) {
            const currentTableData = newGroupedByTable[tableName] || { ...primaryKeys };
            const columnName = Object.keys(unallocatedColumns)[i]
            const columnDef = unallocatedColumns[columnName]
            const mergedMetaData = { ...currentTableData, [columnName]: columnDef }; // Simulate adding column
            const columnCount = Object.keys(mergedMetaData).length;
            const exceedsColumnLimit = columnCount >= MAX_COLUMN_COUNT

            const { exceedsLimit, nearlyExceedsLimit } = estimateRowSize(mergedMetaData, dialectConfig.dialect);
            if (!nearlyExceedsLimit && !exceedsColumnLimit) {
                // ✅ Add the column if within limits
                if (!newGroupedByTable[tableName]) {
                    newGroupedByTable[tableName] = { ...primaryKeys }; // Ensure primary keys exist in new table
                }
                newGroupedByTable[tableName][columnName] = columnDef;
                delete unallocatedColumns[columnName]; // ✅ Remove from unallocated list
                i--
            } else {
                tableName = getNextTableName(tableName);
                i--
            }
        }
    }

    return newGroupedByTable
}

export function organizeSplitData(data: Record<string, any>[], splitMetaData: Record<string, MetadataHeader>): Record<string, Record<string, any>[]> {
    const groupedData: Record<string, Record<string, any>[]> = {};
    data.forEach((row) => {
        // ✅ Initialize an object for each table's row data
        const rowDataByTable: Record<string, Record<string, any>> = {};

        Object.entries(splitMetaData).forEach(([tableName, columns]) => {
            rowDataByTable[tableName] = {}; // ✅ Ensure each table has a row initialized

            Object.keys(columns).forEach((columnName) => {
                if (row.hasOwnProperty(columnName)) {
                    rowDataByTable[tableName][columnName] = row[columnName];
                }
            });

            // ✅ Only add to groupedData if it has at least one column
            if (Object.keys(rowDataByTable[tableName]).length > 0) {
                if (!groupedData[tableName]) {
                    groupedData[tableName] = [];
                }
                groupedData[tableName].push(rowDataByTable[tableName]);
            }
        });
    });

    return groupedData;
}

export function splitInsertData(data: Record<string, any>[], config: DatabaseConfig): Record<string, any>[][] {
    const {
      insertStack = 1000
    } = config;

    const chunks: Record<string, any>[][] = [];
    for (let i = 0; i < data.length; i += insertStack) {
      chunks.push(data.slice(i, i + insertStack));
    }
  
    return chunks;
}  

export function getInsertValues(metaData: MetadataHeader, row: Record<string, any>, dialectConfig: DialectConfig, databaseConfig?: DatabaseConfig): any[] {
    return Object.entries(metaData).map(([column, meta]) => {
      let value = row[column];
  
      if (value === null || value === undefined) {
        // Use calculated default if provided
        if (meta.calculatedDefault !== undefined) {
          value = meta.calculatedDefault;
        } else {
          value = null;
        }
      }
  
      return sqlize(value, meta.type, dialectConfig, databaseConfig);
    });
}

export function sqlize(value: any, columnType: string | null, dialectConfig: DialectConfig, databaseConfig?: DatabaseConfig ): any {
    try {
        if (value === null) return null;
        if(!columnType) {return value};

        const type = columnType.toLowerCase();
        const rules: SqlizeRule[] = dialectConfig.sqlize;
        let strValue = typeof value === "string" ? value : String(value);

        const isDateLike = groupings.dateGroup.includes(columnType);
        if (isDateLike) {
            const match = strValue.match(/\/Date\((\d+)(?:[+-]\d+)?\)\//);
            if (match) {
                const millis = parseInt(match[1], 10);
                strValue = new Date(millis).toISOString();
            } else {
                const cleaned = strValue.replace(/[^\d:-\sT]/g, "");
                const parsedDate = new Date(cleaned);
                if (!isNaN(parsedDate.getTime())) {
                strValue = parsedDate.toISOString();
                }
            }
        }

        const isNumberLike = groupings.intGroup.includes(columnType) || groupings.specialIntGroup.includes(columnType);
        if (isNumberLike) {
            const normalised = normalizeNumber(value) || strValue;
            const precision = databaseConfig?.decimalMaxLength ?? defaults.decimalMaxLength;
            strValue = roundStringDecimal(normalised, precision);
        }

        for (const rule of rules) {
            const appliesToType =
            rule.type === true || (Array.isArray(rule.type) && rule.type.includes(type));
      
            if (appliesToType) {
              const regex = new RegExp(rule.regex, "g");
              strValue = strValue.replace(regex, rule.replace);
            }
        }

        return strValue
    } catch (error) {
        return value
    }
}
  
export function getNextTableName(tableName: string): string {
    const match = tableName.match(/^(.*?)(__part_(\d+))?$/); // Match `table__part_001`
    if (match && match[3]) {
        const baseName = match[1]; // Extract "table"
        const num = parseInt(match[3], 10) + 1; // Increment existing number
        return `${baseName}__part_${String(num).padStart(3, "0")}`; // Zero-padded
    }
    return `${tableName}__part_001`; // If no number exists, start at __part_001
};

export async function wait_x_mseconds (x: number) {
    return new Promise (resolve => {
        setTimeout(() => {    
            resolve(null)
        }, x)
    })
}

function roundStringDecimal(valueStr: string, precision: number): string {
    if (!valueStr.includes('.')) return valueStr;
  
    const [intPart, decimalPartRaw] = valueStr.split('.');
    const decimalPart = decimalPartRaw.slice(0, precision);
    const nextDigit = decimalPartRaw.charAt(precision);
  
    if (!nextDigit || parseInt(nextDigit, 10) < 5) {
      // No rounding needed, just trim excess
      return decimalPart.length > 0
        ? `${intPart}.${decimalPart}`
        : intPart;
    }
  
    // Perform manual rounding
    let full = `${intPart}.${decimalPart}`;
    let roundedNum = Number(full);
    const multiplier = Math.pow(10, precision);
    roundedNum = Math.round(roundedNum * multiplier) / multiplier;
  
    return roundedNum.toString();
}

export function generateSafeConstraintName(table: string, column: string, type: 'unique' | 'index' = 'unique'): string {
    const base = `${table}_${column}_${type}`;
    
    if (base.length <= 63) return base;

    // Truncate and append a hash for uniqueness
    const hash = crypto.createHash('md5').update(base).digest('hex').slice(0, 6);
    const truncated = base.slice(0, 63 - hash.length - 1); // -1 for underscore

    return `${truncated}_${hash}`;
}