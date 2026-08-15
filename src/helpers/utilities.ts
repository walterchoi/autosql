import { defaults, DEFAULT_LENGTHS, MYSQL_MAX_ROW_SIZE, POSTGRES_MAX_ROW_SIZE, MAX_COLUMN_COUNT } from "../config/defaults";
import { DatabaseConfig, MetadataHeader, DialectConfig, AlterTableChanges, supportedDialects, SqlizeRule, QueryResult } from "../config/types";
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
            categorical: defaults.categorical,
            autoIndexing: defaults.autoIndexing,
            sampling: defaults.sampling,
            samplingMinimum: defaults.samplingMinimum,
            metaData: config.metaData || {}, // Ensuring headers remain intact
            maxKeyLength: defaults.maxKeyLength,
            maxCompositeKeyColumns: defaults.maxCompositeKeyColumns,
            maxVarcharLength: defaults.maxVarcharLength,
            autoSplit: defaults.autoSplit,
            insertStack: defaults.insertStack,
            insertType: defaults.insertType as "UPDATE" | "INSERT",
            safeMode: defaults.safeMode,
            deleteColumns: defaults.deleteColumns,
            useWorkers: defaults.useWorkers,
            maxWorkers: defaults.maxWorkers,
            workerTaskTimeout: defaults.workerTaskTimeout,
            useStagingInsert: defaults.useStagingInsert,
            addHistory: defaults.addHistory,
            addTimestamps: defaults.addTimestamps,
            decimalToVarchar: defaults.decimalToVarchar,
            addNested: defaults.addNested,
            excludeBlankColumns: defaults.excludeBlankColumns,
            stagingPrefix: defaults.stagingPrefix,
            historyTableSuffix: defaults.historyTableSuffix,
            sanitizeInvalidChars: defaults.sanitizeInvalidChars,
            bulkLoad: defaults.bulkLoad,
            upgradeCharset: defaults.upgradeCharset,
            surrogateKey: defaults.surrogateKey,
            surrogateKeyColumn: defaults.surrogateKeyColumn,
            useSchemaLock: defaults.useSchemaLock,
            schemaLockTimeout: defaults.schemaLockTimeout,
            schemaHistory: defaults.schemaHistory,
            schemaHistoryTable: defaults.schemaHistoryTable,
            strictDriftDetection: defaults.strictDriftDetection,
            detectDrift: defaults.detectDrift,
            streamingStagingPrefix: defaults.streamingStagingPrefix,
            streamMaxRetries: defaults.streamMaxRetries,
            keepOrphanedStagingTables: defaults.keepOrphanedStagingTables,
        };

        // Merge provided config with defaults
        const merged = { ...defaultConfig, ...config };

        // Validate numeric bounds
        if (merged.insertStack !== undefined && merged.insertStack <= 0) {
            throw new Error("insertStack must be greater than 0.");
        }
        if (merged.maxWorkers !== undefined && merged.maxWorkers < 1) {
            throw new Error("maxWorkers must be at least 1.");
        }
        if (merged.workerTaskTimeout !== undefined && merged.workerTaskTimeout < 0) {
            throw new Error("workerTaskTimeout must be 0 (disabled) or a positive number of seconds.");
        }
        if (merged.pseudoUnique !== undefined && (merged.pseudoUnique <= 0 || merged.pseudoUnique > 1)) {
            throw new Error("pseudoUnique must be between 0 (exclusive) and 1 (inclusive).");
        }
        if (merged.categorical !== undefined && (merged.categorical <= 0 || merged.categorical >= 1)) {
            throw new Error("categorical must be between 0 (exclusive) and 1 (exclusive).");
        }
        if (merged.addHistory && !merged.useStagingInsert) {
            throw new Error("addHistory requires useStagingInsert to be enabled.");
        }
        if (merged.schemaLockTimeout !== undefined && merged.schemaLockTimeout <= 0) {
            throw new Error("schemaLockTimeout must be greater than 0.");
        }
        if (merged.streamMaxRetries !== undefined && merged.streamMaxRetries < 1) {
            throw new Error("streamMaxRetries must be at least 1.");
        }
        if ((merged.thousandsSeparator === undefined) !== (merged.decimalSeparator === undefined)) {
            throw new Error("thousandsSeparator and decimalSeparator must be provided together.");
        }
        if (merged.sourceTimeZone !== undefined) {
            // Validate the zone up front (fail loud). A bad zone would otherwise surface per-row inside
            // sqlize's try/catch, which swallows and returns the raw value — a silent skip of the
            // conversion the caller asked for.
            try {
                new Intl.DateTimeFormat("en-US", { timeZone: merged.sourceTimeZone });
            } catch {
                throw new Error(`Invalid sourceTimeZone "${merged.sourceTimeZone}": must be a valid IANA time zone name (e.g. "America/New_York", "Australia/Sydney", "UTC").`);
            }
        }
        if (merged.rejectedRowsTable && merged.sqlDialect === "sqlserver") {
            // The rejected-rows builders emit Postgres-only DDL (BIGSERIAL/JSONB/TIMESTAMPTZ) and `$n`
            // placeholders, so on SQL Server the divert would fail — and rather than let the load run and
            // then fail loud only if rows actually need diverting, refuse it here. SQL Server
            // streaming/degradation parity is deferred (roadmap D-F); fail loud rather than risk a
            // silent drop (A5).
            throw new Error("rejectedRowsTable is not yet supported on SQL Server (the rejected-rows table builder emits Postgres-only DDL/placeholders; SQL Server streaming/degradation parity is deferred — see roadmap D-F). Omit rejectedRowsTable on SQL Server for now.");
        }
        if (merged.schemaHistory && merged.sqlDialect === "sqlserver") {
            // The history-table bootstrap emits Postgres-only DDL (BIGSERIAL/JSONB), so schemaHistory
            // silently no-ops (or errors) on SQL Server today. Fail loud rather than report "no drift"
            // while blind. SQL Server parity is deferred (roadmap D-F) (A19).
            throw new Error("schemaHistory is not yet supported on SQL Server (the history-table bootstrap emits Postgres-only DDL; SQL Server parity is deferred — see roadmap D-F). Disable schemaHistory on SQL Server for now.");
        }
        if (merged.surrogateKey) {
            // A surrogate is unique per physical insert, so these features are incoherent with it:
            // history diffs join on the primary key (nothing to match), nested extraction keys
            // child tables off the parent key, and split tables would each need their own key.
            const incompatible = [
                merged.addHistory && "addHistory",
                merged.addNested && "addNested",
                merged.autoSplit && "autoSplit",
            ].filter(Boolean);
            if (incompatible.length > 0) {
                throw new Error(`surrogateKey is not compatible with: ${incompatible.join(", ")}.`);
            }
        }

        return merged;
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
        const serverType = normalizedRow["data_type"].toLowerCase();
        // MySQL has no native boolean: `tinyint(1)` is the boolean convention, while a plain
        // `tinyint` is a small integer (autosql itself stores 0–255 values as `tinyint`). DATA_TYPE
        // is just "tinyint" for both, so `serverToLocal` mapped every tinyint to boolean — on
        // re-ingest that produced a spurious boolean→int conversion (`SET x = CASE WHEN x THEN 1
        // ELSE 0 END`) that collapsed values to 0/1. Use COLUMN_TYPE (which carries the display
        // width) to map only `tinyint(1)` to boolean; any other tinyint stays an integer.
        const columnType = String(normalizedRow["column_type"] || "").toLowerCase();
        const isNonBooleanTinyint = serverType === "tinyint" && columnType !== "" && columnType !== "tinyint(1)";
        const dataType = isNonBooleanTinyint
            ? "tinyint"
            : (dialectConfig?.translate?.serverToLocal[serverType] || serverType);
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

        // Real DB name of a non-primary unique index the column belongs to (when the query
        // supplies it) — sourced from the same catalog view as getUniqueIndexesQuery so it matches
        // getDropUniqueConstraintQuery. Empty string → treat as absent.
        const uniqueIndexName = normalizedRow["unique_index_name"];

        metadata[tableName][normalizedRow.column_name] = {
            type: dataType,
            length: normalizedLength,
            allowNull: normalizedRow["is_nullable"] === "YES",
            unique: columnKey === "UNIQUE",
            uniqueName: uniqueIndexName ? String(uniqueIndexName) : undefined,
            primary: columnKey === "PRIMARY",
            index: columnKey === "INDEX",
            autoIncrement: autoIncrement,
            decimal: lengthInfo.decimal ?? undefined,
            // Introspected DDL default expression → ddlDefault, NOT default. `default` feeds
            // getInsertValues' missing-value substitution; an introspected expression string
            // (`'active'::character varying`, `CURRENT_TIMESTAMP`) bound as a row value corrupts the
            // stored data (A3). DDL builders that need the expression read ddlDefault instead.
            ddlDefault: normalizedRow["column_default"],
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

export function isMetadataHeader(input: any): input is MetadataHeader {
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
            ("length" in column && column.length != null && typeof column.length !== "number") ||
            ("allowNull" in column && column.allowNull != null && typeof column.allowNull !== "boolean") ||
            ("unique" in column && column.unique != null && typeof column.unique !== "boolean") ||
            ("index" in column && column.index != null && typeof column.index !== "boolean") ||
            ("pseudounique" in column && column.pseudounique != null && typeof column.pseudounique !== "boolean") ||
            ("primary" in column && column.primary != null && typeof column.primary !== "boolean") ||
            ("autoIncrement" in column && column.autoIncrement != null && typeof column.autoIncrement !== "boolean") ||
            ("decimal" in column && column.decimal != null && typeof column.decimal !== "number")
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
      insertStack = 100
    } = config;

    const chunks: Record<string, any>[][] = [];
    for (let i = 0; i < data.length; i += insertStack) {
      chunks.push(data.slice(i, i + insertStack));
    }
  
    return chunks;
}  

/**
 * Remove characters that a SQL text column cannot store, so free-text values with
 * pasted/garbage bytes do not hard-fail the insert. Strips NUL (U+0000) - illegal in
 * Postgres text and a statement-truncation risk elsewhere - and replaces unpaired UTF-16
 * surrogates (a lone high or low surrogate, which cannot encode to valid UTF-8) with the
 * Unicode replacement character U+FFFD. Well-formed text (including emoji, whose surrogates
 * are paired) is returned unchanged. Opt-in via `databaseConfig.sanitizeInvalidChars`.
 */
export function sanitizeString(value: string): string {
    return value
        .replace(/\u0000/g, "")
        .replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "\uFFFD");
}

export function getInsertValues(metaData: MetadataHeader, row: Record<string, any>, dialectConfig?: DialectConfig, databaseConfig?: DatabaseConfig, sqlizeValues: boolean = false): any[] {
    const sanitize = databaseConfig?.sanitizeInvalidChars === true;
    // In surrogate-key mode the auto-increment column is database-generated and carries no data
    // value, so it must be omitted from the value list (Postgres would reject a NULL into a
    // BIGSERIAL NOT NULL column). This is gated on `surrogateKey`: a genuine AUTO_INCREMENT /
    // SERIAL primary key on an ordinary table is introspected as autoIncrement:true too, and
    // callers legitimately supply values for it to upsert — those must NOT be dropped. The insert
    // builders apply the same gate to the column list, keeping columns and params aligned.
    const excludeAutoIncrement = databaseConfig?.surrogateKey === true;
    const newRow = Object.entries(metaData)
      .filter(([, meta]) => !(excludeAutoIncrement && meta.autoIncrement === true))
      .map(([column, meta]) => {
      let value = row[column];

      if (value === null || value === undefined) {
        // Use calculated default if provided
        if (meta.calculatedDefault !== undefined) {
          value = meta.calculatedDefault;
        } else if(meta.default !== undefined) {
            value = meta.default;
        } else {
            value = null;
        }
      }
      let out: any;
      if(sqlizeValues && dialectConfig) {
        out = sqlize(value, meta.type, dialectConfig, databaseConfig);
      } else {
        out = value;
      }
      // Applied after value resolution so it covers both the raw and sqlized paths, and only
      // to strings — the driver still parameter-binds the result, so this is purely about
      // storability, not escaping.
      if (sanitize && typeof out === "string") {
        out = sanitizeString(out);
      }
      return out;
    });
    return newRow
}

// One Intl.DateTimeFormat per zone. Constructing it is ~100× the cost of a formatToParts call, and
// this only runs when `sourceTimeZone` is set (never on the default path). Cached module-level so the
// per-value insert hot path pays construction once per zone, not once per value.
const tzFormatterCache = new Map<string, Intl.DateTimeFormat>();
function tzFormatter(timeZone: string): Intl.DateTimeFormat {
    let f = tzFormatterCache.get(timeZone);
    if (!f) {
        f = new Intl.DateTimeFormat("en-US", {
            timeZone, hourCycle: "h23",
            year: "numeric", month: "2-digit", day: "2-digit",
            hour: "2-digit", minute: "2-digit", second: "2-digit",
        });
        tzFormatterCache.set(timeZone, f);
    }
    return f;
}

// Minutes `timeZone` is ahead of UTC at the given absolute instant. Host-independent: it reads an
// EXPLICIT IANA zone via Intl, never the host's local Date methods (which would reintroduce A1).
function tzOffsetMinutes(utcMs: number, timeZone: string): number {
    const p: Record<string, string> = {};
    for (const part of tzFormatter(timeZone).formatToParts(new Date(utcMs))) p[part.type] = part.value;
    const asIfUtc = Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second);
    return (asIfUtc - utcMs) / 60000;
}

// Interpret Y-Mo-D h:mi:se[.frac] as a WALL-CLOCK in `timeZone` and return the matching UTC instant
// as an ISO string. Two passes so a DST boundary (offset differs either side of the wall time)
// resolves to one deterministic instant. Uses Date.UTC + Intl only (host-independent). Precision on
// this converted path is milliseconds.
function zonedWallClockToUtcIso(Y: string, Mo: string, D: string, h: string, mi: string, se: string, frac: string | undefined, timeZone: string): string {
    const ms = frac ? Math.round(parseFloat(frac) * 1000) : 0;
    const naiveUtc = Date.UTC(+Y, +Mo - 1, +D, +h, +mi, +se, ms);
    const off1 = tzOffsetMinutes(naiveUtc, timeZone);
    let utc = naiveUtc - off1 * 60000;
    const off2 = tzOffsetMinutes(utc, timeZone);
    if (off2 !== off1) utc = naiveUtc - off2 * 60000;
    return new Date(utc).toISOString();
}

/**
 * Normalise a date/time string for `sqlize` into an ISO-ish form the per-dialect sqlize regex rules
 * then reduce to the final literal (`T`→space, strip trailing `Z`).
 *
 * CRITICAL (A1): never route a ZONELESS value through `new Date()`. `new Date("2024-01-15 12:00:00")`
 * parses it in the Node process's LOCAL zone, and `toISOString()` re-emits UTC — silently shifting the
 * wall-clock by the host's UTC offset on any non-UTC machine (a corruption UTC-only CI cannot see).
 * Only a zone-qualified value (`Z` or `±HH:MM`) denotes an absolute instant and may be converted to
 * UTC. `date`/`time` columns keep the wall-clock portion regardless of any zone — converting a zoned
 * value to UTC can shift the stored day/time (`2024-01-15T02:00:00+05:00` into a `date` → 2024-01-14).
 */
function normalizeDateValue(strValue: string, columnType: string, sourceTimeZone?: string): string {
    const s = strValue.trim();
    const dateOnly = columnType === "date";
    const timeOnly = columnType === "time";

    // ASP.NET "/Date(<epoch-ms>[±offset])/" — epoch ms is an absolute UTC instant.
    const aspNet = s.match(/\/Date\((\d+)(?:[+-]\d+)?\)\//);
    if (aspNet) {
        const iso = new Date(parseInt(aspNet[1], 10)).toISOString(); // 2024-01-15T12:00:00.000Z
        if (dateOnly) return iso.slice(0, 10);
        if (timeOnly) return iso.slice(11, 19);
        return iso;
    }

    // Zone-qualified datetime → absolute instant, safe to convert to UTC (the zone is explicit, no
    // local guessing). Never for date/time columns — see the day-shift note above.
    const hasZone = /\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?\s*(?:Z|[+-]\d{2}:?\d{2})$/.test(s);
    if (hasZone && !dateOnly && !timeOnly) {
        const d = new Date(s);
        if (!isNaN(d.getTime())) return d.toISOString();
        // Unparseable despite a zone marker — fall through to textual handling.
    }

    // Otherwise keep the WALL-CLOCK exactly as written: parse the components textually, never via Date.
    const dt = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?)?/);
    if (dt) {
        const [, Y, Mo, D, h, mi, se, frac] = dt;
        if (timeOnly) return h !== undefined ? `${h}:${mi}:${se ?? "00"}${frac ?? ""}` : s;
        if (dateOnly || h === undefined) return `${Y}-${Mo}-${D}`;
        // datetime/timestamp/datetimetz, zoneless. If the caller declared the source zone, interpret
        // this wall-clock in THAT zone and convert to a UTC instant; otherwise preserve the wall-clock
        // verbatim (no timezone assumed). Neither path involves the host zone.
        if (sourceTimeZone) {
            return zonedWallClockToUtcIso(Y, Mo, D, h, mi, se ?? "00", frac, sourceTimeZone);
        }
        // Canonical ISO (with Z) from the SAME digits; the dialect rules reduce it to
        // 'YYYY-MM-DD HH:MM:SS' with the wall-clock unchanged.
        return `${Y}-${Mo}-${D}T${h}:${mi}:${se ?? "00"}${frac ?? ""}Z`;
    }

    // A bare time value "HH:MM[:SS]" for a time column.
    if (timeOnly) {
        const tm = s.match(/^(\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?/);
        if (tm) return `${tm[1]}:${tm[2]}:${tm[3] ?? "00"}${tm[4] ?? ""}`;
    }

    // Unrecognised shape — do NOT risk a local-zone shift; hand the original to the driver/DB.
    return s;
}

export function sqlize(value: any, columnType: string | null, dialectConfig: DialectConfig, databaseConfig?: DatabaseConfig ): any {
    try {
        if (value === null) return null;
        if(!columnType) {return value};

        const type = columnType.toLowerCase();
        const rules: SqlizeRule[] = dialectConfig.sqlize;
        if (type === "json") {
            try {
                if (typeof value === "string") {
                    try {
                        // Try parsing it first (in case it's a JSON string)
                        const parsed = JSON.parse(value);
                        return JSON.stringify(parsed); // ✅ Store re-stringified version
                    } catch {
                        // ❌ Failed to parse: just return original string
                        return value;
                    }
                } else if (typeof value === "object") {
                    // ✅ Valid object → stringify
                    return JSON.stringify(value);
                } else {
                    // ⚠️ Unexpected type (number, boolean, etc.)
                    return JSON.stringify({ value });
                }
            } catch (err: any) {
                databaseConfig?.logger?.warn?.(`[sqlize] Failed to handle JSON value for column: ${JSON.stringify({ value, error: err.message || String(err) })}`);
                return null; // ❌ Fallback to NULL if completely unusable
            }
        }
        
        let strValue = typeof value === "string" ? value : String(value);

        // Boolean columns store a canonical 0/1. Without this, a string flag ("true"/"false",
        // as CSV and most text sources deliver them) reaches the driver unchanged: MySQL rejects
        // it against a TINYINT(1) (`Incorrect integer value: 'true'`) and the raw distinct strings
        // ("true"/"TRUE"/1) also inflate the sampled cardinality into a spurious UNIQUE. Normalise
        // the boolean domain to "1"/"0" — both dialects accept it (MySQL tinyint, PG boolean input).
        if (columnType === "boolean") {
            const b = strValue.trim().toLowerCase();
            // Absence stays null (matches the inference layer, which treats ""/"null" as nullish) —
            // a missing flag must not silently become false.
            if (b === "" || b === "null") return null;
            if (b === "1" || b === "true" || b === "t" || b === "yes") return "1";
            if (b === "0" || b === "false" || b === "f" || b === "no") return "0";
            // Out of domain: leave unchanged so the DB surfaces it rather than silently coercing.
            return strValue;
        }

        const isDateLike = groupings.dateGroup.includes(columnType);
        if (isDateLike) {
            strValue = normalizeDateValue(strValue, columnType, databaseConfig?.sourceTimeZone);
        }

        const isNumberLike = groupings.intGroup.includes(columnType) || groupings.specialIntGroup.includes(columnType);
        if (isNumberLike) {
            const normalised = normalizeNumber(value, databaseConfig?.thousandsSeparator, databaseConfig?.decimalSeparator) || strValue;
            // Round the value to the same scale the column is sized to: the configured cap, else the
            // dialect's numeric limit (so we don't silently round to an arbitrary low default).
            const precision = databaseConfig?.decimalMaxLength ?? dialectConfig.maxDecimalScale;
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

        if(strValue === '' || strValue === 'null') {
            return null
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

export function getTempTableName(tableName: string, stagingPrefix = "temp_staging__"): string {
    return tableName.startsWith(stagingPrefix) ? tableName : `${stagingPrefix}${tableName}`;
}

export function getTrueTableName(tableName: string, stagingPrefix = "temp_staging__", historyTableSuffix = "__history"): string {
    let result = tableName;
    if (result.startsWith(stagingPrefix)) {
        result = result.slice(stagingPrefix.length);
    }
    if (result.endsWith(historyTableSuffix)) {
        result = result.slice(0, -historyTableSuffix.length);
    }
    return result;
}

export function getHistoryTableName(tableName: string, historyTableSuffix = "__history"): string {
    return tableName.endsWith(historyTableSuffix) ? tableName : `${tableName}${historyTableSuffix}`;
}

export async function wait_x_mseconds (x: number) {
    return new Promise (resolve => {
        setTimeout(() => {    
            resolve(null)
        }, x)
    })
}

// Round a numeric string to `precision` decimal places, half-up (away from zero), using digit-string
// arithmetic only — never float (A4). The previous float path (`Math.round(Number(...))`) had two
// defects: it fed digits already sliced to `precision` back through Math.round, so the carry digit was
// gone and it ALWAYS truncated downward (2.675 → 2.67, currency bias); and above ~15 significant
// digits Number()/Math.pow lose precision. String carry is exact at any magnitude, so the former
// `precision > 15` truncation compromise (D-G) is no longer needed. Round-half-away-from-zero matches
// MySQL/Postgres numeric rounding.
function roundStringDecimal(valueStr: string, precision: number): string {
    if (!valueStr.includes('.')) return valueStr;

    let sign = "";
    let body = valueStr;
    if (body.startsWith('-')) { sign = "-"; body = body.slice(1); }
    else if (body.startsWith('+')) { body = body.slice(1); }

    const [intPart, decimalPartRaw = ""] = body.split('.');

    // Already within the cap — nothing to trim or round.
    if (decimalPartRaw.length <= precision) return valueStr;

    const kept = decimalPartRaw.slice(0, precision);
    const roundUp = decimalPartRaw.charCodeAt(precision) - 48 >= 5;

    if (!roundUp) {
        return sign + (precision > 0 && kept.length > 0 ? `${intPart}.${kept}` : intPart);
    }

    // Half-up: add 1 in the last kept place by incrementing the concatenated integer+kept digits, then
    // re-split. A carry can grow the integer part (999 → 1000) or ripple across the decimal point.
    const incremented = incrementDigits(intPart + kept);
    if (precision === 0) return sign + incremented;
    const splitAt = incremented.length - precision;
    return sign + `${incremented.slice(0, splitAt) || "0"}.${incremented.slice(splitAt)}`;
}

// Add 1 to a non-negative integer represented as a digit string (no float, arbitrary length).
function incrementDigits(digits: string): string {
    const arr = (digits || "0").split('');
    let i = arr.length - 1;
    while (i >= 0) {
        if (arr[i] === '9') { arr[i] = '0'; i--; }
        else { arr[i] = String(Number(arr[i]) + 1); break; }
    }
    if (i < 0) arr.unshift('1');
    return arr.join('');
}

export function generateSafeConstraintName(table: string, column: string, type: 'unique' | 'index' = 'unique'): string {
    const base = `${table}_${column}_${type}`;

    // Keep the generated name within the TIGHTEST dialect identifier limit (Postgres, 63 BYTES) so its
    // output always passes escapeIdentifier for every dialect. Measure AND truncate by UTF-8 bytes: a
    // character count can return a name ≤63 chars but >63 bytes for a multibyte (é / CJK) source
    // column, which escapeIdentifier then rejects (the very name this helper produced to stay legal).
    if (Buffer.byteLength(base, "utf8") <= 63) return base;

    // Truncate on whole-character boundaries until the byte budget, then append a hash for uniqueness.
    const hash = crypto.createHash('md5').update(base).digest('hex').slice(0, 6);
    const budget = 63 - hash.length - 1; // room for the "_" separator + hash
    let truncated = "";
    let bytes = 0;
    for (const ch of base) {
        const chBytes = Buffer.byteLength(ch, "utf8");
        if (bytes + chBytes > budget) break;
        truncated += ch;
        bytes += chBytes;
    }

    return `${truncated}_${hash}`;
}

export function normalizeResultKeys<T extends Record<string, any>>(row: T): Record<string, any> {
    return Object.fromEntries(
        Object.entries(row).map(([key, value]) => [key.toLowerCase(), value])
    );
}

export function throwIfFailedResults(results: QueryResult[], action = "operation") {
    const failed = results.filter(r => !r.success);

    if (failed.length > 0) {
      const message = `One or more ${action} failed (${failed.length}):\n` +
        failed
          .map(r => `- ${r.table || "Unknown Table"}: ${r.error || "Unknown Error"}`)
          .join("\n");

      // Carry the first failed query's driver error code on the thrown Error so a top-level
      // catch (autoSQL / autoSQLChunked / stream end) can surface it as QueryResult.errorCode.
      const err = new Error(message) as Error & { code?: string };
      const withCode = failed.find(r => r.errorCode);
      if (withCode?.errorCode) err.code = withCode.errorCode;
      throw err;
    }
}

export function normalizeName(name: string): string {
    return name.toLowerCase().replace(/[^a-z0-9]/g, "");
}
  