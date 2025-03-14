import { defaults } from "../config/defaults";
import { DatabaseConfig, ColumnDefinition, DialectConfig } from "../config/types";
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
            minimumUnique: defaults.minimumUnique,
            maximumUniqueLength: defaults.maximumUniqueLength,
            maxNonTextLength: defaults.maxNonTextLength,
            pseudoUnique: defaults.pseudoUnique,
            autoIndexing: defaults.autoIndexing,
            sampling: defaults.sampling,
            samplingMinimum: defaults.samplingMinimum,
            metaData: config.metaData || {}, // Ensuring headers remain intact
            maxKeyLength: defaults.maxKeyLength
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

export function parseDatabaseMetaData(rows: any[], dialectConfig?: DialectConfig): Record<string, ColumnDefinition> | null {
    const metadata: Record<string, ColumnDefinition> = {};

    rows.forEach((row: any) => {
        // Normalize column keys to lowercase
        const normalizedRow = Object.keys(row).reduce((acc, key) => {
            acc[key.toLowerCase()] = row[key];
            return acc;
        }, {} as Record<string, any>);
        // Extract normalized values
        const lengthInfo = parseDatabaseLength(String(normalizedRow["length"]));
        const dataType = dialectConfig?.translate?.serverToLocal[normalizedRow.data_type.toLowerCase()] || normalizedRow["data_type"].toLowerCase();
        const columnKey = (normalizedRow["column_key"] || "").toUpperCase();
        
        let normalizedLength: number | undefined = lengthInfo.length;
        if (dialectConfig?.noLength.includes(dataType)) {
            normalizedLength = undefined; // ✅ Force `undefined` for `TEXT`, `JSON`, `BOOLEAN`, etc.
        } else if (dialectConfig?.optionalLength.includes(dataType) && lengthInfo.length === undefined) {
            normalizedLength = undefined; // ✅ Ensure `undefined` if the DB does not store it
        }

        const autoIncrement = String(normalizedRow["extra"] || "").includes("auto_increment") || String(normalizedRow["column_default"] || "").includes("nextval");

        metadata[normalizedRow["column_name"]] = {
            type: dataType,
            length: normalizedLength,
            allowNull: normalizedRow["is_nullable"] === "YES",
            unique: columnKey === "UNIQUE",
            primary: columnKey === "PRIMARY",
            index: columnKey === "INDEX",
            autoIncrement: autoIncrement,
            decimal: lengthInfo.decimal ?? undefined
        };
    });

    return Object.keys(metadata).length > 0 ? metadata : null;
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