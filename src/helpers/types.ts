import { regexPatterns } from "./regex";
import { groupings } from "./groupings";
import { normalizeNumber } from "./utilities";

// Using regex, when provided a data point, predict what data type this will be
export async function predictType(data: any): Promise<string | null> {
    try {
        let currentType: string | null = null;
        let strData : string | null = null;
        strData = typeof data === "string" ? data : String(data);

        // ✅ Detect and normalize numbers
        if (regexPatterns.number.test(strData) || regexPatterns.decimal.test(strData)) {
            strData = normalizeNumber(strData);
            if (!strData) return "varchar"; // Invalid format
        }

        if (regexPatterns.boolean.test(strData)) {
            currentType = "boolean";
        } else if (regexPatterns.binary.test(strData)) {
            currentType = "binary";
        } else if (regexPatterns.number.test(strData)) {
            currentType = "int";
        } else if (regexPatterns.decimal.test(strData)) {
            currentType = "decimal";
        } else if (regexPatterns.exponential.test(strData)) {
            currentType = "exponential";
        } else if (regexPatterns.datetimetz.test(strData)) {
            currentType = "datetimetz";
        } else if (regexPatterns.datetime.test(strData)) {
            currentType = "datetime";
        } else if (regexPatterns.date.test(strData)) {
            currentType = "date";
        } else if (regexPatterns.time.test(strData)) {
            currentType = "time";
        } else if (regexPatterns.json.test(strData)) {
            currentType = "json";
        } else {
            currentType = "varchar";
        }

        // Handle integer type differentiation
        if (currentType === "int") {
            const numValue = Number(strData);

            // Check if the number is within JavaScript's safe integer range
            if (!isNaN(numValue) && Number.isSafeInteger(numValue)) {
                if (numValue <= 127 && numValue >= -128) {
                    currentType = "tinyint";
                } else if (numValue <= 32767 && numValue >= -32768) {
                    currentType = "smallint";
                } else if (numValue <= 2147483647 && numValue >= -2147483648) {
                    currentType = "int";
                } else if (numValue <= Number.MAX_SAFE_INTEGER && numValue >= -Number.MAX_SAFE_INTEGER) {
                    currentType = "bigint";
                }
            } else {
                // Use BigInt for checking larger values safely
                try {
                    const bigIntValue = BigInt(strData);
                    if (bigIntValue <= 9223372036854775807n && bigIntValue >= -9223372036854775808n) {
                        currentType = "bigint";
                    } else {
                        currentType = "varchar";
                    }
                } catch {
                    currentType = "varchar"; // If conversion fails, it's not a valid number
                }
            }
        }

        // Handle text-based types
        if (currentType === "json" || currentType === "varchar") {
            const length = strData.length;
            if (length < 6553) {
                return currentType;
            } else if (length < 65535) {
                return "text";
            } else if (length < 16777215) {
                return "mediumtext";
            } else if (length < 4294967295) {
                return "longtext";
            } else {
                throw new Error("data_too_long: Data is too long for longtext field");
            }
        }

        // Handle invalid date cases
        if (["datetime", "date", "time"].includes(currentType) && strData === "Invalid Date") {
            return null;
        }

        return currentType;
    } catch (error) {
        throw new Error(`Error in predictType: ${error}`);
    }
}

export async function collateTypes(currentType: string | null, overallType: string | null): Promise<string> {
    try {
        if (!currentType && !overallType) {
            throw new Error("No data types provided for collation");
        }

        if (!overallType) return currentType!;
        if (!currentType) return overallType;

        // If types are the same, return one of them
        if (currentType === overallType) return currentType;

        let currentTypeGroup: string | null = null;
        let overallTypeGroup: string | null = null;

        // Identify the type grouping
        if (groupings.intGroup.includes(currentType)) currentTypeGroup = "int";
        else if (groupings.specialIntGroup.includes(currentType)) currentTypeGroup = "specialInt";
        else if (groupings.textGroup.includes(currentType)) currentTypeGroup = "text";
        else if (groupings.specialTextGroup.includes(currentType)) currentTypeGroup = "specialText";
        else if (groupings.dateGroup.includes(currentType)) currentTypeGroup = "date";

        if (groupings.intGroup.includes(overallType)) overallTypeGroup = "int";
        else if (groupings.specialIntGroup.includes(overallType)) overallTypeGroup = "specialInt";
        else if (groupings.textGroup.includes(overallType)) overallTypeGroup = "text";
        else if (groupings.specialTextGroup.includes(overallType)) overallTypeGroup = "specialText";
        else if (groupings.dateGroup.includes(overallType)) overallTypeGroup = "date";

        let collatedType: string | null = null;

        // Handle different groupings
        if (currentTypeGroup !== overallTypeGroup) {
            if ((currentType === "exponent" && overallTypeGroup === "int") || (overallType === "exponent" && currentTypeGroup === "int")) {
                collatedType = "exponent";
            } else if ((currentType === "double" && overallTypeGroup === "int") || (overallType === "double" && currentTypeGroup === "int")) {
                collatedType = "double";
            } else if ((currentType === "decimal" && overallTypeGroup === "int") || (overallType === "decimal" && currentTypeGroup === "int")) {
                collatedType = "decimal";
            } else if (overallTypeGroup === "text" || currentTypeGroup === "text") {
                for (let i = groupings.textGroup.length - 1; i >= 0; i--) {
                    if (groupings.textGroup[i] === currentType || groupings.textGroup[i] === overallType) {
                        collatedType = groupings.textGroup[i];
                        break;
                    }
                }
            } else if (["specialText", "date"].includes(overallTypeGroup!) || ["specialText", "date"].includes(currentTypeGroup!)) {
                collatedType = "varchar";
            }

            return collatedType || "varchar";
        }

        // Handle similar groupings
        if (overallTypeGroup === currentTypeGroup) {
            if (overallTypeGroup === "specialInt") {
                for (let i = groupings.specialIntGroup.length - 1; i >= 0; i--) {
                    if (groupings.specialIntGroup[i] === currentType || groupings.specialIntGroup[i] === overallType) {
                        return groupings.specialIntGroup[i];
                    }
                }
            } else if (overallTypeGroup === "int") {
                for (let i = groupings.intGroup.length - 1; i >= 0; i--) {
                    if (groupings.intGroup[i] === currentType || groupings.intGroup[i] === overallType) {
                        return groupings.intGroup[i];
                    }
                }
            } else if (overallTypeGroup === "text") {
                for (let i = groupings.textGroup.length - 1; i >= 0; i--) {
                    if (groupings.textGroup[i] === currentType || groupings.textGroup[i] === overallType) {
                        return groupings.textGroup[i];
                    }
                }
            } else if (overallTypeGroup === "date") {
                return "datetime"; // Dates and times can be stored as datetime
            }
        }

        throw new Error(`Unknown data type collation for: ${overallType}, ${currentType}`);
    } catch (error) {
        throw new Error(`Error in collateTypes: ${error}`);
    }
}

export async function updateColumnType(column: any, dataPoint: string) {
    const detectedType = await predictType(dataPoint);
    if (!detectedType) {
        column.allowNull = true;
        return;
    }

    if (detectedType !== column.type) {
        column.type = await collateTypes(detectedType, column.type);
    }
}