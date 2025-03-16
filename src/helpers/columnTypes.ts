import { regexPatterns } from "../config/regex";
import { groupings } from "../config/groupings";
import { normalizeNumber, setToArray } from "./utilities";

// Using regex, when provided a data point, predict what data type this will be
export function predictType(data: any): string | null {
    try {
        if(data === undefined || data === null) {
            return null
        }
        let currentType: string | null = null;
        let strData : string | null = null;
        let json : boolean = false;
        if (typeof data === "object" && data !== null) {
            strData = JSON.stringify(data);
        } else if (typeof data === "string") {
            strData = data
        } else {
            strData = String(data); // For non-objects, just convert to string
        }
        try {
            JSON.parse(strData);
            json = true
        } catch (e) {}

        // ✅ Detect and normalize numbers
        if (regexPatterns.number.test(strData) || regexPatterns.decimal.test(strData)) {
            strData = normalizeNumber(strData);
            if (!strData) {
                console.log('!strData - varchar')
                return "varchar"; // Invalid format
            }
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
        } else if (json) {
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

export function collateTypes(typeSetOrArray: Set<string | null> | (string | null)[]): string {
    try {
        if (!typeSetOrArray || (typeSetOrArray instanceof Set ? typeSetOrArray.size === 0 : typeSetOrArray.length === 0)) {
            throw new Error("No data types provided for collation");
        }
        let types: string[]
        // Convert set to array and filter out nulls
        if (typeSetOrArray instanceof Set) {
            types = setToArray(typeSetOrArray).filter((t): t is string => t !== null);
        } else {
            types = typeSetOrArray.filter((t): t is string => t !== null);
        }

        if (types.length === 0) {
            return "varchar"; // Default fallback if all inputs were null
        }

        // If there's only one unique type, return it
        const uniqueTypes = [...new Set(types)];
        if (uniqueTypes.length === 1) {
            return uniqueTypes[0]!;
        }

        let overallType: string | null = null;

        for (const currentType of uniqueTypes) {
            if (!overallType) {
                overallType = currentType;
                continue;
            }

            if (currentType === overallType) {
                continue;
            }

            let currentTypeGroup: string | null = null;
            let overallTypeGroup: string | null = null;

            // Identify the type grouping
            if (currentType) {
                if (groupings.intGroup.includes(currentType)) currentTypeGroup = "int";
                else if (groupings.specialIntGroup.includes(currentType)) currentTypeGroup = "specialInt";
                else if (groupings.textGroup.includes(currentType)) currentTypeGroup = "text";
                else if (groupings.specialTextGroup.includes(currentType)) currentTypeGroup = "specialText";
                else if (groupings.dateGroup.includes(currentType)) currentTypeGroup = "date";
            }

            if (overallType) {
                if (groupings.intGroup.includes(overallType)) overallTypeGroup = "int";
                else if (groupings.specialIntGroup.includes(overallType)) overallTypeGroup = "specialInt";
                else if (groupings.textGroup.includes(overallType)) overallTypeGroup = "text";
                else if (groupings.specialTextGroup.includes(overallType)) overallTypeGroup = "specialText";
                else if (groupings.dateGroup.includes(overallType)) overallTypeGroup = "date";
            }

            let collatedType: string | null = null;

            // ✅ Handle boolean + binary → binary
            if ((currentType === "boolean" && overallType === "binary") || (currentType === "binary" && overallType === "boolean")) {
                overallType = "binary";
                continue;
            }

            // ✅ Handle decimal + exponential → exponential
            if ((currentType === "decimal" && overallType === "exponential") || (overallType === "decimal" && currentType === "exponential")) {
                overallType = "exponential";
                continue;
            }

            // ✅ Handle datetimetz + datetime → datetimetz
            if ((currentType === "datetimetz" && overallType === "datetime") || (overallType === "datetimetz" && currentType === "datetime")) {
                overallType = "datetimetz";
                continue;
            }

            // Handle different groupings
            if (currentTypeGroup !== overallTypeGroup) {
                if ((currentType === "exponential" && overallTypeGroup === "int") || (overallType === "exponential" && currentTypeGroup === "int")) {
                    collatedType = "exponential";
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

                overallType = collatedType || "varchar";
                continue;
            }

            // Handle similar groupings
            if (overallTypeGroup === currentTypeGroup) {
                if (overallTypeGroup === "specialInt") {
                    for (let i = groupings.specialIntGroup.length - 1; i >= 0; i--) {
                        if (groupings.specialIntGroup[i] === currentType || groupings.specialIntGroup[i] === overallType) {
                            overallType = groupings.specialIntGroup[i];
                            break;
                        }
                    }
                } else if (overallTypeGroup === "int") {
                    for (let i = groupings.intGroup.length - 1; i >= 0; i--) {
                        if (groupings.intGroup[i] === currentType || groupings.intGroup[i] === overallType) {
                            overallType = groupings.intGroup[i];
                            break;
                        }
                    }
                } else if (overallTypeGroup === "text") {
                    for (let i = groupings.textGroup.length - 1; i >= 0; i--) {
                        if (groupings.textGroup[i] === currentType || groupings.textGroup[i] === overallType) {
                            overallType = groupings.textGroup[i];
                            break;
                        }
                    }
                } else if (overallTypeGroup === "date") {
                    overallType = "datetime"; // Dates and times should be stored as datetime
                }
            }
        }

        return overallType || "varchar";
    } catch (error) {
        throw error
    }
}

export async function updateColumnType(column: any, dataPoint: string) {
    const detectedType = predictType(dataPoint);
    if (!detectedType) {
        column.allowNull = true;
        return;
    }

    if (detectedType !== column.type) {
        column.type = collateTypes([detectedType, column.type]);
    }
}