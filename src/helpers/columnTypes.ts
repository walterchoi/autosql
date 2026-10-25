import { REGEX_PATTERNS } from "../config/regex";
import { GROUPINGS } from "../config/groupings";
import { normalizeNumber, setToArray } from "./utilities";

// Using regex, when provided a data point, predict what data type this will be.
// thousandsSeparator/decimalSeparator disambiguate locale-specific number formats (e.g.
// whether "1.000" is 1 or 1000) — see normalizeNumber. Omit both to use the auto-heuristic.
export function predictType(data: any, thousandsSeparator?: string, decimalSeparator?: string, onAmbiguousSeparator?: () => void): string | null {
    try {
        if(data === undefined || data === null) {
            return null
        }
        let currentType: string | null = null;
        let strData : string | null = null;
        let json : boolean = false;

        // Fast path for native JS scalars: a boolean/finite-number is unambiguous, so skip the
        // stringify + regex chain + normalizeNumber the string path runs for every value — the
        // dominant per-value inference cost. Non-finite numbers, Dates, objects and strings still
        // take the full string path.
        if (typeof data === "boolean") {
            return "boolean";
        }
        if (typeof data === "number" && Number.isFinite(data)) {
            strData = String(data);
            // A huge integer can stringify as "1e+21" — check exponential form first.
            if (/[eE]/.test(strData)) {
                currentType = "exponent";
            } else if (Number.isInteger(data)) {
                currentType = "int";
            } else {
                currentType = "decimal";
            }
        } else {
        if (typeof data === "object" && data !== null) {
            strData = JSON.stringify(data);
        } else if (typeof data === "string") {
            strData = data
        } else {
            strData = String(data);
        }
        // Gate the JSON.parse attempt on the first char (`[`/`{`/`"`) instead of always trying it:
        // behaviour-identical since only those shapes can parse, but skips the try/catch exception
        // throw (slow in V8) that a non-JSON string triggered on every value otherwise.
        if (/^\s*[[{"]/.test(strData)) {
            try {
                JSON.parse(strData);
                json = true
            } catch {}
        }

        // Fidelity: a digit string with a leading zero ("007", "07030", phone numbers) is an
        // identifier, not a number — coercing to int would drop the leading zeros. Keep as text.
        if (/^-?0[0-9]+$/.test(strData)) {
            return "varchar";
        }

        if (REGEX_PATTERNS.number.test(strData) || REGEX_PATTERNS.decimal.test(strData)) {
            strData = normalizeNumber(strData, thousandsSeparator, decimalSeparator, onAmbiguousSeparator);

            if (!strData) {
                return "varchar";
            }
        }

        if (REGEX_PATTERNS.boolean.test(strData)) {
            currentType = "boolean";
        } else if (REGEX_PATTERNS.number.test(strData)) {
            currentType = "int";
        } else if (REGEX_PATTERNS.decimal.test(strData)) {
            currentType = "decimal";
        } else if (REGEX_PATTERNS.exponential.test(strData)) {
            currentType = "exponent";
        } else if (REGEX_PATTERNS.datetimetz.test(strData)) {
            currentType = "datetimetz";
        } else if (REGEX_PATTERNS.datetime.test(strData)) {
            currentType = "datetime";
        } else if (REGEX_PATTERNS.date.test(strData)) {
            currentType = "date";
        } else if (REGEX_PATTERNS.time.test(strData)) {
            currentType = "time";
        } else if (json) {
            currentType = "json";
        } else {
            currentType = "varchar";
        }
        } // end string path (native number/boolean took the fast path above)

        if (currentType === "int") {
            const numValue = Number(strData);

            if(strData.split('.').length > 1) {
                currentType = "decimal"
            } else if (!isNaN(numValue) && Number.isSafeInteger(numValue)) {
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
                try {
                    const bigIntValue = BigInt(strData);
                    if (bigIntValue <= 9223372036854775807n && bigIntValue >= -9223372036854775808n) {
                        currentType = "bigint";
                    } else {
                        currentType = "varchar";
                    }
                } catch {
                    currentType = "varchar";
                }
            }
        }

        if (currentType === "json") {
            if (strData.length > 4294967295) {
                throw new Error("data_too_long: Data is too long for JSON field");
            }
            return "json";
        }
        if (currentType === "varchar") {
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
            return 'varchar'
        }
        let types: string[]
        if (typeSetOrArray instanceof Set) {
            types = setToArray(typeSetOrArray).filter((t): t is string => t !== null);
        } else {
            types = typeSetOrArray.filter((t): t is string => t !== null);
        }

        if (types.length === 0) {
            return "varchar";
        }

        const uniqueTypes = [...new Set(types)];
        if (uniqueTypes.length === 1) {
            return uniqueTypes[0];
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

            if (currentType) {
                if (GROUPINGS.intGroup.includes(currentType)) currentTypeGroup = "int";
                else if (GROUPINGS.specialIntGroup.includes(currentType)) currentTypeGroup = "specialInt";
                else if (GROUPINGS.textGroup.includes(currentType)) currentTypeGroup = "text";
                else if (GROUPINGS.specialTextGroup.includes(currentType)) currentTypeGroup = "specialText";
                else if (GROUPINGS.dateGroup.includes(currentType)) currentTypeGroup = "date";
            }

            if (overallType) {
                if (GROUPINGS.intGroup.includes(overallType)) overallTypeGroup = "int";
                else if (GROUPINGS.specialIntGroup.includes(overallType)) overallTypeGroup = "specialInt";
                else if (GROUPINGS.textGroup.includes(overallType)) overallTypeGroup = "text";
                else if (GROUPINGS.specialTextGroup.includes(overallType)) overallTypeGroup = "specialText";
                else if (GROUPINGS.dateGroup.includes(overallType)) overallTypeGroup = "date";
            }

            let collatedType: string | null = null;

            // boolean + binary → binary
            if ((currentType === "boolean" && overallType === "binary") || (currentType === "binary" && overallType === "boolean")) {
                overallType = "binary";
                continue;
            }

            // decimal + exponent → exponent
            if ((currentType === "decimal" && overallType === "exponent") || (overallType === "decimal" && currentType === "exponent")) {
                overallType = "exponent";
                continue;
            }

            // datetimetz + datetime → datetimetz
            if ((currentType === "datetimetz" && overallType === "datetime") || (overallType === "datetimetz" && currentType === "datetime")) {
                overallType = "datetimetz";
                continue;
            }

            if (currentTypeGroup !== overallTypeGroup) {
                if ((currentType === "exponent" && overallTypeGroup === "int") || (overallType === "exponent" && currentTypeGroup === "int")) {
                    collatedType = "exponent";
                } else if ((currentType === "double" && overallTypeGroup === "int") || (overallType === "double" && currentTypeGroup === "int")) {
                    collatedType = "double";
                } else if ((currentType === "decimal" && overallTypeGroup === "int") || (overallType === "decimal" && currentTypeGroup === "int")) {
                    collatedType = "decimal";
                } else if (overallTypeGroup === "text" || currentTypeGroup === "text") {
                    for (let i = GROUPINGS.textGroup.length - 1; i >= 0; i--) {
                        if (GROUPINGS.textGroup[i] === currentType || GROUPINGS.textGroup[i] === overallType) {
                            collatedType = GROUPINGS.textGroup[i];
                            break;
                        }
                    }
                } else if (["specialText", "date"].includes(overallTypeGroup!) || ["specialText", "date"].includes(currentTypeGroup!)) {
                    collatedType = "varchar";
                }

                overallType = collatedType || "varchar";
                continue;
            }

            if (overallTypeGroup === currentTypeGroup) {
                if (overallTypeGroup === "specialInt") {
                    // Widen toward the later entry (decimal < double < exponent). decimal+double ->
                    // double is deliberate: "double" only enters via a pre-existing DOUBLE column,
                    // and narrowing back to decimal would be a lossy/erroring ALTER.
                    for (let i = GROUPINGS.specialIntGroup.length - 1; i >= 0; i--) {
                        if (GROUPINGS.specialIntGroup[i] === currentType || GROUPINGS.specialIntGroup[i] === overallType) {
                            overallType = GROUPINGS.specialIntGroup[i];
                            break;
                        }
                    }
                } else if (overallTypeGroup === "int") {
                    for (let i = GROUPINGS.intGroup.length - 1; i >= 0; i--) {
                        if (GROUPINGS.intGroup[i] === currentType || GROUPINGS.intGroup[i] === overallType) {
                            overallType = GROUPINGS.intGroup[i];
                            break;
                        }
                    }
                } else if (overallTypeGroup === "text") {
                    for (let i = GROUPINGS.textGroup.length - 1; i >= 0; i--) {
                        if (GROUPINGS.textGroup[i] === currentType || GROUPINGS.textGroup[i] === overallType) {
                            overallType = GROUPINGS.textGroup[i];
                            break;
                        }
                    }
                } else if (overallTypeGroup === "date") {
                    // Dates and times collapse to datetime — but preserve timezone-awareness:
                    // any tz-aware member (e.g. date + datetimetz) widens the mix to datetimetz.
                    overallType = (currentType === "datetimetz" || overallType === "datetimetz") ? "datetimetz" : "datetime";
                }
            }
        }

        return overallType || "varchar";
    } catch (error) {
        throw error
    }
}

export function updateColumnType(column: any, dataPoint: string) {
    const detectedType = predictType(dataPoint);
    if (!detectedType) {
        column.allowNull = true;
        return;
    }

    if (detectedType !== column.type) {
        column.type = collateTypes([detectedType, column.type]);
    }
}