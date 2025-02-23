import { defaults } from "../config/defaults";
export function isObject(val: any): boolean {
    return val !== null && typeof val === "object";
}

export function shuffleArray<T>(array: T[]): T[] {
    const shuffled = [...array];
    for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled;
}

export function validateConfig(config: { [key: string]: any }): { [key: string]: any } {
    try {
        if (!config.sql_dialect) {
            throw new Error(
                JSON.stringify({
                    err: "no sql dialect",
                    step: "get_meta_data",
                    description: "invalid configuration was provided to get_meta_data step",
                    resolution: "please provide a sql_dialect (such as pgsql, mysql) to use as part of the configuration object"
                })
            );
        }

        // Extract input config values, using defaults when not provided
        const {
            minimum_unique = defaults.minimum_unique,
            maximum_unique_length = defaults.maximum_unique_length,
            max_non_text_length = defaults.max_non_text_length,
            pseudo_unique = defaults.pseudo_unique,
            primary = defaults.primary,
            auto_indexing = defaults.auto_indexing,
            auto_id = defaults.auto_id,
            sampling = defaults.sampling,
            sampling_minimum = defaults.sampling_minimum
        } = config;

        // Validate conflicting configuration settings
        if (primary && auto_id && JSON.stringify(primary) !== JSON.stringify(["ID"])) {
            throw new Error(
                JSON.stringify({
                    err: "primary key and auto_id was specified",
                    step: "get_meta_data",
                    description: "invalid configuration was provided to get_meta_data step",
                    resolution: "please only use ONE of primary OR auto_id configuration for this step, not both"
                })
            );
        }

        return {
            sql_dialect: config.sql_dialect, // Required field
            minimum_unique,
            maximum_unique_length,
            max_non_text_length,
            pseudo_unique,
            primary,
            auto_indexing,
            auto_id,
            sampling,
            sampling_minimum
        };
    } catch (error) {
        throw new Error(`Error in validateConfig: ${error}`);
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

export function normalizeNumber(input: string, thousandsIndicatorOverride?: string, decimalIndicatorOverride?: string): string | null {
    if ((thousandsIndicatorOverride && !decimalIndicatorOverride) || (!thousandsIndicatorOverride && decimalIndicatorOverride)) {
        throw new Error("Both 'thousandsIndicatorOverride' and 'decimalIndicatorOverride' must be provided together.");
    }
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
        let tempInput = input.replaceAll(usedThousands, unusedThousands);
        tempInput = tempInput.replaceAll(usedDecimal, unusedDecimal);

        // Replace placeholders with final characters (comma for thousands, dot for decimal)
        tempInput = tempInput.replaceAll(unusedThousands, ",").replaceAll(unusedDecimal, ".");

        input = tempInput;
    }

    // 🚨 Ensure `-` appears only at the start
    if (input.includes("-") && input.indexOf("-") !== 0) return null;

    const isNegative = input.startsWith("-");
    if (isNegative) input = input.slice(1); // Remove `-` temporarily for processing

    if (!input || /[^0-9., ]/.test(input)) return null; // Reject if non-numeric characters exist

    const dotCount = (input.match(/\./g) || []).length;
    const commaCount = (input.match(/,/g) || []).length;
    input = input.replace(/ /g, "");

    // 🚨 Reject cases
    if (
        !/\d/.test(input) || // No digits present
        (dotCount > 1 && commaCount > 1) || // Too many of both
        input.includes(".,") || input.includes(",.") || // Misplaced combinations
        /\d[.,]{2,}\d/.test(input) // Double separators like "1..234"
    ) {
        return null;
    }

    // 🚨 Check incorrect ordering of separators
    const firstComma = input.indexOf(",");
    const lastComma = input.lastIndexOf(",")
    const firstDot = input.indexOf(".");
    const lastDot = input.lastIndexOf(".")

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

    const decimalSplit = input.split(decimalIndicator);
    
    if (decimalSplit.length > 2) return null; // More than one decimal, invalid

    let preDecimal = decimalSplit[0];
    let postDecimal = decimalSplit[1] || ""; // Optional decimal part

    // Validate thousands separator formatting
    if (thousandsIndicator) {
        const thousandsSplit = preDecimal.split(thousandsIndicator);
        for (let i = 0; i < thousandsSplit.length; i++) {
            const part = thousandsSplit[i];
            if (i === 0 && part.length > 3) return null; // First group must be ≤ 3 digits
            if (i > 0 && part.length !== 3) return null; // Subsequent groups must be exactly 3 digits
        }
        // Remove thousands separator
        preDecimal = thousandsSplit.join("");
    }

    const normalized = `${isNegative ? "-" : ""}${preDecimal}${postDecimal ? "." + postDecimal : ""}`;
    return normalized;
}