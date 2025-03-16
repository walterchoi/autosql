import { isNumeric, isInteger, isFloating, isText, isBoolean, isDate, isTime } from "../../../config/groupings";

// Generates a `USING` clause for ALTER COLUMN to safely convert data types.
export function getUsingClause(columnName: string, oldType: string, newType: string): string {
    if (oldType === newType) return `"${columnName}"`;

    // ✅ BOOLEAN → NUMERIC (1 for TRUE, 0 for FALSE)
    if (isBoolean(oldType) && isNumeric(newType)) {
        return `CASE WHEN "${columnName}" IS NULL THEN NULL WHEN "${columnName}" THEN 1 ELSE 0 END`;
    }

    // ✅ NUMERIC → BOOLEAN (1 = TRUE, everything else = FALSE)
    if (isNumeric(oldType) && isBoolean(newType)) {
        return `CASE WHEN "${columnName}" IS NULL THEN NULL WHEN "${columnName}" = 1 THEN TRUE ELSE FALSE END`;
    }

    // ✅ BOOLEAN → TEXT
    if (isBoolean(oldType) && isText(newType)) {
        return `CASE WHEN "${columnName}" IS NULL THEN NULL WHEN "${columnName}" THEN 'true' ELSE 'false' END`;
    }

    // ✅ TEXT → BOOLEAN (Handles common boolean text values)
    if (isText(oldType) && isBoolean(newType)) {
        return `CASE 
                    WHEN "${columnName}" IS NULL THEN NULL 
                    WHEN LOWER(TRIM("${columnName}")) IN ('true', 't', 'yes', 'y', '1', 'on') THEN TRUE 
                    WHEN LOWER(TRIM("${columnName}")) IN ('false', 'f', 'no', 'n', '0', 'off') THEN FALSE 
                    ELSE NULL 
                END`;
    }

    // ✅ INTEGER → FLOATING POINT
    if (isInteger(oldType) && isFloating(newType)) {
        return `"${columnName}"::DECIMAL`;
    }

    // ✅ FLOATING POINT → INTEGER (ROUND to prevent precision loss)
    if (isFloating(oldType) && isInteger(newType)) {
        return `ROUND("${columnName}")::${newType}`;
    }

    // ✅ TEXT → NUMERIC (Handle empty strings safely)
    if (isText(oldType) && isNumeric(newType)) {
        return `NULLIF("${columnName}", '')::${newType}`;
    }

    // ✅ JSON → TEXT (Convert JSON to String)
    if (oldType === "json" && isText(newType)) {
        return `"${columnName}"::TEXT`;
    }

    // ✅ TEXT → JSON (Convert valid JSON strings)
    if (isText(oldType) && newType === "json") {
        return `"${columnName}"::JSONB`;
    }

    // ✅ TEXT → DATE/TIME (Use TO_TIMESTAMP for Datetime conversions)
    if (isText(oldType) && newType === "datetime") {
        return `TO_TIMESTAMP(NULLIF("${columnName}", ''), 'YYYY-MM-DD HH24:MI:SS')`;
    }
    if (isText(oldType) && newType === "datetimetz") {
        return `TO_TIMESTAMP(NULLIF("${columnName}", ''), 'YYYY-MM-DD HH24:MI:SS TZ')`;
    }
    if (isText(oldType) && newType === "date") {
        return `TO_DATE(NULLIF("${columnName}", ''), 'YYYY-MM-DD')`;
    }
    if (isText(oldType) && newType === "time") {
        return `TO_TIME(NULLIF("${columnName}", ''), 'HH24:MI:SS')`;
    }

    // ✅ DATE → TEXT (Format Date as String)
    if (isDate(oldType) && isText(newType)) {
        return `TO_CHAR("${columnName}", 'YYYY-MM-DD HH24:MI:SS')`;
    }

    // ✅ TIME → TEXT (Format Time as String)
    if (isTime(oldType) && isText(newType)) {
        return `TO_CHAR("${columnName}", 'HH24:MI:SS')`;
    }

    // ✅ Default: Simple Cast (with NULL handling)
    return `NULLIF("${columnName}", '')::${newType}`;
}