import { isNumeric, isInteger, isFloating, isText, isBoolean, isDate, isTime } from "../../../config/groupings";
import { escapeIdentifier, assertSafeTypeToken } from "../../utils/escape";

const q = (name: string) => escapeIdentifier(name, "sqlserver");

// SQL Server has no USING clause on ALTER COLUMN; return a bare T-SQL CAST/CASE expression the
// caller splices into a two-step convert or computed SELECT. Mirrors Postgres getUsingClause but
// in T-SQL: booleans use `bit` 1/0, casts use CAST(... AS <type>) not `::`, text→numeric guards
// empty strings with NULLIF.
export function getUsingExpression(columnName: string, oldType: string, newType: string): string {
    if (oldType === newType) return q(columnName);
    assertSafeTypeToken(newType);

    // BOOLEAN → NUMERIC (1 for TRUE, 0 for FALSE)
    if (isBoolean(oldType) && isNumeric(newType)) {
        return `CASE WHEN ${q(columnName)} IS NULL THEN NULL WHEN ${q(columnName)} = 1 THEN 1 ELSE 0 END`;
    }

    // NUMERIC → BOOLEAN (1 = TRUE, else FALSE) — SQL Server booleans are `bit`
    if (isNumeric(oldType) && isBoolean(newType)) {
        return `CASE WHEN ${q(columnName)} IS NULL THEN NULL WHEN ${q(columnName)} = 1 THEN 1 ELSE 0 END`;
    }

    // BOOLEAN → TEXT
    if (isBoolean(oldType) && isText(newType)) {
        return `CASE WHEN ${q(columnName)} IS NULL THEN NULL WHEN ${q(columnName)} = 1 THEN 'true' ELSE 'false' END`;
    }

    // TEXT → BOOLEAN (common boolean text values) — emit `bit` 1/0
    if (isText(oldType) && isBoolean(newType)) {
        return `CASE
                    WHEN ${q(columnName)} IS NULL THEN NULL
                    WHEN LOWER(LTRIM(RTRIM(${q(columnName)}))) IN ('true', 't', 'yes', 'y', '1', 'on') THEN 1
                    WHEN LOWER(LTRIM(RTRIM(${q(columnName)}))) IN ('false', 'f', 'no', 'n', '0', 'off') THEN 0
                    ELSE NULL
                END`;
    }

    // INTEGER → FLOATING POINT
    if (isInteger(oldType) && isFloating(newType)) {
        return `CAST(${q(columnName)} AS DECIMAL)`;
    }

    // FLOATING POINT → INTEGER (ROUND to prevent precision loss)
    if (isFloating(oldType) && isInteger(newType)) {
        return `CAST(ROUND(${q(columnName)}, 0) AS INT)`;
    }

    // TEXT → NUMERIC (empty strings → NULL)
    if (isText(oldType) && isNumeric(newType)) {
        return `CAST(NULLIF(${q(columnName)}, '') AS DECIMAL)`;
    }

    // JSON → TEXT
    if (oldType === "json" && isText(newType)) {
        return `CAST(${q(columnName)} AS NVARCHAR(MAX))`;
    }

    // TEXT → JSON (SQL Server has no native JSON type; store as NVARCHAR(MAX))
    if (isText(oldType) && newType === "json") {
        return `CAST(${q(columnName)} AS NVARCHAR(MAX))`;
    }

    // TEXT → DATE/TIME
    if (isText(oldType) && newType === "datetime") {
        return `CAST(NULLIF(${q(columnName)}, '') AS DATETIME2)`;
    }
    if (isText(oldType) && newType === "datetimetz") {
        return `CAST(NULLIF(${q(columnName)}, '') AS DATETIMEOFFSET)`;
    }
    if (isText(oldType) && newType === "date") {
        return `CAST(NULLIF(${q(columnName)}, '') AS DATE)`;
    }
    if (isText(oldType) && newType === "time") {
        return `CAST(NULLIF(${q(columnName)}, '') AS TIME)`;
    }

    // DATE → TEXT
    if (isDate(oldType) && isText(newType)) {
        return `CONVERT(NVARCHAR(MAX), ${q(columnName)}, 120)`;
    }

    // TIME → TEXT
    if (isTime(oldType) && isText(newType)) {
        return `CONVERT(NVARCHAR(MAX), ${q(columnName)}, 108)`;
    }

    // Default: simple CAST (empty string → NULL for text sources)
    if (isText(oldType)) {
        return `CAST(NULLIF(${q(columnName)}, '') AS ${assertSafeTypeToken(newType)})`;
    }
    return `CAST(${q(columnName)} AS ${assertSafeTypeToken(newType)})`;
}
