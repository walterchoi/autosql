import { isNumeric, isInteger, isFloating, isText, isBoolean, isDate, isTime } from "../../../config/groupings";
import { escapeIdentifier, assertSafeTypeToken } from "../../utils/escape";
import { pgsqlConfig } from "../../config/pgsqlConfig";

const q = (name: string) => escapeIdentifier(name, "pgsql");

// Translate a LOCAL inference type to its Postgres SERVER type for a `::cast` (A12): branches key off
// local types but the emitted cast must name a real Postgres type (`::double`/`::datetime`/`::tinyint`
// don't exist, ALTER fails at run time). Mirrors SET DATA TYPE; map is a fixed set of trusted tokens.
const toServer = (t: string): string => pgsqlConfig.translate.localToServer[t] ?? t;

// Generates a `USING` clause for ALTER COLUMN to safely convert data types.
export function getUsingClause(columnName: string, oldType: string, newType: string): string {
    if (oldType === newType) return q(columnName);
    assertSafeTypeToken(newType);

    // BOOLEAN → NUMERIC (1 for TRUE, 0 for FALSE)
    if (isBoolean(oldType) && isNumeric(newType)) {
        return `CASE WHEN ${q(columnName)} IS NULL THEN NULL WHEN ${q(columnName)} THEN 1 ELSE 0 END`;
    }

    // NUMERIC → BOOLEAN (1 = TRUE, else FALSE)
    if (isNumeric(oldType) && isBoolean(newType)) {
        return `CASE WHEN ${q(columnName)} IS NULL THEN NULL WHEN ${q(columnName)} = 1 THEN TRUE ELSE FALSE END`;
    }

    // BOOLEAN → TEXT
    if (isBoolean(oldType) && isText(newType)) {
        return `CASE WHEN ${q(columnName)} IS NULL THEN NULL WHEN ${q(columnName)} THEN 'true' ELSE 'false' END`;
    }

    // TEXT → BOOLEAN (common boolean text values)
    if (isText(oldType) && isBoolean(newType)) {
        return `CASE 
                    WHEN ${q(columnName)} IS NULL THEN NULL 
                    WHEN LOWER(TRIM(${q(columnName)})) IN ('true', 't', 'yes', 'y', '1', 'on') THEN TRUE 
                    WHEN LOWER(TRIM(${q(columnName)})) IN ('false', 'f', 'no', 'n', '0', 'off') THEN FALSE 
                    ELSE NULL 
                END`;
    }

    // INTEGER → FLOATING POINT
    if (isInteger(oldType) && isFloating(newType)) {
        return `${q(columnName)}::DECIMAL`;
    }

    // FLOATING POINT → INTEGER (ROUND to prevent precision loss)
    if (isFloating(oldType) && isInteger(newType)) {
        return `ROUND(${q(columnName)})::${toServer(newType)}`;
    }

    // NUMERIC → NUMERIC widening: plain cast. The default `NULLIF(col, '')::type` below is only
    // correct for a TEXT source; on a numeric source Postgres errors (`invalid input syntax … : ""`),
    // which a later chunk hits when autoSQLChunked widens a key column (e.g. tinyint → smallint).
    if (isNumeric(oldType) && isNumeric(newType)) {
        return `${q(columnName)}::${toServer(newType)}`;
    }

    // TEXT → NUMERIC (empty strings → NULL)
    if (isText(oldType) && isNumeric(newType)) {
        return `NULLIF(${q(columnName)}, '')::${toServer(newType)}`;
    }

    // JSON → TEXT
    if (oldType === "json" && isText(newType)) {
        return `${q(columnName)}::TEXT`;
    }

    // TEXT → JSON
    if (isText(oldType) && newType === "json") {
        return `${q(columnName)}::JSONB`;
    }

    // TEXT → DATE/TIME (TO_TIMESTAMP for datetime conversions)
    if (isText(oldType) && newType === "datetime") {
        return `TO_TIMESTAMP(NULLIF(${q(columnName)}, ''), 'YYYY-MM-DD HH24:MI:SS')`;
    }
    if (isText(oldType) && newType === "datetimetz") {
        return `TO_TIMESTAMP(NULLIF(${q(columnName)}, ''), 'YYYY-MM-DD HH24:MI:SS TZ')`;
    }
    if (isText(oldType) && newType === "date") {
        return `TO_DATE(NULLIF(${q(columnName)}, ''), 'YYYY-MM-DD')`;
    }
    if (isText(oldType) && newType === "time") {
        return `TO_TIME(NULLIF(${q(columnName)}, ''), 'HH24:MI:SS')`;
    }

    // DATE → TEXT
    if (isDate(oldType) && isText(newType)) {
        return `TO_CHAR(${q(columnName)}, 'YYYY-MM-DD HH24:MI:SS')`;
    }

    // TIME → TEXT
    if (isTime(oldType) && isText(newType)) {
        return `TO_CHAR(${q(columnName)}, 'HH24:MI:SS')`;
    }

    // Default: simple cast (with NULL handling)
    return `NULLIF(${q(columnName)}, '')::${toServer(newType)}`;
}