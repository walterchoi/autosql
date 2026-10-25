import { isNumeric, isInteger, isFloating, isText, isBoolean, isDate, isTime } from "../../../config/groupings";
import { escapeIdentifier, assertSafeTypeToken } from "../../utils/escape";
import { PGSQL_CONFIG } from "../../config/pgsqlConfig";

const q = (name: string) => escapeIdentifier(name, "pgsql");

// Translate a LOCAL inference type to its Postgres SERVER type for a `::cast` (A12): branches
// key off local types, but `::double`/`::datetime`/`::tinyint` aren't real Postgres types and
// would fail the cast at run time.
const toServer = (t: string): string => PGSQL_CONFIG.translate.localToServer[t] ?? t;

// Generates a `USING` clause for ALTER COLUMN to safely convert data types.
export function getUsingClause(columnName: string, oldType: string, newType: string): string {
    if (oldType === newType) return q(columnName);
    assertSafeTypeToken(newType);

    if (isBoolean(oldType) && isNumeric(newType)) {
        return `CASE WHEN ${q(columnName)} IS NULL THEN NULL WHEN ${q(columnName)} THEN 1 ELSE 0 END`;
    }

    if (isNumeric(oldType) && isBoolean(newType)) {
        return `CASE WHEN ${q(columnName)} IS NULL THEN NULL WHEN ${q(columnName)} = 1 THEN TRUE ELSE FALSE END`;
    }

    if (isBoolean(oldType) && isText(newType)) {
        return `CASE WHEN ${q(columnName)} IS NULL THEN NULL WHEN ${q(columnName)} THEN 'true' ELSE 'false' END`;
    }

    if (isText(oldType) && isBoolean(newType)) {
        return `CASE 
                    WHEN ${q(columnName)} IS NULL THEN NULL 
                    WHEN LOWER(TRIM(${q(columnName)})) IN ('true', 't', 'yes', 'y', '1', 'on') THEN TRUE 
                    WHEN LOWER(TRIM(${q(columnName)})) IN ('false', 'f', 'no', 'n', '0', 'off') THEN FALSE 
                    ELSE NULL 
                END`;
    }

    if (isInteger(oldType) && isFloating(newType)) {
        return `${q(columnName)}::DECIMAL`;
    }

    if (isFloating(oldType) && isInteger(newType)) {
        return `ROUND(${q(columnName)})::${toServer(newType)}`;
    }

    // Plain cast for numeric widening — the default `NULLIF(col, '')::type` below is only correct
    // for a TEXT source; on a numeric source Postgres errors (`invalid input syntax … : ""`),
    // which autoSQLChunked hits when a later chunk widens a key column (e.g. tinyint → smallint).
    if (isNumeric(oldType) && isNumeric(newType)) {
        return `${q(columnName)}::${toServer(newType)}`;
    }

    if (isText(oldType) && isNumeric(newType)) {
        return `NULLIF(${q(columnName)}, '')::${toServer(newType)}`;
    }

    if (oldType === "json" && isText(newType)) {
        return `${q(columnName)}::TEXT`;
    }

    if (isText(oldType) && newType === "json") {
        return `${q(columnName)}::JSONB`;
    }

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

    if (isDate(oldType) && isText(newType)) {
        return `TO_CHAR(${q(columnName)}, 'YYYY-MM-DD HH24:MI:SS')`;
    }

    if (isTime(oldType) && isText(newType)) {
        return `TO_CHAR(${q(columnName)}, 'HH24:MI:SS')`;
    }

    return `NULLIF(${q(columnName)}, '')::${toServer(newType)}`;
}