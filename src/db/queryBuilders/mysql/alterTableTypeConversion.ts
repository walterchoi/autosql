import { isNumeric, isInteger, isFloating, isText, isBoolean, isDate, isTime } from "../../../config/groupings";
import { escapeIdentifier, assertSafeTypeToken } from "../../utils/escape";

const q = (name: string) => escapeIdentifier(name, "mysql");

// Generates a `USING` clause for ALTER COLUMN to safely convert data types.
export function getUsingClause(columnName: string, oldType: string, newType: string, tableName: string, schema?: string): string | null {
    const schemaPrefix = schema ? `${q(schema)}.` : "";
    if (oldType === newType) return q(columnName);
    assertSafeTypeToken(newType);

    // ✅ BOOLEAN → NUMERIC
    if (isBoolean(oldType) && isNumeric(newType)) {
        return `UPDATE ${schemaPrefix}${q(tableName)} SET ${q(columnName)} = CASE WHEN ${q(columnName)} THEN 1 ELSE 0 END WHERE ${q(columnName)} IS NOT NULL;`;
    }

    // ✅ NUMERIC → BOOLEAN
    if (isNumeric(oldType) && isBoolean(newType)) {
        return `UPDATE ${schemaPrefix}${q(tableName)} SET ${q(columnName)} = CASE WHEN ${q(columnName)} = 1 THEN TRUE ELSE FALSE END WHERE ${q(columnName)} IS NOT NULL;`;
    }

    // ✅ TEXT → BOOLEAN
    if (isText(oldType) && isBoolean(newType)) {
        return `UPDATE ${schemaPrefix}${q(tableName)} SET ${q(columnName)} = 
                    CASE 
                        WHEN LOWER(TRIM(${q(columnName)})) IN ('true', 't', 'yes', 'y', '1', 'on') THEN TRUE 
                        WHEN LOWER(TRIM(${q(columnName)})) IN ('false', 'f', 'no', 'n', '0', 'off') THEN FALSE 
                        ELSE NULL 
                    END 
                WHERE ${q(columnName)} IS NOT NULL;`;
    }

    // ✅ INTEGER → FLOATING POINT
    if (isInteger(oldType) && isFloating(newType)) {
        return `UPDATE ${schemaPrefix}${q(tableName)} SET ${q(columnName)} = ${q(columnName)} * 1.0 WHERE ${q(columnName)} IS NOT NULL;`;
    }

    // ✅ FLOATING POINT → INTEGER
    if (isFloating(oldType) && isInteger(newType)) {
        return `UPDATE ${schemaPrefix}${q(tableName)} SET ${q(columnName)} = ROUND(${q(columnName)}) WHERE ${q(columnName)} IS NOT NULL;`;
    }

    // ✅ TEXT → NUMERIC
    if (isText(oldType) && isNumeric(newType)) {
        return `UPDATE ${schemaPrefix}${q(tableName)} SET ${q(columnName)} = NULLIF(${q(columnName)}, '') WHERE ${q(columnName)} IS NOT NULL;`;
    }

    // ✅ TEXT → DATE/TIME
    if (isText(oldType) && newType === "datetime") {
        return `UPDATE ${schemaPrefix}${q(tableName)} SET ${q(columnName)} = STR_TO_DATE(${q(columnName)}, '%Y-%m-%d %H:%i:%s') WHERE ${q(columnName)} IS NOT NULL;`;
    }
    if (isText(oldType) && newType === "date") {
        return `UPDATE ${schemaPrefix}${q(tableName)} SET ${q(columnName)} = STR_TO_DATE(${q(columnName)}, '%Y-%m-%d') WHERE ${q(columnName)} IS NOT NULL;`;
    }
    if (isText(oldType) && newType === "time") {
        return `UPDATE ${schemaPrefix}${q(tableName)} SET ${q(columnName)} = STR_TO_DATE(${q(columnName)}, '%H:%i:%s') WHERE ${q(columnName)} IS NOT NULL;`;
    }

    return null;
}