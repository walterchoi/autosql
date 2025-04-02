import { isNumeric, isInteger, isFloating, isText, isBoolean, isDate, isTime } from "../../../config/groupings";

// Generates a `USING` clause for ALTER COLUMN to safely convert data types.
export function getUsingClause(columnName: string, oldType: string, newType: string, tableName: string, schema?: string): string | null {
    const schemaPrefix = schema ? `\`${schema}\`.` : "";
    if (oldType === newType) return `"${columnName}"`;

    // ✅ BOOLEAN → NUMERIC
    if (isBoolean(oldType) && isNumeric(newType)) {
        return `UPDATE ${schemaPrefix}\`${tableName}\` SET \`${columnName}\` = CASE WHEN \`${columnName}\` THEN 1 ELSE 0 END WHERE \`${columnName}\` IS NOT NULL;`;
    }

    // ✅ NUMERIC → BOOLEAN
    if (isNumeric(oldType) && isBoolean(newType)) {
        return `UPDATE ${schemaPrefix}\`${tableName}\` SET \`${columnName}\` = CASE WHEN \`${columnName}\` = 1 THEN TRUE ELSE FALSE END WHERE \`${columnName}\` IS NOT NULL;`;
    }

    // ✅ TEXT → BOOLEAN
    if (isText(oldType) && isBoolean(newType)) {
        return `UPDATE ${schemaPrefix}\`${tableName}\` SET \`${columnName}\` = 
                    CASE 
                        WHEN LOWER(TRIM(\`${columnName}\`)) IN ('true', 't', 'yes', 'y', '1', 'on') THEN TRUE 
                        WHEN LOWER(TRIM(\`${columnName}\`)) IN ('false', 'f', 'no', 'n', '0', 'off') THEN FALSE 
                        ELSE NULL 
                    END 
                WHERE \`${columnName}\` IS NOT NULL;`;
    }

    // ✅ INTEGER → FLOATING POINT
    if (isInteger(oldType) && isFloating(newType)) {
        return `UPDATE ${schemaPrefix}\`${tableName}\` SET \`${columnName}\` = \`${columnName}\` * 1.0 WHERE \`${columnName}\` IS NOT NULL;`;
    }

    // ✅ FLOATING POINT → INTEGER
    if (isFloating(oldType) && isInteger(newType)) {
        return `UPDATE ${schemaPrefix}\`${tableName}\` SET \`${columnName}\` = ROUND(\`${columnName}\`) WHERE \`${columnName}\` IS NOT NULL;`;
    }

    // ✅ TEXT → NUMERIC
    if (isText(oldType) && isNumeric(newType)) {
        return `UPDATE ${schemaPrefix}\`${tableName}\` SET \`${columnName}\` = NULLIF(\`${columnName}\`, '') WHERE \`${columnName}\` IS NOT NULL;`;
    }

    // ✅ TEXT → DATE/TIME
    if (isText(oldType) && newType === "datetime") {
        return `UPDATE ${schemaPrefix}\`${tableName}\` SET \`${columnName}\` = STR_TO_DATE(\`${columnName}\`, '%Y-%m-%d %H:%i:%s') WHERE \`${columnName}\` IS NOT NULL;`;
    }
    if (isText(oldType) && newType === "date") {
        return `UPDATE ${schemaPrefix}\`${tableName}\` SET \`${columnName}\` = STR_TO_DATE(\`${columnName}\`, '%Y-%m-%d') WHERE \`${columnName}\` IS NOT NULL;`;
    }
    if (isText(oldType) && newType === "time") {
        return `UPDATE ${schemaPrefix}\`${tableName}\` SET \`${columnName}\` = STR_TO_DATE(\`${columnName}\`, '%H:%i:%s') WHERE \`${columnName}\` IS NOT NULL;`;
    }

    return null;
}