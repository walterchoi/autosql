import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig } from "../../../config/types";
import { pgsqlConfig } from "../../config/pgsqlConfig";
import { compareMetaData } from '../../../helpers/metadata';
import { getUsingClause } from "./alterTableTypeConversion";
import { generateSafeConstraintName, getTempTableName } from "../../../helpers/utilities";
const dialectConfig = pgsqlConfig

export class PostgresTableQueryBuilder {
    static getCreateTableQuery(table: string, headers: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput[] {
        const maxIndexCount = dialectConfig.maxIndexCount || 64;
        let remainingIndexSlots = maxIndexCount;
        let sqlQueries: QueryInput[] = [];
        const schemaPrefix = databaseConfig?.schema ? `"${databaseConfig.schema}".` : "";
        let sqlQuery = `CREATE TABLE IF NOT EXISTS ${schemaPrefix}"${table}" (\n`;
        let primaryKeys: string[] = [];
        let uniqueKeys: string[] = [];
        let indexes: string[] = [];
    
        for (const [columnName, column] of Object.entries(headers)) {
            if (!column.type) throw new Error(`Missing type for column ${columnName}`);
    
            let columnType = column.type.toLowerCase();
            if (dialectConfig.translate.localToServer[columnType]) {
                columnType = dialectConfig.translate.localToServer[columnType];
            }
    
            let columnDef = `"${columnName}" ${columnType}`;
    
            // Handle column lengths
            if (column.length && dialectConfig.requireLength.includes(columnType)) {
                columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
            }
    
            // Use SERIAL for Auto-incrementing Primary Keys
            if (column.autoIncrement) {
                if (columnType === "int" || columnType === "bigint") {
                    columnDef = `"${columnName}" SERIAL`;
                } else {
                    throw new Error(`AUTO_INCREMENT (SERIAL) is not supported on type ${columnType} in PostgreSQL`);
                }
            }
    
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined && !column.autoIncrement) {
                const replacement = dialectConfig.defaultTranslation[column.default] || column.default;
                columnDef += ` DEFAULT ${replacement}`;
            }
    
            if (column.primary) primaryKeys.push(`"${columnName}"`);
            if (column.unique) uniqueKeys.push(`"${columnName}"`);
            if (column.index) indexes.push(`"${columnName}"`);
    
            sqlQuery += `${columnDef},\n`;
        }
    
        if (primaryKeys.length) {
            sqlQuery += `PRIMARY KEY (${primaryKeys.join(", ")}),\n`
            remainingIndexSlots--; // 🔢 count primary key toward the limit
        }
        const includedUniqueKeys = uniqueKeys.slice(0, remainingIndexSlots);
        if (includedUniqueKeys.length) {
            sqlQuery += `${includedUniqueKeys
                .map((key) => {
                    const columnName = key.replace(/"/g, '');
                    const constraintName = generateSafeConstraintName(table, columnName, 'unique');
                    remainingIndexSlots --;
                    return `CONSTRAINT "${constraintName}" UNIQUE("${columnName}")`;
                })
                .join(', ')},\n`;
        }
    
        sqlQuery = sqlQuery.slice(0, -2) + "\n);";
        sqlQueries.push({ query: sqlQuery, params: []}); // Store CREATE TABLE query as first item
    
        // Create indexes separately
        const limitedIndexes = indexes.slice(0, remainingIndexSlots);
        for (const index of limitedIndexes) {
            if (!index) continue; // Skip empty index names
        
            const cleanIndex = index.replace(/"/g, '');
            const indexName = generateSafeConstraintName(table, cleanIndex, 'index');
            sqlQueries.push({
                query: `CREATE INDEX "${indexName}" ON ${schemaPrefix}"${table}" ("${cleanIndex}");`,
                params: []
            });
        }        
    
        return sqlQueries;
    }

    static getAlterTableQuery(table: string, changes: AlterTableChanges, schema?: string, databaseConfig?: DatabaseConfig): QueryInput[] {
        let queries: QueryInput[] = [];
        let alterStatements: string[] = [];
    
        // ✅ Handle `DROP COLUMN`
        if(databaseConfig?.deleteColumns) {
            changes.dropColumns.forEach(columnName => {
                alterStatements.push(`DROP COLUMN "${columnName}"`);
            });
        }
    
        // ✅ Handle `RENAME COLUMN`
        changes.renameColumns.forEach(({ oldName, newName }) => {
            alterStatements.push(`RENAME COLUMN "${oldName}" TO "${newName}"`);
        });
    
        // ✅ Handle `ADD COLUMN`
        for (const [columnName, column] of Object.entries(changes.addColumns)) {
    
            if(!column.type) { throw new Error(`Attempted to add a new column '${columnName}' without a type`)}
            let columnType = column.type.toLowerCase()
            if (dialectConfig.translate.localToServer[columnType]) {
                columnType = dialectConfig.translate.localToServer[columnType];
            }
            let columnDef = `"${columnName}" ${columnType}`;
            if (column.length && !pgsqlConfig.noLength.includes(column.type ?? "")) {
                columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
            }
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) columnDef += ` DEFAULT '${column.default}'`;
    
            alterStatements.push(`ADD COLUMN ${columnDef}`);
        };
    
        // ✅ Handle `ALTER COLUMN` - Consolidate changes per column
        const alterColumnMap: { [key: string]: string[] } = {};
    
        for (const [columnName, column] of Object.entries(changes.modifyColumns)) {
    
            if (!alterColumnMap[columnName]) {
                alterColumnMap[columnName] = [];
            }
    
            if (column.type) {
                let columnType = column.type.toLowerCase()
                if (dialectConfig.translate.localToServer[columnType]) {
                    columnType = dialectConfig.translate.localToServer[columnType];
                }
                let columnDef = `SET DATA TYPE ${columnType}`;
                if (column.length && !pgsqlConfig.noLength.includes(column.type ?? "")) {
                    columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
                }
                if (column.previousType && column.previousType !== column.type) {
                    const usingExpr = getUsingClause(columnName, column.previousType, column.type);
                    if (usingExpr) columnDef += ` USING ${usingExpr}`;
                }
                alterColumnMap[columnName].push(columnDef);
            }

            if (column.allowNull || changes.nullableColumns.includes(columnName)) {
                alterColumnMap[columnName].push(`DROP NOT NULL`);
            }
            if (column.default !== undefined) {
                alterColumnMap[columnName].push(`SET DEFAULT '${column.default}'`);
            }
        };
    
        // ✅ Generate consolidated `ALTER COLUMN` statements
        Object.keys(alterColumnMap).forEach(columnName => {
            const changes = alterColumnMap[columnName].join(", ");
            alterStatements.push(`ALTER COLUMN "${columnName}" ${changes}`);
        });
    
        // ✅ Handle `NULLABLE COLUMNS` separately (if not already modified)
        changes.nullableColumns.forEach(columnName => {
            if (!alterColumnMap[columnName]) {
                alterStatements.push(`ALTER COLUMN "${columnName}" DROP NOT NULL`);
            }
        });
    
        // ✅ Combine all `ALTER TABLE` statements
        const schemaPrefix = schema ? `"${schema}".` : "";
        if (alterStatements.length > 0) {
            queries.push({ query: `ALTER TABLE ${schemaPrefix}"${table}" ${alterStatements.join(", ")};`, params: [] });
        }
    
        return queries;
    }

    static getDropTableQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `"${schema}".` : "";
        return { query: `DROP TABLE IF EXISTS ${schemaPrefix}"${table}";`, params: []};
    }

    static getCreateTempTableQuery(table: string, schema?: string): QueryInput {
        const tempTableName = getTempTableName(table);
        const schemaPrefix = schema ? `"${schema}".` : "";
        return {query: `CREATE TABLE IF NOT EXISTS ${schemaPrefix}"${tempTableName}"
        AS SELECT * FROM ${schemaPrefix}"${table}" LIMIT 0;`, params: []};
    }

    static getTableExistsQuery(schema: string, table: string): QueryInput {
        return {
            query: "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
            params: [schema, table],
        };
    }

    static getTableMetaDataQuery(schema: string, table: string): QueryInput {
        return {
            query: `SELECT 
                    DISTINCT ON (c.COLUMN_NAME) 
                    c.COLUMN_NAME, 
                    c.DATA_TYPE,
                    c.COLUMN_DEFAULT,
                    CASE
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN CONCAT(c.NUMERIC_PRECISION,',',c.NUMERIC_SCALE)
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NULL THEN c.NUMERIC_PRECISION::varchar
                        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN c.CHARACTER_MAXIMUM_LENGTH::varchar
                        ELSE NULL
                    END AS LENGTH,
                    c.IS_NULLABLE, 
                    CASE 
                        WHEN EXISTS (
                            SELECT 1 
                            FROM pg_index pi 
                            JOIN pg_attribute pa ON pa.attrelid = pi.indrelid 
                            AND pa.attnum = ANY(pi.indkey) 
                            AND pa.attname = c.COLUMN_NAME
                            WHERE pi.indrelid = t.oid 
                            AND pi.indisprimary = TRUE
                        ) THEN 'PRIMARY'
                        WHEN EXISTS (
                            SELECT 1 
                            FROM pg_constraint pc 
                            JOIN pg_attribute pa ON pa.attrelid = pc.conrelid 
                            AND pa.attnum = ANY(pc.conkey)
                            AND pa.attname = c.COLUMN_NAME
                            WHERE pc.conrelid = t.oid 
                            AND pc.contype = 'u'
                        ) THEN 'UNIQUE'
                        WHEN EXISTS (
                            SELECT 1 
                            FROM pg_index pi
                            JOIN pg_attribute pa ON pa.attrelid = pi.indrelid 
                            AND pa.attnum = ANY(pi.indkey) 
                            AND pa.attname = c.COLUMN_NAME
                            WHERE pi.indrelid = t.oid 
                            AND pi.indisunique = FALSE
                            AND pi.indisprimary = FALSE
                        ) THEN 'INDEX'
                        ELSE NULL
                    END AS COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS AS c
                LEFT JOIN pg_class AS t 
                    ON t.relname = c.TABLE_NAME 
                    AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = c.TABLE_SCHEMA)
                WHERE c.TABLE_SCHEMA = $1 AND c.TABLE_NAME = $2;
                `,
            params: [schema, table],
        };
    }

    static getSplitTablesQuery(table: string, schema?: string): QueryInput {
        return {
            query: `SELECT 
                    DISTINCT ON (c.COLUMN_NAME, c.TABLE_NAME) 
                    c.COLUMN_NAME,
                    c.TABLE_NAME,
                    c.DATA_TYPE,
                    c.COLUMN_DEFAULT,
                    CASE
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN CONCAT(c.NUMERIC_PRECISION,',',c.NUMERIC_SCALE)
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NULL THEN c.NUMERIC_PRECISION::varchar
                        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN c.CHARACTER_MAXIMUM_LENGTH::varchar
                        ELSE NULL
                    END AS LENGTH,
                    c.IS_NULLABLE, 
                    CASE 
                        WHEN EXISTS (
                            SELECT 1 
                            FROM pg_index pi 
                            JOIN pg_attribute pa ON pa.attrelid = pi.indrelid 
                            AND pa.attnum = ANY(pi.indkey) 
                            AND pa.attname = c.COLUMN_NAME
                            WHERE pi.indrelid = t.oid 
                            AND pi.indisprimary = TRUE
                        ) THEN 'PRIMARY'
                        WHEN EXISTS (
                            SELECT 1 
                            FROM pg_constraint pc 
                            JOIN pg_attribute pa ON pa.attrelid = pc.conrelid 
                            AND pa.attnum = ANY(pc.conkey)
                            AND pa.attname = c.COLUMN_NAME
                            WHERE pc.conrelid = t.oid 
                            AND pc.contype = 'u'
                        ) THEN 'UNIQUE'
                        WHEN EXISTS (
                            SELECT 1 
                            FROM pg_index pi
                            JOIN pg_attribute pa ON pa.attrelid = pi.indrelid 
                            AND pa.attnum = ANY(pi.indkey) 
                            AND pa.attname = c.COLUMN_NAME
                            WHERE pi.indrelid = t.oid 
                            AND pi.indisunique = FALSE
                            AND pi.indisprimary = FALSE
                        ) THEN 'INDEX'
                        ELSE NULL
                    END AS COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS AS c
                LEFT JOIN pg_class AS t 
                    ON t.relname = c.TABLE_NAME 
                    AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = c.TABLE_SCHEMA)
                WHERE c.table_schema = $1 
                AND c.table_name LIKE $2 || '__part_%'
                AND c.table_name SIMILAR TO $2 || '__part_[0-9]+'
                ORDER BY c.COLUMN_NAME, c.TABLE_NAME, c.ordinal_position;
            `,
            params: [schema, table],
        }
    }
}