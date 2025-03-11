import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig } from "../../../config/types";
import { pgsqlConfig } from "../../config/pgsqlConfig";
import { compareHeaders } from '../../../helpers/headers';
const dialectConfig = pgsqlConfig

export class PostgresTableQueryBuilder {
    static getCreateTableQuery(table: string, headers: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput[] {
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
            if (column.length && !dialectConfig.noLength.includes(columnType)) {
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
            if (column.default !== undefined) {
                const replacement = dialectConfig.defaultTranslation[column.default] || column.default;
                columnDef += ` DEFAULT ${replacement}`;
            }
    
            if (column.primary) primaryKeys.push(`"${columnName}"`);
            if (column.unique) uniqueKeys.push(`"${columnName}"`);
            if (column.index) indexes.push(`"${columnName}"`);
    
            sqlQuery += `${columnDef},\n`;
        }
    
        if (primaryKeys.length) sqlQuery += `PRIMARY KEY (${primaryKeys.join(", ")}),\n`;
        if (uniqueKeys.length) sqlQuery += `${uniqueKeys.map((key) => `UNIQUE(${key})`).join(", ")},\n`;
    
        sqlQuery = sqlQuery.slice(0, -2) + "\n);";
        sqlQueries.push({ query: sqlQuery, params: []}); // Store CREATE TABLE query as first item
    
        // Create indexes separately
        for (const index of indexes) {
            sqlQueries.push({ query: `CREATE INDEX "${table}_${index.replace(/"/g, "")}_idx" ON ${schemaPrefix}"${table}" ("${index.replace(/"/g, "")}");`, params: []});
        }
    
        return sqlQueries;
    }    
    // ✅ Get changes using compareHeaders()
    //const { addColumns, modifyColumns } = compareHeaders(oldHeaders, newHeaders, pgsqlConfig);

    static getAlterTableQuery(table: string, changes: AlterTableChanges, schema?: string): QueryInput[] {
        let queries: QueryInput[] = [];
        let alterStatements: string[] = [];
    
        // ✅ Handle `DROP COLUMN`
        changes.dropColumns.forEach(columnName => {
            alterStatements.push(`DROP COLUMN "${columnName}"`);
        });
    
        // ✅ Handle `RENAME COLUMN`
        changes.renameColumns.forEach(({ oldName, newName }) => {
            alterStatements.push(`RENAME COLUMN "${oldName}" TO "${newName}"`);
        });
    
        // ✅ Handle `ADD COLUMN`
        for (const [columnName, column] of Object.entries(changes.addColumns)) {
    
            let columnDef = `"${columnName}" ${column.type}`;
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
                let columnDef = `SET DATA TYPE ${column.type}`;
                if (column.length && !pgsqlConfig.noLength.includes(column.type ?? "")) {
                    columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
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
                        CASE 
                            WHEN c.COLUMN_DEFAULT ILIKE 'nextval%' THEN 'auto_increment'
                            WHEN c.COLUMN_DEFAULT ILIKE 'CURRENT_TIMESTAMP%' THEN 'on update CURRENT_TIMESTAMP'
                            ELSE NULL
                        END AS EXTRA,
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
                    WHERE c.TABLE_SCHEMA = $1 AND c.TABLE_NAME = $2;`,
            params: [schema, table],
        };
    }
}