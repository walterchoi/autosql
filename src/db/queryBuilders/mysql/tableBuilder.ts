import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig } from "../../../config/types";
import { mysqlConfig } from "../../config/mysqlConfig";
import { compareHeaders } from '../../../helpers/headers';
const dialectConfig = mysqlConfig

export class MySQLTableQueryBuilder {
    static getCreateTableQuery(table: string, headers: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput[] {
        let sqlQueries: QueryInput[] = [];
        const schemaPrefix = databaseConfig?.schema ? `\`${databaseConfig.schema}\`.` : "";
        let sqlQuery = `CREATE TABLE IF NOT EXISTS ${schemaPrefix}\`${table}\` (\n`;
        let primaryKeys: string[] = [];
        let uniqueKeys: string[] = [];
        let indexes: string[] = [];
    
        for (const [columnName, column] of Object.entries(headers)) {
            if (!column.type) throw new Error(`Missing type for column ${columnName}`);
    
            let columnType = column.type.toLowerCase();
            if (dialectConfig.translate.localToServer[columnType]) {
                columnType = dialectConfig.translate.localToServer[columnType];
            }
    
            let columnDef = `\`${columnName}\` ${columnType}`;
    
            // Handle column lengths
            if (column.length && !dialectConfig.noLength.includes(columnType)) {
                columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
            }
    
            // Convert BOOLEAN → TINYINT(1) for MySQL
            if (column.type === "boolean") {
                columnDef = `\`${columnName}\` TINYINT(1)`;
            }
    
            // Apply AUTO_INCREMENT only to valid integer types
            if (column.autoIncrement) {
                if (["int", "bigint", "smallint", "tinyint"].includes(columnType)) {
                    columnDef += " AUTO_INCREMENT";
                } else {
                    throw new Error(`AUTO_INCREMENT is not supported on type ${columnType} in MySQL`);
                }
            }
    
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) {
                const replacement = dialectConfig.defaultTranslation[column.default];
                columnDef += ` DEFAULT ${replacement ? replacement : column.default}`;
            }
    
            if (column.primary) primaryKeys.push(`\`${columnName}\``);
            if (column.unique) uniqueKeys.push(`\`${columnName}\``);
            if (column.index) indexes.push(`\`${columnName}\``);
    
            sqlQuery += `${columnDef},\n`;
        }
    
        if (primaryKeys.length) sqlQuery += `PRIMARY KEY (${primaryKeys.join(", ")}),\n`;
        if (uniqueKeys.length) sqlQuery += `${uniqueKeys.map((key) => `UNIQUE(${key})`).join(", ")},\n`;
    
        sqlQuery = sqlQuery.slice(0, -2) + `) ENGINE=${databaseConfig?.engine || dialectConfig.engine} 
            DEFAULT CHARSET=${databaseConfig?.charset || dialectConfig.charset} 
            COLLATE=${databaseConfig?.collate || dialectConfig.collate};`;

        sqlQueries.push({query: sqlQuery, params: []}); // Store CREATE TABLE query as first item
    
        // Create indexes separately
        for (const index of indexes) {
            sqlQueries.push({ query: `CREATE INDEX \`${index.replace(/`/g, "")}_idx\` ON ${schemaPrefix}\`${table}\` (\`${index.replace(/`/g, "")}\`);`, params: []});
        }
    
        return sqlQueries;
    }

    static getAlterTableQuery(table: string, changes: AlterTableChanges, schema?: string): QueryInput[] {
        let queries: QueryInput[] = [];
        let alterStatements: string[] = [];
    
        // ✅ Handle `DROP COLUMN`
        changes.dropColumns.forEach(columnName => {
            alterStatements.push(`DROP COLUMN \`${columnName}\``);
        });
    
        // ✅ Handle `RENAME COLUMN`
        changes.renameColumns.forEach(({ oldName, newName }) => {
            alterStatements.push(`CHANGE COLUMN \`${oldName}\` \`${newName}\` varchar(255) NOT NULL`); // Default type assumption
        });
    
        // ✅ Handle `ADD COLUMN`
        for (const [columnName, column] of Object.entries(changes.addColumns)) {
            let columnDef = `\`${columnName}\` ${column.type}`;
            if (column.length && !dialectConfig.noLength.includes(column.type || "")) {
                columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
            }
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) columnDef += ` DEFAULT '${column.default}'`;
    
            alterStatements.push(`ADD COLUMN ${columnDef}`);
        };
    
        // ✅ Handle `MODIFY COLUMN` - Include `NULL` conditionally
        for (const [columnName, column] of Object.entries(changes.modifyColumns)) {

            let columnDef = `\`${columnName}\` ${column.type}`;
            if (column.length && !dialectConfig.noLength.includes(column.type || "")) {
                columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
            }
            
            // ✅ Apply NULL or NOT NULL depending on whether the column is in nullableColumns
            if (changes.nullableColumns.includes(columnName)) {
                columnDef += " NULL";
            } else if (!column.allowNull) {
                columnDef += " NOT NULL";
            }

            if (column.default !== undefined) {
                columnDef += ` DEFAULT '${column.default}'`;
            }

            alterStatements.push(`MODIFY COLUMN ${columnDef}`);
        };

        // ✅ Handle standalone `NULLABLE COLUMNS` not in modifyColumns
        changes.nullableColumns.forEach(columnName => {
            if (!Object.prototype.hasOwnProperty.call(changes.modifyColumns, columnName)) {
                alterStatements.push(`MODIFY COLUMN \`${columnName}\` DROP NOT NULL`);
            }            
        });
        const schemaPrefix = schema ? `\`${schema}\`.` : "";

        // ✅ Combine all `ALTER TABLE` statements
        if (alterStatements.length > 0) {
            queries.push({ query: `ALTER TABLE ${schemaPrefix}\`${table}\` ${alterStatements.join(", ")};`, params: [] });
        }
    
        return queries;
    }
    

    static getDropTableQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `\`${schema}\`.` : "";
        return {query: `DROP TABLE IF EXISTS ${schemaPrefix}\`${table}\`;`, params: []};
    }

    static getTableExistsQuery(schema: string, table: string): QueryInput {
        return {
            query: "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            params: [schema, table],
        };
    }

    static getTableMetaDataQuery(schema: string, table: string): QueryInput {
        return {
            query: `SELECT 
    c.COLUMN_NAME, 
    c.DATA_TYPE,
    c.EXTRA,
    CASE 
        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN CONCAT(c.NUMERIC_PRECISION,',',c.NUMERIC_SCALE)
        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NULL THEN c.NUMERIC_PRECISION
        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN c.CHARACTER_MAXIMUM_LENGTH
        ELSE NULL 
    END AS LENGTH, 
    c.IS_NULLABLE, 
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
                ON tc.TABLE_SCHEMA = k.TABLE_SCHEMA 
                AND tc.TABLE_NAME = k.TABLE_NAME 
                AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = c.TABLE_SCHEMA
              AND tc.TABLE_NAME = c.TABLE_NAME
              AND k.COLUMN_NAME = c.COLUMN_NAME
              AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) THEN 'PRIMARY'
        WHEN EXISTS (
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
                ON tc.TABLE_SCHEMA = k.TABLE_SCHEMA 
                AND tc.TABLE_NAME = k.TABLE_NAME 
                AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = c.TABLE_SCHEMA
              AND tc.TABLE_NAME = c.TABLE_NAME
              AND k.COLUMN_NAME = c.COLUMN_NAME
              AND tc.CONSTRAINT_TYPE = 'UNIQUE'
        ) THEN 'UNIQUE'
        WHEN EXISTS (
            SELECT 1 
            FROM INFORMATION_SCHEMA.STATISTICS AS i
            WHERE i.TABLE_SCHEMA = c.TABLE_SCHEMA
              AND i.TABLE_NAME = c.TABLE_NAME
              AND i.COLUMN_NAME = c.COLUMN_NAME
              AND i.INDEX_NAME <> 'PRIMARY'
        ) THEN 'INDEX'
        ELSE NULL 
    END AS COLUMN_KEY
    FROM INFORMATION_SCHEMA.COLUMNS AS c
    WHERE c.TABLE_SCHEMA = ?
    AND c.TABLE_NAME = ?;`,
            params: [schema, table],
        };
    }
}