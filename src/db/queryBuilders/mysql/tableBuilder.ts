import { ColumnDefinition, QueryInput, AlterTableChanges } from "../../../config/types";
import { mysqlConfig } from "../../config/mysqlConfig";
import { compareHeaders } from '../../../helpers/headers';
const dialectConfig = mysqlConfig

export class MySQLTableQueryBuilder {
    static getCreateTableQuery(table: string, headers: { [column: string]: ColumnDefinition }[]): QueryInput[] {
        let sqlQueries: QueryInput[] = [];
        let sqlQuery = `CREATE TABLE IF NOT EXISTS \`${table}\` (\n`;
        let primaryKeys: string[] = [];
        let uniqueKeys: string[] = [];
        let indexes: string[] = [];
    
        for (const columnObj of headers) {
            const columnName = Object.keys(columnObj)[0]; // Extract column name
            const column = columnObj[columnName];
    
            if (!column.type) throw new Error(`Missing type for column ${columnName}`);
    
            let columnType = column.type.toLowerCase();
            if (dialectConfig.translate.local_to_server[columnType]) {
                columnType = dialectConfig.translate.local_to_server[columnType];
            }
    
            let columnDef = `\`${columnName}\` ${columnType}`;
    
            // Handle column lengths
            if (column.length && dialectConfig.require_length.includes(columnType)) {
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
                const replacement = dialectConfig.default_translation[column.default];
                columnDef += ` DEFAULT ${replacement ? replacement : column.default}`;
            }
    
            if (column.primary) primaryKeys.push(`\`${columnName}\``);
            if (column.unique) uniqueKeys.push(`\`${columnName}\``);
            if (column.index) indexes.push(`\`${columnName}\``);
    
            sqlQuery += `${columnDef},\n`;
        }
    
        if (primaryKeys.length) sqlQuery += `PRIMARY KEY (${primaryKeys.join(", ")}),\n`;
        if (uniqueKeys.length) sqlQuery += `${uniqueKeys.map((key) => `UNIQUE(${key})`).join(", ")},\n`;
    
        sqlQuery = sqlQuery.slice(0, -2) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;";
        sqlQueries.push({query: sqlQuery, params: []}); // Store CREATE TABLE query as first item
    
        // Create indexes separately
        for (const index of indexes) {
            sqlQueries.push({ query: `CREATE INDEX \`${index.replace(/`/g, "")}_idx\` ON \`${table}\` (\`${index.replace(/`/g, "")}\`);`, params: []});
        }
    
        return sqlQueries;
    }

    static getAlterTableQuery(table: string, changes: AlterTableChanges): QueryInput[] {
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
        changes.addColumns.forEach(columnObj => {
            const columnName = Object.keys(columnObj)[0];
            const column = columnObj[columnName];
    
            let columnDef = `\`${columnName}\` ${column.type}`;
            if (column.length && !dialectConfig.no_length.includes(column.type || "")) {
                columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
            }
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) columnDef += ` DEFAULT '${column.default}'`;
    
            alterStatements.push(`ADD COLUMN ${columnDef}`);
        });
    
        // ✅ Handle `MODIFY COLUMN` - Include `NULL` conditionally
        changes.modifyColumns.forEach(columnObj => {
            const columnName = Object.keys(columnObj)[0];
            const column = columnObj[columnName];

            let columnDef = `\`${columnName}\` ${column.type}`;
            if (column.length && !dialectConfig.no_length.includes(column.type || "")) {
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
        });

        // ✅ Handle standalone `NULLABLE COLUMNS` not in modifyColumns
        changes.nullableColumns.forEach(columnName => {
            if (!changes.modifyColumns.some(col => Object.keys(col)[0] === columnName)) {
                alterStatements.push(`MODIFY COLUMN \`${columnName}\` DROP NOT NULL`);
            }
        });
    
        // ✅ Handle `REMOVE UNIQUE CONSTRAINT`
        changes.noLongerUnique.forEach(columnName => {
            alterStatements.push(`DROP INDEX \`${columnName}_unique\``);
        });
    
        // ✅ Combine all `ALTER TABLE` statements
        if (alterStatements.length > 0) {
            queries.push({ query: `ALTER TABLE \`${table}\` ${alterStatements.join(", ")};`, params: [] });
        }
    
        return queries;
    }
    

    static getDropTableQuery(table: string): QueryInput {
        return {query: `DROP TABLE IF EXISTS \`${table}\`;`, params: []};
    }
}