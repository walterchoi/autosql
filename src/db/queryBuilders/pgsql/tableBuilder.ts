import { ColumnDefinition, QueryInput, AlterTableChanges } from "../../../config/types";
import { pgsqlConfig } from "../../config/pgsqlConfig";
import { compareHeaders } from '../../../helpers/headers';
const dialectConfig = pgsqlConfig

export class PostgresTableQueryBuilder {
    static getCreateTableQuery(table: string, headers: { [column: string]: ColumnDefinition }[]): QueryInput[] {
        let sqlQueries: QueryInput[] = [];
        let sqlQuery = `CREATE TABLE IF NOT EXISTS "${table}" (\n`;
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
    
            let columnDef = `"${columnName}" ${columnType}`;
    
            // Handle column lengths
            if (column.length && dialectConfig.require_length.includes(columnType)) {
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
                const replacement = dialectConfig.default_translation[column.default] || column.default;
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
            sqlQueries.push({ query: `CREATE INDEX "${index.replace(/"/g, "")}_idx" ON "${table}" ("${index.replace(/"/g, "")}");`, params: []});
        }
    
        return sqlQueries;
    }    
    // ✅ Get changes using compareHeaders()
    //const { addColumns, modifyColumns } = compareHeaders(oldHeaders, newHeaders, pgsqlConfig);

    static getAlterTableQuery(table: string, changes: AlterTableChanges): QueryInput[] {
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
        changes.addColumns.forEach(columnObj => {
            const columnName = Object.keys(columnObj)[0];
            const column = columnObj[columnName];
    
            let columnDef = `"${columnName}" ${column.type}`;
            if (column.length && !pgsqlConfig.no_length.includes(column.type ?? "")) {
                columnDef += `(${column.length}${column.decimal ? `,${column.decimal}` : ""})`;
            }
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) columnDef += ` DEFAULT '${column.default}'`;
    
            alterStatements.push(`ADD COLUMN ${columnDef}`);
        });
    
        // ✅ Handle `ALTER COLUMN` - Consolidate changes per column
        const alterColumnMap: { [key: string]: string[] } = {};
    
        changes.modifyColumns.forEach(columnObj => {
            const columnName = Object.keys(columnObj)[0];
            const column = columnObj[columnName];
    
            if (!alterColumnMap[columnName]) {
                alterColumnMap[columnName] = [];
            }
    
            if (column.type) {
                let columnDef = `SET DATA TYPE ${column.type}`;
                if (column.length && !pgsqlConfig.no_length.includes(column.type ?? "")) {
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
        });
    
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
    
        // ✅ Handle `REMOVE UNIQUE CONSTRAINT`
        changes.noLongerUnique.forEach(columnName => {
            alterStatements.push(`DROP INDEX IF EXISTS "${columnName}_unique"`);
        });
    
        // ✅ Combine all `ALTER TABLE` statements
        if (alterStatements.length > 0) {
            queries.push({ query: `ALTER TABLE "${table}" ${alterStatements.join(", ")};`, params: [] });
        }
    
        return queries;
    }    

    static getDropTableQuery(table: string): QueryInput {
        return { query: `DROP TABLE IF EXISTS "${table}";`, params: []};
    }
}