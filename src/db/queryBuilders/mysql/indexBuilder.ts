import { QueryInput } from "../../../config/types";

export class MySQLIndexQueryBuilder {
    static getPrimaryKeysQuery(table: string, schema?: string): QueryInput {
        return {
            query: `
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_NAME = ? 
                ${schema ? "AND TABLE_SCHEMA = ?" : ""}
                AND CONSTRAINT_NAME = 'PRIMARY';
            `,
            params: schema ? [table, schema] : [table]
        };
    }
    
    static getForeignKeyConstraintsQuery(table: string, schema?: string): QueryInput {
        return {
            query: `
                SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE REFERENCED_TABLE_NAME = ? 
                ${schema ? "AND TABLE_SCHEMA = ?" : ""};
            `,
            params: schema ? [table, schema] : [table]
        };
    }
    
    static getViewDependenciesQuery(table: string, schema?: string): QueryInput {
        return {
            query: `
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.VIEWS 
                WHERE VIEW_DEFINITION LIKE CONCAT('%', ?, '%') 
                ${schema ? "AND TABLE_SCHEMA = ?" : ""};
            `,
            params: schema ? [table, schema] : [table]
        };
    }    

    static getDropPrimaryKeyQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `\`${schema}\`.` : "";
        return {
            query: `ALTER TABLE ${schemaPrefix}\`${table}\` DROP PRIMARY KEY;`,
            params: []
        };
    }

    static getAddPrimaryKeyQuery(table: string, primaryKeys: string[], schema?: string): QueryInput {
        return {
            query: `ALTER TABLE \`${table}\` ADD PRIMARY KEY (${primaryKeys.map(pk => `\`${pk}\``).join(", ")});`,
            params: []
        };
    }

    static getUniqueIndexesQuery(table: string, columnName?: string, schema?: string): QueryInput {
        let query = `
            SELECT DISTINCT index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns
            FROM information_schema.statistics
            WHERE table_name = ?
            AND non_unique = 0
        `;
        
        const params = [table];
    
        if (columnName) {
            query += " AND column_name = ?";
            params.push(columnName);
        }

        if (schema) {
            query += " AND table_schema = ?";
            params.push(schema);
        }
    
        query += " GROUP BY index_name;";
    
        return { query, params };
    }
}