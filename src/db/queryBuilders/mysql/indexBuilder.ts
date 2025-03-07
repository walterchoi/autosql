import { QueryInput } from "../../../config/types";

export class MySQLIndexQueryBuilder {
    static getPrimaryKeysQuery(table: string): QueryInput {
        return {
            query: `
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_NAME = ? 
                AND CONSTRAINT_NAME = 'PRIMARY';
            `,
            params: [table]
        };
    }

    static getForeignKeyConstraintsQuery(table: string): QueryInput {
        return {
            query: `
                SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE REFERENCED_TABLE_NAME = ?;
            `,
            params: [table]
        };
    }

    static getViewDependenciesQuery(table: string): QueryInput {
        return {
            query: `
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.VIEWS 
                WHERE VIEW_DEFINITION LIKE CONCAT('%', ?, '%');
            `,
            params: [table]
        };
    }

    static getDropPrimaryKeyQuery(table: string): QueryInput {
        return {
            query: `ALTER TABLE \`${table}\` DROP PRIMARY KEY;`,
            params: []
        };
    }

    static getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput {
        return {
            query: `ALTER TABLE \`${table}\` ADD PRIMARY KEY (${primaryKeys.map(pk => `\`${pk}\``).join(", ")});`,
            params: []
        };
    }

    static getUniqueIndexesQuery(table: string, columnName?: string): QueryInput {
        let query = `
            SELECT DISTINCT index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns
            FROM information_schema.statistics
            WHERE table_schema = DATABASE() 
            AND table_name = ?
            AND non_unique = 0
        `;
        
        const params = [table];
    
        if (columnName) {
            query += " AND column_name = ?";
            params.push(columnName);
        }
    
        query += " GROUP BY index_name;";
    
        return { query, params };
    }    
}