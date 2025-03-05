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
}