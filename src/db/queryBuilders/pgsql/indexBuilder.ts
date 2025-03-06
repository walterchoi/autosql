import { QueryInput } from "../../../config/types";

export class PostgresIndexQueryBuilder {
    static getPrimaryKeysQuery(table: string): QueryInput {
        return {
            query: `
                SELECT a.attname AS column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass
                AND i.indisprimary;
            `,
            params: [table]
        };
    }

    static getForeignKeyConstraintsQuery(table: string): QueryInput {
        return {
            query: `
                SELECT conname AS constraint_name, conrelid::regclass AS table_name 
                FROM pg_constraint 
                WHERE confrelid = $1::regclass;
            `,
            params: [table]
        };
    }

    static getViewDependenciesQuery(table: string): QueryInput {
        return {
            query: `
                SELECT viewname 
                FROM pg_views 
                WHERE definition LIKE '%' || $1 || '%';
            `,
            params: [table]
        };
    }

    static getDropPrimaryKeyQuery(table: string): QueryInput {
        return {
            query: `ALTER TABLE "${table}" DROP CONSTRAINT "${table}_pkey";`,
            params: []
        };
    }

    static getAddPrimaryKeyQuery(table: string, primaryKeys: string[]): QueryInput {
        return {
            query: `ALTER TABLE "${table}" ADD PRIMARY KEY (${primaryKeys.map(pk => `"${pk}"`).join(", ")});`,
            params: []
        };
    }
}
