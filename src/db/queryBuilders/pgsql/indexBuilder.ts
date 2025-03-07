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

    static getUniqueIndexesQuery(table: string, columnName?: string): QueryInput {
        let query = `
            SELECT i.relname AS indexname, array_to_string(array_agg(a.attname), ', ') AS columns
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relname = $1
            AND ix.indisunique = true
        `;
        
        const params = [table];
    
        if (columnName) {
            query += " AND a.attname = $2";
            params.push(columnName);
        }
    
        query += " GROUP BY i.relname;";
    
        return { query, params };
    }    
}