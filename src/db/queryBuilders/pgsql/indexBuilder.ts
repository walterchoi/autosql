import { QueryInput } from "../../../config/types";
import { getTempTableName } from "../../../helpers/utilities";

export class PostgresIndexQueryBuilder {
    static getPrimaryKeysQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `"${schema}".` : "";
        return {
            query: `
                SELECT a.attname AS column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '${schemaPrefix}${table}'::regclass
                AND i.indisprimary;
            `,
            params: []
        };
    }

    static getForeignKeyConstraintsQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `"${schema}".` : "";
        return {
            query: `
                SELECT conname AS constraint_name, conrelid::regclass AS table_name 
                FROM pg_constraint 
                WHERE confrelid = '${schemaPrefix}${table}'::regclass;
            `,
            params: []
        };
    }

    static getViewDependenciesQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `"${schema}".` : "";
        return {
            query: `
                SELECT viewname 
                FROM pg_views 
                WHERE schemaname = '${schema}' 
                AND definition LIKE '%' || $1 || '%';
            `,
            params: [table]
        };
    }

    static getDropPrimaryKeyQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `"${schema}".` : "";
        return {
            query: `ALTER TABLE ${schemaPrefix}"${table}" DROP CONSTRAINT "${table}_pkey";`,
            params: []
        };
    }

    static getDropUniqueConstraintQuery(table: string, indexName: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `"${schema}".` : "";
        return {
          query: `ALTER TABLE ${schemaPrefix}"${table}" DROP CONSTRAINT "${indexName}";`,
          params: []
        };
    }

    static getAddPrimaryKeyQuery(table: string, primaryKeys: string[], schema?: string): QueryInput {
        const schemaPrefix = schema ? `"${schema}".` : "";
        return {
            query: `ALTER TABLE ${schemaPrefix}"${table}" ADD PRIMARY KEY (${primaryKeys.map(pk => `"${pk}"`).join(", ")});`,
            params: []
        };
    }

    static getUniqueIndexesQuery(table: string, columnName?: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `"${schema}".` : "";
        let query = `
            SELECT i.relname AS index_name, array_to_string(array_agg(a.attname), ', ') AS columns
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

    static generateConstraintConflictBreakdownQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] }, schema?: string): QueryInput {
        const schemaPrefix = schema ? `"${schema}".` : "";
        const tempTable = getTempTableName(table);
        const t1 = "t1";
        const t2 = "t2";
      
        const conflictColumns = Object.entries(structure.uniques).map(([index_name, cols]) => {
          const condition = cols.map(col => `${t1}."${col}" = ${t2}."${col}"`).join(" AND ");
          const alias = index_name;
      
          return `  SUM(CASE WHEN ${condition} THEN 1 ELSE 0 END) AS "${alias}"`;
        });
      
        const primaryMismatch = structure.primary.length
          ? structure.primary.map(col => `${t1}."${col}" IS DISTINCT FROM ${t2}."${col}"`).join(" OR ")
          : "FALSE";
      
        const query = `SELECT
            ${conflictColumns.join(",\n")}
            FROM ${schemaPrefix}"${table}" ${t1}
            JOIN ${schemaPrefix}"${tempTable}" ${t2}
            ON (${primaryMismatch});
            `.trim();
      
        return {
          query,
          params: []
        };
    }
}