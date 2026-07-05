import { QueryInput } from "../../../config/types";
import { getTempTableName } from "../../../helpers/utilities";
import { escapeIdentifier, escapeLiteral } from "../../utils/escape";

const q = (name: string) => escapeIdentifier(name, "pgsql");

export class PostgresIndexQueryBuilder {
    static getPrimaryKeysQuery(table: string, schema?: string): QueryInput {
        const qualified = schema ? `${q(schema)}.${q(table)}` : q(table);
        return {
            query: `
                SELECT a.attname AS column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = ${escapeLiteral(qualified, "pgsql")}::regclass
                AND i.indisprimary;
            `,
            params: []
        };
    }

    static getForeignKeyConstraintsQuery(table: string, schema?: string): QueryInput {
        const qualified = schema ? `${q(schema)}.${q(table)}` : q(table);
        return {
            query: `
                SELECT conname AS constraint_name, conrelid::regclass AS table_name 
                FROM pg_constraint 
                WHERE confrelid = ${escapeLiteral(qualified, "pgsql")}::regclass;
            `,
            params: []
        };
    }

    static getViewDependenciesQuery(table: string, schema?: string): QueryInput {
        return {
            query: schema
                ? `SELECT viewname FROM pg_views WHERE schemaname = $1 AND definition LIKE '%' || $2 || '%';`
                : `SELECT viewname FROM pg_views WHERE definition LIKE '%' || $1 || '%';`,
            params: schema ? [schema, table] : [table]
        };
    }

    static getDropPrimaryKeyQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        return {
            query: `ALTER TABLE ${schemaPrefix}${q(table)} DROP CONSTRAINT ${q(`${table}_pkey`)};`,
            params: []
        };
    }

    static getDropUniqueConstraintQuery(table: string, indexName: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        return {
          query: `ALTER TABLE ${schemaPrefix}${q(table)} DROP CONSTRAINT ${q(indexName)};`,
          params: []
        };
    }

    static getAddPrimaryKeyQuery(table: string, primaryKeys: string[], schema?: string): QueryInput {
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        return {
            query: `ALTER TABLE ${schemaPrefix}${q(table)} ADD PRIMARY KEY (${primaryKeys.map(pk => q(pk)).join(", ")});`,
            params: []
        };
    }

    static getUniqueIndexesQuery(table: string, columnName?: string, schema?: string): QueryInput {
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

    static generateConstraintConflictBreakdownQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] }, schema?: string, stagingPrefix?: string): QueryInput {
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        const tempTable = getTempTableName(table, stagingPrefix);
        const t1 = "t1";
        const t2 = "t2";

        const conflictColumns = Object.entries(structure.uniques).map(([index_name, cols]) => {
          const condition = cols.map(col => `${t1}.${q(col)} = ${t2}.${q(col)}`).join(" AND ");
          const alias = index_name;

          return `  SUM(CASE WHEN ${condition} THEN 1 ELSE 0 END) AS ${q(alias)}`;
        });

        const primaryMismatch = structure.primary.length
          ? structure.primary.map(col => `${t1}.${q(col)} IS DISTINCT FROM ${t2}.${q(col)}`).join(" OR ")
          : "FALSE";

        const query = `SELECT
            ${conflictColumns.join(",\n")}
            FROM ${schemaPrefix}${q(table)} ${t1}
            JOIN ${schemaPrefix}${q(tempTable)} ${t2}
            ON (${primaryMismatch});
            `.trim();
      
        return {
          query,
          params: []
        };
    }
}