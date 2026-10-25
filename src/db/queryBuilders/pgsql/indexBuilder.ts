import type { QueryInput } from "../../../config/types";
import { getTempTableName } from "../../../helpers/utilities";
import { escapeIdentifier, escapeLiteral } from "../../utils/escape";
import { getAddPrimaryKeyQuery } from "../shared/indexBuilderShared";

const q = (name: string) => escapeIdentifier(name, "pgsql");

// A namespace of related query builders, not a data constant — see no-extraneous-class.
// eslint-disable-next-line @typescript-eslint/naming-convention
export const PostgresIndexQueryBuilder = {
    getPrimaryKeysQuery(table: string, schema?: string): QueryInput {
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
    },

    getForeignKeyConstraintsQuery(table: string, schema?: string): QueryInput {
        const qualified = schema ? `${q(schema)}.${q(table)}` : q(table);
        return {
            query: `
                SELECT conname AS constraint_name, conrelid::regclass AS table_name 
                FROM pg_constraint 
                WHERE confrelid = ${escapeLiteral(qualified, "pgsql")}::regclass;
            `,
            params: []
        };
    },

    getViewDependenciesQuery(table: string, schema?: string): QueryInput {
        return {
            query: schema
                ? `SELECT viewname FROM pg_views WHERE schemaname = $1 AND definition LIKE '%' || $2 || '%';`
                : `SELECT viewname FROM pg_views WHERE definition LIKE '%' || $1 || '%';`,
            params: schema ? [schema, table] : [table]
        };
    },

    getDropPrimaryKeyQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        return {
            query: `ALTER TABLE ${schemaPrefix}${q(table)} DROP CONSTRAINT IF EXISTS ${q(`${table}_pkey`)};`,
            params: []
        };
    },

    getDropUniqueConstraintQuery(table: string, indexName: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        return {
          query: `ALTER TABLE ${schemaPrefix}${q(table)} DROP CONSTRAINT ${q(indexName)};`,
          params: []
        };
    },

    getAddPrimaryKeyQuery(table: string, primaryKeys: string[], schema?: string): QueryInput {
        return getAddPrimaryKeyQuery("pgsql", table, primaryKeys, schema);
    },

    getUniqueIndexesQuery(table: string, columnName?: string, schema?: string): QueryInput {
        // pg_class.relname isn't schema-qualified, so without the pg_namespace join this could
        // match a same-named table's index in a DIFFERENT schema (MySQL's sibling already scopes
        // by table_schema).
        let query = `
            SELECT i.relname AS index_name, array_to_string(array_agg(a.attname), ', ') AS columns
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relname = $1
            AND ix.indisunique = true
        `;

        const params = [table];

        if (columnName) {
            query += ` AND a.attname = $${params.length + 1}`;
            params.push(columnName);
        }

        if (schema) {
            query += ` AND n.nspname = $${params.length + 1}`;
            params.push(schema);
        }

        query += " GROUP BY i.relname;";

        return { query, params };
    },

    generateConstraintConflictBreakdownQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] }, schema?: string, stagingPrefix?: string): QueryInput {
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
};