import { QueryInput } from "../../../config/types";
import { getTempTableName } from "../../../helpers/utilities";
import { escapeIdentifier, escapeLiteral } from "../../utils/escape";

const q = (name: string) => escapeIdentifier(name, "sqlserver");
const lit = (value: string | number | boolean) => escapeLiteral(value, "sqlserver");

export class SqlServerIndexQueryBuilder {
    static getPrimaryKeysQuery(table: string, schema?: string): QueryInput {
        const qualified = schema ? `${q(schema)}.${q(table)}` : q(table);
        return {
            query: `
                SELECT c.name AS column_name
                FROM sys.indexes i
                JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                WHERE i.object_id = OBJECT_ID(${lit(qualified)})
                  AND i.is_primary_key = 1
                ORDER BY ic.key_ordinal;
            `,
            params: []
        };
    }

    static getForeignKeyConstraintsQuery(table: string, schema?: string): QueryInput {
        const qualified = schema ? `${q(schema)}.${q(table)}` : q(table);
        return {
            query: `
                SELECT fk.name AS constraint_name, OBJECT_NAME(fk.parent_object_id) AS table_name
                FROM sys.foreign_keys fk
                WHERE fk.referenced_object_id = OBJECT_ID(${lit(qualified)});
            `,
            params: []
        };
    }

    static getViewDependenciesQuery(table: string, schema?: string): QueryInput {
        // Match views whose definition references the table name. sys.sql_modules holds the view
        // definition text; sys.views gives the view name.
        return {
            query: schema
                ? `SELECT v.name AS viewname
                   FROM sys.views v
                   JOIN sys.sql_modules m ON m.object_id = v.object_id
                   JOIN sys.schemas s ON s.schema_id = v.schema_id
                   WHERE s.name = @p0 AND m.definition LIKE '%' + @p1 + '%';`
                : `SELECT v.name AS viewname
                   FROM sys.views v
                   JOIN sys.sql_modules m ON m.object_id = v.object_id
                   WHERE m.definition LIKE '%' + @p0 + '%';`,
            params: schema ? [schema, table] : [table]
        };
    }

    static getDropPrimaryKeyQuery(table: string, schema?: string): QueryInput {
        // SQL Server PK constraint names are not fixed (no `_pkey` convention), so look up the
        // actual PK name and drop it dynamically via EXEC.
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        const qualified = `${schemaPrefix}${q(table)}`;
        return {
            query:
                `DECLARE @pk sysname; ` +
                `SELECT @pk = kc.name FROM sys.key_constraints kc ` +
                `WHERE kc.parent_object_id = OBJECT_ID(${lit(qualified)}) AND kc.type = 'PK'; ` +
                `IF @pk IS NOT NULL EXEC('ALTER TABLE ${qualified} DROP CONSTRAINT [' + @pk + ']');`,
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
        // Unique indexes on the table, columns comma-joined via STRING_AGG. Table name is bound
        // as @p0; the schema filter (when given) and the optional column filter follow as the
        // next @p placeholders, in params-array order.
        let query = `
            SELECT i.name AS index_name, STRING_AGG(c.name, ', ') AS columns
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            JOIN sys.tables t ON t.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE i.is_unique = 1
              AND t.name = @p0
        `;
        const params: string[] = [table];

        if (schema) {
            query += ` AND s.name = @p${params.length}`;
            params.push(schema);
        }
        if (columnName) {
            query += ` AND c.name = @p${params.length}`;
            params.push(columnName);
        }

        query += ` GROUP BY i.name;`;

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

        // NULL-safe "distinct" test for T-SQL (no IS DISTINCT FROM): a column differs if the
        // values are unequal, or exactly one side is NULL.
        const primaryMismatch = structure.primary.length
            ? structure.primary
                .map(col => `((${t1}.${q(col)} <> ${t2}.${q(col)}) OR (${t1}.${q(col)} IS NULL AND ${t2}.${q(col)} IS NOT NULL) OR (${t1}.${q(col)} IS NOT NULL AND ${t2}.${q(col)} IS NULL))`)
                .join(" OR ")
            : "1=0";

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
