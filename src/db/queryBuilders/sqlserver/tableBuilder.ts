import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig, ColumnDefinition } from "../../../config/types";
import { sqlServerConfig } from "../../config/sqlServerConfig";
import { DialectConfig } from "../../../config/types";
import { generateSafeConstraintName, getTempTableName } from "../../../helpers/utilities";
import { escapeIdentifier, escapeLiteral, assertSafeTypeToken, assertSafeLength, renderColumnDefault } from "../../utils/escape";

const dialectConfig = sqlServerConfig;
const q = (name: string) => escapeIdentifier(name, "sqlserver");
const lit = (value: string | number | boolean) => escapeLiteral(value, "sqlserver");

// SQL Server's default schema when the caller doesn't supply one. The `IF NOT EXISTS`
// guards compare against sys.schemas.name, so an empty schema would match nothing.
const DEFAULT_SCHEMA = "dbo";

/**
 * Render the SQL Server column type token (without NOT NULL / DEFAULT / IDENTITY). The type
 * is translated local→server via the dialect config, then lengths are appended separately so
 * `assertSafeTypeToken` only ever sees the bare token (it rejects parentheses).
 *
 * Text-like local types (text/mediumtext/longtext/json) and any varchar whose length exceeds
 * 4000 render as `NVARCHAR(MAX)`; varchar/text within range render as `nvarchar(n)`. Integer
 * types take no length; decimal renders as `decimal(p,s)`.
 */
function renderColumnType(column: ColumnDefinition, cfg: DialectConfig): string {
    const localType = (column.type || "").toLowerCase();
    let serverType = localType;
    if (cfg.translate.localToServer[localType]) {
        serverType = cfg.translate.localToServer[localType];
    }

    // Unbounded text local types always map to NVARCHAR(MAX) — the "(max)" token can't pass
    // through assertSafeTypeToken, so it is appended after asserting the bare token.
    const isUnboundedText = ["text", "mediumtext", "longtext", "json"].includes(localType);
    // A varchar wider than SQL Server's 4000 NVARCHAR limit must also spill to NVARCHAR(MAX).
    const isOverlongVarchar =
        (localType === "varchar" || serverType === "nvarchar") &&
        typeof column.length === "number" &&
        column.length > 4000;

    if (isUnboundedText || isOverlongVarchar) {
        assertSafeTypeToken("nvarchar");
        return "nvarchar(MAX)";
    }

    assertSafeTypeToken(serverType);
    let rendered = serverType;

    if (column.length && cfg.requireLength.includes(serverType)) {
        rendered += `(${assertSafeLength(column.length)}${column.decimal && cfg.decimals.includes(serverType) ? `,${assertSafeLength(column.decimal || 0)}` : ""})`;
    }

    return rendered;
}

export class SqlServerTableQueryBuilder {
    static getCreateTableQuery(table: string, headers: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput[] {
        const maxIndexCount = dialectConfig.maxIndexCount || 64;
        let remainingIndexSlots = maxIndexCount;
        let sqlQueries: QueryInput[] = [];
        const schema = databaseConfig?.schema || DEFAULT_SCHEMA;
        const schemaPrefix = `${q(schema)}.`;
        const primaryKeys: string[] = [];
        const uniqueKeys: string[] = [];
        const indexes: string[] = [];

        let columnDefs = "";

        for (const [columnName, column] of Object.entries(headers)) {
            if (!column.type) throw new Error(`Missing type for column ${columnName}`);

            let columnDef = `${q(columnName)} ${renderColumnType(column, dialectConfig)}`;

            // Auto-increment uses IDENTITY(1,1) in SQL Server (there is no SERIAL family). It
            // is appended to the rendered type; SQL Server accepts IDENTITY on int/bigint/etc.
            if (column.autoIncrement) {
                columnDef += ` IDENTITY(1,1)`;
            }

            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined && !column.autoIncrement) {
                columnDef += ` DEFAULT ${renderColumnDefault(column.default, dialectConfig)}`;
            }

            if (column.primary) primaryKeys.push(columnName);
            if (column.unique) uniqueKeys.push(columnName);
            if (column.index) indexes.push(columnName);

            columnDefs += `${columnDef},\n`;
        }

        if (primaryKeys.length) {
            columnDefs += `PRIMARY KEY (${primaryKeys.map(q).join(", ")}),\n`;
            remainingIndexSlots--; // 🔢 count primary key toward the limit
        }
        const includedUniqueKeys = uniqueKeys.slice(0, remainingIndexSlots);
        if (includedUniqueKeys.length) {
            columnDefs += `${includedUniqueKeys
                .map((key) => {
                    const columnName = key;
                    const constraintName = generateSafeConstraintName(table, columnName, 'unique');
                    remainingIndexSlots--;
                    return `CONSTRAINT ${q(constraintName)} UNIQUE(${q(columnName)})`;
                })
                .join(', ')},\n`;
        }

        columnDefs = columnDefs.slice(0, -2); // strip trailing ",\n"

        // SQL Server has no `CREATE TABLE IF NOT EXISTS`; guard with sys.tables/sys.schemas.
        const createTable =
            `IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id ` +
            `WHERE t.name = ${lit(table)} AND s.name = ${lit(schema)})\n` +
            `BEGIN\n` +
            `CREATE TABLE ${schemaPrefix}${q(table)} (\n${columnDefs}\n);\n` +
            `END;`;
        sqlQueries.push({ query: createTable, params: [] }); // Store CREATE TABLE query as first item

        // Create indexes separately (one QueryInput per index), same as the pg builder.
        const limitedIndexes = indexes.slice(0, remainingIndexSlots);
        for (const index of limitedIndexes) {
            if (!index) continue; // Skip empty index names

            const indexName = generateSafeConstraintName(table, index, 'index');
            sqlQueries.push({
                query: `CREATE INDEX ${q(indexName)} ON ${schemaPrefix}${q(table)} (${q(index)});`,
                params: []
            });
        }

        return sqlQueries;
    }

    static getAlterTableQuery(table: string, changes: AlterTableChanges, schema?: string, databaseConfig?: DatabaseConfig): QueryInput[] {
        let queries: QueryInput[] = [];
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        const qualified = `${schemaPrefix}${q(table)}`;

        // ✅ Handle `RENAME COLUMN` — SQL Server renames via sp_rename. The NEW name is passed
        // WITHOUT brackets/qualification; the OLD name is the fully-qualified column. Emit each
        // rename first, one QueryInput per rename (sp_rename cannot be batched).
        changes.renameColumns.forEach(({ oldName, newName }) => {
            const oldQualified = schema
                ? `${q(schema)}.${q(table)}.${q(oldName)}`
                : `${q(table)}.${q(oldName)}`;
            queries.push({
                query: `EXEC sp_rename ${lit(oldQualified)}, ${lit(newName)}, 'COLUMN';`,
                params: []
            });
        });

        // ✅ Handle `ADD COLUMN` — SQL Server uses `ADD` (not `ADD COLUMN`) and multiple adds
        // can be comma-separated under a single `ALTER TABLE ... ADD`.
        const addClauses: string[] = [];
        for (const [columnName, column] of Object.entries(changes.addColumns)) {
            if (!column.type) { throw new Error(`Attempted to add a new column '${columnName}' without a type`); }

            let columnDef = `${q(columnName)} ${renderColumnType(column, dialectConfig)}`;
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) {
                columnDef += ` DEFAULT ${renderColumnDefault(column.default, dialectConfig)}`;
            } else if (!column.allowNull && column.calculated) {
                // A NOT NULL calculated timestamp added to a table that may already contain rows
                // needs a DEFAULT, or ADD fails the NOT NULL constraint on those pre-existing
                // rows. Backfill with CURRENT_TIMESTAMP (every calculated column is a timestamp).
                columnDef += ` DEFAULT CURRENT_TIMESTAMP`;
            }
            addClauses.push(columnDef);
        }
        if (addClauses.length > 0) {
            queries.push({ query: `ALTER TABLE ${qualified} ADD ${addClauses.join(", ")};`, params: [] });
        }

        // ✅ Handle `MODIFY` (type / nullability) — SQL Server can't batch multiple ALTER COLUMN
        // in one statement, so each column is its own `ALTER TABLE ... ALTER COLUMN` statement.
        // There is no per-column `SET DEFAULT` in ALTER COLUMN: changing a default means
        // dropping and re-adding a default constraint, which is deferred here — a default change
        // is skipped rather than emitting an invalid statement (see note below).
        const handledNullable = new Set<string>();
        for (const [columnName, column] of Object.entries(changes.modifyColumns)) {
            handledNullable.add(columnName);
            const wantsNull = column.allowNull || changes.nullableColumns.includes(columnName);

            let serverType: string;
            if (column.type) {
                serverType = renderColumnType(column, dialectConfig);
            } else {
                // No type change requested: SQL Server ALTER COLUMN still requires a type, so
                // this branch only applies when previousType is known. Fall back to the current
                // type token if available, otherwise skip (nothing safe to emit).
                if (!column.previousType) {
                    if (wantsNull) {
                        // Can't ALTER COLUMN without a type; nothing valid to emit for a bare
                        // nullability change without a known type — skip.
                    }
                    continue;
                }
                serverType = renderColumnType({ ...column, type: column.previousType }, dialectConfig);
            }

            // NOTE: SQL Server default-constraint handling (drop + re-add a DEFAULT constraint)
            // is deferred — a default change in a modify is intentionally NOT emitted here to
            // avoid an invalid `ALTER COLUMN ... SET DEFAULT` statement.
            const nullability = wantsNull ? "NULL" : "NOT NULL";
            queries.push({
                query: `ALTER TABLE ${qualified} ALTER COLUMN ${q(columnName)} ${serverType} ${nullability};`,
                params: []
            });
        }

        // ✅ Handle `NULLABLE COLUMNS` separately (if not already modified). Without a type we
        // cannot emit a valid SQL Server ALTER COLUMN, and a modify above already handled any
        // column present in modifyColumns, so these are left to the modify path when typed.
        changes.nullableColumns.forEach(columnName => {
            if (handledNullable.has(columnName)) return;
            const meta = changes.modifyColumns[columnName];
            const typeSource = meta?.type || meta?.previousType;
            if (!typeSource) return; // No known type → can't emit a valid ALTER COLUMN
            const serverType = renderColumnType({ ...(meta || { type: typeSource }), type: typeSource }, dialectConfig);
            queries.push({
                query: `ALTER TABLE ${qualified} ALTER COLUMN ${q(columnName)} ${serverType} NULL;`,
                params: []
            });
        });

        // ✅ Handle `DROP COLUMN` — gate on databaseConfig.deleteColumns exactly like pg. SQL
        // Server allows comma-separated columns under a single `DROP COLUMN`.
        if (databaseConfig?.deleteColumns && changes.dropColumns.length > 0) {
            const dropCols = changes.dropColumns.map(c => q(c)).join(", ");
            queries.push({ query: `ALTER TABLE ${qualified} DROP COLUMN ${dropCols};`, params: [] });
        }

        return queries;
    }

    static getDropTableQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        const qualified = `${schemaPrefix}${q(table)}`;
        return {
            query: `IF OBJECT_ID(${lit(qualified)}, 'U') IS NOT NULL DROP TABLE ${qualified};`,
            params: []
        };
    }

    static getCreateTempTableQuery(table: string, schema?: string, stagingPrefix?: string): QueryInput {
        const tempTableName = getTempTableName(table, stagingPrefix);
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        const stagingQualified = `${schemaPrefix}${q(tempTableName)}`;
        // Clone the source table structure empty. `SELECT * INTO ... WHERE 1=0` creates the
        // staging table with matching columns but no rows; guard so it is only created once.
        return {
            query: `IF OBJECT_ID(${lit(stagingQualified)}, 'U') IS NULL ` +
                `SELECT * INTO ${stagingQualified} FROM ${schemaPrefix}${q(table)} WHERE 1=0;`,
            params: []
        };
    }

    static getTableExistsQuery(schema: string, table: string): QueryInput {
        return {
            query: "SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @p0 AND TABLE_NAME = @p1",
            params: [schema, table],
        };
    }

    static getTableMetaDataQuery(schema: string, table: string): QueryInput {
        // Columns come from INFORMATION_SCHEMA.COLUMNS; COLUMN_KEY is derived from sys.* per
        // column. LENGTH mirrors the pg CONCAT/precision rules; nvarchar(max) reports -1 for
        // CHARACTER_MAXIMUM_LENGTH, so -1 is mapped to NULL (no length). Deduplicated to one
        // row per column via GROUP BY (SQL Server has no DISTINCT ON).
        return {
            query: `SELECT
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.COLUMN_DEFAULT,
                    CASE
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN CONCAT(c.NUMERIC_PRECISION, ',', c.NUMERIC_SCALE)
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NULL THEN CAST(c.NUMERIC_PRECISION AS VARCHAR)
                        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL AND c.CHARACTER_MAXIMUM_LENGTH <> -1 THEN CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR)
                        ELSE NULL
                    END AS LENGTH,
                    c.IS_NULLABLE,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM sys.indexes i
                            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id
                            WHERE i.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
                              AND i.is_primary_key = 1
                              AND sc.name = c.COLUMN_NAME
                        ) THEN 'PRIMARY'
                        WHEN EXISTS (
                            SELECT 1
                            FROM sys.indexes i
                            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id
                            WHERE i.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
                              AND (i.is_unique_constraint = 1 OR (i.is_unique = 1 AND i.is_primary_key = 0))
                              AND sc.name = c.COLUMN_NAME
                        ) THEN 'UNIQUE'
                        WHEN EXISTS (
                            SELECT 1
                            FROM sys.indexes i
                            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id
                            WHERE i.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
                              AND i.type_desc = 'NONCLUSTERED'
                              AND i.is_primary_key = 0
                              AND i.is_unique_constraint = 0
                              AND sc.name = c.COLUMN_NAME
                        ) THEN 'INDEX'
                        ELSE NULL
                    END AS COLUMN_KEY,
                    (
                        SELECT STRING_AGG(i.name, ',') WITHIN GROUP (ORDER BY i.name)
                        FROM sys.indexes i
                        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                        JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id
                        WHERE i.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
                          AND i.is_unique = 1
                          AND i.is_primary_key = 0
                          AND sc.name = c.COLUMN_NAME
                    ) AS UNIQUE_INDEX_NAME,
                    -- IDENTITY flag: SQL Server's analogue of MySQL AUTO_INCREMENT / Postgres nextval.
                    -- Wrapped in MAX() so it needn't join the GROUP BY (constant per grouped column);
                    -- normalizeResultKeys lowercases IS_IDENTITY → is_identity for deriveColumnMetadata.
                    MAX(COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity')) AS IS_IDENTITY
                FROM INFORMATION_SCHEMA.COLUMNS AS c
                WHERE c.TABLE_SCHEMA = @p0 AND c.TABLE_NAME = @p1
                GROUP BY c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_DEFAULT, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.CHARACTER_MAXIMUM_LENGTH, c.IS_NULLABLE, c.TABLE_SCHEMA, c.TABLE_NAME;
                `,
            params: [schema, table],
        };
    }

    static getSplitTablesQuery(table: string, schema?: string): QueryInput {
        // Match the `<table>__part_<digits>` partition tables. SQL Server has no SIMILAR TO, so
        // the "suffix is all digits" test is emulated: LIKE '...__part_%' to require the prefix,
        // and NOT LIKE '...__part_%[^0-9]%' to reject any non-digit after the prefix. @p1 carries
        // the base table name (escaped for LIKE wildcards would over-complicate; the base names
        // here are autosql-generated identifiers).
        return {
            query: `SELECT
                    c.COLUMN_NAME,
                    c.TABLE_NAME,
                    c.DATA_TYPE,
                    c.COLUMN_DEFAULT,
                    CASE
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN CONCAT(c.NUMERIC_PRECISION, ',', c.NUMERIC_SCALE)
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NULL THEN CAST(c.NUMERIC_PRECISION AS VARCHAR)
                        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL AND c.CHARACTER_MAXIMUM_LENGTH <> -1 THEN CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR)
                        ELSE NULL
                    END AS LENGTH,
                    c.IS_NULLABLE,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM sys.indexes i
                            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id
                            WHERE i.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
                              AND i.is_primary_key = 1
                              AND sc.name = c.COLUMN_NAME
                        ) THEN 'PRIMARY'
                        WHEN EXISTS (
                            SELECT 1
                            FROM sys.indexes i
                            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id
                            WHERE i.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
                              AND (i.is_unique_constraint = 1 OR (i.is_unique = 1 AND i.is_primary_key = 0))
                              AND sc.name = c.COLUMN_NAME
                        ) THEN 'UNIQUE'
                        WHEN EXISTS (
                            SELECT 1
                            FROM sys.indexes i
                            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id
                            WHERE i.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
                              AND i.type_desc = 'NONCLUSTERED'
                              AND i.is_primary_key = 0
                              AND i.is_unique_constraint = 0
                              AND sc.name = c.COLUMN_NAME
                        ) THEN 'INDEX'
                        ELSE NULL
                    END AS COLUMN_KEY,
                    -- IDENTITY flag (see getTableMetaDataQuery) — MAX() keeps it out of the GROUP BY.
                    MAX(COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity')) AS IS_IDENTITY
                FROM INFORMATION_SCHEMA.COLUMNS AS c
                WHERE c.TABLE_SCHEMA = @p0
                  AND c.TABLE_NAME LIKE @p1 + '__part_%'
                  AND c.TABLE_NAME NOT LIKE @p1 + '__part_%[^0-9]%'
                GROUP BY c.COLUMN_NAME, c.TABLE_NAME, c.DATA_TYPE, c.COLUMN_DEFAULT, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.CHARACTER_MAXIMUM_LENGTH, c.IS_NULLABLE, c.TABLE_SCHEMA;
            `,
            params: [schema, table],
        };
    }
}
