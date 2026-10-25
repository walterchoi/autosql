import type { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig } from "../../../config/types";
import { MYSQL_CONFIG } from "../../config/mysqlConfig";
import { getUsingClause } from "./alterTableTypeConversion";
import { getTempTableName } from "../../../helpers/utilities";
import { escapeIdentifier, assertSafeTypeToken, assertSafeLength, renderColumnDefault } from "../../utils/escape";
import { buildCreateIndexQueries } from "../shared/tableBuilderShared";
const DIALECT_CONFIG = MYSQL_CONFIG
const q = (name: string) => escapeIdentifier(name, "mysql");

// A namespace of related query builders, not a data constant — see no-extraneous-class.
// eslint-disable-next-line @typescript-eslint/naming-convention
export const MySQLTableQueryBuilder = {
    getCreateTableQuery(table: string, headers: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput[] {
        const maxIndexCount = DIALECT_CONFIG.maxIndexCount || 64;
        let remainingIndexSlots = maxIndexCount;
        const sqlQueries: QueryInput[] = [];
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        let sqlQuery = `CREATE TABLE IF NOT EXISTS ${schemaPrefix}${q(table)} (\n`;
        const primaryKeys: string[] = [];
        const uniqueKeys: string[] = [];
        const indexes: string[] = [];
    
        for (const [columnName, column] of Object.entries(headers)) {
            if (!column.type) throw new Error(`Missing type for column ${columnName}`);
    
            let columnType = column.type.toLowerCase();
            if (DIALECT_CONFIG.translate.localToServer[columnType]) {
                columnType = DIALECT_CONFIG.translate.localToServer[columnType];
            }
    
            assertSafeTypeToken(columnType);
            let columnDef = `${q(columnName)} ${columnType}`;

            // Never emit a display width for `tinyint`: it's cosmetic, and `tinyint(1)` is the
            // boolean convention — a non-boolean tinyint(1) reads back as boolean and triggers
            // destructive re-typing on the next ingest.
            if (column.length && !DIALECT_CONFIG.noLength.includes(columnType) && columnType !== "tinyint") {
                columnDef += `(${assertSafeLength(column.length)}${column.decimal && DIALECT_CONFIG.decimals.includes(columnType) ? `,${assertSafeLength(column.decimal || 0)}` : ""})`;
            }

            // MySQL has no native boolean type.
            if (column.type === "boolean") {
                columnDef = `${q(columnName)} TINYINT(1)`;
            }

            if (column.autoIncrement) {
                if (["int", "bigint", "smallint", "tinyint"].includes(columnType)) {
                    columnDef += " AUTO_INCREMENT";
                } else {
                    throw new Error(`AUTO_INCREMENT is not supported on type ${columnType} in MySQL`);
                }
            }
    
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) {
                columnDef += ` DEFAULT ${renderColumnDefault(column.default, DIALECT_CONFIG)}`;
            }
    
            if (column.primary) primaryKeys.push(columnName);
            if (column.unique) uniqueKeys.push(columnName);
            if (column.index) indexes.push(columnName);
    
            sqlQuery += `${columnDef},\n`;
        }
    
        if (primaryKeys.length) {
            sqlQuery += `PRIMARY KEY (${primaryKeys.map(q).join(", ")}),\n`;
            remainingIndexSlots--; // count primary key toward the limit
        }
        const includedUniqueKeys = uniqueKeys.slice(0, remainingIndexSlots);
        if (includedUniqueKeys.length) {
            sqlQuery += `${includedUniqueKeys.map((key) => `UNIQUE(${q(key)})`).join(", ")},\n`;
            remainingIndexSlots -= includedUniqueKeys.length;
        }
    
        sqlQuery = sqlQuery.slice(0, -2) + `) ENGINE=${databaseConfig?.engine || DIALECT_CONFIG.engine} 
            DEFAULT CHARSET=${databaseConfig?.charset || DIALECT_CONFIG.charset} 
            COLLATE=${databaseConfig?.collate || DIALECT_CONFIG.collate};`;

        sqlQueries.push({query: sqlQuery, params: []});
    
        sqlQueries.push(...buildCreateIndexQueries("mysql", table, indexes, remainingIndexSlots, schemaPrefix));

        return sqlQueries;
    },

    getAlterTableQuery(table: string, changes: AlterTableChanges, schema?: string, databaseConfig?: DatabaseConfig): QueryInput[] {
        const queries: QueryInput[] = [];
        const alterStatements: string[] = [];
    
        if(databaseConfig?.deleteColumns) {
            changes.dropColumns.forEach(columnName => {
                alterStatements.push(`DROP COLUMN ${q(columnName)}`);
            });
        }

        changes.renameColumns.forEach(({ oldName, newName }) => {
            const columnType = databaseConfig?.metaData?.[table]?.[newName]?.type || databaseConfig?.metaData?.[table]?.[oldName]?.type || 'varchar'
            const columnLength = databaseConfig?.metaData?.[table]?.[newName]?.length || databaseConfig?.metaData?.[table]?.[oldName]?.length || 1
            const columnDecimal = databaseConfig?.metaData?.[table]?.[newName]?.decimal || databaseConfig?.metaData?.[table]?.[oldName]?.decimal || 0
            const columnNullable : boolean = databaseConfig?.metaData?.[table]?.[newName]?.allowNull || databaseConfig?.metaData?.[table]?.[oldName]?.allowNull || false
            let columnLengthSQL: string = '';
            if (columnLength && !DIALECT_CONFIG.noLength.includes(columnType || "")) {
                columnLengthSQL += `(${assertSafeLength(columnLength)}${columnDecimal ? `,${assertSafeLength(columnDecimal)}` : ""})`;
            }
            assertSafeTypeToken(columnType);
            alterStatements.push(`CHANGE COLUMN ${q(oldName)} ${q(newName)} ${columnType}${columnLengthSQL} ${columnNullable ? 'NULL' : 'NOT NULL'}`);
        });
    
        for (const [columnName, column] of Object.entries(changes.addColumns)) {
            if(!column.type) { throw new Error(`Attempted to add a new column '${columnName}' without a type`)}
            let columnType = column.type.toLowerCase()
            if (DIALECT_CONFIG.translate.localToServer[columnType]) {
                columnType = DIALECT_CONFIG.translate.localToServer[columnType];
            }
            assertSafeTypeToken(columnType);
            // Same boolean→TINYINT(1) invariant as CREATE, so the round-trip stays consistent across CREATE/ALTER.
            let columnDef = `${q(columnName)} ${columnType}`;
            if (column.type === "boolean") {
                columnDef = `${q(columnName)} TINYINT(1)`;
            } else if (column.length && !DIALECT_CONFIG.noLength.includes(columnType) && columnType !== "tinyint") {
                columnDef += `(${assertSafeLength(column.length)}${column.decimal && DIALECT_CONFIG.decimals.includes(columnType) ? `,${assertSafeLength(column.decimal || 0)}` : ""})`;
            }
            if (!column.allowNull) columnDef += " NOT NULL";
            if (column.default !== undefined) {
                columnDef += ` DEFAULT ${renderColumnDefault(column.default, DIALECT_CONFIG)}`;
            } else if (!column.allowNull && column.calculated) {
                // A NOT NULL calculated timestamp needs a DEFAULT or ADD COLUMN fails on existing
                // rows; backfill with the alter-time value (true creation time unknown) — new rows
                // get calculatedDefault at insert.
                columnDef += ` DEFAULT CURRENT_TIMESTAMP`;
            }

            alterStatements.push(`ADD COLUMN ${columnDef}`);
        };

        for (const [columnName, column] of Object.entries(changes.modifyColumns)) {
            if(!column.type) { throw new Error(`Attempted to add a new column '${columnName}' without a type`)}
            let columnType = column.type.toLowerCase()
            if (DIALECT_CONFIG.translate.localToServer[columnType]) {
                columnType = DIALECT_CONFIG.translate.localToServer[columnType];
            }
            assertSafeTypeToken(columnType);
            // Same boolean→TINYINT(1) invariant as CREATE/ADD.
            let columnDef = `${q(columnName)} ${columnType}`;
            if (column.type === "boolean") {
                columnDef = `${q(columnName)} TINYINT(1)`;
            } else if (column.length && !DIALECT_CONFIG.noLength.includes(columnType) && columnType !== "tinyint") {
                columnDef += `(${assertSafeLength(column.length)}${column.decimal && DIALECT_CONFIG.decimals.includes(columnType) ? `,${assertSafeLength(column.decimal || 0)}` : ""})`;
            }

            if (changes.nullableColumns.includes(columnName)) {
                columnDef += " NULL";
            } else if (!column.allowNull) {
                columnDef += " NOT NULL";
            }

            if (column.default !== undefined) {
                columnDef += ` DEFAULT ${renderColumnDefault(column.default, DIALECT_CONFIG)}`;
            }

            if (column.previousType && column.previousType !== column.type) {
                const updateQuery = getUsingClause(columnName, column.previousType, column.type, table, schema);
                if (updateQuery) {
                    queries.push({ query: updateQuery, params: [] });
                }
            }

            alterStatements.push(`MODIFY COLUMN ${columnDef}`);
        };

        // nullableColumns not already covered by modifyColumns need their own statement.
        changes.nullableColumns.forEach(columnName => {
            if (!Object.prototype.hasOwnProperty.call(changes.modifyColumns, columnName)) {
                alterStatements.push(`MODIFY COLUMN ${q(columnName)} DROP NOT NULL`);
            }
        });
        const schemaPrefix = schema ? `${q(schema)}.` : "";

        if (alterStatements.length > 0) {
            queries.push({ query: `ALTER TABLE ${schemaPrefix}${q(table)} ${alterStatements.join(", ")};`, params: [] });
        }
    
        return queries;
    },

    getDropTableQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        return {query: `DROP TABLE IF EXISTS ${schemaPrefix}${q(table)};`, params: []};
    },

    getCreateTempTableQuery(table: string, schema?: string, stagingPrefix?: string): QueryInput {
        const tempTableName = getTempTableName(table, stagingPrefix);
        const schemaPrefix = schema ? `${q(schema)}.` : "";
        return {query: `CREATE TABLE IF NOT EXISTS ${schemaPrefix}${q(tempTableName)}
        AS SELECT * FROM ${schemaPrefix}${q(table)} LIMIT 0;`, params: []};
    },

    getTableExistsQuery(schema: string, table: string): QueryInput {
        return {
            query: "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            params: [schema, table],
        };
    },

    getTableMetaDataQuery(schema: string, table: string): QueryInput {
        return {
            query: `SELECT
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.COLUMN_TYPE,
    c.EXTRA,
    CASE 
        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN CONCAT(c.NUMERIC_PRECISION,',',c.NUMERIC_SCALE)
        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NULL THEN c.NUMERIC_PRECISION
        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN c.CHARACTER_MAXIMUM_LENGTH
        ELSE NULL 
    END AS LENGTH, 
    c.IS_NULLABLE, 
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
                ON tc.TABLE_SCHEMA = k.TABLE_SCHEMA 
                AND tc.TABLE_NAME = k.TABLE_NAME 
                AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = c.TABLE_SCHEMA
              AND tc.TABLE_NAME = c.TABLE_NAME
              AND k.COLUMN_NAME = c.COLUMN_NAME
              AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) THEN 'PRIMARY'
        WHEN EXISTS (
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
                ON tc.TABLE_SCHEMA = k.TABLE_SCHEMA 
                AND tc.TABLE_NAME = k.TABLE_NAME 
                AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = c.TABLE_SCHEMA
              AND tc.TABLE_NAME = c.TABLE_NAME
              AND k.COLUMN_NAME = c.COLUMN_NAME
              AND tc.CONSTRAINT_TYPE = 'UNIQUE'
        ) THEN 'UNIQUE'
        WHEN EXISTS (
            SELECT 1 
            FROM INFORMATION_SCHEMA.STATISTICS AS i
            WHERE i.TABLE_SCHEMA = c.TABLE_SCHEMA
              AND i.TABLE_NAME = c.TABLE_NAME
              AND i.COLUMN_NAME = c.COLUMN_NAME
              AND i.INDEX_NAME <> 'PRIMARY'
        ) THEN 'INDEX'
        ELSE NULL 
        END AS COLUMN_KEY,
    (
        SELECT GROUP_CONCAT(DISTINCT i.INDEX_NAME ORDER BY i.INDEX_NAME SEPARATOR ',')
        FROM INFORMATION_SCHEMA.STATISTICS AS i
        WHERE i.TABLE_SCHEMA = c.TABLE_SCHEMA
          AND i.TABLE_NAME = c.TABLE_NAME
          AND i.COLUMN_NAME = c.COLUMN_NAME
          AND i.NON_UNIQUE = 0
          AND i.INDEX_NAME <> 'PRIMARY'
    ) AS UNIQUE_INDEX_NAME
    FROM INFORMATION_SCHEMA.COLUMNS AS c
    WHERE c.TABLE_SCHEMA = ?
    AND c.TABLE_NAME = ?;
    `,
            params: [schema, table],
        };
    },

    getSplitTablesQuery(table: string, schema?: string): QueryInput {
        return {
            query: `
                SELECT 
                    c.COLUMN_NAME,
                    c.TABLE_NAME,
                    c.DATA_TYPE,
                    c.COLUMN_TYPE,
                    c.EXTRA,
                    CASE 
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL 
                            THEN CONCAT(c.NUMERIC_PRECISION, ',', c.NUMERIC_SCALE)
                        WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NULL 
                            THEN c.NUMERIC_PRECISION
                        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL 
                            THEN c.CHARACTER_MAXIMUM_LENGTH
                        ELSE NULL 
                    END AS LENGTH, 
                    c.IS_NULLABLE, 
                    CASE 
                        WHEN EXISTS (
                            SELECT 1 
                            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
                                ON tc.TABLE_SCHEMA = k.TABLE_SCHEMA 
                                AND tc.TABLE_NAME = k.TABLE_NAME 
                                AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                            WHERE tc.TABLE_SCHEMA = c.TABLE_SCHEMA
                            AND tc.TABLE_NAME = c.TABLE_NAME
                            AND k.COLUMN_NAME = c.COLUMN_NAME
                            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        ) THEN 'PRIMARY'
                        WHEN EXISTS (
                            SELECT 1 
                            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
                                ON tc.TABLE_SCHEMA = k.TABLE_SCHEMA 
                                AND tc.TABLE_NAME = k.TABLE_NAME 
                                AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                            WHERE tc.TABLE_SCHEMA = c.TABLE_SCHEMA
                            AND tc.TABLE_NAME = c.TABLE_NAME
                            AND k.COLUMN_NAME = c.COLUMN_NAME
                            AND tc.CONSTRAINT_TYPE = 'UNIQUE'
                        ) THEN 'UNIQUE'
                        WHEN EXISTS (
                            SELECT 1 
                            FROM INFORMATION_SCHEMA.STATISTICS AS i
                            WHERE i.TABLE_SCHEMA = c.TABLE_SCHEMA
                            AND i.TABLE_NAME = c.TABLE_NAME
                            AND i.COLUMN_NAME = c.COLUMN_NAME
                            AND i.INDEX_NAME <> 'PRIMARY'
                        ) THEN 'INDEX'
                        ELSE NULL 
                    END AS COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS AS c
                JOIN INFORMATION_SCHEMA.TABLES AS t 
                    ON c.TABLE_SCHEMA = t.TABLE_SCHEMA 
                    AND c.TABLE_NAME = t.TABLE_NAME
                WHERE t.TABLE_NAME LIKE CONCAT(?, '__part_%') 
                    AND t.TABLE_NAME REGEXP CONCAT('^', ?, '__part_[0-9]+$')
                    ${schema ? `AND t.TABLE_SCHEMA = ?` : ""}
                ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION;
            `,
            params: schema ? [table, table, schema] : [table, table],
        };
    }
};