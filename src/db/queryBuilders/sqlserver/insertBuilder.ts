import { MetadataHeader, QueryInput, DatabaseConfig, InsertInput } from "../../../config/types";
import { sqlServerConfig } from "../../config/sqlServerConfig";
import { getInsertValues, getTempTableName, getHistoryTableName, getTrueTableName } from "../../../helpers/utilities";
import { escapeIdentifier } from "../../utils/escape";

const dialectConfig = sqlServerConfig;
const q = (name: string) => escapeIdentifier(name, "sqlserver");

export class SqlServerInsertQueryBuilder {
    static getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT"): QueryInput {
        let table: string;
        let rows: Record<string, any>[];
        let header: MetadataHeader;
        let insertType: "UPDATE" | "INSERT";

        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";

        if (typeof tableOrInput === "object" && "table" in tableOrInput) {
            table = tableOrInput.table;
            rows = tableOrInput.data;
            header = tableOrInput.comparedMetaData?.updatedMetaData || tableOrInput.metaData;
            insertType = tableOrInput?.insertType || databaseConfig?.insertType || "UPDATE";
        } else {
            table = tableOrInput;
            rows = data!;
            header = metaData!;
            insertType = inputInsertType || databaseConfig?.insertType || "UPDATE";
        }

        if (!rows || rows.length === 0) {
            throw new Error(`No data provided for insert into table "${table}"`);
        }

        // In surrogate-key mode, omit the database-generated auto-increment (surrogate) column
        // from the INSERT column list, matching getInsertValues. Gated on `surrogateKey` so a
        // genuine IDENTITY primary key whose values a caller supplies for upsert is not dropped.
        const columns = Object.keys(header).filter(col => !(databaseConfig?.surrogateKey && header[col].autoIncrement === true));

        // Flatten values (0-indexed @p placeholders, in params-array order).
        let params: any[] = [];
        if (typeof rows[0] === "object" && !Array.isArray(rows[0])) {
            const normalisedChunk = (rows as Record<string, any>[]).map(row =>
                getInsertValues(header, row, undefined, databaseConfig, false) // ⬅ false = flatten
            );
            params = normalisedChunk.flat();
        } else {
            params = rows.flat() as any[];
        }

        const quotedCols = columns.map(col => q(col)).join(", ");
        const target = `${schemaPrefix}${q(table)}`;

        const valueTuples = rows
            .map((_, rowIndex) => {
                const baseIndex = rowIndex * columns.length;
                const placeholders = columns.map((_, colIndex) => `@p${baseIndex + colIndex}`);
                return `(${placeholders.join(", ")})`;
            })
            .join(", ");

        const primaryKeys = Object.keys(header).filter(col => header[col].primary === true);

        // Plain append, OR an upsert with no primary key to merge on: emit a plain INSERT (SQL
        // Server MERGE requires a key). No throw — fall back silently, matching the spec.
        if (insertType !== "UPDATE" || primaryKeys.length === 0) {
            const query = `INSERT INTO ${target} (${quotedCols}) VALUES ${valueTuples};`;
            return { query, params };
        }

        // Upsert via MERGE. Source is a table-valued constructor of the bound row values.
        const updateCols = columns.filter((col) => {
            const colMeta = header[col];
            const isPrimary = primaryKeys.includes(col);
            const isProtectedCalc = colMeta.calculated === true && colMeta.updatedCalculated === false;
            return !isPrimary && !isProtectedCalc;
        });

        const sourceColList = columns.map(col => q(col)).join(", ");
        const onMatch = primaryKeys.map(pk => `target.${q(pk)} = source.${q(pk)}`).join(" AND ");
        const insertCols = columns.map(col => q(col)).join(", ");
        const insertVals = columns.map(col => `source.${q(col)}`).join(", ");

        let query =
            // WITH (HOLDLOCK): serialise the match+insert so two concurrent upserts of the same key
            // can't both fall to WHEN NOT MATCHED and double-insert (the classic MERGE race) (A11).
            `MERGE INTO ${target} WITH (HOLDLOCK) AS target ` +
            `USING (VALUES ${valueTuples}) AS source (${sourceColList}) ` +
            `ON ${onMatch} `;

        if (updateCols.length > 0) {
            const updateSet = updateCols.map(col => `target.${q(col)} = source.${q(col)}`).join(", ");
            query += `WHEN MATCHED THEN UPDATE SET ${updateSet} `;
        }
        // WHEN MATCHED omitted entirely when there are no updatable columns = DO NOTHING equivalent.
        query += `WHEN NOT MATCHED THEN INSERT (${insertCols}) VALUES (${insertVals});`;

        return { query, params };
    }

    static getInsertFromStagingQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT"): QueryInput {
        let table: string;
        let header: MetadataHeader;
        let insertType: "UPDATE" | "INSERT";

        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";

        if (typeof tableOrInput === "object" && "table" in tableOrInput) {
            table = tableOrInput.table;
            header = tableOrInput.comparedMetaData?.updatedMetaData || tableOrInput.metaData;
            insertType = tableOrInput?.insertType || databaseConfig?.insertType || "UPDATE";
        } else {
            table = tableOrInput;
            header = metaData!;
            insertType = inputInsertType || databaseConfig?.insertType || "UPDATE";
        }

        const stagingPrefix = typeof tableOrInput === "object" ? tableOrInput.stagingPrefix : undefined;
        const tempTable = getTempTableName(table, stagingPrefix);

        // In surrogate-key mode, omit the database-generated auto-increment (surrogate) column
        // from the INSERT column list, matching getInsertValues.
        const columns = Object.keys(header).filter(col => !(databaseConfig?.surrogateKey && header[col].autoIncrement === true));
        const escapedCols = columns.map(col => q(col)).join(", ");
        const selectCols = columns.map(col => q(col)).join(", ");

        const target = `${schemaPrefix}${q(table)}`;
        const source = `${schemaPrefix}${q(tempTable)}`;

        const primaryKeys = Object.keys(header).filter(col => header[col].primary === true);

        // Plain append, OR no primary key to merge on: plain INSERT ... SELECT (no MERGE, no throw).
        if (insertType !== "UPDATE" || primaryKeys.length === 0) {
            const query = `INSERT INTO ${target} (${escapedCols}) SELECT ${selectCols} FROM ${source};`;
            return { query, params: [] };
        }

        const updateCols = columns.filter((col) => {
            const colMeta = header[col];
            const isPrimary = primaryKeys.includes(col);
            const isProtectedCalc = colMeta.calculated === true && colMeta.updatedCalculated === false;
            return !isPrimary && !isProtectedCalc;
        });

        const onMatch = primaryKeys.map(pk => `target.${q(pk)} = source.${q(pk)}`).join(" AND ");
        const insertCols = columns.map(col => q(col)).join(", ");
        const insertVals = columns.map(col => `source.${q(col)}`).join(", ");

        let query =
            // WITH (HOLDLOCK): serialise the match+insert so two concurrent upserts of the same key
            // can't both fall to WHEN NOT MATCHED and double-insert (the classic MERGE race) (A11).
            `MERGE INTO ${target} WITH (HOLDLOCK) AS target ` +
            `USING ${source} AS source ` +
            `ON ${onMatch} `;

        if (updateCols.length > 0) {
            const updateSet = updateCols.map(col => `target.${q(col)} = source.${q(col)}`).join(", ");
            query += `WHEN MATCHED THEN UPDATE SET ${updateSet} `;
        }
        query += `WHEN NOT MATCHED THEN INSERT (${insertCols}) VALUES (${insertVals});`;

        return { query, params: [] };
    }

    static getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput {
        let table: string;
        let header: MetadataHeader;

        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";

        let stagingPrefix: string | undefined;
        let historyTableSuffix: string | undefined;
        if (typeof tableOrInput === "object" && "table" in tableOrInput) {
            stagingPrefix = tableOrInput.stagingPrefix;
            historyTableSuffix = tableOrInput.historyTableSuffix;
            table = getTrueTableName(tableOrInput.table, stagingPrefix, historyTableSuffix);
            header = tableOrInput.comparedMetaData?.updatedMetaData || tableOrInput.metaData;
        } else {
            table = getTrueTableName(tableOrInput);
            header = metaData!;
        }

        const historyTable = getHistoryTableName(table, historyTableSuffix);
        const tempTable = getTempTableName(table, stagingPrefix);

        const filteredCols = Object.keys(header).filter(col => col !== "dwh_as_at");
        const primaryKeys = filteredCols.filter(col => header[col].primary);
        const nonPrimaryCols = filteredCols.filter(
            col => !header[col].primary && header[col].calculated !== true
        );

        const t1 = "t1";
        const t2 = "t2";

        const valuesCols = filteredCols.map(col => q(col)).join(", ");
        const selectCols = filteredCols.map(col => `${t1}.${q(col)}`).join(", ");

        const joinCondition = primaryKeys
            .map(pk => `${t1}.${q(pk)} = ${t2}.${q(pk)}`)
            .join(" AND ");

        // NULL-safe difference test for T-SQL (no IS DISTINCT FROM).
        const diffCondition = nonPrimaryCols
            .map(col => `((${t1}.${q(col)} <> ${t2}.${q(col)}) OR (${t1}.${q(col)} IS NULL AND ${t2}.${q(col)} IS NOT NULL) OR (${t1}.${q(col)} IS NOT NULL AND ${t2}.${q(col)} IS NULL))`)
            .join(" OR ");

        // INNER JOIN (not LEFT): a before-image only for rows the merge will update — present in
        // BOTH the real table and this batch. A LEFT JOIN kept every real-table row; for a row absent
        // from the batch t2 is all NULL, the NULL-safe diff test evaluates TRUE, and it was wrongly
        // historised — recording the whole unchanged table on every incremental load (A2).
        const query = `
        INSERT INTO ${schemaPrefix}${q(historyTable)} (${valuesCols}, ${q("dwh_as_at")})
        SELECT ${selectCols}, CURRENT_TIMESTAMP
        FROM ${schemaPrefix}${q(table)} ${t1}
        INNER JOIN ${schemaPrefix}${q(tempTable)} ${t2}
          ON ${joinCondition}
        WHERE ${diffCondition};
        `.trim();

        return {
            query,
            params: []
        };
    }
}
