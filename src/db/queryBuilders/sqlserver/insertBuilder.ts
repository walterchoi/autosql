import { MetadataHeader, QueryInput, DatabaseConfig, InsertInput } from "../../../config/types";
import { getTempTableName } from "../../../helpers/utilities";
import { escapeIdentifier } from "../../utils/escape";
import {
    resolveStatementInput, resolveStagingInput, resolveHistoryInput,
    insertColumns, primaryKeysOf, updatableColumns, flattenInsertParams,
    buildHistoryQuery, HistoryHooks,
} from "../shared/insertBuilderShared";

const q = (name: string) => escapeIdentifier(name, "sqlserver");

const HISTORY_HOOKS: HistoryHooks = {
    quote: q,
    // T-SQL has no IS DISTINCT FROM — expand the NULL-safe difference test explicitly.
    colDiff: (a, b) => `((${a} <> ${b}) OR (${a} IS NULL AND ${b} IS NOT NULL) OR (${a} IS NOT NULL AND ${b} IS NULL))`,
    now: "CURRENT_TIMESTAMP",
    placeholder: (n) => `@p${n}`, // unused on SQL Server (no pkFilter path), provided for completeness
};

// The MERGE tail shared by the direct and staging upserts. WITH (HOLDLOCK) serialises match+insert so
// two concurrent upserts of the same key can't both fall to WHEN NOT MATCHED and double-insert (A11).
function buildMerge(target: string, sourceClause: string, columns: string[], header: MetadataHeader, primaryKeys: string[]): string {
    const updateCols = updatableColumns(columns, header, primaryKeys);
    const onMatch = primaryKeys.map(pk => `target.${q(pk)} = source.${q(pk)}`).join(" AND ");
    let query = `MERGE INTO ${target} WITH (HOLDLOCK) AS target ${sourceClause} ON ${onMatch} `;
    if (updateCols.length > 0) {
        query += `WHEN MATCHED THEN UPDATE SET ${updateCols.map(col => `target.${q(col)} = source.${q(col)}`).join(", ")} `;
    }
    // WHEN MATCHED omitted with no updatable columns = DO NOTHING equivalent.
    query += `WHEN NOT MATCHED THEN INSERT (${columns.map(col => q(col)).join(", ")}) VALUES (${columns.map(col => `source.${q(col)}`).join(", ")});`;
    return query;
}

export class SqlServerInsertQueryBuilder {
    static getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT"): QueryInput {
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        const { table, rows, header, insertType } = resolveStatementInput(tableOrInput, data, metaData, databaseConfig, inputInsertType);
        if (!rows || rows.length === 0) throw new Error(`No data provided for insert into table "${table}"`);

        const columns = insertColumns(header, databaseConfig);
        const params = flattenInsertParams(rows, header, databaseConfig);

        const quotedCols = columns.map(col => q(col)).join(", ");
        const target = `${schemaPrefix}${q(table)}`;
        const valueTuples = rows.map((_, rowIndex) => {
            const baseIndex = rowIndex * columns.length;
            return `(${columns.map((_, colIndex) => `@p${baseIndex + colIndex}`).join(", ")})`;
        }).join(", ");

        const primaryKeys = primaryKeysOf(header);
        // Plain append, OR an upsert with no key to merge on: plain INSERT (MERGE needs a key; no throw).
        if (insertType !== "UPDATE" || primaryKeys.length === 0) {
            return { query: `INSERT INTO ${target} (${quotedCols}) VALUES ${valueTuples};`, params };
        }
        const source = `USING (VALUES ${valueTuples}) AS source (${quotedCols})`;
        return { query: buildMerge(target, source, columns, header, primaryKeys), params };
    }

    static getInsertFromStagingQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT"): QueryInput {
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        const { table, header, insertType, stagingPrefix } = resolveStagingInput(tableOrInput, metaData, databaseConfig, inputInsertType);
        const tempTable = getTempTableName(table, stagingPrefix);

        const columns = insertColumns(header, databaseConfig);
        const escapedCols = columns.map(col => q(col)).join(", ");
        const selectCols = columns.map(col => q(col)).join(", ");
        const target = `${schemaPrefix}${q(table)}`;
        const source = `${schemaPrefix}${q(tempTable)}`;
        const primaryKeys = primaryKeysOf(header);

        if (insertType !== "UPDATE" || primaryKeys.length === 0) {
            return { query: `INSERT INTO ${target} (${escapedCols}) SELECT ${selectCols} FROM ${source};`, params: [] };
        }
        return { query: buildMerge(target, `USING ${source} AS source`, columns, header, primaryKeys), params: [] };
    }

    static getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput {
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        return buildHistoryQuery(resolveHistoryInput(tableOrInput, metaData), schemaPrefix, HISTORY_HOOKS);
    }
}
