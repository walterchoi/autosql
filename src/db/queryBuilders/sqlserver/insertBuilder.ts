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

// MERGE tail shared by direct and staging upserts. WITH (HOLDLOCK) serialises match+insert so two
// concurrent upserts of the same key can't both hit WHEN NOT MATCHED and double-insert (A11).
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
        // A surrogate/DB-generated PK is excluded from the insert columns (insertColumns), so it can't
        // be a merge key — its value is generated on both sides, so ON target.pk=source.pk would
        // self-match on values neither side controls. Merge only on PKs actually being written; if none
        // remain (a surrogate-key table), plain append — a surrogate is unique per insert (spec-4 §3.7).
        const mergeKeys = primaryKeys.filter(pk => columns.includes(pk));
        if (insertType !== "UPDATE" || mergeKeys.length === 0) {
            return { query: `INSERT INTO ${target} (${quotedCols}) VALUES ${valueTuples};`, params };
        }
        const source = `USING (VALUES ${valueTuples}) AS source (${quotedCols})`;
        return { query: buildMerge(target, source, columns, header, mergeKeys), params };
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
        // Merge only on PKs actually written — a surrogate PK is excluded from the insert columns and its
        // staging clone regenerates IDENTITY values, so merging on it self-matches. None left → plain
        // append (surrogate is unique per insert; the real table generates fresh keys) (spec-4 §3.7).
        const mergeKeys = primaryKeys.filter(pk => columns.includes(pk));

        if (insertType !== "UPDATE" || mergeKeys.length === 0) {
            return { query: `INSERT INTO ${target} (${escapedCols}) SELECT ${selectCols} FROM ${source};`, params: [] };
        }
        return { query: buildMerge(target, `USING ${source} AS source`, columns, header, mergeKeys), params: [] };
    }

    static getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput {
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        return buildHistoryQuery(resolveHistoryInput(tableOrInput, metaData), schemaPrefix, HISTORY_HOOKS);
    }
}
