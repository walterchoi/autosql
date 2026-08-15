import { MetadataHeader, QueryInput, DatabaseConfig, InsertInput } from "../../../config/types";
import { getTempTableName } from "../../../helpers/utilities";
import { escapeIdentifier } from "../../utils/escape";
import {
    resolveStatementInput, resolveStagingInput, resolveHistoryInput,
    insertColumns, primaryKeysOf, updatableColumns, flattenInsertParams,
    buildHistoryQuery, HistoryHooks,
} from "../shared/insertBuilderShared";

const q = (name: string) => escapeIdentifier(name, "mysql");

// Emit an upsert tail for MySQL's `ON DUPLICATE KEY UPDATE`. No updatable columns but a key present →
// a no-op self-update so a duplicate key is SKIPPED, matching PG (DO NOTHING) / SQL Server (A11).
function upsertTail(columns: string[], header: MetadataHeader): string {
    const primaryKeys = primaryKeysOf(header);
    const updateCols = updatableColumns(columns, header, primaryKeys);
    if (updateCols.length > 0) {
        return ` ON DUPLICATE KEY UPDATE ${updateCols.map(col => `${q(col)} = VALUES(${q(col)})`).join(", ")}`;
    }
    if (primaryKeys.length > 0) {
        return ` ON DUPLICATE KEY UPDATE ${q(primaryKeys[0])} = ${q(primaryKeys[0])}`;
    }
    return "";
}

const HISTORY_HOOKS: HistoryHooks = {
    quote: q,
    colDiff: (a, b) => `${a} <=> ${b} = FALSE`,
    now: "NOW()",
    placeholder: () => "?",
};

export class MySQLInsertQueryBuilder {
    static getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT"): QueryInput {
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        const { table, rows, header, insertType } = resolveStatementInput(tableOrInput, data, metaData, databaseConfig, inputInsertType);
        if (!rows || rows.length === 0) throw new Error(`No data provided for insert into table "${table}"`);

        const columns = insertColumns(header, databaseConfig);
        const params = flattenInsertParams(rows, header, databaseConfig);

        const escapedCols = columns.map(col => q(col)).join(", ");
        const valuePlaceholders = rows.map(() => `(${columns.map(() => `?`).join(", ")})`).join(", ");

        let query = `INSERT INTO ${schemaPrefix}${q(table)} (${escapedCols}) VALUES ${valuePlaceholders}`;
        if (insertType === "UPDATE") query += upsertTail(columns, header);
        return { query, params };
    }

    static getInsertFromStagingQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT", pkFilter?: Record<string, any>): QueryInput {
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        const { table, header, insertType, stagingPrefix } = resolveStagingInput(tableOrInput, metaData, databaseConfig, inputInsertType);
        const tempTable = getTempTableName(table, stagingPrefix);

        const columns = insertColumns(header, databaseConfig);
        const escapedCols = columns.map(col => q(col)).join(", ");
        const selectCols = columns.map(col => q(col)).join(", ");

        let query = `INSERT INTO ${schemaPrefix}${q(table)} (${escapedCols}) SELECT ${selectCols} FROM ${schemaPrefix}${q(tempTable)}`;

        const params: any[] = [];
        if (pkFilter) {
            const pkCols = primaryKeysOf(header);
            query += " WHERE " + pkCols.map(pk => { params.push(pkFilter[pk]); return `${q(pk)} = ?`; }).join(" AND ");
        }
        if (insertType === "UPDATE") query += upsertTail(columns, header);
        return { query, params };
    }

    static getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, pkFilter?: Record<string, any>): QueryInput {
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        return buildHistoryQuery(resolveHistoryInput(tableOrInput, metaData), schemaPrefix, HISTORY_HOOKS, pkFilter);
    }
}
