import { MetadataHeader, QueryInput, DatabaseConfig, InsertInput } from "../../../config/types";
import { getTempTableName } from "../../../helpers/utilities";
import { escapeIdentifier } from "../../utils/escape";
import {
    resolveStatementInput, resolveStagingInput, resolveHistoryInput,
    insertColumns, primaryKeysOf, updatableColumns, flattenInsertParams,
    buildHistoryQuery, HistoryHooks,
} from "../shared/insertBuilderShared";

const q = (name: string) => escapeIdentifier(name, "pgsql");

const HISTORY_HOOKS: HistoryHooks = {
    quote: q,
    colDiff: (a, b) => `${a} IS DISTINCT FROM ${b}`,
    now: "CURRENT_TIMESTAMP",
    placeholder: (n) => `$${n}`,
};

export class PostgresInsertQueryBuilder {
    static getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT"): QueryInput {
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        const { table, rows, header, insertType } = resolveStatementInput(tableOrInput, data, metaData, databaseConfig, inputInsertType);
        if (!rows || rows.length === 0) throw new Error(`No data provided for insert into table "${table}"`);

        const columns = insertColumns(header, databaseConfig);
        const params = flattenInsertParams(rows, header, databaseConfig);

        const quotedCols = columns.map(col => q(col)).join(", ");
        const valuePlaceholders = rows.map((_, rowIndex) => {
            const baseIndex = rowIndex * columns.length;
            return `(${columns.map((_, colIndex) => `$${baseIndex + colIndex + 1}`).join(", ")})`;
        }).join(", ");

        let query = `INSERT INTO ${schemaPrefix}${q(table)} (${quotedCols}) VALUES ${valuePlaceholders}`;
        if (insertType === "UPDATE") {
            const primaryKeys = primaryKeysOf(header);
            if (primaryKeys.length === 0) throw new Error(`Postgres requires primary key(s) to use ON CONFLICT for table "${table}"`);
            const updateCols = updatableColumns(columns, header, primaryKeys);
            const conflictClause = `ON CONFLICT (${primaryKeys.map(col => q(col)).join(", ")})`;
            query += updateCols.length > 0
                ? ` ${conflictClause} DO UPDATE SET ${updateCols.map(col => `${q(col)} = EXCLUDED.${q(col)}`).join(", ")}`
                : ` ${conflictClause} DO NOTHING`;
        }
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
            query += " WHERE " + pkCols.map(pk => { params.push(pkFilter[pk]); return `${q(pk)} = $${params.length}`; }).join(" AND ");
        }
        if (insertType === "UPDATE") {
            const primaryKeys = primaryKeysOf(header);
            const updateCols = updatableColumns(columns, header, primaryKeys);
            if (primaryKeys.length > 0 && updateCols.length > 0) {
                query += ` ON CONFLICT (${primaryKeys.map(pk => q(pk)).join(", ")}) DO UPDATE SET ${updateCols.map(col => `${q(col)} = EXCLUDED.${q(col)}`).join(", ")}`;
            } else {
                query += ` ON CONFLICT DO NOTHING`;
            }
        }
        return { query, params };
    }

    static getInsertChangedRowsToHistoryQuery(tableOrInput: string | InsertInput, metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, pkFilter?: Record<string, any>): QueryInput {
        const schemaPrefix = databaseConfig?.schema ? `${q(databaseConfig.schema)}.` : "";
        return buildHistoryQuery(resolveHistoryInput(tableOrInput, metaData), schemaPrefix, HISTORY_HOOKS, pkFilter);
    }
}
