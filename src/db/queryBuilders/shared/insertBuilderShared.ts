import { MetadataHeader, DatabaseConfig, InsertInput, QueryInput } from "../../../config/types";
import { getInsertValues, getTempTableName, getHistoryTableName, getTrueTableName } from "../../../helpers/utilities";

// Shared, dialect-agnostic logic for the three insert builders (R2). The SQL *assembly* diverges too
// much to unify (INSERT…ON CONFLICT / ON DUPLICATE KEY / MERGE), but the input resolution, column
// derivation, updatable-column rule and the row-level history query are identical — and were copy-pasted
// three times (so e.g. the A2 history fix had to be made in triplicate). These live here once.

export interface ResolvedStatement { table: string; rows: Record<string, any>[]; header: MetadataHeader; insertType: "UPDATE" | "INSERT"; }
export interface ResolvedStaging { table: string; header: MetadataHeader; insertType: "UPDATE" | "INSERT"; stagingPrefix?: string; }
export interface ResolvedHistory { table: string; header: MetadataHeader; stagingPrefix?: string; historyTableSuffix?: string; }

const isInput = (v: any): v is InsertInput => typeof v === "object" && v !== null && "table" in v;
const headerOf = (input: InsertInput): MetadataHeader => input.comparedMetaData?.updatedMetaData || input.metaData;
const insertTypeOf = (input: InsertInput, cfg?: DatabaseConfig): "UPDATE" | "INSERT" => input?.insertType || cfg?.insertType || "UPDATE";

export function resolveStatementInput(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, cfg?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT"): ResolvedStatement {
    if (isInput(tableOrInput)) {
        return { table: tableOrInput.table, rows: tableOrInput.data, header: headerOf(tableOrInput), insertType: insertTypeOf(tableOrInput, cfg) };
    }
    return { table: tableOrInput, rows: data!, header: metaData!, insertType: inputInsertType || cfg?.insertType || "UPDATE" };
}

export function resolveStagingInput(tableOrInput: string | InsertInput, metaData?: MetadataHeader, cfg?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT"): ResolvedStaging {
    if (isInput(tableOrInput)) {
        return { table: tableOrInput.table, header: headerOf(tableOrInput), insertType: insertTypeOf(tableOrInput, cfg), stagingPrefix: tableOrInput.stagingPrefix };
    }
    return { table: tableOrInput, header: metaData!, insertType: inputInsertType || cfg?.insertType || "UPDATE", stagingPrefix: undefined };
}

export function resolveHistoryInput(tableOrInput: string | InsertInput, metaData?: MetadataHeader): ResolvedHistory {
    if (isInput(tableOrInput)) {
        return { table: tableOrInput.table, header: headerOf(tableOrInput), stagingPrefix: tableOrInput.stagingPrefix, historyTableSuffix: tableOrInput.historyTableSuffix };
    }
    return { table: tableOrInput, header: metaData! };
}

// Columns for the INSERT list: drop the DB-generated auto-increment surrogate (only in surrogateKey
// mode — a genuine AUTO_INCREMENT/SERIAL/IDENTITY key whose values a caller supplies must be kept).
export function insertColumns(header: MetadataHeader, cfg?: DatabaseConfig): string[] {
    return Object.keys(header).filter(col => !(cfg?.surrogateKey && header[col].autoIncrement === true));
}

export function primaryKeysOf(header: MetadataHeader): string[] {
    return Object.keys(header).filter(col => header[col].primary === true);
}

// The columns an upsert may UPDATE: everything except the primary key(s) and protected calculated
// fields (calculated && updatedCalculated === false).
export function updatableColumns(columns: string[], header: MetadataHeader, primaryKeys: string[]): string[] {
    return columns.filter(col => {
        const m = header[col];
        return !primaryKeys.includes(col) && !(m.calculated === true && m.updatedCalculated === false);
    });
}

// Flatten a chunk of row objects into the bound-parameter array (or pass through pre-flattened arrays).
export function flattenInsertParams(rows: Record<string, any>[], header: MetadataHeader, cfg?: DatabaseConfig): any[] {
    if (typeof rows[0] === "object" && !Array.isArray(rows[0])) {
        return rows.map(row => getInsertValues(header, row, undefined, cfg, false)).flat();
    }
    return (rows as any[]).flat();
}

export interface HistoryHooks {
    quote: (name: string) => string;
    // A NULL-safe "values differ" test between two already-quoted column refs (t1.col, t2.col).
    colDiff: (t1Col: string, t2Col: string) => string;
    now: string;                     // server clock: NOW() (MySQL) or CURRENT_TIMESTAMP
    placeholder: (n: number) => string; // 1-based bound-parameter placeholder (only used with pkFilter)
}

// Row-level history before-image (A2): capture the CURRENT row for every batch row that will change.
// INNER JOIN (not LEFT) so only rows present in BOTH the real table and the staged batch are captured
// — a LEFT JOIN historised every absent row on each incremental load. Optional pkFilter scopes it to a
// single primary key (the atomic degradation path, same transaction as that PK's merge).
export function buildHistoryQuery(resolved: ResolvedHistory, schemaPrefix: string, hooks: HistoryHooks, pkFilter?: Record<string, any>): QueryInput {
    const q = hooks.quote;
    const table = getTrueTableName(resolved.table, resolved.stagingPrefix, resolved.historyTableSuffix);
    const header = resolved.header;
    const historyTable = getHistoryTableName(table, resolved.historyTableSuffix);
    const tempTable = getTempTableName(table, resolved.stagingPrefix);

    const filteredCols = Object.keys(header).filter(col => col !== "dwh_as_at");
    const primaryKeys = filteredCols.filter(col => header[col].primary);
    const nonPrimaryCols = filteredCols.filter(col => !header[col].primary && header[col].calculated !== true);

    const t1 = "t1", t2 = "t2";
    const valuesCols = filteredCols.map(col => q(col)).join(", ");
    const selectCols = filteredCols.map(col => `${t1}.${q(col)}`).join(", ");
    const joinCondition = primaryKeys.map(pk => `${t1}.${q(pk)} = ${t2}.${q(pk)}`).join(" AND ");
    const diffCondition = nonPrimaryCols.map(col => hooks.colDiff(`${t1}.${q(col)}`, `${t2}.${q(col)}`)).join(" OR ");

    const params: any[] = [];
    let whereClause = diffCondition;
    if (pkFilter) {
        const pkFilterClause = primaryKeys.map(pk => { params.push(pkFilter[pk]); return `${t1}.${q(pk)} = ${hooks.placeholder(params.length)}`; }).join(" AND ");
        whereClause = `(${diffCondition}) AND ${pkFilterClause}`;
    }

    const query = `INSERT INTO ${schemaPrefix}${q(historyTable)} (${valuesCols}, ${q("dwh_as_at")})
SELECT ${selectCols}, ${hooks.now}
FROM ${schemaPrefix}${q(table)} ${t1}
INNER JOIN ${schemaPrefix}${q(tempTable)} ${t2}
  ON ${joinCondition}
WHERE ${whereClause};`;
    return { query, params };
}
