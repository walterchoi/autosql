import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig, InsertInput } from "../../../config/types";
import { pgsqlConfig } from "../../config/pgsqlConfig";
import { getInsertValues, getTempTableName, getHistoryTableName, getTrueTableName } from "../../../helpers/utilities";
import { compareMetaData } from '../../../helpers/metadata';
import { escapeIdentifier } from "../../utils/escape";
const dialectConfig = pgsqlConfig
const q = (name: string) => escapeIdentifier(name, "pgsql");

export class PostgresInsertQueryBuilder {
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
        // genuine SERIAL primary key (introspected as autoIncrement:true) whose values a caller
        // supplies for upsert is not dropped.
        const columns = Object.keys(header).filter(col => !(databaseConfig?.surrogateKey && header[col].autoIncrement === true));

        // Flatten values
        let params: any[] = [];
        if (typeof rows[0] === "object" && !Array.isArray(rows[0])) {
            const normalisedChunk = (rows as Record<string, any>[]).map(row =>
                getInsertValues(header, row, undefined, databaseConfig, false) // ⬅ false = flatten (databaseConfig enables opt-in sanitizeInvalidChars)
            );
            params = normalisedChunk.flat();
        } else {
            params = rows.flat() as any[];
        }

        const quotedCols = columns.map(col => q(col)).join(", ");
        const valuePlaceholders = rows
            .map((_, rowIndex) => {
            const baseIndex = rowIndex * columns.length;
            const placeholders = columns.map((_, colIndex) => `$${baseIndex + colIndex + 1}`);
            return `(${placeholders.join(", ")})`;
            })
            .join(", ");

        let query = `INSERT INTO ${schemaPrefix}${q(table)} (${quotedCols}) VALUES ${valuePlaceholders}`;
        if (insertType === "UPDATE") {
            const primaryKeys = Object.keys(header).filter(
            (col) => header[col].primary === true
            );

            if (primaryKeys.length === 0) {
            throw new Error(`Postgres requires primary key(s) to use ON CONFLICT for table "${table}"`);
            }

            const updateCols = columns.filter((col) => {
            const colMeta = header[col];
            const isPrimary = primaryKeys.includes(col);
            const isProtectedCalc =
                colMeta.calculated === true && colMeta.updatedCalculated === false;
                return !isPrimary && !isProtectedCalc;
            });

            const conflictClause = `ON CONFLICT (${primaryKeys.map(col => q(col)).join(", ")})`;

            if (updateCols.length > 0) {
            const updateSet = updateCols
                .map(col => `${q(col)} = EXCLUDED.${q(col)}`)
                .join(", ");
            query += ` ${conflictClause} DO UPDATE SET ${updateSet}`;
            } else {
            query += ` ${conflictClause} DO NOTHING`;
            }
        }

        const result: QueryInput = {
            query,
            params
        };

        return result;
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
        // from the INSERT column list, matching getInsertValues. Gated on `surrogateKey` so a
        // genuine SERIAL primary key (introspected as autoIncrement:true) whose values a caller
        // supplies for upsert is not dropped.
        const columns = Object.keys(header).filter(col => !(databaseConfig?.surrogateKey && header[col].autoIncrement === true));
        const escapedCols = columns.map(col => q(col)).join(", ");
        const selectCols = columns.map(col => q(col)).join(", ");

        let query = `INSERT INTO ${schemaPrefix}${q(table)} (${escapedCols}) SELECT ${selectCols} FROM ${schemaPrefix}${q(tempTable)}`;
      
        if (insertType === "UPDATE") {
          const primaryKeys = Object.keys(header).filter(col => header[col].primary === true);
      
          const updateCols = columns.filter((col) => {
            const colMeta = header[col];
            const isPrimary = primaryKeys.includes(col);
            const isProtectedCalc = colMeta.calculated === true && colMeta.updatedCalculated === false;
            return !isPrimary && !isProtectedCalc;
          });
      
          if (primaryKeys.length > 0 && updateCols.length > 0) {
            const updateSet = updateCols
              .map(col => `${q(col)} = EXCLUDED.${q(col)}`)
              .join(", ");
            query += ` ON CONFLICT (${primaryKeys.map(pk => q(pk)).join(", ")}) DO UPDATE SET ${updateSet}`;
          } else {
            // Optional: skip conflict update if nothing valid to update
            query += ` ON CONFLICT DO NOTHING`;
          }
        }
      
        return {
          query,
          params: []
        };
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

      const diffCondition = nonPrimaryCols
        .map(col => `${t1}.${q(col)} IS DISTINCT FROM ${t2}.${q(col)}`)
        .join(" OR ");

      // On the opt-in staging-degradation path a run-scoped as_at is supplied so a later per-row
      // divert can compensate exactly this run's before-images; otherwise use the DB clock (the
      // unchanged default for every existing history user).
      const asAt = (typeof tableOrInput === "object" && "table" in tableOrInput) ? tableOrInput.historyAsAt : undefined;
      const asAtExpr = asAt ? "$1" : "CURRENT_TIMESTAMP";
      const params = asAt ? [asAt] : [];

      const query = `
        INSERT INTO ${schemaPrefix}${q(historyTable)} (${valuesCols}, "dwh_as_at")
        SELECT ${selectCols}, ${asAtExpr}
        FROM ${schemaPrefix}${q(table)} ${t1}
        LEFT JOIN ${schemaPrefix}${q(tempTable)} ${t2}
          ON ${joinCondition}
        WHERE ${diffCondition};
        `.trim();
      return {
        query,
        params
      };
    }

    /**
     * Delete the row-level history before-images written by THIS run (identified by the exact
     * engine-supplied `dwh_as_at`) for the given rejected rows' primary keys. Used to compensate
     * history after a staging merge degraded to per-row and diverted some rows — those rows never
     * changed the real table, so their before-image must not remain. Keyed on `dwh_as_at` = this
     * run's value AND the PK tuple, so it can never touch a prior load's history for the same PK.
     */
    static getDeleteHistoryRowsQuery(historyTable: string, primaryKeys: string[], rejectedRows: Record<string, any>[], asAt: string, schema?: string): QueryInput {
      const schemaPrefix = schema ? `${q(schema)}.` : "";
      const ref = `${schemaPrefix}${q(historyTable)}`;
      const params: any[] = [asAt];
      let idx = 2; // $1 = asAt
      const pkTuple = `(${primaryKeys.map(pk => q(pk)).join(", ")})`;
      const rowPlaceholders = rejectedRows.map(row => {
        const placeholders = primaryKeys.map(pk => { params.push(row[pk]); return `$${idx++}`; });
        return `(${placeholders.join(", ")})`;
      }).join(", ");
      return {
        query: `DELETE FROM ${ref} WHERE "dwh_as_at" = $1 AND ${pkTuple} IN (${rowPlaceholders});`,
        params
      };
    }
    
}