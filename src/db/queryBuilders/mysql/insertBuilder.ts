import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig, InsertInput } from "../../../config/types";
import { mysqlConfig } from "../../config/mysqlConfig";
import { getInsertValues, getTempTableName, getHistoryTableName, getTrueTableName } from "../../../helpers/utilities";
import { compareMetaData } from '../../../helpers/metadata';
const dialectConfig = mysqlConfig

export class MySQLInsertQueryBuilder {
    static getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, databaseConfig?: DatabaseConfig, inputInsertType?: "UPDATE" | "INSERT"): QueryInput {
        let table: string;
        let rows: Record<string, any>[];
        let header: MetadataHeader;
        let insertType: "UPDATE" | "INSERT";
        
        const schemaPrefix = databaseConfig?.schema ? `\`${databaseConfig.schema}\`.` : "";

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

        const columns = Object.keys(header);

        // Flatten values
        let params: any[] = [];
        if (typeof rows[0] === "object" && !Array.isArray(rows[0])) {
            const normalisedChunk = (rows as Record<string, any>[]).map(row =>
                getInsertValues(header, row, undefined, undefined, false) // ⬅ false = flatten
            );
            params = normalisedChunk.flat();
        } else {
            params = rows.flat() as any[];
        }

        const escapedCols = columns.map(col => `\`${col}\``).join(", ");
        const valuePlaceholders = rows
            .map(() => `(${columns.map(() => `?`).join(", ")})`)
            .join(", ");

        let query = `INSERT INTO ${schemaPrefix}\`${table}\` (${escapedCols}) VALUES ${valuePlaceholders}`;

        if (insertType === "UPDATE") {
            // Get primary key columns
            const primaryKeys = Object.keys(header).filter(
              (col) => header[col].primary === true
            );
          
            const updateCols = columns.filter((col) => {
              const colMeta = header[col];
          
              // Exclude primary keys and protected calculated fields
              const isPrimary = primaryKeys.includes(col);
              const isProtectedCalc = colMeta.calculated === true && colMeta.updatedCalculated === false;
          
              return !isPrimary && !isProtectedCalc;
            });
          
            if (updateCols.length > 0) {
              const updateSet = updateCols
                .map(col => `\`${col}\` = VALUES(\`${col}\`)`)
                .join(", ");
              query += ` ON DUPLICATE KEY UPDATE ${updateSet}`;
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
        
        const schemaPrefix = databaseConfig?.schema ? `\`${databaseConfig.schema}\`.` : "";

        if (typeof tableOrInput === "object" && "table" in tableOrInput) {
            table = tableOrInput.table;
            header = tableOrInput.comparedMetaData?.updatedMetaData || tableOrInput.metaData;
            insertType = tableOrInput?.insertType || databaseConfig?.insertType || "UPDATE";
        } else {
            table = tableOrInput;
            header = metaData!;
            insertType = inputInsertType || databaseConfig?.insertType || "UPDATE";
        }

        const tempTable = getTempTableName(table);
      
        const columns = Object.keys(header);
        const escapedCols = columns.map(col => `\`${col}\``).join(", ");
        const selectCols = columns.map(col => `\`${col}\``).join(", ");
      
        let query = `INSERT INTO ${schemaPrefix}\`${table}\` (${escapedCols}) SELECT ${selectCols} FROM ${schemaPrefix}\`${tempTable}\``;
      
        if (insertType === "UPDATE") {
            const primaryKeys = Object.keys(header).filter(col => header[col].primary === true);
      
            const updateCols = columns.filter((col) => {
            const colMeta = header[col];
            const isPrimary = primaryKeys.includes(col);
            const isProtectedCalc = colMeta.calculated === true && colMeta.updatedCalculated === false;
            return !isPrimary && !isProtectedCalc;
        });
      
        if (updateCols.length > 0) {
            const updateSet = updateCols
              .map(col => `\`${col}\` = VALUES(\`${col}\`)`)
              .join(", ");
            query += ` ON DUPLICATE KEY UPDATE ${updateSet}`;
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
        
          const schemaPrefix = databaseConfig?.schema ? `\`${databaseConfig.schema}\`.` : "";
        
          if (typeof tableOrInput === "object" && "table" in tableOrInput) {
            table = getTrueTableName(tableOrInput.table);
            header = tableOrInput.comparedMetaData?.updatedMetaData || tableOrInput.metaData;
          } else {
            table = getTrueTableName(tableOrInput);
            header = metaData!;
          }
        
          const historyTable = getHistoryTableName(table);
          const tempTable = getTempTableName(table);
          const filteredCols = Object.keys(header).filter(col => col !== "dwh_as_at");
          const primaryKeys = filteredCols.filter(col => header[col].primary);
          const nonPrimaryCols = filteredCols.filter(
            col => !header[col].primary && header[col].calculated !== true
          );
        
          const t1 = "t1";
          const t2 = "t2";
        
          const valuesCols = filteredCols.map(col => `\`${col}\``).join(", ");
          const selectCols = filteredCols.map(col => `${t1}.\`${col}\``).join(", ");
        
          const joinCondition = primaryKeys
            .map(pk => `${t1}.\`${pk}\` = ${t2}.\`${pk}\``)
            .join(" AND ");
        
          const diffCondition = nonPrimaryCols
            .map(col => `${t1}.\`${col}\` <=> ${t2}.\`${col}\` = FALSE`)
            .join(" OR ");
        
          const query = `
            INSERT INTO ${schemaPrefix}\`${historyTable}\` (${valuesCols}, \`dwh_as_at\`)
            SELECT ${selectCols}, NOW()
            FROM ${schemaPrefix}\`${table}\` ${t1}
            LEFT JOIN ${schemaPrefix}\`${tempTable}\` ${t2}
              ON ${joinCondition}
            WHERE ${diffCondition};
            `.trim();
          return {
            query,
            params: []
          };
        }
}