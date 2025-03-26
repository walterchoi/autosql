import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig, InsertInput } from "../../../config/types";
import { mysqlConfig } from "../../config/mysqlConfig";
import { getInsertValues } from "../../../helpers/utilities";
import { compareMetaData } from '../../../helpers/metadata';
const dialectConfig = mysqlConfig

export class MySQLInsertQueryBuilder {
    static getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput {
        let table: string;
        let rows: Record<string, any>[];
        let header: MetadataHeader;
        
        const schemaPrefix = databaseConfig?.schema ? `\`${databaseConfig.schema}\`.` : "";

        if (typeof tableOrInput === "object" && "table" in tableOrInput) {
            table = tableOrInput.table;
            rows = tableOrInput.data;
            header = tableOrInput.comparedMetaData?.updatedMetaData || tableOrInput.metaData;
        } else {
            table = tableOrInput;
            rows = data!;
            header = metaData!;
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

        const insertType = databaseConfig?.insertType || "UPDATE";

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
}