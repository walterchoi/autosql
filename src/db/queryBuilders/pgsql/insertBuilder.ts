import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig, InsertInput } from "../../../config/types";
import { pgsqlConfig } from "../../config/pgsqlConfig";
import { compareMetaData } from '../../../helpers/metadata';
const dialectConfig = pgsqlConfig

export class PostgresInsertQueryBuilder {
    static getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader, databaseConfig?: DatabaseConfig): QueryInput {
        let table: string;
        let rows: Record<string, any>[];
        let header: MetadataHeader;

        const schemaPrefix = databaseConfig?.schema ? `"${databaseConfig.schema}".` : "";

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
        const params: any[] = [];
        for (const row of rows) {
            for (const col of columns) {
            params.push(row[col] ?? null);
            }
        }

        const quotedCols = columns.map(col => `"${col}"`).join(", ");
        const valuePlaceholders = rows
            .map((_, rowIndex) => {
            const baseIndex = rowIndex * columns.length;
            const placeholders = columns.map((_, colIndex) => `$${baseIndex + colIndex + 1}`);
            return `(${placeholders.join(", ")})`;
            })
            .join(", ");

        let query = `INSERT INTO ${schemaPrefix}"${table}" (${quotedCols}) VALUES ${valuePlaceholders}`;

        const insertType = databaseConfig?.insertType || "UPDATE";

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

            const conflictClause = `ON CONFLICT (${primaryKeys.map(col => `"${col}"`).join(", ")})`;

            if (updateCols.length > 0) {
            const updateSet = updateCols
                .map(col => `"${col}" = EXCLUDED."${col}"`)
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
}