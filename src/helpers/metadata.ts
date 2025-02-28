import { validateConfig, shuffleArray, calculateColumnLength } from './utilities';
import { sqlDialectConfig } from "../config/sqldialect";
import { initializeHeaders } from './headers';
import { updateColumnType } from './types';
import { predictIndexes } from './keys';

export interface ColumnDefinition {
    type: string | null;
    length?: number;
    allowNull?: boolean;
    unique?: boolean;
    index?: boolean;
    pseudounique?: boolean;
    primary?: boolean;
    autoIncrement?: boolean;
    default?: any;
    decimal?: number;
}

export function initializeMetaData(headers: string[]): Record<string, any>[] {
    try {
        return headers.map(header => ({
            [header]: {
                type: null,
                length: 0,
                allowNull: false,
                unique: false,
                index: false,
                pseudounique: false,
                primary: false,
                autoIncrement: false,
                default: undefined,
                decimal: 0
            }
        }));
    } catch (error) {
        throw new Error(`Error in initializeMetaData: ${error}`);
    }
}

export async function processDataRow(row: Record<string, any>, headers: any[], sqlLookupTable: any, uniqueCheck: Record<string, Set<string>>) {
    for (const header of headers) {
        const headerName = Object.keys(header)[0];
        const column = header[headerName];

        let dataPoint: any = row[headerName];

        if (dataPoint === null || dataPoint === undefined || dataPoint === "\\N" || dataPoint === "null") {
            column.allowNull = true;
            continue;
        }

        if (typeof dataPoint === "object") {
            dataPoint = JSON.stringify(dataPoint);
        } else {
            dataPoint = String(dataPoint);
        }

        uniqueCheck[headerName].add(dataPoint);

        await updateColumnType(column, dataPoint);
        calculateColumnLength(column, dataPoint, sqlLookupTable);
    }
}

export function finalizeMetadata(headers: any[], validatedConfig: any, sqlLookupTable: any) {
    for (const header of headers) {
        const headerName = Object.keys(header)[0];
        const column = header[headerName];

        if (column.unique && column.length > validatedConfig.maximum_unique_length) {
            column.unique = false;
        }

        if (column.length > validatedConfig.max_non_text_length) {
            column.type = column.length < 6553 ? "varchar"
                : column.length < 65535 ? "text"
                : column.length < 16777215 ? "mediumtext"
                : "longtext";
        }
    }

    return validatedConfig.auto_indexing ? predictIndexes(validatedConfig, validatedConfig.primary) : headers;
}

export async function getMetaData(config: { [key: string]: any }, data: Record<string, any>[]) {
    try {
        const validatedConfig = validateConfig(config);
        const sqlDialect = validatedConfig.sql_dialect as keyof typeof sqlDialectConfig;

        if (!sqlDialectConfig[sqlDialect]) {
            throw new Error(`Unsupported SQL dialect: ${sqlDialect}`);
        }

        const sqlLookupTable = await import(sqlDialectConfig[sqlDialect].helper_json);
        let headers = initializeHeaders(validatedConfig, data);

        const uniqueCheck: Record<string, Set<string>> = {};
        headers.forEach(header => {
            const headerName = Object.keys(header)[0];
            uniqueCheck[headerName] = new Set();
        });

        if (validatedConfig.sampling > 0 && validatedConfig.sampling < 1) {
            let sampleSize = Math.round(data.length * validatedConfig.sampling);
            if (sampleSize < validatedConfig.sampling_minimum) sampleSize = validatedConfig.sampling_minimum;
            data = shuffleArray(data).slice(0, sampleSize);
        }

        for (const row of data) {
            await processDataRow(row, headers, sqlLookupTable, uniqueCheck);
        }

        return finalizeMetadata(headers, validatedConfig, sqlLookupTable);
    } catch (error) {
        throw new Error(`Error in getMetaData: ${error}`);
    }
}