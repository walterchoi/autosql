import { DatabaseConfig } from '../config/types';
import { normalizeNumber, validateConfig, shuffleArray, calculateColumnLength } from './utilities';
import { groupings } from '../config/groupings';
import { collateTypes } from './columnTypes';
import { updateColumnType, predictType } from './columnTypes';
import { defaults } from '../config/defaults';
import { predictIndexes } from './keys';
import { Database } from '../db/database';
import { supportedDialects, DialectConfig, MetadataHeader, metaDataInterim } from '../config/types';
import { mysqlConfig } from "../db/config/mysqlConfig";
import { pgsqlConfig } from "../db/config/pgsqlConfig";

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

export async function getDataHeaders(data: Record<string, any>[], databaseConfig: DatabaseConfig): Promise<MetadataHeader> {
    const sampling = databaseConfig.sampling;
    const samplingMinimum = databaseConfig.samplingMinimum;
    let metaData : MetadataHeader = {};
    const allColumns = new Set<string>();
    let metaDataInterim : metaDataInterim = {};

    if ((sampling !== undefined || samplingMinimum !== undefined) && (sampling === undefined || samplingMinimum === undefined)) {
        throw new Error("Both sampling percentage and sampling minimum must be provided together.");
    }

    let sampleData = data;
    let remainingData: Record<string, any>[] = [];

    if (sampling !== undefined && samplingMinimum !== undefined && data.length > samplingMinimum) {
        let sampleSize = Math.round(data.length * sampling);
        sampleSize = Math.max(sampleSize, samplingMinimum); // Ensure minimum sample size

        const shuffledData = shuffleArray(data);
        sampleData = shuffledData.slice(0, sampleSize); // Shuffle and take sample
        remainingData = shuffledData.slice(sampleSize); // Store remaining data
    }

    for (const row of sampleData) {
        const rowColumns = Object.keys(row);
        rowColumns.forEach(column => allColumns.add(column))
        for (const column of allColumns) {
            const value = row[column]

            if(metaData[column] == undefined) {
                metaData[column] = {
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
            }
            if(metaDataInterim[column] == undefined) {
                metaDataInterim[column] = {
                    uniqueSet: new Set(),
                    valueCount: 0,
                    nullCount: 0,
                    types: new Set(),
                    length: 0,
                    decimal: 0,
                }
            }

            if (value === '' || value === null || value === undefined || value === '\\N' || value === 'null') {
                metaData[column].allowNull = true;
                metaDataInterim[column].nullCount++;
                continue;
            }
            const type = predictType(value)
            if(!type) continue;
            
            metaDataInterim[column].valueCount++;
            metaDataInterim[column].uniqueSet.add(value);
            metaDataInterim[column].types.add(type);
            if (groupings.intGroup.includes(type) || groupings.specialIntGroup.includes(type)) {
                let valueStr = normalizeNumber(value);
                if(!valueStr) {
                    valueStr = String(value).trim();
                }
                const decimalLen = valueStr.includes(".") ? valueStr.split(".")[1].length : 0;
                const integerLen = valueStr.split(".")[0].length;

                metaDataInterim[column].decimal = Math.max(metaDataInterim[column].decimal, decimalLen);
                metaDataInterim[column].decimal = Math.min(metaDataInterim[column].decimal, databaseConfig.decimalMaxLength || 10);

                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, integerLen + metaDataInterim[column].decimal);
            } else {
                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, String(value).length);
            }
        }
    }

    for (const column in metaDataInterim) {
        const type = collateTypes(metaDataInterim[column].types);
        metaData[column].type = type;
        metaData[column].length = metaDataInterim[column].length || 0;
        metaData[column].decimal = metaDataInterim[column].decimal || 0;

        const uniquePercentage = metaDataInterim[column].uniqueSet.size / metaDataInterim[column].valueCount;
        if(uniquePercentage == 1) {
            metaData[column].unique = true;
        } else if (uniquePercentage >= (databaseConfig.pseudoUnique || defaults.pseudoUnique)) {
            metaData[column].pseudounique = true;
        }
        if(metaDataInterim[column].nullCount !== 0) {
            metaData[column].allowNull = true;
        }
    }

    for (const row of remainingData) {
        for (const column of allColumns) {
            const value = row[column]
            const type = metaData[column].type;
            if(!type) continue;
            if (groupings.intGroup.includes(type) || groupings.specialIntGroup.includes(type)) {
                let valueStr = normalizeNumber(value);
                if(!valueStr) {
                    valueStr = String(value).trim();
                }
                const decimalLen = valueStr.includes(".") ? valueStr.split(".")[1].length : 0;
                const integerLen = valueStr.split(".")[0].length;


                metaDataInterim[column].decimal = Math.max(metaDataInterim[column].decimal, decimalLen);
                metaDataInterim[column].decimal = Math.min(metaDataInterim[column].decimal, databaseConfig.decimalMaxLength || 10);

                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, integerLen + metaDataInterim[column].decimal);
            } else {
                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, String(value).length);
            }
        }
    }

    for (const column in metaDataInterim) {
        metaData[column].length = metaDataInterim[column].length || 0;
        metaData[column].decimal = metaDataInterim[column].decimal || 0;
    }

    return metaData
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

export async function getMetaData(databaseOrConfig: Database | DatabaseConfig, data: Record<string, any>[]) : Promise<MetadataHeader> {
    try {
        let validatedConfig: DatabaseConfig;
        let dbInstance: Database | undefined;
        let dialectConfig: DialectConfig;

        // Determine if input is a Database instance or a config object
        if (databaseOrConfig instanceof Database) {
            dbInstance = databaseOrConfig;
            validatedConfig = validateConfig(dbInstance.getConfig()); // Use existing Database config
            dialectConfig = dbInstance.getDialectConfig();
        } else {
            validatedConfig = validateConfig(databaseOrConfig); // Use provided config
            if(validatedConfig.sqlDialect == 'mysql') {
                dialectConfig = mysqlConfig;
            } else if(validatedConfig.sqlDialect == 'pgsql') {
                dialectConfig = pgsqlConfig;
            } else {
                throw new Error(`Unsupported SQL dialect: ${validatedConfig.sqlDialect}`);
            }
        }
        
        const sqlDialect = validatedConfig.sqlDialect as supportedDialects
        if (!sqlDialect) {
            throw new Error(`Unsupported SQL dialect: ${sqlDialect}`);
        }
        
        const metaData = await getDataHeaders(data, validatedConfig)
        
        return metaData;
        
    } catch (error) {
        throw new Error(`Error in getMetaData: ${error}`);
    }
}