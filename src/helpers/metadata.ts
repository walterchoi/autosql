import { DatabaseConfig } from '../config/types';
import { normalizeNumber, validateConfig, shuffleArray, sqlize } from './utilities';
import { groupings } from '../config/groupings';
import { collateTypes } from './columnTypes';
import { predictType } from './columnTypes';
import { defaults, nonCategoricalTypes } from '../config/defaults';
import { predictIndexes } from './keys';
import { Database } from '../db/database';
import { supportedDialects, DialectConfig, ColumnDefinition, MetadataHeader, metaDataInterim, AlterTableChanges } from '../config/types';
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

    const dialect = databaseConfig.sqlDialect;
    const dialectConfig: DialectConfig = dialect === 'mysql' ? mysqlConfig : pgsqlConfig;

    let sampleData = data;
    let remainingData: Record<string, any>[] = [];

    if (sampling !== undefined && sampling > 0 && samplingMinimum !== undefined && data.length > samplingMinimum) {
        let sampleSize = Math.round(data.length * sampling);
        sampleSize = Math.max(sampleSize, samplingMinimum); // Ensure minimum sample size

        const shuffledData = shuffleArray(data);
        sampleData = shuffledData.slice(0, sampleSize); // Shuffle and take sample
        remainingData = shuffledData.slice(sampleSize); // Store remaining data
    }

    // Cap uniqueSet per column to avoid unbounded memory on high-cardinality data.
    // Threshold = minimum entries needed to confirm pseudounique (lower-bound approach):
    // once uniqueSet.size / valueCount >= pseudoUnique, the column is definitively
    // pseudounique and we stop inserting. Unique (100%) is only confirmed for columns
    // that never saturate. For large datasets use sampling for full precision.
    const pseudoUniqueThreshold = databaseConfig.pseudoUnique || defaults.pseudoUnique;
    const uniqueSetCap = Math.ceil(pseudoUniqueThreshold * sampleData.length) + 1;

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
                    uniqueSaturated: false,
                    valueCount: 0,
                    nullCount: 0,
                    types: new Set(),
                    length: 0,
                    decimal: 0,
                    trueMaxDecimal: 0
                }
            }

            if (value === '' || value === null || value === undefined || value === '\\N' || value === 'null') {
                metaData[column].allowNull = true;
                metaDataInterim[column].nullCount++;
                continue;
            }
            const type = predictType(value)
            if(!type) continue;
            const sqlizedValue = sqlize(value, type, dialectConfig, databaseConfig)
            metaDataInterim[column].valueCount++;
            if (!metaDataInterim[column].uniqueSaturated) {
                metaDataInterim[column].uniqueSet.add(sqlizedValue);
                if (metaDataInterim[column].uniqueSet.size >= uniqueSetCap) {
                    metaDataInterim[column].uniqueSaturated = true;
                }
            }
            metaDataInterim[column].types.add(type);
            if (groupings.intGroup.includes(type) || groupings.specialIntGroup.includes(type)) {
                let valueStr = normalizeNumber(value);
                if(!valueStr) {
                    valueStr = String(value).trim();
                }
                const decimalLen = valueStr.includes(".") ? valueStr.split(".")[1].length : 0;
                const integerLen = valueStr.split(".")[0].length;
                metaDataInterim[column].decimal = Math.max(metaDataInterim[column].decimal, decimalLen);
                metaDataInterim[column].trueMaxDecimal = Math.max(metaDataInterim[column].trueMaxDecimal, metaDataInterim[column].decimal, decimalLen);
                metaDataInterim[column].decimal = Math.min(metaDataInterim[column].decimal, databaseConfig.decimalMaxLength || 10);

                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, integerLen + metaDataInterim[column].decimal);
            } else {
                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, String(value).length);
            }
        }
    }

    for (const column in metaDataInterim) {
        const type = collateTypes(metaDataInterim[column].types);
        metaDataInterim[column].collated_type = type;
        metaData[column].type = type;
        metaData[column].length = metaDataInterim[column].length || 0;
        metaData[column].decimal = metaDataInterim[column].decimal || 0;

        const uniqueSize = metaDataInterim[column].uniqueSet.size;
        const valueCount = metaDataInterim[column].valueCount;
        const saturated = metaDataInterim[column].uniqueSaturated;
        // When saturated, uniqueSize/valueCount is a lower bound on the true unique percentage.
        const uniquePercentage = uniqueSize / valueCount;
        if (!saturated && uniquePercentage == 1 && uniqueSize > 0) {
            metaData[column].unique = true;
        } else if (uniquePercentage >= (databaseConfig.pseudoUnique || defaults.pseudoUnique) && uniqueSize > 0) {
            metaData[column].pseudounique = true;
        } else if (!saturated && uniquePercentage <= (databaseConfig.categorical || defaults.categorical) && uniqueSize > 0 && !nonCategoricalTypes.includes(type)) {
            metaData[column].categorical = true;
        } else if (!saturated && uniqueSize == 1 && metaDataInterim[column].nullCount == 0 && valueCount > 0) {
            metaData[column].singleValue = true;
        }
        if(metaDataInterim[column].nullCount !== 0) {
            metaData[column].allowNull = true;
        }
        if(metaData[column].length > (databaseConfig.maxKeyLength || defaults.maxKeyLength) && metaData[column].unique) {
            metaData[column].unique = false
        }
        if(metaData[column].type === 'varchar' && metaData[column].length > (databaseConfig.maxVarcharLength || defaults.maxVarcharLength)) {
            metaData[column].type = 'text'
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
                metaDataInterim[column].trueMaxDecimal = Math.max(metaDataInterim[column].trueMaxDecimal, metaDataInterim[column].decimal, decimalLen);
                metaDataInterim[column].decimal = Math.min(metaDataInterim[column].decimal, databaseConfig.decimalMaxLength || 10);

                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, integerLen + metaDataInterim[column].decimal);
            } else {
                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, String(value).length);
            }
        }
    }

    for (const column in metaDataInterim) {
        // If type is not decimal, but decimal is set, add + 1 (for the dot) to length and set decimal to 0. Do this to metaDataInterim[column] so that it can be used later.
        // Also replace the metaDataInterim[column].decimal with metaDataInterim[column].trueMaxDecimal as if decimals were rounded due to exceeding the max decimal length, we want to keep the true max decimal length when converting to a non-decimal type.
        if (!dialectConfig.decimals.includes(metaDataInterim[column].collated_type || 'varchar')) {
            metaDataInterim[column].length = metaDataInterim[column].length + (metaDataInterim[column].decimal > 0 ? 1 : 0) - metaDataInterim[column].decimal + metaDataInterim[column].trueMaxDecimal;
            metaDataInterim[column].decimal = 0;
        }
        metaData[column].length = metaDataInterim[column].length || 0;
        metaData[column].decimal = metaDataInterim[column].decimal || 0;
    }
    
    const excludeBlankColumns = databaseConfig.excludeBlankColumns;
    if(excludeBlankColumns) {
        const emptyOrNullKeys = Object.entries(metaDataInterim)
        .filter(([_, meta]) => 
            meta.uniqueSet.size === 0 &&
            meta.valueCount === 0 &&
            meta.nullCount > 0
        )
        .map(([key]) => key);
        for (const key of emptyOrNullKeys) {
            delete metaData[key];
        }
    }
    return metaData
}

export async function getMetaData(databaseOrConfig: Database | DatabaseConfig, data: Record<string, any>[], primaryKey?: string[]) : Promise<MetadataHeader> {
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
        
        const headers = await getDataHeaders(data, validatedConfig)
        let metaData : MetadataHeader
        if(validatedConfig.autoIndexing) {
            metaData = predictIndexes(headers, validatedConfig.maxKeyLength, primaryKey || validatedConfig.primaryKey, data)
        } else {
            metaData = headers
        }
        
        return metaData;
        
    } catch (error) {
        throw new Error(`Error in getMetaData: ${error}`);
    }
}

export function compareMetaData(oldHeadersOriginal: MetadataHeader | null, newHeadersOriginal: MetadataHeader, dialectConfig?: DialectConfig, logger?: { warn?: (msg: string) => void }): { changes: AlterTableChanges; updatedMetaData: MetadataHeader } {
    if(!oldHeadersOriginal) {
        return { 
            changes: {
                addColumns: {},
                modifyColumns: {},
                dropColumns: [],
                renameColumns: [],
                nullableColumns: [],
                noLongerUnique: [],
                primaryKeyChanges: [],
            },
            updatedMetaData: newHeadersOriginal
        }
    }
    const newHeaders : MetadataHeader = JSON.parse(JSON.stringify(newHeadersOriginal));
    const oldHeaders : MetadataHeader = JSON.parse(JSON.stringify(oldHeadersOriginal));
    const addColumns: MetadataHeader = {};
    const modifyColumns: MetadataHeader = {};
    const dropColumns: string[] = [];
    const renameColumns: { oldName: string; newName: string }[] = [];
    const nullableColumns: string[] = [];
    const noLongerUnique: string[] = [];
    let oldPrimaryKeys: string[] = [];
    let newPrimaryKeys: string[] = [];
    let primaryKeyChanges: string[] = [];
    let renamedPrimaryKeys: { oldName: string; newName: string }[] = [];

    // ✅ Identify removed columns
    for (const oldColumnName of Object.keys(oldHeaders)) {
        if (!newHeaders.hasOwnProperty(oldColumnName)) {
            dropColumns.push(oldColumnName);
        }
    }

    // ✅ Identify renamed columns
    for (const oldColumnName of Object.keys(oldHeaders)) {
        for (const newColumnName of Object.keys(newHeaders)) {
            const oldColumn = oldHeaders[oldColumnName];
            const newColumn = newHeaders[newColumnName];

            if (oldColumnName !== newColumnName &&
                !(oldColumnName in newHeaders) &&
                !(newColumnName in oldHeaders) &&
                JSON.stringify(oldColumn) === JSON.stringify(newColumn)) {
                renameColumns.push({ oldName: oldColumnName, newName: newColumnName });

                if (oldColumn.primary && newColumn.primary) {
                    renamedPrimaryKeys.push({ oldName: oldColumnName, newName: newColumnName });
                }

                dropColumns.splice(dropColumns.indexOf(oldColumnName), 1);
                delete newHeaders[newColumnName];
            }
        }
    }

    // ✅ Identify added & modified columns
    for (const [columnName, newColumn] of Object.entries(newHeaders)) {
        if (!oldHeaders.hasOwnProperty(columnName)) {
            // New column - needs to be added
            addColumns[columnName] = newColumn;
        } else {
            const oldColumn = oldHeaders[columnName];
            let modified = false;
            let modifiedColumn: ColumnDefinition = { ...oldColumn };

            const oldType = oldColumn.type ?? "varchar";
            const newType = newColumn.type ?? "varchar";

            // ✅ Use `collateTypes()` to determine the best compatible type
            const recommendedType = collateTypes([oldType, newType]);

            if (recommendedType !== oldType) {
                logger?.warn?.(`Converting ${columnName}: ${oldType} → ${recommendedType}`);
                modifiedColumn.type = recommendedType;
                modifiedColumn.previousType = oldType;
                modified = true;
            } else {
                modifiedColumn.type = recommendedType;
                modifiedColumn.previousType = oldType;
            }

            // ✅ Merge column lengths safely
            const oldLength = oldColumn.length ?? 0;
            const newLength = newColumn.length ?? 0;
            const oldDecimal = oldColumn.decimal ?? 0;
            const newDecimal = newColumn.decimal ?? 0;
            
            // ✅ Remove `length` if the new type is in `no_length`
            if (dialectConfig?.noLength.includes(modifiedColumn.type || newColumn.type || oldColumn.type || "varchar")) {
                delete modifiedColumn.length;
                delete modifiedColumn.decimal;
            } else {

                if (dialectConfig?.decimals.includes(modifiedColumn.type || newColumn.type || oldColumn.type || "varchar")) {
                    // ✅ If type supports decimals, merge decimal values correctly
                    const oldPreDecimal = oldLength - oldDecimal;
                    const newPreDecimal = newLength - newDecimal;

                    const maxPreDecimal = Math.max(oldPreDecimal, newPreDecimal);
                    const maxDecimal = Math.max(oldDecimal, newDecimal);

                    modifiedColumn.length = maxPreDecimal + maxDecimal;
                    modifiedColumn.decimal = maxDecimal;
                } else {
                    // ✅ If type does not support decimals, just merge length
                    modifiedColumn.length = Math.max(oldLength, newLength);
                    delete modifiedColumn.decimal;
                }                
            }

            // ✅ Allow `NOT NULL` to `NULL`, but not vice versa
            if (newColumn.allowNull && !oldColumn.allowNull) {
                modifiedColumn.allowNull = true;
                nullableColumns.push(columnName);
                modified = true;
            }

            // ✅ Remove unique constraint if it's no longer unique
            if (oldColumn.unique && !newColumn.unique) {
                noLongerUnique.push(columnName);
            }

            // ✅ Ensure a type is set
            if(!modifiedColumn.type) {
                throw new Error(`Missing type for column ${columnName}`);
            }

            // ✅ Remove `length` if it's 0 and not required
            if (modifiedColumn.length === 0) {
                delete modifiedColumn.length;
            }

            // ✅ Ensure decimals only exist where applicable
            if (!dialectConfig?.decimals.includes(modifiedColumn.type)) {
                delete modifiedColumn.decimal;
            }

            // ✅ Only set modified flag if the length or decimal has changed
            if(modifiedColumn.length && oldColumn.length && modifiedColumn.length > oldColumn.length) {
                modified = true;
            }

            if (modified) {
                modifyColumns[columnName] = modifiedColumn;
            }
        }
    }

    for (const columnName of Object.keys(oldHeaders)) {
        if (oldHeaders[columnName].primary) {
            oldPrimaryKeys.push(columnName);
        }
    }
    for (const columnName of Object.keys(newHeaders)) {
        if (newHeaders[columnName].primary) {
            newPrimaryKeys.push(columnName);
        }
    }

    // ✅ Identify true primary key changes (excluding length-only modifications)
    const structuralPrimaryKeyChanges = newPrimaryKeys.filter(pk => !oldPrimaryKeys.includes(pk));

    // ✅ Only update primaryKeyChanges if there's an actual key change
    if (structuralPrimaryKeyChanges.length > 0 || renamedPrimaryKeys.length > 0) {
    primaryKeyChanges = [...new Set([...oldPrimaryKeys, ...newPrimaryKeys])];

        for (const { oldName, newName } of renamedPrimaryKeys) {
            if (primaryKeyChanges.includes(oldName)) {
                primaryKeyChanges.push(newName); // ✅ Add new key
            }
        }

        // ✅ Remove old names of renamed primary keys from the final key list
        for (const { oldName } of renamedPrimaryKeys) {
            primaryKeyChanges = primaryKeyChanges.filter(pk => pk !== oldName);
        }
    }

    const updatedMetaData: MetadataHeader = {
        ...oldHeaders,
        ...addColumns
    };
    
    // ✅ Apply modifications
    for (const col in modifyColumns) {
        updatedMetaData[col] = modifyColumns[col];
    }

    // ✅ Remove dropped columns
    for (const col of dropColumns) {
        delete updatedMetaData[col];
    }

    // ✅ Apply renames
    for (const { oldName, newName } of renameColumns) {
        updatedMetaData[newName] = updatedMetaData[oldName];
        delete updatedMetaData[oldName];
    }

    return {
        changes: {
            addColumns,
            modifyColumns,
            dropColumns,
            renameColumns,
            nullableColumns,
            noLongerUnique,
            primaryKeyChanges,
        },
        updatedMetaData
    };
}