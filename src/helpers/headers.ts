import { initializeMetaData } from './metadata';
import { DialectConfig, MetadataHeader, ColumnDefinition, AlterTableChanges } from '../config/types';
import { collateTypes } from './columnTypes';
import { parseDatabaseLength, mergeColumnLengths, shuffleArray } from './utilities';

export function getHeaders(data: Record<string, any>[], sampling?: number, samplingMinimum?: number ): string[] {
    try {
        if ((sampling !== undefined || samplingMinimum !== undefined) && (sampling === undefined || samplingMinimum === undefined)) {
            throw new Error("Both sampling percentage and sampling minimum must be provided together.");
        }

        let sampleData = data;

        // Apply sampling if conditions are met
        if (sampling !== undefined && samplingMinimum !== undefined && data.length > samplingMinimum) {
            let sampleSize = Math.round(data.length * sampling);
            sampleSize = Math.max(sampleSize, samplingMinimum); // Ensure minimum sample size

            sampleData = shuffleArray(data).slice(0, sampleSize); // Shuffle and take sample
        }

        const columnSet: Set<string> = new Set();

        for (const row of sampleData) {
            for (const column of Object.keys(row)) {
                columnSet.add(column);
            }
        }

        return [...columnSet]; // Convert Set to array
    } catch (error) {
        throw new Error(`Error in getHeaders: ${error}`);
    }
}

export function initializeHeaders(validatedConfig: any, data: Record<string, any>[]): MetadataHeader[] {
    let headers: MetadataHeader[] = validatedConfig.metaData ?? initializeMetaData(getHeaders(data));
    return headers;
}

export function compareHeaders(oldHeadersOriginal: MetadataHeader, newHeadersOriginal: MetadataHeader, dialectConfig?: DialectConfig): AlterTableChanges {
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

            if (oldColumnName !== newColumnName && JSON.stringify(oldColumn) === JSON.stringify(newColumn)) {
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
                console.warn(`🔄 Converting ${columnName}: ${oldType} → ${recommendedType}`);
                modifiedColumn.type = recommendedType;
                modified = true;
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
            if(modifiedColumn.length !== oldColumn.length || modifiedColumn.decimal !== newColumn.decimal) {
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
    const structuralPrimaryKeyChanges = oldPrimaryKeys.filter(pk => !newPrimaryKeys.includes(pk))
    .concat(newPrimaryKeys.filter(pk => !oldPrimaryKeys.includes(pk)));

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

    return {
        addColumns,
        modifyColumns,
        dropColumns,
        renameColumns,
        nullableColumns,
        noLongerUnique,
        primaryKeyChanges,
    };
}