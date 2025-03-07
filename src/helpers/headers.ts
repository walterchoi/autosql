import { initializeMetaData } from './metadata';
import { DialectConfig, MetadataHeader, ColumnDefinition, AlterTableChanges } from '../config/types';
import { collateTypes } from './types';
import { parseDatabaseLength, mergeColumnLengths } from './utilities';

export function getHeaders(data: Record<string, any>[]): string[] {
    try {
        const allColumns: string[] = [];

        data.forEach(row => {
            Object.keys(row).forEach(column => allColumns.push(column));
        });

        return Array.from(new Set(allColumns));
    } catch (error) {
        throw new Error(`Error in getHeaders: ${error}`);
    }
}

export function initializeHeaders(validatedConfig: any, data: Record<string, any>[]): MetadataHeader[] {
    let headers: MetadataHeader[] = validatedConfig.meta_data ?? initializeMetaData(getHeaders(data));

    if (validatedConfig.auto_id && !headers.some((header: MetadataHeader) => Object.keys(header)[0] === "ID")) {
        headers.push({
            ID: {
                type: "int",
                length: 8,
                allowNull: false,
                unique: true,
                index: true,
                pseudounique: true,
                primary: true,
                autoIncrement: true,
                default: undefined
            }
        });
    }

    return headers;
}

export function compareHeaders(oldHeaders: { [column: string]: ColumnDefinition }[], newHeaders: { [column: string]: ColumnDefinition }[], dialectConfig?: DialectConfig): AlterTableChanges {
    
    const addColumns: { [column: string]: ColumnDefinition }[] = [];
    const modifyColumns: { [column: string]: ColumnDefinition }[] = [];
    const dropColumns: string[] = [];
    const renameColumns: { oldName: string; newName: string }[] = [];
    const nullableColumns: string[] = [];
    const noLongerUnique: string[] = [];

    const oldMap = new Map<string, ColumnDefinition>(
        oldHeaders.map(header => [Object.keys(header)[0], header[Object.keys(header)[0]]])
    );

    const newMap = new Map<string, ColumnDefinition>(
        newHeaders.map(header => [Object.keys(header)[0], header[Object.keys(header)[0]]])
    );

    // ✅ Identify removed columns
    for (const oldColumnName of oldMap.keys()) {
        if (!newMap.has(oldColumnName)) {
            dropColumns.push(oldColumnName);
        }
    }

    // ✅ Identify renamed columns
    for (const oldColumnName of oldMap.keys()) {
        for (const newColumnName of newMap.keys()) {
            const oldColumn = oldMap.get(oldColumnName);
            const newColumn = newMap.get(newColumnName);

            // ✅ A rename is detected if:
            // - The columns do not share the same name
            // - The type and properties are identical
            if (oldColumnName !== newColumnName && oldColumn && newColumn && JSON.stringify(oldColumn) === JSON.stringify(newColumn)) {
                renameColumns.push({ oldName: oldColumnName, newName: newColumnName });

                // ✅ Remove renamed columns from dropColumns and addColumns lists
                dropColumns.splice(dropColumns.indexOf(oldColumnName), 1);
                newMap.delete(newColumnName);
            }
        }
    }

    // ✅ Identify added & modified columns
    for (const [columnName, newColumn] of newMap.entries()) {
        if (!oldMap.has(columnName)) {
            // New column - needs to be added
            addColumns.push({ [columnName]: newColumn });
        } else {
            const oldColumn = oldMap.get(columnName)!;
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
            if (dialectConfig?.no_length.includes(modifiedColumn.type || newColumn.type || oldColumn.type || "varchar")) {
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
                modifyColumns.push({ [columnName]: modifiedColumn });
            }
        }
    }

    return { addColumns, modifyColumns, dropColumns, renameColumns, nullableColumns, noLongerUnique };
}