import { initializeMetaData } from './metadata';
import { MetadataHeader, ColumnDefinition } from '../config/types';
import { collateTypes } from './types';
import { DialectConfig } from '../db/config/interfaces';
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

export function compareHeaders(oldHeaders: { [column: string]: ColumnDefinition }[], newHeaders: { [column: string]: ColumnDefinition }[], dialectConfig?: DialectConfig): 
{ addColumns: { [column: string]: ColumnDefinition }[], modifyColumns: { [column: string]: ColumnDefinition }[] } {
    
    const addColumns: { [column: string]: ColumnDefinition }[] = [];
    const modifyColumns: { [column: string]: ColumnDefinition }[] = [];

    const oldMap = new Map<string, ColumnDefinition>(
        oldHeaders.map(header => [Object.keys(header)[0], header[Object.keys(header)[0]]])
    );

    for (const newHeader of newHeaders) {
        const columnName = Object.keys(newHeader)[0];
        const newColumn = newHeader[columnName];

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

            // ✅ Remove `length` if the new type is in `no_length`
            if (dialectConfig?.no_length.includes(modifiedColumn.type || newColumn.type || oldColumn.type || "varchar")) {
                delete modifiedColumn.length;
                delete modifiedColumn.decimal;
            } else {
                // ✅ Merge column lengths safely
                const oldLength = oldColumn.length ?? 0;
                const newLength = newColumn.length ?? 0;
                const oldDecimal = oldColumn.decimal ?? 0;
                const newDecimal = newColumn.decimal ?? 0;

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
                modified = true;
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

    return { addColumns, modifyColumns };
}