import { ColumnDefinition, initializeMetaData } from './metadata';
import { MetadataHeader } from '../config/types';
import { collateTypes } from './types';
import safeTypeChanges from "../db/config/safeTypeChanges"

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

export function compareHeaders(oldHeaders: { [column: string]: ColumnDefinition }[], newHeaders: { [column: string]: ColumnDefinition }[]): 
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

            // ✅ Increase Column Length if Required
            if (newColumn.length && (!oldColumn.length || newColumn.length > oldColumn.length)) {
                modifiedColumn.length = newColumn.length;
                modified = true;
            }

            // ✅ Increase Decimal Precision if Required
            if (newColumn.decimal && (!oldColumn.decimal || newColumn.decimal > oldColumn.decimal)) {
                modifiedColumn.decimal = newColumn.decimal;
                modified = true;
            }

            // ✅ Allow `NOT NULL` to `NULL`, but not vice versa
            if (newColumn.allowNull && !oldColumn.allowNull) {
                modifiedColumn.allowNull = true;
                modified = true;
            }

            if (modified) {
                modifyColumns.push({ [columnName]: modifiedColumn });
            }
        }
    }

    return { addColumns, modifyColumns };
}