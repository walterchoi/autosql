import { initializeMetaData } from './metadata';
import { MetadataHeader } from '../config/types';

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