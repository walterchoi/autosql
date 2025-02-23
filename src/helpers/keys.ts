import { defaults } from "../config/defaults";
import { groupings } from "./groupings";

export function predictIndexes(config: { meta_data: Record<string, any>[]; max_key_length?: number }, primaryKey?: string[]): Record<string, any>[] {
    try {
        const headers = config.meta_data;
        const maxKeyLength = config.max_key_length || defaults.max_key_length;
        let primaryKeyFound = false;

        // Iterate over headers and predict indexes
        headers.forEach(header => {
            const headerName = Object.keys(header)[0];
            const column = header[headerName];

            // Consider date-related columns as indexes
            if (groupings.dateGroup.includes(column.type) && column.length < maxKeyLength) {
                column.index = true;
            }

            // Consider unique and pseudo-unique columns as indexes
            if ((column.pseudounique || column.unique) && column.length < maxKeyLength) {
                column.index = true;
            }

            // If an explicit primary key is defined, set it
            if (primaryKey && primaryKey.includes(headerName)) {
                column.primary = true;
                primaryKeyFound = true;
            }
        });

        // Predict composite primary keys if none were explicitly set
        if (!primaryKeyFound) {
            const potentialPrimaryKeys: string[] = [];

            headers.forEach(header => {
                const headerName = Object.keys(header)[0];
                const column = header[headerName];

                if (
                    !column.allowNull &&
                    groupings.keysGroup.includes(column.type) &&
                    (column.unique || column.pseudounique) &&
                    column.length < maxKeyLength
                ) {
                    potentialPrimaryKeys.push(headerName);
                }
            });

            // Use composite keys if multiple viable columns exist
            if (potentialPrimaryKeys.length > 1) {
                potentialPrimaryKeys.forEach(key => {
                    headers.find(header => Object.keys(header)[0] === key)![key].primary = true;
                });
            } else if (potentialPrimaryKeys.length === 1) {
                headers.find(header => Object.keys(header)[0] === potentialPrimaryKeys[0])![potentialPrimaryKeys[0]].primary = true;
            }
        }

        return headers;
    } catch (error) {
        throw new Error(`Error in predictIndexes: ${error}`);
    }
}