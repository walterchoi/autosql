import { defaults } from "../config/defaults";
import { groupings } from "./groupings";

export function predictIndexes(config: { meta_data: Record<string, any>[]; max_key_length?: number }, primaryKey?: string[]): Record<string, any>[] {
    try {
        const headers = config.meta_data.map(header => ({ ...header })); // Copy to avoid mutation
        const maxKeyLength = config.max_key_length || defaults.max_key_length;
        let primaryKeyFound = false;

        const headerMap = new Map<string, any>(); // Store references for quick lookup
        headers.forEach(header => {
            const headerName = Object.keys(header)[0];
            headerMap.set(headerName, header[headerName]);
        });

        let uniqueKey: string | null = null;
        let potentialPrimaryKeys: string[] = [];

        // Step 1: Predict indexes for date-related, unique, and pseudo-unique columns
        headerMap.forEach((column, headerName) => {
            // Exclude long text fields from indexing
            if (groupings.textGroup.includes(column.type) && column.length >= maxKeyLength) {
                return;
            }

            if (groupings.dateGroup.includes(column.type) && column.length < maxKeyLength) {
                column.index = true;
            }

            if ((column.pseudounique || column.unique) && column.length < maxKeyLength) {
                column.index = true;
            }

            // If an explicit primary key is defined, set it
            if (primaryKey && primaryKey.includes(headerName)) {
                column.primary = true;
                primaryKeyFound = true;
            }

            // ✅ Only consider unique columns that do NOT allow nulls as a primary key candidate
            if (column.unique && !column.allowNull && !uniqueKey) {
                uniqueKey = headerName;
            }

            // ✅ Track potential composite primary keys
            if (
                !column.allowNull &&
                groupings.keysGroup.includes(column.type) &&
                (column.unique || column.pseudounique) &&
                column.length < maxKeyLength
            ) {
                potentialPrimaryKeys.push(headerName);
            }
        });

        // Step 2: Assign primary key(s)
        if (!primaryKeyFound) {
            if (uniqueKey) {
                // ✅ If a single unique column exists (and is NOT nullable), make it the primary key
                headerMap.get(uniqueKey)!.primary = true;
            } else if (potentialPrimaryKeys.length > 1) {
                // ✅ Otherwise, create a composite primary key
                potentialPrimaryKeys.forEach(key => {
                    headerMap.get(key)!.primary = true;
                });
            } else if (potentialPrimaryKeys.length === 1) {
                headerMap.get(potentialPrimaryKeys[0])!.primary = true;
            }
        }

        return headers;
    } catch (error) {
        throw new Error(`Error in predictIndexes: ${(error as Error).message}`);
    }
}