import { defaults } from "../config/defaults";
import { MetadataHeader } from "../config/types";
import { groupings } from "../config/groupings";

export function predictIndexes(config: { meta_data: MetadataHeader; maxKeyLength?: number }, primaryKey?: string[]): MetadataHeader {
    try {
        const headers: MetadataHeader = JSON.parse(JSON.stringify(config.meta_data)); // Deep copy to avoid mutation
        const maxKeyLength = config.maxKeyLength || defaults.maxKeyLength;
        let primaryKeyFound = false;

        let uniqueKey: string | null = null;
        let potentialPrimaryKeys: string[] = [];

        // ✅ Step 1: Predict indexes for date-related, unique, and pseudo-unique columns
        for (const [columnName, column] of Object.entries(headers)) {
            const columnType = column.type ?? "varchar";
            // Exclude long text fields from indexing
            if (groupings.textGroup.includes(columnType) && column.length && column.length >= maxKeyLength) {
                continue;
            }

            if (groupings.dateGroup.includes(columnType) && column.length && column.length < maxKeyLength) {
                column.index = true;
            }

            if ((column.pseudounique || column.unique) && column.length && column.length < maxKeyLength) {
                column.index = true;
            }

            // If an explicit primary key is defined, set it
            if (primaryKey && primaryKey.includes(columnName)) {
                column.primary = true;
                primaryKeyFound = true;
            }

            // ✅ Only consider unique columns that do NOT allow nulls as a primary key candidate
            if (column.unique && !column.allowNull && !uniqueKey) {
                uniqueKey = columnName;
            }

            // ✅ Track potential composite primary keys
            if (
                !column.allowNull &&
                groupings.keysGroup.includes(columnType) &&
                (column.unique || column.pseudounique) &&
                column.length &&
                column.length < maxKeyLength
            ) {
                potentialPrimaryKeys.push(columnName);
            }
        }

        // ✅ Step 2: Assign primary key(s)
        if (!primaryKeyFound) {
            if (uniqueKey) {
                // ✅ If a single unique column exists (and is NOT nullable), make it the primary key
                headers[uniqueKey].primary = true;
            } else if (potentialPrimaryKeys.length > 1) {
                // ✅ Otherwise, create a composite primary key
                for (const key of potentialPrimaryKeys) {
                    headers[key].primary = true;
                }
            } else if (potentialPrimaryKeys.length === 1) {
                headers[potentialPrimaryKeys[0]].primary = true;
            }
        }

        return headers;
    } catch (error) {
        throw new Error(`Error in predictIndexes: ${(error as Error).message}`);
    }
}