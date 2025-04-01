import { defaults } from "../config/defaults";
import { MetadataHeader } from "../config/types";
import { groupings } from "../config/groupings";
import { generateCombinations, isCombinationUnique } from "../helpers/utilities";

export function predictIndexes(meta_data: MetadataHeader, maxKeyLengthInput?: number, primaryKey?: string[], data?: Record<string, any>[]): MetadataHeader {
    try {
        const headers: MetadataHeader = JSON.parse(JSON.stringify(meta_data)); // Deep copy to avoid mutation
        const maxKeyLength = maxKeyLengthInput || defaults.maxKeyLength;
        let primaryKeyFound = false;

        let requiredPrimaryKeys: string[] = [];
        let potentialPrimaryKeys: string[] = [];
        let potentialCompositeKeys: string[] = [];
        let NullablePseudoUniqueColumns: string[] = [];

        // ✅ Step 1: Predict indexes for date-related, unique, and pseudo-unique columns
        for (const [columnName, column] of Object.entries(headers)) {
            const columnType = column.type ?? "varchar";
            const columnLength = column.length ?? 255
            const isNumeric = groupings.intGroup.includes(columnType) || groupings.specialIntGroup.includes(columnType);
            const isDecimal = (column.decimal !== 0 && column.decimal !== undefined) || column.type == 'decimal' || groupings.specialIntGroup.includes(columnType); // Identify decimal columns
            const isText = groupings.textGroup.includes(columnType) && columnType !== "varchar";
            const isDate = groupings.dateGroup.includes(columnType);

            // Exclude long text fields from indexing
            if (isText) continue;
            // Exclude any field longer than max key length
            if (columnLength >= maxKeyLength) continue;
            // Exclude decimals from indexes
            if (isDecimal) continue;

            // Include dates, unique values and pseudouniques as indexes
            if (isDate || column.unique || column.pseudounique) {
                column.index = true;
            }

            // If an explicit primary key is defined, set it
            if (primaryKey && primaryKey.includes(columnName)) {
                column.primary = true;
                headers[columnName].primary = true;
                if (column.unique && !column.allowNull) {
                    potentialPrimaryKeys.push(columnName);
                    primaryKeyFound = true;
                } else {
                    requiredPrimaryKeys.push(columnName);
                }
            } else if (column.unique && !column.allowNull) { // ✅ Only consider unique columns that do NOT allow nulls as a primary key candidate
                potentialPrimaryKeys.push(columnName);
            } else if ((column.pseudounique || column.categorical) && !column.allowNull) {
                potentialCompositeKeys.push(columnName)
            } else if (column.pseudounique) {
                NullablePseudoUniqueColumns.push(columnName)
            }
        }
        
        if (!primaryKeyFound) {
            let selectedPrimaryKey: string[] | null = null;
            if (potentialPrimaryKeys && potentialPrimaryKeys.length > 0) {
                let idLikeKey: string | null = null;
                let numericKey: string | null = null;
                let shortestKey: string = potentialPrimaryKeys[0];
              
                for (const key of potentialPrimaryKeys) {
                  const type = headers[key]?.type ?? "";
              
                  // Prefer key ending in 'id' or '_id'
                  if (!idLikeKey && /(_id|id)$/i.test(key)) {
                    idLikeKey = key;
                  }
              
                  // Prefer numeric type
                  if (!numericKey && groupings.intGroup.includes(type)) {
                    numericKey = key;
                  }
              
                  // Track shortest as fallback
                  if (key.length < shortestKey.length) {
                    shortestKey = key;
                  }
                }
              
                // Pick in order of priority
                selectedPrimaryKey = idLikeKey
                  ? [idLikeKey]
                  : numericKey
                  ? [numericKey]
                  : shortestKey
                  ? [shortestKey]
                  : null;
            }      
            
            // ✅ If no unique column exists, try pseudo-unique combinations using data
            let foundUniqueCombination = false;
            const dateColumns = Object.keys(headers).filter(
                col => groupings.dateGroup.includes(headers[col].type ?? "") && headers[col].allowNull !== true
            );
            
            if (!selectedPrimaryKey && data && data.length > 0) {
                // Find the smallest set of pseudo-unique columns that together are unique
                for (let i = 1; i <= potentialCompositeKeys.length; i++) {
                    const combinations = generateCombinations(potentialCompositeKeys, i);
        
                    for (const combo of combinations) {
                        const fullCombo = Array.from(new Set([...requiredPrimaryKeys, ...combo]));
                        if (isCombinationUnique(data, fullCombo)) {
                            selectedPrimaryKey = fullCombo;
                            foundUniqueCombination = true;
                            break;
                        }
                    }
                    if (foundUniqueCombination) break;
                }
            }

            if (!selectedPrimaryKey && !foundUniqueCombination && data && data.length > 0) {
                const extendedColumns = [...potentialCompositeKeys, ...dateColumns];

                for (let i = 1; i <= extendedColumns.length; i++) {
                    const combinations = generateCombinations(extendedColumns, i);
    
                    for (const combo of combinations) {
                        const fullCombo = Array.from(new Set([...requiredPrimaryKeys, ...combo]));
                        if (isCombinationUnique(data, fullCombo)) {
                            selectedPrimaryKey = fullCombo; // ✅ Assign combo with date column
                            foundUniqueCombination = true;
                            break;
                        }
                    }
                    if (foundUniqueCombination) break;
                }
            }

            if (selectedPrimaryKey) {
                for (const key of selectedPrimaryKey) {
                    headers[key].primary = true;
                }
            }
        }
        return headers;
    } catch (error) {
        throw new Error(`Error in predictIndexes: ${(error as Error).message}`);
    }
}