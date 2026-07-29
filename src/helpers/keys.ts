import { defaults } from "../config/defaults";
import { MetadataHeader, ColumnDefinition, DatabaseConfig } from "../config/types";
import { groupings } from "../config/groupings";
import { generateCombinations, isCombinationUnique } from "../helpers/utilities";

/** Column definition for an auto-generated surrogate primary key (BIGINT AUTO_INCREMENT / BIGSERIAL). */
function surrogateColumnDefinition(): ColumnDefinition {
    return {
        type: "bigint",
        length: 0,
        allowNull: false,
        // A PRIMARY KEY is implicitly unique; leaving `unique` false avoids emitting a redundant
        // UNIQUE constraint alongside the PK.
        unique: false,
        index: false,
        pseudounique: false,
        primary: true,
        autoIncrement: true,
        decimal: 0,
        default: undefined,
    };
}

/**
 * Apply the opt-in surrogate primary key. A surrogate is an auto-increment column added when a
 * dataset has no natural key, so a table can still be created (and Postgres upserts have a
 * conflict target). This is deliberately *sticky* to the existing table so re-ingestion is
 * idempotent — `compareMetaData` must see no key change on run 2, otherwise it would try to
 * drop the surrogate or thrash the primary key:
 *
 *  - **Existing table with a surrogate** → keep that exact column as the sole primary key,
 *    regardless of what this batch happened to infer (a coincidentally-unique batch must not
 *    introduce a competing natural key).
 *  - **Existing table without a surrogate** → never introduce one now; respect the schema that
 *    was chosen when the table was created.
 *  - **New table (no existing metadata)** → inject a surrogate only when no natural primary key
 *    was found by `predictIndexes`.
 *
 * No-op unless `config.surrogateKey` is enabled. Never mutates the input.
 */
export function applySurrogateKey(
    metaData: MetadataHeader,
    existingMetaData: MetadataHeader | null | undefined,
    config: DatabaseConfig
): MetadataHeader {
    if (!config.surrogateKey) return metaData;

    if (existingMetaData) {
        const existingSurrogate = Object.entries(existingMetaData).find(
            ([, col]) => col.autoIncrement === true && col.primary === true
        )?.[0];
        // Respect the existing schema's key strategy; only stay sticky to an existing surrogate.
        if (!existingSurrogate) return metaData;

        const result: MetadataHeader = JSON.parse(JSON.stringify(metaData));
        for (const col in result) {
            if (result[col].primary) result[col].primary = false;
        }
        result[existingSurrogate] = surrogateColumnDefinition();
        return result;
    }

    // New table: a surrogate is only a *fallback* — a natural key always wins.
    const hasPrimary = Object.values(metaData).some(col => col.primary === true);
    if (hasPrimary) return metaData;

    const name = config.surrogateKeyColumn || defaults.surrogateKeyColumn;
    if (metaData[name]) {
        throw new Error(
            `surrogateKey: cannot add surrogate column "${name}" because a column with that name ` +
            `already exists in the data. Set \`surrogateKeyColumn\` to a non-colliding name.`
        );
    }
    // Prepend so the id column reads first in the created table.
    return { [name]: surrogateColumnDefinition(), ...metaData };
}

export function predictIndexes(meta_data: MetadataHeader, maxKeyLengthInput?: number, primaryKey?: string[], data?: Record<string, any>[], maxCompositeKeyColumns?: number): MetadataHeader {
    // P4: bound the composite-key search. Without a cap it tries every subset of pseudo-unique
    // columns (O(2^N)), each scanned over all rows — a real hang on a wide, key-less table.
    const maxComposite = Math.max(1, maxCompositeKeyColumns ?? 4);
    try {
        const headers: MetadataHeader = JSON.parse(JSON.stringify(meta_data)); // Deep copy to avoid mutation
        const maxKeyLength = maxKeyLengthInput || defaults.maxKeyLength;
        let primaryKeyFound = false;

        let requiredPrimaryKeys: string[] = [];
        let potentialPrimaryKeys: string[] = [];
        let potentialCompositeKeys: string[] = [];
        let NullablePseudoUniqueColumns: string[] = [];

        // Key limits are enforced in bytes, so a ~200-char multibyte (CJK/emoji) value can be
        // ~600 bytes and exceed the key limit even though its char length looks fine. When the
        // sample data is available, capture each column's max byte length in one pass so the
        // index/key checks below can gate on it (falls back to char length otherwise).
        const maxByteLenByColumn: Record<string, number> = {};
        if (data && data.length) {
            for (const row of data) {
                for (const col in row) {
                    const v = row[col];
                    if (v === null || v === undefined) continue;
                    const b = Buffer.byteLength(String(v), "utf8");
                    if (b > (maxByteLenByColumn[col] ?? 0)) maxByteLenByColumn[col] = b;
                }
            }
        }

        // ✅ Step 1: Predict indexes for date-related, unique, and pseudo-unique columns
        for (const [columnName, column] of Object.entries(headers)) {
            const columnType = column.type ?? "varchar";
            const columnLength = Math.max(column.length ?? 0, maxByteLenByColumn[columnName] ?? 0) || 255
            const isNumeric = groupings.intGroup.includes(columnType) || groupings.specialIntGroup.includes(columnType);
            const isDecimal = (column.decimal !== 0 && column.decimal !== undefined) || column.type == 'decimal' || groupings.specialIntGroup.includes(columnType); // Identify decimal columns
            const isText = groupings.textGroup.includes(columnType) && columnType !== "varchar";
            const isDate = groupings.dateGroup.includes(columnType);

            // An explicitly requested primary key must be honored even if it is long/text/
            // decimal — otherwise it is silently dropped and a different key (or none) is
            // chosen. The auto-index exclusions below apply only to non-explicit columns.
            const isExplicitPrimaryKey = !!(primaryKey && primaryKey.includes(columnName));

            // Exclude long text fields from indexing
            if (!isExplicitPrimaryKey && isText) continue;
            // Exclude any field longer than max key length
            if (!isExplicitPrimaryKey && columnLength >= maxKeyLength) continue;
            // Exclude decimals from indexes
            if (!isExplicitPrimaryKey && isDecimal) continue;

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
              
                  // Prefer a key that is exactly "id" or ends in "_id". Anchored so ordinary
                  // words ending in "id" (paid, void, valid, grid, rapid) are not mistaken for
                  // identifier columns and wrongly preferred as the primary key.
                  if (!idLikeKey && /(^id$|_id$)/i.test(key)) {
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
                const cap1 = Math.min(potentialCompositeKeys.length, maxComposite);
                for (let i = 1; i <= cap1; i++) {
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

                const cap2 = Math.min(extendedColumns.length, maxComposite);
                for (let i = 1; i <= cap2; i++) {
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