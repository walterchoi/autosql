import { DatabaseConfig } from '../config/types';
import { normalizeNumber, validateConfig, shuffleArray, sqlize } from './utilities';
import { groupings, isNumeric } from '../config/groupings';
import { collateTypes } from './columnTypes';
import { predictType } from './columnTypes';
import { defaults, nonCategoricalTypes, DEFAULT_LENGTHS } from '../config/defaults';
import { predictIndexes } from './keys';
import { Database } from '../db/database';
import { DialectConfig, ColumnDefinition, MetadataHeader, metaDataInterim, AlterTableChanges } from '../config/types';
import { mysqlConfig } from "../db/config/mysqlConfig";
import { pgsqlConfig } from "../db/config/pgsqlConfig";
import { sqlServerConfig } from "../db/config/sqlServerConfig";

export function initializeMetaData(headers: string[]): Record<string, any>[] {
    try {
        return headers.map(header => ({
            [header]: {
                type: null,
                length: 0,
                allowNull: false,
                unique: false,
                index: false,
                pseudounique: false,
                primary: false,
                autoIncrement: false,
                default: undefined,
                decimal: 0
            }
        }));
    } catch (error) {
        throw new Error(`Error in initializeMetaData: ${error}`);
    }
}

/**
 * Provided-schema ("assumeSchema", A-4) helpers. A caller that already knows the schema hands it in
 * so AutoSQL skips per-value type inference (expensive predictType/sqlize regex) and its footguns
 * (e.g. small ints mis-typed as boolean).
 */

/** Every distinct column key present across the data rows. Cheap — key iteration, no per-value regex. */
export function collectDataColumns(data: Record<string, any>[]): Set<string> {
    const cols = new Set<string>();
    for (const row of data) {
        for (const key in row) cols.add(key);
    }
    return cols;
}

/** True when the provided schema declares every column present in the data (inference can be skipped). */
export function schemaCoversColumns(schema: MetadataHeader, columns: Set<string>): boolean {
    for (const col of columns) {
        if (!(col in schema)) return false;
    }
    return true;
}

/** Overlay provided column definitions onto an inferred header — provided wins per declared column. */
export function overlaySchema(inferred: MetadataHeader, provided: MetadataHeader): MetadataHeader {
    return { ...inferred, ...provided };
}

/**
 * Fill a provided (possibly sparse) schema with `ColumnDefinition` defaults so DDL builders get
 * complete definitions. Provided values win; only `type` is required. A length-requiring type
 * (varchar/decimal) with no length gets a default so the DDL stays valid.
 */
export function fillColumnDefaults(schema: MetadataHeader): MetadataHeader {
    const result: MetadataHeader = {};
    for (const [col, def] of Object.entries(schema)) {
        if (!def || !def.type) {
            throw new Error(`assumeSchema: column "${col}" is missing a required "type".`);
        }
        const filled: ColumnDefinition = {
            length: 0,
            allowNull: false,
            unique: false,
            index: false,
            pseudounique: false,
            primary: false,
            autoIncrement: false,
            decimal: 0,
            ...def,
        };
        const defaultLen = DEFAULT_LENGTHS[filled.type as keyof typeof DEFAULT_LENGTHS];
        if ((filled.length ?? 0) === 0 && defaultLen) {
            filled.length = defaultLen;
        }
        result[col] = filled;
    }
    return result;
}

export async function getDataHeaders(data: Record<string, any>[], databaseConfig: DatabaseConfig): Promise<MetadataHeader> {
    const sampling = databaseConfig.sampling;
    const samplingMinimum = databaseConfig.samplingMinimum;
    let metaData : MetadataHeader = {};
    const allColumns = new Set<string>();
    let metaDataInterim : metaDataInterim = {};

    if ((sampling !== undefined || samplingMinimum !== undefined) && (sampling === undefined || samplingMinimum === undefined)) {
        throw new Error("Both sampling percentage and sampling minimum must be provided together.");
    }

    const dialect = databaseConfig.sqlDialect;
    const dialectConfig: DialectConfig = dialect === 'mysql' ? mysqlConfig : dialect === 'sqlserver' ? sqlServerConfig : pgsqlConfig;
    // Decimal scale ceiling: configured cap if set, else the dialect's numeric limit (keep full
    // precision up to what the DB can hold). Deeper values are rounded (with a warning), or with
    // `decimalToVarchar` stored as text to preserve them exactly.
    const decimalScaleCap = databaseConfig.decimalMaxLength ?? dialectConfig.maxDecimalScale;

    let sampleData = data;
    let remainingData: Record<string, any>[] = [];

    if (sampling !== undefined && sampling > 0 && samplingMinimum !== undefined && data.length > samplingMinimum) {
        let sampleSize = Math.round(data.length * sampling);
        sampleSize = Math.max(sampleSize, samplingMinimum); // Ensure minimum sample size

        const shuffledData = shuffleArray(data);
        sampleData = shuffledData.slice(0, sampleSize); // Shuffle and take sample
        remainingData = shuffledData.slice(sampleSize); // Store remaining data
    }

    // Cap uniqueSet per column at sampled rows + 1: enough to observe every distinct sample value
    // and confirm 100% uniqueness, bounded to in-memory data. A cap tied to the pseudounique ratio
    // (ceil(0.9 * N)) saturated a truly-unique column at ~0.9N rows, so `unique` was never
    // confirmable for a dense column of ~10+ rows — always mislabeled pseudounique.
    const uniqueSetCap = sampleData.length + 1;
    const forceStringSet = new Set<string>(databaseConfig.forceStringColumns ?? []);
    const booleanSet = new Set<string>(databaseConfig.booleanColumns ?? []);
    // A `booleanColumns`-hinted value must be a real boolean: true/false, 0/1 (case-insensitive),
    // or null/blank. Anything else is rejected rather than silently coerced (forcing to boolean is
    // lossy). Nullish is allowed and handled by the normal null path.
    const isBooleanDomainValue = (v: any): boolean => {
        if (v === '' || v === null || v === undefined || v === '\\N' || v === 'null') return true;
        return ["0", "1", "true", "false"].includes(String(v).trim().toLowerCase());
    };
    const assertBooleanColumn = (column: string, value: any) => {
        if (booleanSet.has(column) && !isBooleanDomainValue(value)) {
            throw new Error(`Column "${column}" is declared in booleanColumns but received a non-boolean value: ${JSON.stringify(value)}. Allowed: true/false, 0/1, or null.`);
        }
    };

    // A24c: track columns with a lone-separator number ambiguous between thousands-grouping and a
    // decimal (e.g. "1,234"). normalizeNumber() flags these via the callback below; we assume decimal
    // but warn once per run so the caller can disambiguate via thousandsSeparator/decimalSeparator.
    // Single hoisted closure (no per-value alloc) records the current column, set before each predictType().
    const ambiguousSeparatorColumns = new Set<string>();
    let ambiguitySeparatorColumn: string | null = null;
    const noteAmbiguousSeparator = () => {
        if (ambiguitySeparatorColumn !== null) ambiguousSeparatorColumns.add(ambiguitySeparatorColumn);
    };

    for (const row of sampleData) {
        const rowColumns = Object.keys(row);
        rowColumns.forEach(column => allColumns.add(column))
        for (const column of allColumns) {
            const value = row[column]

            if(metaData[column] == undefined) {
                metaData[column] = {
                    type: null,
                    length: 0,
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                }
            }
            if(metaDataInterim[column] == undefined) {
                metaDataInterim[column] = {
                    uniqueSet: new Set(),
                    uniqueSaturated: false,
                    valueCount: 0,
                    nullCount: 0,
                    types: new Set(),
                    length: 0,
                    byteLength: 0,
                    decimal: 0,
                    trueMaxDecimal: 0,
                    intLen: 0
                }
            }

            if (value === '' || value === null || value === undefined || value === '\\N' || value === 'null') {
                metaData[column].allowNull = true;
                metaDataInterim[column].nullCount++;
                continue;
            }

            // booleanColumns: reject out-of-domain values early (type forced to boolean at collation below).
            assertBooleanColumn(column, value);

            // forceStringColumns: skip type inference, track only raw string length
            if (forceStringSet.has(column)) {
                const strValue = String(value);
                metaDataInterim[column].valueCount++;
                if (!metaDataInterim[column].uniqueSaturated) {
                    metaDataInterim[column].uniqueSet.add(strValue);
                    if (metaDataInterim[column].uniqueSet.size >= uniqueSetCap) {
                        metaDataInterim[column].uniqueSaturated = true;
                    }
                }
                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, strValue.length);
                metaDataInterim[column].byteLength = Math.max(metaDataInterim[column].byteLength, Buffer.byteLength(strValue, "utf8"));
                continue;
            }

            ambiguitySeparatorColumn = column;
            const type = predictType(value, databaseConfig.thousandsSeparator, databaseConfig.decimalSeparator, noteAmbiguousSeparator)
            if(!type) continue;
            const sqlizedValue = sqlize(value, type, dialectConfig, databaseConfig)
            metaDataInterim[column].valueCount++;
            if (!metaDataInterim[column].uniqueSaturated) {
                metaDataInterim[column].uniqueSet.add(sqlizedValue);
                if (metaDataInterim[column].uniqueSet.size >= uniqueSetCap) {
                    metaDataInterim[column].uniqueSaturated = true;
                }
            }
            metaDataInterim[column].types.add(type);
            // `exponent` is a specialInt but normalizeNumber() returns null for "1.23e10", so the
            // numeric length branch would compute a bogus decimal length. It maps to a no-length
            // DOUBLE anyway, so route it to the plain string-length branch.
            if ((groupings.intGroup.includes(type) || groupings.specialIntGroup.includes(type)) && type !== "exponent") {
                let valueStr = normalizeNumber(value, databaseConfig.thousandsSeparator, databaseConfig.decimalSeparator);
                if(!valueStr) {
                    valueStr = String(value).trim();
                }
                const decimalLen = valueStr.includes(".") ? valueStr.split(".")[1].length : 0;
                const integerLen = valueStr.split(".")[0].length;
                metaDataInterim[column].intLen = Math.max(metaDataInterim[column].intLen ?? 0, integerLen);
                metaDataInterim[column].decimal = Math.max(metaDataInterim[column].decimal, decimalLen);
                metaDataInterim[column].trueMaxDecimal = Math.max(metaDataInterim[column].trueMaxDecimal, metaDataInterim[column].decimal, decimalLen);
                metaDataInterim[column].decimal = Math.min(metaDataInterim[column].decimal, decimalScaleCap);

                // Precision = max integer digits + max scale, as independent running maxes. Summing
                // this row's integer length with the running scale (old behaviour) under-counted when
                // the widest-integer and widest-scale values were different rows (e.g. decimal(3,2)
                // overflowing on 10.5).
                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, (metaDataInterim[column].intLen ?? 0) + metaDataInterim[column].decimal);
            } else {
                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, String(value).length);
                metaDataInterim[column].byteLength = Math.max(metaDataInterim[column].byteLength, Buffer.byteLength(String(value), "utf8"));
            }
        }
    }

    // A17: a column first appearing partway through the sample never had EARLIER rows counted as nulls
    // (it wasn't in `allColumns` yet), so a sparse column looks NOT-NULL and fully-unique — a spurious
    // PRIMARY-KEY candidate the same batch then fails to insert. Reconcile: count every row scanned
    // before a column's first value as a null, so it's correctly inferred nullable (and not a PK).
    const totalSampledRows = sampleData.length;
    for (const column in metaDataInterim) {
        const interim = metaDataInterim[column];
        const seen = interim.valueCount + interim.nullCount;
        if (seen < totalSampledRows) interim.nullCount += totalSampledRows - seen;
    }

    for (const column in metaDataInterim) {
        // forceStringColumns always resolve to varchar, booleanColumns always to boolean,
        // regardless of observed types (0/1 would otherwise infer as int).
        const type = forceStringSet.has(column)
            ? "varchar"
            : booleanSet.has(column)
                ? "boolean"
                : collateTypes(metaDataInterim[column].types);
        metaDataInterim[column].collated_type = type;
        metaData[column].type = type;
        metaData[column].length = metaDataInterim[column].length || 0;
        metaData[column].decimal = metaDataInterim[column].decimal || 0;

        // A decimal whose true scale exceeds the cap (`decimalMaxLength`, else the dialect's numeric
        // limit) can't be stored at full precision as a number. Either store as exact text (opt-in
        // `decimalToVarchar`) or round + warn so it's visible. See decisions.md D-G.
        if (type === "decimal" && (metaDataInterim[column].trueMaxDecimal ?? 0) > decimalScaleCap) {
            const trueScale = metaDataInterim[column].trueMaxDecimal ?? 0;
            const intLen = metaDataInterim[column].intLen ?? 0;
            if (databaseConfig.decimalToVarchar) {
                metaData[column].type = "varchar";
                metaData[column].length = intLen + trueScale + 2;
                metaData[column].decimal = 0;
                databaseConfig.logger?.warn?.(`Column "${column}": decimal values carry up to ${trueScale} fractional digit(s), beyond the ${decimalScaleCap}-digit scale ceiling — storing as text (varchar) to preserve exact precision (decimalToVarchar).`);
            } else {
                databaseConfig.logger?.warn?.(`Column "${column}": decimal values carry up to ${trueScale} fractional digit(s) but the column scale is capped at ${metaData[column].decimal} — stored values will be ROUNDED. Raise decimalMaxLength, or set decimalToVarchar to store the exact value as text.`);
            }
        }

        const uniqueSize = metaDataInterim[column].uniqueSet.size;
        const valueCount = metaDataInterim[column].valueCount;
        const saturated = metaDataInterim[column].uniqueSaturated;
        // When saturated, uniqueSize/valueCount is a lower bound on the true unique percentage.
        const uniquePercentage = uniqueSize / valueCount;
        if (!saturated && uniquePercentage == 1 && uniqueSize > 0) {
            metaData[column].unique = true;
        } else if (uniquePercentage >= (databaseConfig.pseudoUnique || defaults.pseudoUnique) && uniqueSize > 0) {
            metaData[column].pseudounique = true;
        } else if (!saturated && uniquePercentage <= (databaseConfig.categorical || defaults.categorical) && uniqueSize > 0 && !nonCategoricalTypes.includes(type)) {
            metaData[column].categorical = true;
        } else if (!saturated && uniqueSize == 1 && metaDataInterim[column].nullCount == 0 && valueCount > 0) {
            metaData[column].singleValue = true;
        }
        if(metaDataInterim[column].nullCount !== 0) {
            metaData[column].allowNull = true;
        }
        // Key length limits are enforced in bytes, so use the max byte length for multibyte
        // (e.g. CJK/emoji) data — a 200-char value can still exceed the byte key limit.
        const keyByteLen = Math.max(metaData[column].length, metaDataInterim[column].byteLength);
        if(keyByteLen > (databaseConfig.maxKeyLength || defaults.maxKeyLength) && metaData[column].unique) {
            metaData[column].unique = false
        }
        if(metaData[column].type === 'varchar' && metaData[column].length > (databaseConfig.maxVarcharLength || defaults.maxVarcharLength)) {
            metaData[column].type = 'text'
        }
    }

    // Sampling caveat: the unique/pseudounique flags above are sample-only. A column 100% unique in
    // the sample but with duplicates in the unsampled remainder would get a UNIQUE constraint (and
    // possibly PK) at CREATE TABLE, then fail on insert of the full data. Re-validate the columns
    // still flagged `unique` against the full dataset and demote any that aren't. Only runs when
    // sampling split the data (else remainingData is empty and flags already reflect the full set).
    // A truly-unique column is never saturated (distinct == sampleSize < cap), so its uniqueSet holds
    // every sampled value and the continuation below is exact.
    if (remainingData.length > 0) {
        const isNullish = (v: any) => v === '' || v === null || v === undefined || v === '\\N' || v === 'null';
        for (const column in metaData) {
            if (!metaData[column].unique) continue;
            const seen = new Set(metaDataInterim[column].uniqueSet);
            let valueCount = metaDataInterim[column].valueCount;
            let duplicate = false;
            for (const row of remainingData) {
                const value = row[column];
                if (isNullish(value)) continue;
                let normalized: any;
                if (forceStringSet.has(column)) {
                    normalized = String(value);
                } else {
                    const t = predictType(value, databaseConfig.thousandsSeparator, databaseConfig.decimalSeparator);
                    normalized = t ? sqlize(value, t, dialectConfig, databaseConfig) : String(value);
                }
                valueCount++;
                if (seen.has(normalized)) {
                    duplicate = true;
                } else {
                    seen.add(normalized);
                }
            }
            if (duplicate) {
                metaData[column].unique = false;
                // Demote to pseudounique if still highly distinct across the full data, so it can
                // still serve as an index / composite-key candidate downstream.
                const ratio = valueCount > 0 ? seen.size / valueCount : 0;
                if (ratio >= (databaseConfig.pseudoUnique || defaults.pseudoUnique)) {
                    metaData[column].pseudounique = true;
                }
            }
        }
    }

    for (const row of remainingData) {
        for (const column of allColumns) {
            const value = row[column]
            if (value === null || value === undefined) continue;
            // booleanColumns: validate non-sampled values too (type forced to boolean at recollation below).
            assertBooleanColumn(column, value);
            // Re-evaluate type on non-sampled rows too (not just length): a wider value (int needing
            // int/bigint, or decimal/float on an int column) must upgrade the inferred type set, else
            // it overflows/truncates on insert. Re-collated from this set below.
            const valueType = predictType(value, databaseConfig.thousandsSeparator, databaseConfig.decimalSeparator);
            if(!valueType) continue;
            metaDataInterim[column].types.add(valueType);
            if ((groupings.intGroup.includes(valueType) || groupings.specialIntGroup.includes(valueType)) && valueType !== "exponent") {
                let valueStr = normalizeNumber(value, databaseConfig.thousandsSeparator, databaseConfig.decimalSeparator);
                if(!valueStr) {
                    valueStr = String(value).trim();
                }
                const decimalLen = valueStr.includes(".") ? valueStr.split(".")[1].length : 0;
                const integerLen = valueStr.split(".")[0].length;

                metaDataInterim[column].intLen = Math.max(metaDataInterim[column].intLen ?? 0, integerLen);
                metaDataInterim[column].decimal = Math.max(metaDataInterim[column].decimal, decimalLen);
                metaDataInterim[column].trueMaxDecimal = Math.max(metaDataInterim[column].trueMaxDecimal, metaDataInterim[column].decimal, decimalLen);
                metaDataInterim[column].decimal = Math.min(metaDataInterim[column].decimal, decimalScaleCap);

                // Precision = max integer digits + max scale (independent running maxes); see sample-loop comment above.
                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, (metaDataInterim[column].intLen ?? 0) + metaDataInterim[column].decimal);
            } else {
                metaDataInterim[column].length = Math.max(metaDataInterim[column].length, String(value).length);
                metaDataInterim[column].byteLength = Math.max(metaDataInterim[column].byteLength, Buffer.byteLength(String(value), "utf8"));
            }
        }
    }

    for (const column in metaDataInterim) {
        // When sampling was used, re-collate from the full type set (sample + remaining) so non-sampled
        // values can widen the inferred type. Guarded on remainingData so the no-sampling path is
        // untouched. Text promotion is re-applied below off the final length.
        if (remainingData.length > 0) {
            const recollated = forceStringSet.has(column)
                ? "varchar"
                : booleanSet.has(column)
                    ? "boolean"
                    : collateTypes(metaDataInterim[column].types);
            metaDataInterim[column].collated_type = recollated;
            metaData[column].type = recollated;
        }

        // Non-decimal type but decimal set: fold decimal into length (+1 for the dot) and zero decimal,
        // on metaDataInterim[column] for later use. Use trueMaxDecimal (not the capped decimal) so a
        // scale rounded down by the cap is still fully accounted for when converting to a non-decimal type.
        if (!dialectConfig.decimals.includes(metaDataInterim[column].collated_type || 'varchar')) {
            metaDataInterim[column].length = metaDataInterim[column].length + (metaDataInterim[column].decimal > 0 ? 1 : 0) - metaDataInterim[column].decimal + metaDataInterim[column].trueMaxDecimal;
            metaDataInterim[column].decimal = 0;
        }
        metaData[column].length = metaDataInterim[column].length || 0;
        metaData[column].decimal = metaDataInterim[column].decimal || 0;

        // Re-apply length-based text promotion: remainingData may hold longer values than the sample,
        // so varchar→text (and wider) thresholds must be re-checked once the final length is known.
        const finalLen = metaData[column].length || 0;
        // TEXT/MEDIUMTEXT/LONGTEXT caps are byte limits, so promote on byte length — else a multibyte
        // value under the char threshold overflows and the DB silently truncates on insert.
        const finalByteLen = Math.max(finalLen, metaDataInterim[column].byteLength);
        if (metaData[column].type === 'varchar' && finalLen > (databaseConfig.maxVarcharLength || defaults.maxVarcharLength)) {
            metaData[column].type = 'text';
        }
        if (metaData[column].type === 'text' && finalByteLen >= 65535) {
            metaData[column].type = 'mediumtext';
        }
        if (metaData[column].type === 'mediumtext' && finalByteLen >= 16777215) {
            metaData[column].type = 'longtext';
        }
    }

    // An all-null column has no data to infer a type, so it is deferred: NOT added to the schema now,
    // created (correctly typed) when a later batch first carries data. A guessed type is wrong — a
    // `varchar` guess locks the column so later int/date data collates back to varchar, and a bare
    // `varchar` is invalid DDL on MySQL. A column already in the table is unaffected (comes through the
    // old metadata during comparison, still inserted as null). Deferral is now unconditional; it was
    // previously gated on `excludeBlankColumns` (a footgun that could enable the type-guess). See D-C.
    const emptyOrNullKeys = Object.entries(metaDataInterim)
        .filter(([_, meta]) => meta.uniqueSet.size === 0 && meta.valueCount === 0 && meta.nullCount > 0)
        .map(([key]) => key);
    for (const key of emptyOrNullKeys) {
        delete metaData[key];
    }

    // A24c: warn once per run for columns that resolved to numeric AND carried an ambiguous
    // lone-separator value. Suppressed when the caller supplied explicit separators (normalizeNumber
    // takes the override path and never flags), and filtered to numeric columns so a text column that
    // merely contained "1,234" stays silent.
    if (ambiguousSeparatorColumns.size > 0
        && databaseConfig.thousandsSeparator === undefined
        && databaseConfig.decimalSeparator === undefined) {
        const numericAmbiguous = [...ambiguousSeparatorColumns]
            .filter(col => metaData[col] && isNumeric(metaData[col].type ?? ""));
        if (numericAmbiguous.length > 0) {
            const warn = databaseConfig.logger?.warn ?? databaseConfig.logger?.log;
            warn?.(`autosql: column(s) ${numericAmbiguous.map(c => `"${c}"`).join(", ")} contain values with a single ',' or '.' followed by exactly three digits (e.g. "1,234"), which is ambiguous between thousands grouping (1234) and a decimal (1.234). Assuming decimal — if these are grouped integers, pass thousandsSeparator + decimalSeparator to disambiguate.`);
        }
    }

    return metaData
}

export async function getMetaData(databaseOrConfig: Database | DatabaseConfig, data: Record<string, any>[], primaryKey?: string[]) : Promise<MetadataHeader> {
    try {
        let validatedConfig: DatabaseConfig;
        let dbInstance: Database | undefined;
        let dialectConfig: DialectConfig;

        // Determine if input is a Database instance or a config object
        if (databaseOrConfig instanceof Database) {
            dbInstance = databaseOrConfig;
            validatedConfig = validateConfig(dbInstance.getConfig()); // Use existing Database config
            dialectConfig = dbInstance.getDialectConfig();
        } else {
            validatedConfig = validateConfig(databaseOrConfig); // Use provided config
            if(validatedConfig.sqlDialect == 'mysql') {
                dialectConfig = mysqlConfig;
            } else if(validatedConfig.sqlDialect == 'pgsql') {
                dialectConfig = pgsqlConfig;
            } else if(validatedConfig.sqlDialect == 'sqlserver') {
                dialectConfig = sqlServerConfig;
            } else {
                throw new Error(`Unsupported SQL dialect: ${validatedConfig.sqlDialect}`);
            }
        }
        
        const headers = await getDataHeaders(data, validatedConfig)
        let metaData : MetadataHeader
        if(validatedConfig.autoIndexing) {
            metaData = predictIndexes(headers, validatedConfig.maxKeyLength, primaryKey || validatedConfig.primaryKey, data, validatedConfig.maxCompositeKeyColumns)
        } else {
            metaData = headers
        }
        
        return metaData;
        
    } catch (error) {
        throw new Error(`Error in getMetaData: ${error}`);
    }
}

export function compareMetaData(oldHeadersOriginal: MetadataHeader | null, newHeadersOriginal: MetadataHeader, dialectConfig?: DialectConfig, logger?: { warn?: (msg: string) => void }): { changes: AlterTableChanges; updatedMetaData: MetadataHeader } {
    if(!oldHeadersOriginal) {
        return { 
            changes: {
                addColumns: {},
                modifyColumns: {},
                dropColumns: [],
                renameColumns: [],
                nullableColumns: [],
                noLongerUnique: [],
                primaryKeyChanges: [],
            },
            updatedMetaData: newHeadersOriginal
        }
    }
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

    // Identify removed columns
    for (const oldColumnName of Object.keys(oldHeaders)) {
        if (!newHeaders.hasOwnProperty(oldColumnName)) {
            dropColumns.push(oldColumnName);
        }
    }

    // Identify renamed columns — O(n) fingerprint approach. A rename is inferred only when exactly
    // one removed column matches exactly one added column by definition; ambiguous cases (identical
    // definitions on either side) stay drop+add to avoid wrong-column renames.
    //
    // Deliberately conservative: a rename can't be known from metadata alone (data just carries a new
    // key), and DB-parsed `oldHeaders` vs inferred `newHeaders` rarely match exactly, so most
    // evolutions fall through to drop+add. That's the fidelity-first outcome — `deleteColumns` defaults
    // to false, preserving the old column and its data rather than moving data under another name. Do
    // not loosen the match to type-only — that reintroduces wrong-column data association.
    const fingerprint = (col: ColumnDefinition): string =>
        JSON.stringify(Object.fromEntries(Object.entries(col).sort()));

    const removedOldCols = dropColumns.slice();
    const addedNewColNames = Object.keys(newHeaders).filter(col => !(col in oldHeaders));

    const addedByFp = new Map<string, string[]>();
    for (const col of addedNewColNames) {
        const fp = fingerprint(newHeaders[col]);
        if (!addedByFp.has(fp)) addedByFp.set(fp, []);
        addedByFp.get(fp)!.push(col);
    }

    const removedByFp = new Map<string, string[]>();
    for (const col of removedOldCols) {
        const fp = fingerprint(oldHeaders[col]);
        if (!removedByFp.has(fp)) removedByFp.set(fp, []);
        removedByFp.get(fp)!.push(col);
    }

    for (const [fp, oldCols] of removedByFp) {
        const newCols = addedByFp.get(fp) ?? [];
        if (oldCols.length !== 1 || newCols.length !== 1) continue;
        const oldColumnName = oldCols[0];
        const newColumnName = newCols[0];
        renameColumns.push({ oldName: oldColumnName, newName: newColumnName });
        if (oldHeaders[oldColumnName].primary && newHeaders[newColumnName].primary) {
            renamedPrimaryKeys.push({ oldName: oldColumnName, newName: newColumnName });
        }
        dropColumns.splice(dropColumns.indexOf(oldColumnName), 1);
        delete newHeaders[newColumnName];
    }

    // Identify added & modified columns
    for (const [columnName, newColumn] of Object.entries(newHeaders)) {
        if (!oldHeaders.hasOwnProperty(columnName)) {
            // New column added to an EXISTING table (branch only runs when oldHeaders is present).
            // Pre-existing rows have no value, so it must be nullable — a NOT NULL add fails on
            // Postgres ("column contains null values") and silently back-fills 0/'' on MySQL (a
            // data-quality trap). A back-fillable column — a calculated timestamp (dwh_*, DEFAULT
            // CURRENT_TIMESTAMP) or one with an explicit default — keeps its NOT NULL. (R11 / D-A.)
            const canBackfill = newColumn.calculated === true || newColumn.default !== undefined;
            addColumns[columnName] = canBackfill ? newColumn : { ...newColumn, allowNull: true };
        } else {
            const oldColumn = oldHeaders[columnName];
            let modified = false;
            let modifiedColumn: ColumnDefinition = { ...oldColumn };

            const oldType = oldColumn.type ?? "varchar";
            const newType = newColumn.type ?? "varchar";

            // Use `collateTypes()` to determine the best compatible type
            const recommendedType = collateTypes([oldType, newType]);

            if (recommendedType !== oldType) {
                logger?.warn?.(`Converting ${columnName}: ${oldType} → ${recommendedType}`);
                modifiedColumn.type = recommendedType;
                modifiedColumn.previousType = oldType;
                modified = true;
            } else {
                modifiedColumn.type = recommendedType;
                modifiedColumn.previousType = oldType;
            }

            // Merge column lengths safely
            const oldLength = oldColumn.length ?? 0;
            const newLength = newColumn.length ?? 0;
            const oldDecimal = oldColumn.decimal ?? 0;
            const newDecimal = newColumn.decimal ?? 0;
            
            // Remove `length` if the new type is in `no_length`
            if (dialectConfig?.noLength.includes(modifiedColumn.type || newColumn.type || oldColumn.type || "varchar")) {
                delete modifiedColumn.length;
                delete modifiedColumn.decimal;
            } else {

                if (dialectConfig?.decimals.includes(modifiedColumn.type || newColumn.type || oldColumn.type || "varchar")) {
                    // If type supports decimals, merge decimal values correctly
                    const oldPreDecimal = oldLength - oldDecimal;
                    const newPreDecimal = newLength - newDecimal;

                    const maxPreDecimal = Math.max(oldPreDecimal, newPreDecimal);
                    const maxDecimal = Math.max(oldDecimal, newDecimal);

                    modifiedColumn.length = maxPreDecimal + maxDecimal;
                    modifiedColumn.decimal = maxDecimal;
                } else {
                    // If type does not support decimals, just merge length
                    modifiedColumn.length = Math.max(oldLength, newLength);
                    delete modifiedColumn.decimal;
                }                
            }

            // Allow `NOT NULL` to `NULL`, but not vice versa
            if (newColumn.allowNull && !oldColumn.allowNull) {
                modifiedColumn.allowNull = true;
                nullableColumns.push(columnName);
                modified = true;
            }

            // Remove unique constraint if it's no longer unique
            if (oldColumn.unique && !newColumn.unique) {
                noLongerUnique.push(columnName);
            }

            // Ensure a type is set
            if(!modifiedColumn.type) {
                throw new Error(`Missing type for column ${columnName}`);
            }

            // Remove `length` if it's 0 and not required
            if (modifiedColumn.length === 0) {
                delete modifiedColumn.length;
            }

            // Ensure decimals only exist where applicable
            if (!dialectConfig?.decimals.includes(modifiedColumn.type)) {
                delete modifiedColumn.decimal;
            }

            // Only set modified flag if the length or decimal has changed
            if(modifiedColumn.length && oldColumn.length && modifiedColumn.length > oldColumn.length) {
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

    // Identify true primary key changes (excluding length-only modifications)
    const structuralPrimaryKeyChanges = newPrimaryKeys.filter(pk => !oldPrimaryKeys.includes(pk));

    // Only update primaryKeyChanges if there's an actual key change
    if (structuralPrimaryKeyChanges.length > 0 || renamedPrimaryKeys.length > 0) {
    primaryKeyChanges = [...new Set([...oldPrimaryKeys, ...newPrimaryKeys])];

        for (const { oldName, newName } of renamedPrimaryKeys) {
            if (primaryKeyChanges.includes(oldName)) {
                primaryKeyChanges.push(newName); // Add new key
            }
        }

        // Remove old names of renamed primary keys from the final key list
        for (const { oldName } of renamedPrimaryKeys) {
            primaryKeyChanges = primaryKeyChanges.filter(pk => pk !== oldName);
        }
    }

    const updatedMetaData: MetadataHeader = {
        ...oldHeaders,
        ...addColumns
    };
    
    // Apply modifications
    for (const col in modifyColumns) {
        updatedMetaData[col] = modifyColumns[col];
    }

    // Remove dropped columns
    for (const col of dropColumns) {
        delete updatedMetaData[col];
    }

    // Apply renames
    for (const { oldName, newName } of renameColumns) {
        updatedMetaData[newName] = updatedMetaData[oldName];
        delete updatedMetaData[oldName];
    }

    return {
        changes: {
            addColumns,
            modifyColumns,
            dropColumns,
            renameColumns,
            nullableColumns,
            noLongerUnique,
            primaryKeyChanges,
        },
        updatedMetaData
    };
}