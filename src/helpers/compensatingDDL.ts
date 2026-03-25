import { AlterTableChanges, MetadataHeader, QueryInput } from '../config/types';
import { DialectConfig } from '../config/types';

/**
 * Builds best-effort compensating DDL to reverse a failed ALTER TABLE.
 *
 * PostgreSQL: DDL is transactional — `runTransaction` already issued a ROLLBACK.
 * No schema changes were applied, so no compensating queries are emitted. Warnings
 * are still returned for irreversible changes (e.g. dropped columns) so callers
 * can inform the user.
 *
 * MySQL: the single `ALTER TABLE` statement is atomic — if it fails the schema is
 * unchanged. Pre-UPDATE data transformations may have committed (MySQL's implicit
 * COMMIT on DDL), but the schema is still consistent. Compensating queries are
 * emitted as a safety net; `IF EXISTS` / `IF NOT EXISTS` guards make them no-ops
 * when no partial changes were applied.
 */
export function buildCompensatingDDL(
    table: string,
    changes: AlterTableChanges,
    updatedMetaData: MetadataHeader,
    dialectConfig: DialectConfig,
    schema?: string
): { queries: QueryInput[]; warnings: string[] } {
    const warnings: string[] = [];

    // Dropped columns can never be recovered regardless of dialect
    if (changes.dropColumns.length > 0) {
        warnings.push(
            `DDL rollback (${table}): cannot recover dropped column(s) ` +
            `[${changes.dropColumns.join(', ')}] — data in these columns is permanently lost.`
        );
    }

    // Nullable changes are not compensated — nulls may already have been written
    if (changes.nullableColumns.length > 0) {
        warnings.push(
            `DDL rollback (${table}): NOT NULL constraints on ` +
            `[${changes.nullableColumns.join(', ')}] were not restored — nulls may have been introduced.`
        );
    }

    // PostgreSQL: transactional DDL means the DB already rolled back everything.
    // Emitting compensating queries would be redundant and could cause errors
    // (e.g. ALTER COLUMN TYPE when the column is already the old type).
    if (dialectConfig.dialect === 'pgsql') {
        return { queries: [], warnings };
    }

    // MySQL: emit compensating queries as a best-effort safety net.
    const queries: QueryInput[] = [];
    const q = '`';
    const schemaPrefix = schema ? `${q}${schema}${q}.` : '';
    const tbl = `${schemaPrefix}${q}${table}${q}`;

    // 1. Reverse renames (newName → oldName) before other operations
    for (const { oldName, newName } of changes.renameColumns) {
        const col = updatedMetaData[newName] ?? updatedMetaData[oldName];
        if (!col?.type) continue;
        let colType = col.type.toLowerCase();
        if (dialectConfig.translate.localToServer[colType]) {
            colType = dialectConfig.translate.localToServer[colType];
        }
        let typeDef = colType;
        if (col.length && !dialectConfig.noLength.includes(colType)) {
            typeDef += `(${col.length}${
                col.decimal && dialectConfig.decimals.includes(colType) ? `,${col.decimal}` : ''
            })`;
        }
        const nullability = col.allowNull ? 'NULL' : 'NOT NULL';
        queries.push({
            query: `ALTER TABLE ${tbl} CHANGE COLUMN ${q}${newName}${q} ${q}${oldName}${q} ${typeDef} ${nullability};`,
            params: []
        });
    }

    // 2. Restore modified columns to their previous type
    for (const [columnName, column] of Object.entries(changes.modifyColumns)) {
        const prevType = column.previousType;
        if (!prevType || prevType === column.type) continue;
        let colType = prevType.toLowerCase();
        if (dialectConfig.translate.localToServer[colType]) {
            colType = dialectConfig.translate.localToServer[colType];
        }
        let typeDef = colType;
        // Use the merged length — it is >= the original length, so it safely holds
        // all pre-migration values while we restore the old type.
        if (column.length && !dialectConfig.noLength.includes(colType)) {
            typeDef += `(${column.length}${
                column.decimal && dialectConfig.decimals.includes(colType) ? `,${column.decimal}` : ''
            })`;
        }
        const nullability = column.allowNull ? 'NULL' : 'NOT NULL';
        queries.push({
            query: `ALTER TABLE ${tbl} MODIFY COLUMN ${q}${columnName}${q} ${typeDef} ${nullability};`,
            params: []
        });
    }

    // 3. Drop any newly added columns (IF EXISTS → no-op if the ALTER never applied them)
    const addedCols = Object.keys(changes.addColumns);
    if (addedCols.length > 0) {
        const dropClauses = addedCols
            .map(col => `DROP COLUMN IF EXISTS ${q}${col}${q}`)
            .join(', ');
        queries.push({ query: `ALTER TABLE ${tbl} ${dropClauses};`, params: [] });
    }

    return { queries, warnings };
}
