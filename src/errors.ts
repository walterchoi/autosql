/**
 * `useSchemaLock: true` and the per-table advisory lock wasn't acquired within `schemaLockTimeout`:
 * another writer holds it for schema inference or DDL on the same table. Retry, or raise the timeout.
 */
export class SchemaLockTimeoutError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'SchemaLockTimeoutError';
    }
}

/**
 * `strictDriftDetection: true` and the live schema checksum does not match the last one recorded in
 * the schema history table.
 */
export class SchemaDriftError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'SchemaDriftError';
    }
}
