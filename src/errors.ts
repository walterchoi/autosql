/**
 * Thrown when `useSchemaLock: true` and the per-table advisory lock couldn't be acquired within
 * `schemaLockTimeout` (another writer holds it for schema inference/DDL on the same table).
 * Callers can retry or increase `schemaLockTimeout`.
 */
export class SchemaLockTimeoutError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'SchemaLockTimeoutError';
    }
}

/**
 * Thrown when `strictDriftDetection: true` and the live schema checksum does
 * not match the last recorded checksum in the schema history table.
 */
export class SchemaDriftError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'SchemaDriftError';
    }
}
