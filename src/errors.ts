/**
 * Thrown when `useSchemaLock: true` and the per-table advisory lock could not
 * be acquired within the configured `schemaLockTimeout`.  Another writer is
 * currently running schema inference / DDL on the same table.
 *
 * Callers can catch this error and retry, or increase `schemaLockTimeout`.
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
