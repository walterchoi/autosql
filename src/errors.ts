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
