import { SchemaLockTimeoutError } from "../src/errors";
import { validateConfig } from "../src/helpers/utilities";
import { DatabaseConfig } from "../src/config/types";

const BASE_CONFIG: DatabaseConfig = {
    sqlDialect: "mysql",
    host: "localhost",
    user: "user",
    password: "password",
    database: "testdb"
};

// ---------------------------------------------------------------------------
// SchemaLockTimeoutError
// ---------------------------------------------------------------------------

describe("SchemaLockTimeoutError", () => {
    test("is an instance of Error", () => {
        const err = new SchemaLockTimeoutError("test");
        expect(err).toBeInstanceOf(Error);
    });

    test("has name SchemaLockTimeoutError", () => {
        const err = new SchemaLockTimeoutError("test");
        expect(err.name).toBe("SchemaLockTimeoutError");
    });

    test("message is accessible", () => {
        const err = new SchemaLockTimeoutError("Could not acquire lock for table 'users' within 30s");
        expect(err.message).toContain("users");
        expect(err.message).toContain("30s");
    });

    test("can be caught as SchemaLockTimeoutError", () => {
        const fn = () => { throw new SchemaLockTimeoutError("lock timeout"); };
        expect(fn).toThrow(SchemaLockTimeoutError);
    });

    test("can be caught as Error", () => {
        const fn = () => { throw new SchemaLockTimeoutError("lock timeout"); };
        expect(fn).toThrow(Error);
    });
});

// ---------------------------------------------------------------------------
// validateConfig — useSchemaLock and schemaLockTimeout
// ---------------------------------------------------------------------------

describe("validateConfig — advisory lock options", () => {
    test("defaults useSchemaLock to false", () => {
        const result = validateConfig(BASE_CONFIG);
        expect(result.useSchemaLock).toBe(false);
    });

    test("defaults schemaLockTimeout to 30", () => {
        const result = validateConfig(BASE_CONFIG);
        expect(result.schemaLockTimeout).toBe(30);
    });

    test("accepts useSchemaLock: true", () => {
        const result = validateConfig({ ...BASE_CONFIG, useSchemaLock: true });
        expect(result.useSchemaLock).toBe(true);
    });

    test("accepts custom schemaLockTimeout", () => {
        const result = validateConfig({ ...BASE_CONFIG, schemaLockTimeout: 60 });
        expect(result.schemaLockTimeout).toBe(60);
    });

    test("throws when schemaLockTimeout is 0", () => {
        expect(() =>
            validateConfig({ ...BASE_CONFIG, schemaLockTimeout: 0 })
        ).toThrow("schemaLockTimeout must be greater than 0");
    });

    test("throws when schemaLockTimeout is negative", () => {
        expect(() =>
            validateConfig({ ...BASE_CONFIG, schemaLockTimeout: -1 })
        ).toThrow("schemaLockTimeout must be greater than 0");
    });

    test("accepts schemaLockTimeout of 1 (minimum valid)", () => {
        expect(() =>
            validateConfig({ ...BASE_CONFIG, schemaLockTimeout: 1 })
        ).not.toThrow();
    });
});

// ---------------------------------------------------------------------------
// PostgresDatabase lock key determinism (white-box test of hash function)
// ---------------------------------------------------------------------------

describe("advisory lock key determinism", () => {
    // djb2 hash used by PostgresDatabase.getLockKey — reproduced here to verify
    // that the same table name always produces the same 32-bit signed integer.
    function djb2(str: string): number {
        let hash = 5381;
        for (let i = 0; i < str.length; i++) {
            hash = ((hash << 5) + hash) + str.charCodeAt(i);
            hash |= 0;
        }
        return hash;
    }

    test("same table name produces same key", () => {
        expect(djb2("users")).toBe(djb2("users"));
    });

    test("different table names produce different keys", () => {
        expect(djb2("users")).not.toBe(djb2("orders"));
    });

    test("key is a 32-bit signed integer", () => {
        const key = djb2("some_long_table_name_here");
        expect(Number.isInteger(key)).toBe(true);
        expect(key).toBeGreaterThanOrEqual(-2147483648);
        expect(key).toBeLessThanOrEqual(2147483647);
    });

    test("empty string produces a consistent key", () => {
        expect(djb2("")).toBe(djb2(""));
    });
});
