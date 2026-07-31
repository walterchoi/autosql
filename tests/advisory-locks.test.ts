import { SchemaLockTimeoutError } from "../src/errors";
import { validateConfig } from "../src/helpers/utilities";
import { DatabaseConfig } from "../src/config/types";
import { Database } from "../src/db/database";

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
    // White-box test of PostgresDatabase.getLockKey — a pair of int4 keys derived from sha256
    // (used with the pg_advisory_lock(int4, int4) 64-bit form). No connection is needed.
    const pgDb: any = Database.create({ sqlDialect: "pgsql", host: "localhost", user: "u", password: "p", database: "d" });
    const key = (t: string): [number, number] => pgDb.getLockKey(t);

    test("same table name produces the same key pair", () => {
        expect(key("users")).toEqual(key("users"));
    });

    test("different table names produce different key pairs", () => {
        expect(key("users")).not.toEqual(key("orders"));
    });

    test("key is a pair of 32-bit signed integers", () => {
        const pair = key("some_long_table_name_here");
        expect(pair).toHaveLength(2);
        for (const k of pair) {
            expect(Number.isInteger(k)).toBe(true);
            expect(k).toBeGreaterThanOrEqual(-2147483648);
            expect(k).toBeLessThanOrEqual(2147483647);
        }
    });

    test("empty string produces a consistent key", () => {
        expect(key("")).toEqual(key(""));
    });
});

// ---------------------------------------------------------------------------
// releaseSchemaLock — connection disposal on unlock failure (F7)
// ---------------------------------------------------------------------------
// White-box: inject a fake pooled client into schemaLockConnections and drive
// releaseSchemaLock directly. No live DB needed — createPool is lazy.

describe("releaseSchemaLock — connection disposal (F7)", () => {
    test("mysql: returns the connection to the pool on successful RELEASE_LOCK", async () => {
        const db: any = Database.create(BASE_CONFIG); // mysql
        const client = { query: jest.fn().mockResolvedValue([[], []]), release: jest.fn(), destroy: jest.fn() };
        db.schemaLockConnections.set("t", client);
        await db.releaseSchemaLock("t");
        expect(client.release).toHaveBeenCalledTimes(1);
        expect(client.destroy).not.toHaveBeenCalled();
        expect(db.schemaLockConnections.has("t")).toBe(false);
    });

    test("mysql: DESTROYS the connection (never returns a lock-holding one) when RELEASE_LOCK fails, and does not throw", async () => {
        const db: any = Database.create(BASE_CONFIG);
        const client = { query: jest.fn().mockRejectedValue(new Error("connection lost")), release: jest.fn(), destroy: jest.fn() };
        db.schemaLockConnections.set("t", client);
        await expect(db.releaseSchemaLock("t")).resolves.toBeUndefined();
        expect(client.destroy).toHaveBeenCalledTimes(1);
        expect(client.release).not.toHaveBeenCalled();
        expect(db.schemaLockConnections.has("t")).toBe(false);
    });

    test("mysql: no-op when no lock is registered for the table", async () => {
        const db: any = Database.create(BASE_CONFIG);
        await expect(db.releaseSchemaLock("never-locked")).resolves.toBeUndefined();
    });

    test("pgsql: returns the connection to the pool (release()) on successful unlock", async () => {
        const db: any = Database.create({ sqlDialect: "pgsql", host: "localhost", user: "u", password: "p", database: "d" });
        const client = { query: jest.fn().mockResolvedValue({ rows: [] }), release: jest.fn() };
        db.schemaLockConnections.set("t", client);
        await db.releaseSchemaLock("t");
        expect(client.release).toHaveBeenCalledTimes(1);
        expect(client.release).not.toHaveBeenCalledWith(true);
        expect(db.schemaLockConnections.has("t")).toBe(false);
    });

    test("pgsql: DESTROYS the connection (release(true)) when the unlock query fails", async () => {
        const db: any = Database.create({ sqlDialect: "pgsql", host: "localhost", user: "u", password: "p", database: "d" });
        const client = { query: jest.fn().mockRejectedValue(new Error("connection lost")), release: jest.fn() };
        db.schemaLockConnections.set("t", client);
        await expect(db.releaseSchemaLock("t")).resolves.toBeUndefined();
        expect(client.release).toHaveBeenCalledWith(true);
        expect(db.schemaLockConnections.has("t")).toBe(false);
    });
});
