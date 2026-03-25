import { validateConfig } from "../src/helpers/utilities";
import { DatabaseConfig } from "../src/config/types";

const BASE_CONFIG: DatabaseConfig = {
    sqlDialect: "mysql",
    host: "localhost",
    user: "user",
    password: "password",
    database: "testdb"
};

describe("validateConfig — numeric bounds", () => {
    test("throws when insertStack is 0", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, insertStack: 0 })).toThrow("insertStack must be greater than 0");
    });

    test("throws when insertStack is negative", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, insertStack: -5 })).toThrow("insertStack must be greater than 0");
    });

    test("accepts insertStack of 1", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, insertStack: 1 })).not.toThrow();
    });

    test("throws when maxWorkers is 0", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, maxWorkers: 0 })).toThrow("maxWorkers must be at least 1");
    });

    test("throws when maxWorkers is negative", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, maxWorkers: -1 })).toThrow("maxWorkers must be at least 1");
    });

    test("accepts maxWorkers of 1", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, maxWorkers: 1 })).not.toThrow();
    });

    test("throws when pseudoUnique is 0 (exclusive lower bound)", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, pseudoUnique: 0 })).toThrow("pseudoUnique must be between 0 (exclusive) and 1 (inclusive)");
    });

    test("throws when pseudoUnique exceeds 1", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, pseudoUnique: 1.1 })).toThrow("pseudoUnique must be between 0 (exclusive) and 1 (inclusive)");
    });

    test("accepts pseudoUnique of exactly 1 (inclusive upper bound)", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, pseudoUnique: 1 })).not.toThrow();
    });

    test("accepts pseudoUnique of 0.9", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, pseudoUnique: 0.9 })).not.toThrow();
    });

    test("throws when categorical is 0 (exclusive lower bound)", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, categorical: 0 })).toThrow("categorical must be between 0 (exclusive) and 1 (exclusive)");
    });

    test("throws when categorical is 1 (exclusive upper bound)", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, categorical: 1 })).toThrow("categorical must be between 0 (exclusive) and 1 (exclusive)");
    });

    test("accepts categorical of 0.5", () => {
        expect(() => validateConfig({ ...BASE_CONFIG, categorical: 0.5 })).not.toThrow();
    });

    test("accepts fully valid config with all numeric bounds", () => {
        expect(() =>
            validateConfig({
                ...BASE_CONFIG,
                insertStack: 100,
                maxWorkers: 4,
                pseudoUnique: 0.9,
                categorical: 0.3
            })
        ).not.toThrow();
    });

    test("throws when sqlDialect is missing", () => {
        const { sqlDialect, ...withoutDialect } = BASE_CONFIG;
        expect(() => validateConfig(withoutDialect as DatabaseConfig)).toThrow();
    });

    test("throws when addHistory is true but useStagingInsert is false", () => {
        expect(() =>
            validateConfig({ ...BASE_CONFIG, addHistory: true, useStagingInsert: false })
        ).toThrow("addHistory requires useStagingInsert");
    });

    test("accepts addHistory: true when useStagingInsert is true", () => {
        expect(() =>
            validateConfig({ ...BASE_CONFIG, addHistory: true, useStagingInsert: true })
        ).not.toThrow();
    });
});
