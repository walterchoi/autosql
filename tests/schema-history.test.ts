import { computeChecksum } from "../src/helpers/schemaHistory";
import { validateConfig } from "../src/helpers/utilities";
import { DatabaseConfig, MetadataHeader } from "../src/config/types";
import { SchemaDriftError } from "../src/errors";

const BASE: DatabaseConfig = { sqlDialect: "mysql", host: "localhost", user: "u", password: "p", database: "db" };

describe("computeChecksum", () => {
    test("same schema produces same checksum", () => {
        const s: MetadataHeader = { id: { type: "int", primary: true } };
        expect(computeChecksum(s)).toBe(computeChecksum(s));
    });

    test("different schemas produce different checksums", () => {
        const a: MetadataHeader = { id: { type: "int" } };
        const b: MetadataHeader = { id: { type: "varchar", length: 255 } };
        expect(computeChecksum(a)).not.toBe(computeChecksum(b));
    });

    test("checksum is key-order independent", () => {
        const a: MetadataHeader = { id: { type: "int", allowNull: false } };
        const b: MetadataHeader = { id: { allowNull: false, type: "int" } };
        expect(computeChecksum(a)).toBe(computeChecksum(b));
    });

    test("returns a 64-char hex string", () => {
        expect(computeChecksum({ id: { type: "int" } })).toMatch(/^[0-9a-f]{64}$/);
    });

    test("empty schema has a consistent checksum", () => {
        expect(computeChecksum({})).toBe(computeChecksum({}));
    });
});

describe("SchemaDriftError", () => {
    test("is an instance of Error", () => {
        expect(new SchemaDriftError("x")).toBeInstanceOf(Error);
    });
    test("name is SchemaDriftError", () => {
        expect(new SchemaDriftError("x").name).toBe("SchemaDriftError");
    });
    test("can be caught as SchemaDriftError", () => {
        expect(() => { throw new SchemaDriftError("drift"); }).toThrow(SchemaDriftError);
    });
});

describe("validateConfig — schema history options", () => {
    test("schemaHistory defaults to false", () => {
        expect(validateConfig(BASE).schemaHistory).toBe(false);
    });
    test("schemaHistoryTable defaults to autosql_schema_history", () => {
        expect(validateConfig(BASE).schemaHistoryTable).toBe("autosql_schema_history");
    });
    test("strictDriftDetection defaults to false", () => {
        expect(validateConfig(BASE).strictDriftDetection).toBe(false);
    });
    test("detectDrift defaults to true", () => {
        expect(validateConfig(BASE).detectDrift).toBe(true);
    });
    test("accepts schemaHistory: true", () => {
        expect(() => validateConfig({ ...BASE, schemaHistory: true })).not.toThrow();
    });
});

describe("validateConfig — streaming options", () => {
    test("streamingStagingPrefix defaults to autosql_stream__", () => {
        expect(validateConfig(BASE).streamingStagingPrefix).toBe("autosql_stream__");
    });
    test("streamMaxRetries defaults to 3", () => {
        expect(validateConfig(BASE).streamMaxRetries).toBe(3);
    });
    test("keepOrphanedStagingTables defaults to false", () => {
        expect(validateConfig(BASE).keepOrphanedStagingTables).toBe(false);
    });
    test("throws when streamMaxRetries is 0", () => {
        expect(() => validateConfig({ ...BASE, streamMaxRetries: 0 })).toThrow("streamMaxRetries must be at least 1");
    });
    test("accepts streamMaxRetries: 1", () => {
        expect(() => validateConfig({ ...BASE, streamMaxRetries: 1 })).not.toThrow();
    });
});
