import { escapeIdentifier, escapeLiteral, assertSafeTypeToken, assertSafeLength } from "../src/db/utils/escape";
import { MySQLTableQueryBuilder } from "../src/db/queryBuilders/mysql/tableBuilder";
import { PostgresTableQueryBuilder } from "../src/db/queryBuilders/pgsql/tableBuilder";
import { MetadataHeader, QueryInput } from "../src/config/types";
import { getInsertValues } from "../src/helpers/utilities";
import { mysqlConfig } from "../src/db/config/mysqlConfig";
import { pgsqlConfig } from "../src/db/config/pgsqlConfig";

const sql = (qi: QueryInput): string => (typeof qi === "string" ? qi : qi.query);

// Pure query-generation tests (no live DB) covering the SQL-injection hardening:
// identifiers are quote-escaped, type tokens and lengths are validated, and the emitted
// SQL is byte-identical for well-formed names.

describe("escapeIdentifier", () => {
    test("wraps a normal identifier unchanged per dialect", () => {
        expect(escapeIdentifier("user_id", "mysql")).toBe("`user_id`");
        expect(escapeIdentifier("user_id", "pgsql")).toBe('"user_id"');
    });

    test("doubles an embedded quote character so it cannot break out", () => {
        expect(escapeIdentifier("a`b", "mysql")).toBe("`a``b`");
        expect(escapeIdentifier('a"b', "pgsql")).toBe('"a""b"');
    });

    test("rejects empty / non-string / NUL identifiers", () => {
        expect(() => escapeIdentifier("", "mysql")).toThrow();
        expect(() => escapeIdentifier(undefined as any, "mysql")).toThrow();
        expect(() => escapeIdentifier("a\0b", "pgsql")).toThrow();
    });
});

describe("escapeLiteral", () => {
    test("doubles single quotes for both dialects", () => {
        expect(escapeLiteral("O'Brien", "pgsql")).toBe("'O''Brien'");
        expect(escapeLiteral("O'Brien", "mysql")).toBe("'O''Brien'");
    });

    test("escapes backslashes only for MySQL", () => {
        expect(escapeLiteral("a\\b", "mysql")).toBe("'a\\\\b'");
        expect(escapeLiteral("a\\b", "pgsql")).toBe("'a\\b'");
    });
});

describe("assertSafeTypeToken", () => {
    test("accepts local and multi-word server type tokens", () => {
        expect(assertSafeTypeToken("varchar")).toBe("varchar");
        expect(assertSafeTypeToken("timestamp with time zone")).toBe("timestamp with time zone");
        expect(assertSafeTypeToken("double precision")).toBe("double precision");
    });

    test("rejects tokens carrying punctuation used for injection", () => {
        expect(() => assertSafeTypeToken("text; DROP TABLE x")).toThrow();
        expect(() => assertSafeTypeToken("int)")).toThrow();
        expect(() => assertSafeTypeToken('varchar" USING evil')).toThrow();
    });
});

describe("assertSafeLength", () => {
    test("accepts non-negative integers", () => {
        expect(assertSafeLength(255)).toBe(255);
        expect(assertSafeLength(0)).toBe(0);
    });

    test("rejects non-integer / negative / injected lengths", () => {
        expect(() => assertSafeLength("255) NOT NULL, ADD COLUMN evil TEXT" as any)).toThrow();
        expect(() => assertSafeLength(-1)).toThrow();
        expect(() => assertSafeLength(1.5)).toThrow();
    });
});

describe("getInsertValues does not double-escape parameter-bound values", () => {
    // These values are bound as `?`/`$n` params by the drivers, so sqlize must NOT apply
    // quote/backslash escaping — otherwise O'Brien is stored as O''Brien.
    test("MySQL: apostrophes and backslashes pass through untouched", () => {
        const meta: MetadataHeader = { name: { type: "varchar", length: 50 } };
        const [value] = getInsertValues(meta, { name: "O'Brien\\x" }, mysqlConfig, undefined, true);
        expect(value).toBe("O'Brien\\x");
    });

    test("Postgres: apostrophes and backslashes pass through untouched", () => {
        const meta: MetadataHeader = { name: { type: "varchar", length: 50 } };
        const [value] = getInsertValues(meta, { name: "O'Brien\\x" }, pgsqlConfig, undefined, true);
        expect(value).toBe("O'Brien\\x");
    });

    test("type normalization still applies (boolean string -> 1 for MySQL)", () => {
        const meta: MetadataHeader = { flag: { type: "boolean" } };
        const [value] = getInsertValues(meta, { flag: "true" }, mysqlConfig, undefined, true);
        expect(value).toBe("1");
    });
});

describe("CREATE TABLE identifier injection is neutralized", () => {
    const EVIL_COL = "a`, ADD COLUMN evil TEXT, ADD COLUMN `b"; // MySQL break-out attempt
    const EVIL_COL_PG = 'a", ADD COLUMN evil TEXT, ADD COLUMN "b';

    test("MySQL: a malicious column name is emitted only as a fully-escaped identifier", () => {
        const headers: MetadataHeader = { [EVIL_COL]: { type: "varchar", length: 10 } };
        const query = sql(MySQLTableQueryBuilder.getCreateTableQuery("t", headers)[0]);
        // The name appears only inside doubled backticks; the raw single backtick that would
        // close the identifier is doubled, so no ", ADD COLUMN" clause escapes the quoting.
        expect(query).toContain(escapeIdentifier(EVIL_COL, "mysql"));
        expect(query).not.toContain("`a`, ADD COLUMN evil TEXT");
    });

    test("Postgres: a malicious column name is emitted only as a fully-escaped identifier", () => {
        const headers: MetadataHeader = { [EVIL_COL_PG]: { type: "varchar", length: 10 } };
        const query = sql(PostgresTableQueryBuilder.getCreateTableQuery("t", headers)[0]);
        expect(query).toContain(escapeIdentifier(EVIL_COL_PG, "pgsql"));
        expect(query).not.toContain('"a", ADD COLUMN evil TEXT');
    });

    test("normal names produce the exact expected SQL (escaping is transparent)", () => {
        const headers: MetadataHeader = {
            id: { type: "int", length: 11, primary: true },
            name: { type: "varchar", length: 255, allowNull: false },
        };
        const my = sql(MySQLTableQueryBuilder.getCreateTableQuery("test_table", headers)[0]);
        expect(my).toContain("`id` int(11)");
        expect(my).toContain("`name` varchar(255) NOT NULL");
        expect(my).toContain("PRIMARY KEY (`id`)");

        const pg = sql(PostgresTableQueryBuilder.getCreateTableQuery("test_table", headers)[0]);
        expect(pg).toContain('"id" int'); // Postgres does not length-qualify int
        expect(pg).toContain('"name" varchar(255) NOT NULL');
        expect(pg).toContain('PRIMARY KEY ("id")');
    });

    test("an unsafe type token or length is rejected at generation time", () => {
        expect(() =>
            MySQLTableQueryBuilder.getCreateTableQuery("t", { c: { type: "text; DROP TABLE x" } } as any)
        ).toThrow();
        expect(() =>
            MySQLTableQueryBuilder.getCreateTableQuery("t", {
                c: { type: "varchar", length: "255) NOT NULL, ADD COLUMN evil TEXT" as any },
            })
        ).toThrow();
    });
});
