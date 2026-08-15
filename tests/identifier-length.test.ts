import { escapeIdentifier } from "../src/db/utils/escape";

// C7 (BYOD hardening): an over-long identifier — a source-derived table/column name, or an
// autosql-derived staging/history name built from it — must fail LOUDLY with a clear error rather
// than be emitted (MySQL dies mid-load) or, worse, SILENTLY TRUNCATED by Postgres (which can collide
// two distinct names). Limits: MySQL 64 chars, Postgres 63 BYTES, SQL Server 128 chars.

const name = (n: number) => "a".repeat(n);

describe("escapeIdentifier — length validation (C7)", () => {
    describe("at the limit is accepted, one over throws", () => {
        const cases = [
            { dialect: "mysql" as const, limit: 64 },
            { dialect: "pgsql" as const, limit: 63 },
            { dialect: "sqlserver" as const, limit: 128 },
        ];
        for (const { dialect, limit } of cases) {
            test(`${dialect}: ${limit} ok, ${limit + 1} throws`, () => {
                expect(() => escapeIdentifier(name(limit), dialect)).not.toThrow();
                expect(() => escapeIdentifier(name(limit + 1), dialect)).toThrow(/exceeds the .* identifier limit/i);
            });
        }
    });

    test("the error names the identifier and the limit", () => {
        expect(() => escapeIdentifier(name(70), "mysql")).toThrow(/"a{70}" \(70 characters\) exceeds the 64-character identifier limit for mysql/);
    });

    test("Postgres is measured in BYTES (a multibyte name under 63 chars can still exceed 63 bytes)", () => {
        // 32 × "é" = 32 characters but 64 UTF-8 bytes → over the 63-byte Postgres limit.
        const multibyte = "é".repeat(32);
        expect(multibyte.length).toBe(32);
        expect(Buffer.byteLength(multibyte, "utf8")).toBe(64);
        expect(() => escapeIdentifier(multibyte, "pgsql")).toThrow(/64 bytes\) exceeds the 63-byte identifier limit/);
        // The same 32-char name is fine on MySQL (character-counted, well under 64).
        expect(() => escapeIdentifier(multibyte, "mysql")).not.toThrow();
    });

    test("normal identifiers are unaffected (back-compat)", () => {
        expect(escapeIdentifier("users", "mysql")).toBe("`users`");
        expect(escapeIdentifier("temp_staging__orders", "pgsql")).toBe('"temp_staging__orders"');
        expect(escapeIdentifier("my_col", "sqlserver")).toBe("[my_col]");
    });
});
