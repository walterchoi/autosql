import { escapeIdentifier } from "../src/db/utils/escape";
import { generateSafeConstraintName } from "../src/helpers/utilities";
import { Database } from "../src/db/database";
import { MySQLDatabase } from "../src/db/mysql";

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

    // §3 — the limit is counted the way each dialect counts it, so a legal international name isn't
    // false-rejected. An astral char (emoji) is 2 UTF-16 code units but 1 MySQL character.
    test("§3 MySQL counts CODE POINTS (an astral-char name within 64 chars is accepted)", () => {
        expect([..."😀".repeat(40)].length).toBe(40);
        expect("😀".repeat(40).length).toBe(80); // UTF-16 code units — the naive count that used to reject
        expect(() => escapeIdentifier("😀".repeat(40), "mysql")).not.toThrow();
        expect(() => escapeIdentifier("😀".repeat(65), "mysql")).toThrow(); // 65 code points > 64
    });
    test("§3 SQL Server counts UTF-16 code units (sysname = nvarchar(128))", () => {
        expect(() => escapeIdentifier("😀".repeat(64), "sqlserver")).not.toThrow(); // 128 units = limit
        expect(() => escapeIdentifier("😀".repeat(65), "sqlserver")).toThrow();      // 130 units > 128
    });
    test("§3 Postgres byte boundary with astral chars", () => {
        expect(() => escapeIdentifier("😀".repeat(15), "pgsql")).not.toThrow(); // 60 bytes ≤ 63
        expect(() => escapeIdentifier("😀".repeat(16), "pgsql")).toThrow();      // 64 bytes > 63
    });
});

// §1 — the CONTRACT: generateSafeConstraintName's output must always be escapable for every dialect
// (it used to truncate by chars while escapeIdentifier rejects pg by bytes, so a multibyte column
// produced a name the helper itself made "safe" but escapeIdentifier then threw on).
describe("generateSafeConstraintName output is always escapable (C7 §1)", () => {
    const adversarial = [
        "a".repeat(80),            // long ASCII
        "é".repeat(60),            // 2-byte multibyte
        "名".repeat(40),           // 3-byte CJK
        "😀".repeat(30),           // 4-byte astral
        "café_" + "x".repeat(60),  // mixed
        "a".repeat(63),            // ASCII boundary
    ];
    for (const dialect of ["mysql", "pgsql", "sqlserver"] as const) {
        for (const col of adversarial) {
            test(`${dialect}: name for a ${[...col].length}-char column is escapable`, () => {
                const name = generateSafeConstraintName("tbl", col, "unique");
                expect(() => escapeIdentifier(name, dialect)).not.toThrow();
            });
        }
    }
});

// §2 — the derived staging/history name (not the base) is what overflows; the check must fire on it.
describe("derived staging names enforce the identifier limit (C7 §2)", () => {
    const mk = (d: "mysql" | "pgsql") => Database.create({ sqlDialect: d, host: "h", user: "u", password: "p", database: "db", schema: "s" });
    test("mysql: a 55-char base (legal alone) overflows temp_staging__ (69>64) and throws", () => {
        expect(() => mk("mysql").getCreateTempTableQuery("t".repeat(55))).toThrow(/exceeds the .* identifier limit/i);
    });
    test("pgsql: a 58-char base overflows the staging name (72>63 bytes) and throws", () => {
        expect(() => mk("pgsql").getCreateTempTableQuery("t".repeat(58))).toThrow(/exceeds the .* identifier limit/i);
    });
    test("pgsql: a 48-char base stays under the limit — no false reject", () => {
        expect(() => mk("pgsql").getCreateTempTableQuery("t".repeat(48))).not.toThrow();
    });
});

// §4 — the MySQL advisory-lock key (a bound param, so it bypasses escapeIdentifier) has its own
// 64-char GET_LOCK limit; a long BYOD table must not silently break lock acquisition.
describe("MySQL advisory-lock key stays within GET_LOCK's 64-char limit (C7 §4)", () => {
    const key = (t: string) => (MySQLDatabase as any).schemaLockKey(t) as string;
    test("short table → raw key", () => {
        expect(key("orders")).toBe("autosql_schema__orders");
    });
    test("long table → stable hashed key ≤ 64 chars", () => {
        const t = "t".repeat(60);
        expect(key(t).length).toBeLessThanOrEqual(64);
        expect(key(t)).toBe(key(t)); // deterministic
        expect(key(t).startsWith("autosql_schema__")).toBe(true);
    });
    test("two different long tables get different keys (no collision)", () => {
        expect(key("a".repeat(60))).not.toBe(key("b".repeat(60)));
    });
    test("measures/slices by code points — an astral table within 64 chars is NOT truncated", () => {
        // 40 emoji + "autosql_schema__" (16) = 56 CODE POINTS (≤64), though 96 UTF-16 units.
        const t = "😀".repeat(40);
        const k = key(t);
        expect(k).toBe(`autosql_schema__${t}`); // not hashed/truncated
        // A longer one IS truncated, but never leaves a lone surrogate (slice on char boundaries).
        const long = key("😀".repeat(60));
        expect([...long].length).toBeLessThanOrEqual(64);
        expect(long).not.toMatch(/�/); // no replacement char from a split pair
    });
});
