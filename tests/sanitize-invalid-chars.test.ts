import { sanitizeString, getInsertValues } from "../src/helpers/utilities";
import { MetadataHeader, DatabaseConfig } from "../src/config/types";

// Opt-in `sanitizeInvalidChars` removes characters a SQL text column cannot store — NUL
// bytes (which hard-fail Postgres) and unpaired UTF-16 surrogates — while leaving
// well-formed text (including emoji and non-ASCII scripts) untouched.

const NUL = String.fromCharCode(0);      // U+0000
const FFFD = String.fromCharCode(0xfffd); // U+FFFD replacement character

describe("sanitizeString", () => {
    test("strips NUL bytes", () => {
        expect(sanitizeString(`a${NUL}b${NUL}`)).toBe("ab");
    });

    test("replaces a lone high surrogate with U+FFFD", () => {
        expect(sanitizeString("a\uD800b")).toBe(`a${FFFD}b`);
    });

    test("replaces a lone low surrogate with U+FFFD", () => {
        expect(sanitizeString("a\uDC00b")).toBe(`a${FFFD}b`);
    });

    test("preserves valid emoji (paired surrogates)", () => {
        const thumbsUp = "👍"; // 👍
        expect(sanitizeString(`ok ${thumbsUp}!`)).toBe(`ok ${thumbsUp}!`);
    });

    test("preserves non-ASCII scripts and accents", () => {
        const s = "日本語 café Привет";
        expect(sanitizeString(s)).toBe(s);
    });

    test("handles NUL and a lone surrogate together", () => {
        expect(sanitizeString(`x${NUL}\uD800y`)).toBe(`x${FFFD}y`);
    });
});

describe("getInsertValues honours sanitizeInvalidChars", () => {
    const meta = { note: { type: "varchar" } } as unknown as MetadataHeader;

    test("cleans string values when the flag is on", () => {
        const cfg = { sqlDialect: "mysql", sanitizeInvalidChars: true } as DatabaseConfig;
        const [value] = getInsertValues(meta, { note: `hi${NUL}\uD800` }, undefined, cfg, false);
        expect(value).toBe(`hi${FFFD}`);
    });

    test("leaves values untouched when the flag is off (default)", () => {
        const cfg = { sqlDialect: "mysql" } as DatabaseConfig;
        const raw = `hi${NUL}`;
        const [value] = getInsertValues(meta, { note: raw }, undefined, cfg, false);
        expect(value).toBe(raw);
    });

    test("does not coerce non-string values", () => {
        const numMeta = { n: { type: "int" } } as unknown as MetadataHeader;
        const cfg = { sqlDialect: "mysql", sanitizeInvalidChars: true } as DatabaseConfig;
        const [value] = getInsertValues(numMeta, { n: 42 }, undefined, cfg, false);
        expect(value).toBe(42);
    });
});
