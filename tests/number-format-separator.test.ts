import { normalizeNumber, sqlize, validateConfig } from "../src/helpers/utilities";
import { getMetaData } from "../src/helpers/metadata";
import { pgsqlConfig } from "../src/db/config/pgsqlConfig";

// A24c + numberFormat. Two related concerns:
//   A24c — a LONE separator followed by exactly three digits (with 1–3 leading digits) is genuinely
//          ambiguous between thousands-grouping ("1,234" = 1234) and a decimal (1.234). We still
//          assume decimal but flag it (callback) so getMetaData can warn once per numeric column.
//   numberFormat — a US/EU/IN preset that resolves to thousandsSeparator/decimalSeparator in
//          validateConfig, so the SAME fields the load path (sqlize) reads disambiguate the value.

describe("A24c — normalizeNumber flags the genuinely-ambiguous lone-separator shape", () => {
    const fires = (input: string, thousands?: string, decimal?: string) => {
        let fired = false;
        normalizeNumber(input, thousands, decimal, () => { fired = true; });
        return fired;
    };

    test.each([
        ["1,234", "lone comma, 3 trailing, 1 leading"],
        ["1.234", "lone dot, 3 trailing, 1 leading"],
        ["12,345", "2 leading, 3 trailing"],
        ["123,456", "3 leading, 3 trailing"],
        ["1.000", "the columnTypes doc example — 1 or 1000?"],
    ])("fires for %s (%s)", (input) => {
        expect(fires(input)).toBe(true);
        // Parsing is UNCHANGED — still assumed decimal.
        expect(normalizeNumber(input)).toContain(".");
    });

    test.each([
        ["19.99", "2 trailing digits — cannot be a thousands group"],
        ["12.34", "2 trailing digits"],
        ["1,2345", "4 trailing digits"],
        ["1234,567", "4 leading digits — invalid thousands leader in every locale"],
        ["1000.50", "4 leading, 2 trailing"],
        ["1,234,567", "multiple separators — resolved as thousands, not lone"],
        ["1.234.567", "multiple dots — resolved as thousands"],
        ["1000", "no separator"],
        ["123456789", "no separator"],
    ])("does NOT fire for %s (%s)", (input) => {
        expect(fires(input)).toBe(false);
    });

    test("does NOT fire when explicit separators are supplied (no ambiguity to flag)", () => {
        expect(fires("1,234", ",", ".")).toBe(false);
        expect(fires("1.234", ".", ",")).toBe(false);
    });
});

describe("numberFormat — validateConfig resolves the preset into thousands/decimal separators", () => {
    test.each([
        ["US", ",", "."],
        ["IN", ",", "."], // Indian lakh/crore grouping shares US separators
        ["EU", ".", ","],
    ] as const)("%s resolves to thousands=%s decimal=%s", (fmt, thousands, decimal) => {
        const cfg = validateConfig({ sqlDialect: "pgsql", numberFormat: fmt });
        expect(cfg.thousandsSeparator).toBe(thousands);
        expect(cfg.decimalSeparator).toBe(decimal);
    });

    test("explicit thousandsSeparator/decimalSeparator take precedence over the preset", () => {
        const cfg = validateConfig({ sqlDialect: "pgsql", numberFormat: "EU", thousandsSeparator: ",", decimalSeparator: "." });
        expect(cfg.thousandsSeparator).toBe(",");
        expect(cfg.decimalSeparator).toBe(".");
    });

    test("an unknown numberFormat fails loud at validateConfig", () => {
        expect(() => validateConfig({ sqlDialect: "pgsql", numberFormat: "XX" as any })).toThrow(/Invalid numberFormat/);
        expect(() => validateConfig({ sqlDialect: "pgsql", numberFormat: "EU" })).not.toThrow();
    });
});

describe("numberFormat — the arbiter: the resolved separators reach sqlize (the load-time value producer)", () => {
    // sqlize()'s output IS the literal that gets parameter-bound and stored. If numberFormat reaches
    // here, "1.234" under EU stores as 1234 — not just infers int, but the stored value is correct.
    test("EU: lone-dot 3-digit value is stored as a grouped integer, not a decimal", () => {
        const euCfg = validateConfig({ sqlDialect: "pgsql", numberFormat: "EU" });
        expect(sqlize("1.234", "int", pgsqlConfig, euCfg)).toBe("1234");
    });
    test("US: lone-comma 3-digit value is stored as a grouped integer", () => {
        const usCfg = validateConfig({ sqlDialect: "pgsql", numberFormat: "US" });
        expect(sqlize("1,234", "int", pgsqlConfig, usCfg)).toBe("1234");
    });
    test("negative control: WITHOUT numberFormat the same EU value does not become 1234 (this is the corruption the knob prevents)", () => {
        expect(sqlize("1.234", "int", pgsqlConfig, { sqlDialect: "pgsql" })).not.toBe("1234");
    });
});

describe("A24c — getMetaData warns once per ambiguous NUMERIC column, and is suppressed appropriately", () => {
    const ambiguousRows = [{ amt: "1,234" }, { amt: "5,678" }, { amt: "9,012" }];

    test("warns and names the column when a numeric column is genuinely ambiguous", async () => {
        const warnings: string[] = [];
        await getMetaData({ sqlDialect: "pgsql", logger: { warn: (m: string) => warnings.push(m) } }, ambiguousRows);
        expect(warnings.some((w) => w.includes('"amt"'))).toBe(true);
    });

    test("numberFormat suppresses the warning (the ambiguity is resolved)", async () => {
        const warnings: string[] = [];
        await getMetaData({ sqlDialect: "pgsql", numberFormat: "US", logger: { warn: (m: string) => warnings.push(m) } }, ambiguousRows);
        expect(warnings).toHaveLength(0);
    });

    test("a text column that merely CONTAINS an ambiguous value stays silent (isNumeric filter)", async () => {
        const warnings: string[] = [];
        await getMetaData({ sqlDialect: "pgsql", logger: { warn: (m: string) => warnings.push(m) } },
            [{ note: "1,234" }, { note: "hello world" }, { note: "see ref 12,345" }]);
        expect(warnings).toHaveLength(0);
    });
});
