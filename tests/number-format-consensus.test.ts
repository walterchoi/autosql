import { classifySeparatorFormat, resolveDatasetSeparators } from "../src/helpers/numberFormat";

// Dataset-level consensus: pool structural separator evidence across ALL columns into ONE decision,
// so a decisive column resolves an ambiguous sibling. Conflict (both layouts) → let caller warn+default.

describe("classifySeparatorFormat — structural evidence per value", () => {
    test.each([
        ["1,234,567", "us", "two commas can only be thousands grouping"],
        ["12.5", "us", "lone dot, 1 trailing → dot is a decimal"],
        ["19.99", "us", "lone dot, 2 trailing → dot is a decimal"],
        ["1.23", "us", "lone dot, 2 trailing"],
        ["1,234.56", "us", "both present, dot last → dot is decimal"],
        ["1234.5", "us", "4 leading + lone dot → dot is a decimal"],
    ])("%s → %s (%s)", (input, expected) => {
        expect(classifySeparatorFormat(input)).toBe(expected);
    });

    test.each([
        ["1.234.567", "eu", "two dots can only be thousands grouping"],
        ["12,5", "eu", "lone comma, 1 trailing → comma is a decimal"],
        ["19,99", "eu", "lone comma, 2 trailing → comma is a decimal"],
        ["1.234,56", "eu", "both present, comma last → comma is decimal"],
        ["1234,5", "eu", "4 leading + lone comma → comma is a decimal"],
    ])("%s → %s (%s)", (input, expected) => {
        expect(classifySeparatorFormat(input)).toBe(expected);
    });

    test.each([
        ["1,234", "lone comma + exactly 3 trailing → ambiguous"],
        ["1.234", "lone dot + exactly 3 trailing → ambiguous"],
        ["123,456", "3 leading + 3 trailing → ambiguous"],
        ["1234", "no separator"],
        ["1.2.3", "version string — not a numeric candidate"],
        ["192.168.1.1", "IP — not a numeric candidate"],
        ["hello, world", "not numeric"],
    ])("%s → null (%s)", (input) => {
        expect(classifySeparatorFormat(input)).toBeNull();
    });

    test("native (non-string) values never vote", () => {
        expect(classifySeparatorFormat(1234)).toBeNull();
        expect(classifySeparatorFormat(12.5)).toBeNull();
        expect(classifySeparatorFormat(null)).toBeNull();
        expect(classifySeparatorFormat(undefined)).toBeNull();
    });

    test("negative sign does not change the classification", () => {
        expect(classifySeparatorFormat("-1,234,567")).toBe("us");
        expect(classifySeparatorFormat("-19,99")).toBe("eu");
    });
});

describe("resolveDatasetSeparators — one pair for the whole dataset", () => {
    const US = { thousands: ",", decimal: "." };
    const EU = { thousands: ".", decimal: "," };

    test("a decisive column resolves an ambiguous sibling (the core payoff)", () => {
        // amt is ambiguous on its own; total is decisive US → whole dataset is US.
        const rows = [
            { amt: "1,234", total: "1,234,567" },
            { amt: "5,678", total: "2,345,678" },
        ];
        expect(resolveDatasetSeparators(rows)).toEqual(US);
    });

    test("EU resolves the same way", () => {
        const rows = [
            { amt: "1,234", total: "1.234.567" },
            { amt: "5,678", total: "2.345.678" },
        ];
        expect(resolveDatasetSeparators(rows)).toEqual(EU);
    });

    test("a single lone-decimal value is enough (structural certainty)", () => {
        expect(resolveDatasetSeparators([{ amt: "1,234" }, { price: "19.99" }])).toEqual(US);
        expect(resolveDatasetSeparators([{ amt: "1.234" }, { price: "19,99" }])).toEqual(EU);
    });

    test("contradictory structural evidence → conflict (mixed/corrupt data)", () => {
        const rows = [{ a: "1,234,567" }, { b: "1.234.567" }];
        expect(resolveDatasetSeparators(rows)).toEqual({ conflict: true });
    });

    test("only ambiguous values → null (no decision; caller defaults + A24c-warns)", () => {
        expect(resolveDatasetSeparators([{ amt: "1,234" }, { amt: "5,678" }])).toBeNull();
    });

    test("no numeric evidence at all → null", () => {
        expect(resolveDatasetSeparators([{ name: "alice" }, { name: "bob" }])).toBeNull();
    });

    describe("numberFormatMinEvidence floor", () => {
        test("default 1: a single opposing vote is a conflict (strictest — don't guess on contradiction)", () => {
            const rows = [{ a: "1,234,567" }, { a: "1,234,567" }, { b: "1.234.567" }];
            expect(resolveDatasetSeparators(rows, 1)).toEqual({ conflict: true });
        });

        test("floor 2: a lone opposing vote is treated as noise, the supported layout wins", () => {
            const rows = [
                { a: "1,234,567" }, { a: "2,345,678" }, // 2 US votes (meets floor)
                { b: "1.234.567" },                     // 1 EU vote (below floor → noise)
            ];
            expect(resolveDatasetSeparators(rows, 2)).toEqual(US);
        });

        test("floor 2: below-floor support yields no decision", () => {
            expect(resolveDatasetSeparators([{ a: "1,234,567" }], 2)).toBeNull();
        });
    });
});
