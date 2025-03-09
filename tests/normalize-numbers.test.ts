import { normalizeNumber } from "../src/helpers/utilities";

describe("normalizeNumber function", () => {
    /** ✅ Valid Number Formats */
    test("correctly normalizes US format with comma thousands and decimal point", () => {
        expect(normalizeNumber("1,234,567.89")).toBe("1234567.89");
        expect(normalizeNumber("1,000.50")).toBe("1000.50");
        expect(normalizeNumber("12,345.67")).toBe("12345.67");
    });

    test("correctly normalizes EU format with dot thousands and comma decimal", () => {
        expect(normalizeNumber("1.234.567,89")).toBe("1234567.89");
        expect(normalizeNumber("1.000,50")).toBe("1000.50");
        expect(normalizeNumber("12.345,67")).toBe("12345.67");
    });

    test("correctly normalizes French format with space thousands and comma decimal", () => {
        expect(normalizeNumber("1 234 567.89")).toBe("1234567.89");
        expect(normalizeNumber("1 000.50")).toBe("1000.50");
        expect(normalizeNumber("12 345.67")).toBe("12345.67");
    });

    test("correctly normalizes Indian format with e.g. #,##,##,###.##", () => {
        expect(normalizeNumber("1,23,45,678.90")).toBe("12345678.90");
        expect(normalizeNumber("12,345.67")).toBe("12345.67");
    });

    test("correctly normalizes whole numbers with thousand separators", () => {
        expect(normalizeNumber("1,234,567")).toBe("1234567");
        expect(normalizeNumber("1.234.567")).toBe("1234567");
    });

    test("handles single decimal point correctly", () => {
        expect(normalizeNumber("1000.50")).toBe("1000.50");
        expect(normalizeNumber("12.34")).toBe("12.34");
    });

    test("handles single comma as decimal separator", () => {
        expect(normalizeNumber("1000,50")).toBe("1000.50");
        expect(normalizeNumber("12,34")).toBe("12.34");
    });

    test("handles small numbers and negatives", () => {
        expect(normalizeNumber("112.50")).toBe("112.50");
        expect(normalizeNumber("-112.50")).toBe("-112.50");
        expect(normalizeNumber("-127")).toBe("-127");
    });

    /** ❌ Invalid Number Formats */
    test("returns null for mixed separators", () => {
        expect(normalizeNumber("1,234.567.89")).toBe(null);
        expect(normalizeNumber("1.234,567.89")).toBe(null);
        expect(normalizeNumber("12,34.56")).toBe(null);
        expect(normalizeNumber("12.34,56")).toBe(null);
    });

    test("returns null for multiple misplaced separators", () => {
        expect(normalizeNumber("1..234")).toBe(null);
        expect(normalizeNumber("1,,234")).toBe(null);
        expect(normalizeNumber("1234..567")).toBe(null);
        expect(normalizeNumber("1234,,567")).toBe(null);
    });

    test("handles edge cases with no valid numbers", () => {
        expect(normalizeNumber("abc")).toBe(null);
        expect(normalizeNumber("12a34")).toBe(null);
        expect(normalizeNumber(",")).toBe(null);
        expect(normalizeNumber(".")).toBe(null);
        expect(normalizeNumber("")).toBe(null);
    });

    /** ✅ Special Cases */
    test("keeps numbers unchanged when there are no separators", () => {
        expect(normalizeNumber("1000")).toBe("1000");
        expect(normalizeNumber("123456789")).toBe("123456789");
    });

    test("handles large numbers correctly", () => {
        expect(normalizeNumber("9,223,372,036,854,775,807")).toBe("9223372036854775807"); // Max bigint
        expect(normalizeNumber("9.223.372.036.854.775.807")).toBe("9223372036854775807"); // Max bigint EU
    });

    test("handles floating point precision correctly", () => {
        expect(normalizeNumber("1,234,567.89012345")).toBe("1234567.89012345");
        expect(normalizeNumber("1.234.567,89012345")).toBe("1234567.89012345");
    });

    test("handles optional inputs correctly", () => {
        expect(normalizeNumber("1$234$567^89012345", "$", "^")).toBe("1234567.89012345");
        expect(normalizeNumber("1,234", ",", ".")).toBe("1234");
        expect(normalizeNumber("1.234", ",", ".")).toBe("1.234");
        expect(normalizeNumber("1.234", ".", ",")).toBe("1234");
        expect(normalizeNumber("1$234,567", "$", ",")).toBe("1234.567");
        expect(normalizeNumber("1#*#*234.567", "$", ",")).toBe("1234.567");
        expect(() => normalizeNumber("1.234.567^89012345", ".", "")).toThrow("Both 'thousandsIndicatorOverride' and 'decimalIndicatorOverride' must be provided together.");
    });
})