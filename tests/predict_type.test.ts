import { predictType } from "../src/helpers/types";

describe("predictType function", () => {
    /** ✅ Integer Tests */
    test("detects tinyint correctly", async () => {
        expect(await predictType("127")).toBe("tinyint"); // Max tinyint
        expect(await predictType("-128")).toBe("tinyint"); // Min tinyint
    });

    test("detects smallint correctly", async () => {
        expect(await predictType("32767")).toBe("smallint"); // Max smallint
        expect(await predictType("-32768")).toBe("smallint"); // Min smallint
    });

    test("detects int correctly", async () => {
        expect(await predictType("2147483647")).toBe("int"); // Max int
        expect(await predictType("2,147,483,647")).toBe("int"); // Max int with , separator
        expect(await predictType("2.147.483.647")).toBe("int"); // Max int with . separator
        expect(await predictType("-2147483648")).toBe("int"); // Min int
    });

    test("detects bigint correctly", async () => {
        expect(await predictType("9223372036854775807")).toBe("bigint"); // Max bigint
        expect(await predictType("-9223372036854775808")).toBe("bigint"); // Min bigint
    });

    test("detects varchar for extremely large numbers", async () => {
        expect(await predictType("9223372036854775808")).toBe("varchar"); // Beyond bigint
    });

    /** ✅ Decimal & Exponential Tests */
    test("detects various decimal formats correctly", async () => {
        expect(await predictType("123.45")).toBe("decimal"); // Small decimal US format
        expect(await predictType("123,45")).toBe("decimal"); // Small decimal EU format
        expect(await predictType("1,234,567.89")).toBe("decimal"); // US format
        expect(await predictType("1,234,567.89012345")).toBe("decimal"); // US format with long decimal
        expect(await predictType("1.234.567,89")).toBe("decimal"); // EU format
        expect(await predictType("1.234.567,89012345")).toBe("decimal"); // EU format with long decimal
    });

    test("detects exponentials correctly", async () => {
        expect(await predictType("1.23e10")).toBe("exponential");
    });

    /** ✅ Boolean Tests */
    test("detects boolean values correctly", async () => {
        expect(await predictType("true")).toBe("boolean");
        expect(await predictType("True")).toBe("boolean");
        expect(await predictType("false")).toBe("boolean");
        expect(await predictType("False")).toBe("boolean");
        expect(await predictType("1")).toBe("boolean");
        expect(await predictType("0")).toBe("boolean");
    });

    /** ✅ Binary Tests */
    test("detects binary correctly", async () => {
        expect(await predictType("01")).toBe("binary");
        expect(await predictType("10")).toBe("binary");
        expect(await predictType("1010001010")).toBe("binary");
    });

    /** ✅ Date/Time Tests */
    test("detects valid datetime correctly", async () => {
        expect(await predictType("2023-01-01T12:00:00Z")).toBe("datetimetz"); // UTC with Z
        expect(await predictType("2023-01-01 12:00:00")).toBe("datetime"); // Standard format
    });

    test("detects valid date correctly", async () => {
        expect(await predictType("2023-01-01")).toBe("date");
    });

    test("detects valid time correctly", async () => {
        expect(await predictType("12:00:00")).toBe("time");
    });

    test("handles invalid dates correctly", async () => {
        expect(await predictType("Invalid Date")).toBe("varchar");
    });

    /** ✅ JSON Tests */
    test("detects valid JSON correctly", async () => {
        expect(await predictType('{"key": "value"}')).toBe("json");
    });

    test("detects invalid JSON as varchar", async () => {
        expect(await predictType("{key: value}")).toBe("varchar");
    });

    /** ✅ Text Length-Based Tests */
    test("detects varchar for short text", async () => {
        expect(await predictType("Hello")).toBe("varchar");
    });

    test("detects text for medium-length strings", async () => {
        const mediumText = "a".repeat(7000);
        expect(await predictType(mediumText)).toBe("text");
    });

    test("detects mediumtext for longer strings", async () => {
        const longText = "a".repeat(70000);
        expect(await predictType(longText)).toBe("mediumtext");
    });

    test("detects longtext for very long strings", async () => {
        const veryLongText = "a".repeat(20000000);
        expect(await predictType(veryLongText)).toBe("longtext");
    });

    test("throws error for overly long strings", async () => {
        const tooLongText = "a".repeat(5000000000); // Exceeds max `longtext`
        await expect(predictType(tooLongText)).rejects.toThrow("data_too_long");
    });

    /** ✅ Edge Cases */
    test("handles empty string as varchar", async () => {
        expect(await predictType("")).toBe("varchar");
    });

    test("handles null correctly", async () => {
        expect(await predictType(null)).toBe(null);
    });

    test("handles undefined correctly", async () => {
        expect(await predictType(undefined)).toBe(null);
    });

    test("handles non-string objects correctly", async () => {
        expect(await predictType({})).toBe("json");
        expect(await predictType([])).toBe("json");
    });

    /** ✅ Datetime & DatetimeTZ Tests */
    test("detects various valid datetime and datetimetz formats correctly", async () => {
        // ✅ Unix timestamp
        expect(await predictType(`/Date(1700000000000)/`)).toBe("datetime"); // No timezone
        expect(await predictType(`/Date(1700000000000+0000)/`)).toBe("datetimetz"); // With timezone

        // ✅ ISO 8601 formats
        expect(await predictType(`2024-02-20T15:30:00Z`)).toBe("datetimetz"); // UTC
        expect(await predictType(`"2024/02/20 12:45:30+05:30"`)).toBe("datetimetz"); // With timezone offset
        expect(await predictType(`"2024.02.20T23:59:59-08:00"`)).toBe("datetimetz"); // Negative offset
        expect(await predictType(`2024-02-20 14:00:00.123+0200`)).toBe("datetimetz"); // With milliseconds

        // ✅ JavaScript Date Format
        expect(await predictType(`"Tue Feb 20 2024 15:30:00 GMT+0000 (UTC)"`)).toBe("datetimetz"); // JS `Date.toString()`

        // ✅ Human-Readable Formats
        expect(await predictType(`"Feb 20, 2024 10:15 AM UTC"`)).toBe("datetimetz");

        // ✅ Alternative Separator Styles
        expect(await predictType(`2024/02/20T10:10:10Z`)).toBe("datetimetz"); // `/` separator

        // ✅ Truncated datetime (no seconds)
        expect(await predictType(`"2024-02-20 23:59"`)).toBe("datetime"); // No seconds, not timezone-aware
    });
});