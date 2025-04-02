import { predictType } from "../src/helpers/columnTypes";

describe("predictType function", () => {
    /** ✅ Integer Tests */
    test("detects tinyint correctly", async () => {
        expect(predictType("127")).toBe("tinyint"); // Max tinyint
        expect(predictType("-128")).toBe("tinyint"); // Min tinyint
    });

    test("detects smallint correctly", async () => {
        expect(predictType("32767")).toBe("smallint"); // Max smallint
        expect(predictType("-32768")).toBe("smallint"); // Min smallint
    });

    test("detects int correctly", async () => {
        expect(predictType("2147483647")).toBe("int"); // Max int
        expect(predictType("2,147,483,647")).toBe("int"); // Max int with , separator
        expect(predictType("2.147.483.647")).toBe("int"); // Max int with . separator
        expect(predictType("-2147483648")).toBe("int"); // Min int
    });

    test("detects bigint correctly", async () => {
        expect(predictType("9223372036854775807")).toBe("bigint"); // Max bigint
        expect(predictType("-9223372036854775808")).toBe("bigint"); // Min bigint
    });

    test("detects varchar for extremely large numbers", async () => {
        expect(predictType("9223372036854775808")).toBe("varchar"); // Beyond bigint
    });

    /** ✅ Decimal & Exponential Tests */
    test("detects various decimal formats correctly", async () => {
        expect(predictType("123.45")).toBe("decimal"); // Small decimal US format
        expect(predictType("123,45")).toBe("decimal"); // Small decimal EU format
        expect(predictType("1,234,567.89")).toBe("decimal"); // US format
        expect(predictType("1,234,567.89012345")).toBe("decimal"); // US format with long decimal
        expect(predictType("1.234.567,89")).toBe("decimal"); // EU format
        expect(predictType("1.234.567,89012345")).toBe("decimal"); // EU format with long decimal
    });

    test("detects exponentials correctly", async () => {
        expect(predictType("1.23e10")).toBe("exponential");
    });

    /** ✅ Boolean Tests */
    test("detects boolean values correctly", async () => {
        expect(predictType("true")).toBe("boolean");
        expect(predictType("True")).toBe("boolean");
        expect(predictType("false")).toBe("boolean");
        expect(predictType("False")).toBe("boolean");
        expect(predictType("1")).toBe("boolean");
        expect(predictType("0")).toBe("boolean");
    });

    /** ✅ Binary Tests */
    test("detects binary correctly", async () => {
        expect(predictType("01")).toBe("binary");
        expect(predictType("10")).toBe("binary");
        expect(predictType("1010001010")).toBe("binary");
    });

    /** ✅ Date/Time Tests */
    test("detects valid datetime correctly", async () => {
        expect(predictType("2023-01-01T12:00:00Z")).toBe("datetimetz"); // UTC with Z
        expect(predictType("2023-01-01 12:00:00")).toBe("datetime"); // Standard format
    });

    test("detects valid date correctly", async () => {
        expect(predictType("2023-01-01")).toBe("date");
    });

    test("detects valid time correctly", async () => {
        expect(predictType("12:00:00")).toBe("time");
    });

    test("handles invalid dates correctly", async () => {
        expect(predictType("Invalid Date")).toBe("varchar");
    });

    /** ✅ JSON Tests */
    test("detects valid JSON correctly", async () => {
        expect(predictType('{"key": "value"}')).toBe("json");
    });

    test("detects invalid JSON as varchar", async () => {
        expect(predictType("{key: value}")).toBe("varchar");
    });

    /** ✅ Text Length-Based Tests */
    test("detects varchar for short text", async () => {
        expect(predictType("Hello")).toBe("varchar");
    });

    test("detects text for medium-length strings", async () => {
        const mediumText = "a".repeat(7000);
        expect(predictType(mediumText)).toBe("text");
    });

    test("detects mediumtext for longer strings", async () => {
        const longText = "a".repeat(70000);
        expect(predictType(longText)).toBe("mediumtext");
    });

    test("detects longtext for very long strings", async () => {
        const veryLongText = "a".repeat(20000000);
        expect(predictType(veryLongText)).toBe("longtext");
    });

    /** ✅ Edge Cases */
    test("handles empty string as varchar", async () => {
        expect(predictType("")).toBe("varchar");
    });

    test("handles null correctly", async () => {
        expect(predictType(null)).toBe(null);
    });

    test("handles undefined correctly", async () => {
        expect(predictType(undefined)).toBe(null);
    });

    test("handles non-string objects correctly", async () => {
        expect(predictType({})).toBe("json");
        expect(predictType([])).toBe("json");
    });

    /** ✅ Datetime & DatetimeTZ Tests */
    test("detects various valid datetime and datetimetz formats correctly", async () => {
        // ✅ Unix timestamp
        expect(predictType(`/Date(1700000000000)/`)).toBe("datetime"); // No timezone
        expect(predictType(`/Date(1700000000000+0000)/`)).toBe("datetimetz"); // With timezone
        expect(predictType(`/Date(-1501545600000+0000)/`)).toBe("datetimetz"); // With timezone

        // ✅ ISO 8601 formats
        expect(predictType(`2024-02-20T15:30:00Z`)).toBe("datetimetz"); // UTC
        expect(predictType(`"2024/02/20 12:45:30+05:30"`)).toBe("datetimetz"); // With timezone offset
        expect(predictType(`"2024.02.20T23:59:59-08:00"`)).toBe("datetimetz"); // Negative offset
        expect(predictType(`2024-02-20 14:00:00.123+0200`)).toBe("datetimetz"); // With milliseconds

        // ✅ JavaScript Date Format
        expect(predictType(`"Tue Feb 20 2024 15:30:00 GMT+0000 (UTC)"`)).toBe("datetimetz"); // JS `Date.toString()`

        // ✅ Human-Readable Formats
        expect(predictType(`"Feb 20, 2024 10:15 AM UTC"`)).toBe("datetimetz");

        // ✅ Alternative Separator Styles
        expect(predictType(`2024/02/20T10:10:10Z`)).toBe("datetimetz"); // `/` separator

        // ✅ Truncated datetime (no seconds)
        expect(predictType(`"2024-02-20 23:59"`)).toBe("datetime"); // No seconds, not timezone-aware
    });
});