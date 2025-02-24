import { collateTypes } from "../src/helpers/types";

describe("collateTypes function", () => {
    test("collates identical types correctly", async () => {
        expect(await collateTypes(["int", "int", "int"])).toBe("int");
        expect(await collateTypes(["varchar", "varchar"])).toBe("varchar");
    });

    test("collates mixed numeric types correctly", async () => {
        expect(await collateTypes(["int", "decimal"])).toBe("decimal");
        expect(await collateTypes(["smallint", "int", "bigint"])).toBe("bigint");
        expect(await collateTypes(["decimal", "exponential"])).toBe("exponential");
    });

    test("collates boolean and binary correctly", async () => {
        expect(await collateTypes(["boolean", "boolean"])).toBe("boolean");
        expect(await collateTypes(["binary", "binary"])).toBe("binary");
        expect(await collateTypes(["boolean", "binary"])).toBe("binary");
    });

    test("collates date and time types correctly", async () => {
        expect(await collateTypes(["date", "date"])).toBe("date");
        expect(await collateTypes(["datetime", "datetime"])).toBe("datetime");
        expect(await collateTypes(["datetimetz", "datetime"])).toBe("datetimetz");
        expect(await collateTypes(["date", "time"])).toBe("datetime");
    });

    test("collates text-based types correctly", async () => {
        expect(await collateTypes(["json", "json"])).toBe("json");
        expect(await collateTypes(["varchar", "text"])).toBe("text");
        expect(await collateTypes(["text", "mediumtext"])).toBe("mediumtext");
    });

    test("handles conflicting or unknown types gracefully", async () => {
        expect(await collateTypes(["int", "varchar"])).toBe("varchar");
        expect(await collateTypes(["boolean", "datetime"])).toBe("varchar");
        expect(await collateTypes(["json", "int"])).toBe("varchar");
    });

    test("handles empty and null cases correctly", async () => {
        expect(await collateTypes(["varchar", null, "varchar"])).toBe("varchar");
    });

    test("throws error when no data types are provided", async () => {
        await expect(collateTypes([])).rejects.toThrow("Error in collateTypes: No data types provided for collation");
    });    
});
