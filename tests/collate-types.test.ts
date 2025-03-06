import { collateTypes } from "../src/helpers/types";

describe("collateTypes function", () => {
    test("collates identical types correctly", async () => {
        expect(collateTypes(["int", "int", "int"])).toBe("int");
        expect(collateTypes(["varchar", "varchar"])).toBe("varchar");
    });

    test("collates mixed numeric types correctly", async () => {
        expect(collateTypes(["int", "decimal"])).toBe("decimal");
        expect(collateTypes(["smallint", "int", "bigint"])).toBe("bigint");
        expect(collateTypes(["decimal", "exponential"])).toBe("exponential");
    });

    test("collates boolean and binary correctly", async () => {
        expect(collateTypes(["boolean", "boolean"])).toBe("boolean");
        expect(collateTypes(["binary", "binary"])).toBe("binary");
        expect(collateTypes(["boolean", "binary"])).toBe("binary");
    });

    test("collates date and time types correctly", async () => {
        expect(collateTypes(["date", "date"])).toBe("date");
        expect(collateTypes(["datetime", "datetime"])).toBe("datetime");
        expect(collateTypes(["datetimetz", "datetime"])).toBe("datetimetz");
        expect(collateTypes(["date", "time"])).toBe("datetime");
    });

    test("collates text-based types correctly", async () => {
        expect(collateTypes(["json", "json"])).toBe("json");
        expect(collateTypes(["varchar", "text"])).toBe("text");
        expect(collateTypes(["text", "mediumtext"])).toBe("mediumtext");
    });

    test("handles conflicting or unknown types gracefully", async () => {
        expect(collateTypes(["int", "varchar"])).toBe("varchar");
        expect(collateTypes(["boolean", "datetime"])).toBe("varchar");
        expect(collateTypes(["json", "int"])).toBe("varchar");
    });

    test("handles empty and null cases correctly", async () => {
        expect(collateTypes(["varchar", null, "varchar"])).toBe("varchar");
    });

    test("throws error when no data types are provided", async () => {
        try {
            collateTypes([])
            fail("Expected collation to throw an error");
        } catch (error) {
            expect(error).toBeDefined();
        }
    });
});
