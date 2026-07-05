import { predictType, collateTypes } from "../src/helpers/columnTypes";

describe("date range validation (I3)", () => {
    test("valid MM/DD/YYYY and DD/MM/YYYY are dates", () => {
        expect(predictType("12/25/2024")).toBe("date"); // MM/DD
        expect(predictType("25/12/2024")).toBe("date"); // DD/MM
        expect(predictType("2024-01-31")).toBe("date"); // ISO
    });
    test("out-of-range day/month values are NOT dates", () => {
        for (const bad of ["13/25/2024", "99/99/9999", "00/00/1900", "32/01/2024"]) {
            expect(predictType(bad)).not.toBe("date");
        }
    });
});

describe("collateTypes preserves timezone-awareness (I6)", () => {
    test("date + datetimetz widens to datetimetz, not datetime", () => {
        expect(collateTypes(new Set(["date", "datetimetz"]))).toBe("datetimetz");
    });
    test("date + datetime collapses to datetime", () => {
        expect(collateTypes(new Set(["date", "datetime"]))).toBe("datetime");
    });
});

describe("leading-zero fidelity for negatives (I7)", () => {
    test("a negative leading-zero string is preserved as text", () => {
        expect(predictType("-007")).toBe("varchar");
        expect(predictType("007")).toBe("varchar");
    });
    test("an ordinary negative integer is still numeric", () => {
        expect(predictType("-7")).not.toBe("varchar");
    });
});
