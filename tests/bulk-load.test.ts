import { escapeCopyValue, serializeRowsToCopyText } from "../src/helpers/bulkLoad";

// bulkLoad serialization: the tab-delimited text format shared by Postgres COPY and MySQL
// LOAD DATA LOCAL INFILE — TAB fields, newline rows, `\N` NULL, backslash-escaped specials.

const TAB = String.fromCharCode(9);
const NL = String.fromCharCode(10);
const CR = String.fromCharCode(13);
const BS = String.fromCharCode(92); // backslash

describe("escapeCopyValue", () => {
    test("null / undefined become \\N", () => {
        expect(escapeCopyValue(null)).toBe(BS + "N");
        expect(escapeCopyValue(undefined)).toBe(BS + "N");
    });

    test("plain values pass through; numbers stringify", () => {
        expect(escapeCopyValue("abc")).toBe("abc");
        expect(escapeCopyValue(42)).toBe("42");
    });

    test("escapes tab, newline, carriage return and backslash", () => {
        expect(escapeCopyValue("a" + TAB + "b")).toBe("a" + BS + "t" + "b");
        expect(escapeCopyValue("a" + NL + "b")).toBe("a" + BS + "n" + "b");
        expect(escapeCopyValue("a" + CR + "b")).toBe("a" + BS + "r" + "b");
        expect(escapeCopyValue("a" + BS + "b")).toBe("a" + BS + BS + "b"); // backslash doubled
    });
});

describe("serializeRowsToCopyText", () => {
    test("joins fields with TAB, rows with newline, NULL as \\N", () => {
        const out = serializeRowsToCopyText([[1, "a"], [2, null]]);
        expect(out).toBe("1" + TAB + "a" + NL + "2" + TAB + BS + "N" + NL);
    });

    test("escapes specials within a row", () => {
        const out = serializeRowsToCopyText([["x" + TAB + "y", "z" + BS + "w"]]);
        expect(out).toBe("x" + BS + "t" + "y" + TAB + "z" + BS + BS + "w" + NL);
    });
});
