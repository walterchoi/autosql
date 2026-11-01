import { calculateColumnLength } from "../src/helpers/utilities";
import type { ColumnDefinition } from "../src/config/types";

// 3.0.0 fix: `column.length` is optional, so a fresh column (no length set yet) made
// `Math.max(column.length, ...)` silently produce NaN before `?? 0` was added. `any` hid this —
// giving the function a real type surfaced it. Guards the fix and the surrounding behavior it must
// not disturb (growth-only length, the decimal cap, the non-decimal branch).

const LOOKUP = { decimals: ["decimal"], decimals_max_length: 10 };
const freshColumn = (type: string | null = "varchar"): ColumnDefinition => ({ type });

describe("calculateColumnLength — NaN fix", () => {
    test("a fresh non-decimal column's length is the dataPoint's length, not NaN", () => {
        const column = freshColumn("varchar");
        calculateColumnLength(column, "hello", LOOKUP);
        expect(column.length).toBe(5);
        expect(Number.isNaN(column.length)).toBe(false);
    });

    test("a fresh decimal column's length is computed from integer+decimal+3, not NaN", () => {
        const column = freshColumn("decimal");
        calculateColumnLength(column, "123.45", LOOKUP);
        expect(column.decimal).toBe(3); // fractional digits (2) + 1, the function's own convention
        expect(column.length).toBe(3 + 3 + 3); // integerLen(3) + decimal(3) + 3
        expect(Number.isNaN(column.length)).toBe(false);
    });

    test("a fresh decimal column with no fractional part still resolves, not NaN", () => {
        const column = freshColumn("decimal");
        calculateColumnLength(column, "42", LOOKUP);
        expect(column.decimal).toBe(0); // no "." → decimalLen 0, column.decimal stays at its ?? 0 floor
        expect(column.length).toBe(2 + 0 + 3);
        expect(Number.isNaN(column.length)).toBe(false);
    });

    test("length only grows across calls, never shrinks", () => {
        const column = freshColumn("varchar");
        calculateColumnLength(column, "a long first value", LOOKUP);
        const grown = column.length;
        calculateColumnLength(column, "x", LOOKUP);
        expect(column.length).toBe(grown); // the later, shorter value didn't shrink it
    });

    test("decimal places are capped at decimals_max_length (outlier: far more than 10)", () => {
        const column = freshColumn("decimal");
        calculateColumnLength(column, "1.123456789012345", LOOKUP); // 15 fractional digits
        expect(column.decimal).toBe(10); // capped, not 16
    });

    test("decimals_max_length falls back to 10 when the lookup table omits it", () => {
        const column = freshColumn("decimal");
        calculateColumnLength(column, "1.123456789012345", { decimals: ["decimal"] }); // no decimals_max_length
        expect(column.decimal).toBe(10);
    });
});
