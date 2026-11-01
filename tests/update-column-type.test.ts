import { updateColumnType } from "../src/helpers/columnTypes";
import type { ColumnDefinition } from "../src/config/types";

// updateColumnType is a thin orchestration of two independently well-tested functions
// (predictType + collateTypes), but its OWN two branches — a null-prediction setting
// allowNull=true, and a type-mismatch widening via collateTypes — were never directly asserted
// anywhere, only exercised as a side effect inside larger inference-flow tests if at all.

describe("updateColumnType", () => {
    test("an unpredictable value (predictType -> null) sets allowNull, leaves type untouched", () => {
        const column: ColumnDefinition = { type: "int", allowNull: false };
        updateColumnType(column, null as any);
        expect(column.allowNull).toBe(true);
        expect(column.type).toBe("int"); // untouched — no type widening happened
    });

    test("a conflicting value widens the column's type via collateTypes", () => {
        const column: ColumnDefinition = { type: "tinyint", allowNull: false };
        updateColumnType(column, "abc"); // predicts varchar, conflicts with tinyint
        expect(column.type).toBe("varchar");
        expect(column.allowNull).toBe(false); // unaffected — this branch doesn't touch allowNull
    });

    test("a value matching the already-declared type changes nothing", () => {
        const column: ColumnDefinition = { type: "tinyint", allowNull: false };
        updateColumnType(column, "42"); // predicts tinyint, matches already
        expect(column.type).toBe("tinyint");
        expect(column.allowNull).toBe(false);
    });

    test("outlier: an empty string predicts varchar (not null), so it widens rather than setting allowNull", () => {
        const column: ColumnDefinition = { type: "int", allowNull: false };
        updateColumnType(column, "");
        expect(column.type).toBe("varchar");
        expect(column.allowNull).toBe(false);
    });
});
