import { compareHeaders } from "../src/helpers/headers";
import { ColumnDefinition } from '../src/helpers/metadata';

describe("compareHeaders", () => {
    test("Detects new columns correctly", () => {
        const oldHeaders = [{ "id": { type: "int", length: 11, primary: true, allowNull: false } }];
        const newHeaders: { [column: string]: ColumnDefinition }[] = [
            { id: { type: "int", length: 11, primary: true, allowNull: false } },
            { new_col: { type: "varchar", length: 100, allowNull: true } }
        ];
        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.addColumns).toEqual([{ "new_col": { type: "varchar", length: 100, allowNull: true } }]);
        expect(result.modifyColumns).toEqual([]);
    });

    test("Detects safe type changes (smallint → int)", () => {
        const oldHeaders = [{ "age": { type: "smallint", allowNull: false } }];
        const newHeaders = [{ "age": { type: "int", allowNull: false } }];
        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.modifyColumns).toEqual([{ "age": { type: "int", allowNull: false } }]);
    });

    test("Handles increasing column length", () => {
        const oldHeaders = [{ "name": { type: "varchar", length: 50, allowNull: false } }];
        const newHeaders = [{ "name": { type: "varchar", length: 100, allowNull: false } }];
        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.modifyColumns).toEqual([{ "name": { type: "varchar", length: 100, allowNull: false } }]);
    });

    test("Converts unsafe type changes to VARCHAR", () => {
        const oldHeaders = [{ "price": { type: "int", allowNull: false } }];
        const newHeaders = [{ "price": { type: "text", allowNull: false } }];
        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.modifyColumns).toEqual([{ "price": { type: "text", allowNull: false } }]);
    });

    test("Allows NOT NULL to NULL change", () => {
        const oldHeaders = [{ "email": { type: "varchar", length: 255, allowNull: false } }];
        const newHeaders = [{ "email": { type: "varchar", length: 255, allowNull: true } }];
        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.modifyColumns).toEqual([{ "email": { type: "varchar", length: 255, allowNull: true } }]);
    });
});