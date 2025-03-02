import { compareHeaders } from "../src/helpers/headers";
import { ColumnDefinition } from "../src/config/types";
import { DB_CONFIG, Database } from "./utils/testConfig";
import { DialectConfig } from "../src/db/config/interfaces";

describe("compareHeaders", () => {
    test("Detects new columns correctly", () => {
        const oldHeaders: { [column: string]: ColumnDefinition }[] = [
            { id: { type: "int", length: 11, primary: true, allowNull: false } }
        ];

        const newHeaders: { [column: string]: ColumnDefinition }[] = [
            { id: { type: "int", length: 11, primary: true, allowNull: false } },
            { new_col: { type: "varchar", length: 100, allowNull: true } }
        ];

        const result = compareHeaders(oldHeaders, newHeaders);
        console.log(result)
        expect(result.addColumns).toEqual([{ new_col: { type: "varchar", length: 100, allowNull: true } }]);
        expect(result.modifyColumns).toEqual([]);
    });

    test("Detects safe type changes (smallint → int)", () => {
        const oldHeaders = [{ age: { type: "smallint", allowNull: false } }];
        const newHeaders = [{ age: { type: "int", allowNull: false } }];
        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.modifyColumns).toEqual([{ age: { type: "int", allowNull: false } }]);
    });

    test("Handles increasing column length", () => {
        const oldHeaders = [{ name: { type: "varchar", length: 50, allowNull: false } }];
        const newHeaders = [{ name: { type: "varchar", length: 100, allowNull: false } }];
        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.modifyColumns).toEqual([{ name: { type: "varchar", length: 100, allowNull: false } }]);
    });

    test("Converts unsafe type changes to VARCHAR", () => {
        const oldHeaders = [{ price: { type: "int", allowNull: false } }];
        const newHeaders = [{ price: { type: "text", allowNull: false } }];
        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.modifyColumns).toEqual([{ price: { type: "text", allowNull: false } }]);
    });

    test("Allows NOT NULL to NULL change", () => {
        const oldHeaders = [{ email: { type: "varchar", length: 255, allowNull: false } }];
        const newHeaders = [{ email: { type: "varchar", length: 255, allowNull: true } }];
        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.modifyColumns).toEqual([{ email: { type: "varchar", length: 255, allowNull: true } }]);
    });

    test("Handles safe type conversion and length merging", () => {
        const oldHeaders = [{ price: { type: "smallint", length: 5 } }];
        const newHeaders = [{ price: { type: "int", length: 10 } }];

        const result = compareHeaders(oldHeaders, newHeaders);

        expect(result.modifyColumns).toEqual([{ price: { type: "int", length: 10 } }]);
    });
});

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Complex Create Table Query Tests for ${config.sql_dialect.toUpperCase()}`, () => {
        let db: Database;
        let dialectConfig: DialectConfig;

        beforeAll(() => {
            db = Database.create(config);
            dialectConfig = db.getDialectConfig();
        });

        test("Handles merging decimal lengths correctly", () => {
            const oldHeaders = [{ amount: { type: "decimal", length: 8, decimal: 4 } }];
            const newHeaders = [{ amount: { type: "decimal", length: 15, decimal: 2 } }];

            const result = compareHeaders(oldHeaders, newHeaders, dialectConfig);

            expect(result.modifyColumns).toEqual([{ amount: { type: "decimal", length: 17, decimal: 4 } }]);
        });

        test("Removes length for no_length types (e.g., JSON, TEXT)", () => {
            const oldHeaders = [{ description: { type: "varchar", length: 255 } }];
            const newHeaders = [{ description: { type: "text" } }];

            const result = compareHeaders(oldHeaders, newHeaders, dialectConfig);

            expect(result.modifyColumns).toEqual([{ description: { type: "text" } }]);
        });
    });
});