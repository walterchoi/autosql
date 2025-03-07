import { compareHeaders } from "../src/helpers/headers";
import { DialectConfig, ColumnDefinition } from "../src/config/types";
import { DB_CONFIG, Database } from "./utils/testConfig";

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
        expect(result.addColumns).toEqual([{ new_col: { type: "varchar", length: 100, allowNull: true } }]);
        expect(result.modifyColumns).toEqual([]);
    });

    test("Detects removed columns correctly", () => {
        const oldHeaders = [{ old_col: { type: "varchar", length: 100, allowNull: true } }];
        const newHeaders: { [column: string]: ColumnDefinition }[] = [];

        const result = compareHeaders(oldHeaders, newHeaders);
        expect(result.dropColumns).toEqual(["old_col"]);
    });

    test("Detects renamed columns correctly", () => {
        const oldHeaders = [{ old_name: { type: "varchar", length: 100, allowNull: true } }];
        const newHeaders = [{ new_name: { type: "varchar", length: 100, allowNull: true } }];

        const result = compareHeaders(oldHeaders, newHeaders);
        expect(result.renameColumns).toEqual([{ oldName: "old_name", newName: "new_name" }]);
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

    test("Handles NOT NULL to NULL conversion", () => {
        const oldHeaders = [{ email: { type: "varchar", length: 255, allowNull: false } }];
        const newHeaders = [{ email: { type: "varchar", length: 255, allowNull: true } }];

        const result = compareHeaders(oldHeaders, newHeaders);
        expect(result.nullableColumns).toEqual(["email"]);
    });

    test("Handles unique constraint removal", () => {
        const oldHeaders = [{ username: { type: "varchar", length: 100, unique: true, allowNull: false } }];
        const newHeaders = [{ username: { type: "varchar", length: 100, unique: false, allowNull: false } }];

        const result = compareHeaders(oldHeaders, newHeaders);
        expect(result.noLongerUnique).toEqual(["username"]);
    });

    test("Handles safe type conversion and length merging", () => {
        const oldHeaders = [{ price: { type: "smallint", length: 5 } }];
        const newHeaders = [{ price: { type: "int", length: 10 } }];

        const result = compareHeaders(oldHeaders, newHeaders);
        expect(result.modifyColumns).toEqual([{ price: { type: "int", length: 10 } }]);
    });
});

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Complex Compare Headers Tests for ${config.sql_dialect.toUpperCase()}`, () => {
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

        test("Handles NOT NULL to NULL conversion in dialect-specific logic", () => {
            const oldHeaders = [{ email: { type: "varchar", length: 255, allowNull: false } }];
            const newHeaders = [{ email: { type: "varchar", length: 255, allowNull: true } }];

            const result = compareHeaders(oldHeaders, newHeaders, dialectConfig);
            expect(result.nullableColumns).toEqual(["email"]);
        });

        test("Handles unique constraint removal with dialect-specific behavior", () => {
            const oldHeaders = [{ username: { type: "varchar", length: 100, unique: true, allowNull: false } }];
            const newHeaders = [{ username: { type: "varchar", length: 100, unique: false, allowNull: false } }];

            const result = compareHeaders(oldHeaders, newHeaders, dialectConfig);
            expect(result.noLongerUnique).toEqual(["username"]);
        });
    });
});
