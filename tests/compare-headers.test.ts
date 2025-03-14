import { compareMetaData } from "../src/helpers/metadata";
import { DialectConfig, ColumnDefinition, MetadataHeader } from "../src/config/types";
import { DB_CONFIG, Database } from "./utils/testConfig";

describe("compareMetaData", () => {
    test("Detects new columns correctly", () => {
        const oldHeaders: MetadataHeader = {
            id: { type: "int", length: 11, primary: true, allowNull: false }
        };

        const newHeaders: MetadataHeader = {
            id: { type: "int", length: 11, primary: true, allowNull: false },
            new_col: { type: "varchar", length: 100, allowNull: true }
        };

        const result = compareMetaData(oldHeaders, newHeaders);
        expect(result.addColumns).toEqual({ new_col: { type: "varchar", length: 100, allowNull: true } });
        expect(result.modifyColumns).toEqual({});
    });

    test("Detects removed columns correctly", () => {
        const oldHeaders: MetadataHeader = {
            old_col: { type: "varchar", length: 100, allowNull: true }
        };

        const newHeaders: MetadataHeader = {};

        const result = compareMetaData(oldHeaders, newHeaders);
        expect(result.dropColumns).toEqual(["old_col"]);
    });

    test("Detects renamed columns correctly", () => {
        const oldHeaders: MetadataHeader = {
            old_name: { type: "varchar", length: 100, allowNull: true }
        };

        const newHeaders: MetadataHeader = {
            new_name: { type: "varchar", length: 100, allowNull: true }
        };

        const result = compareMetaData(oldHeaders, newHeaders);
        expect(result.renameColumns).toEqual([{ oldName: "old_name", newName: "new_name" }]);
    });

    test("Detects safe type changes (smallint → int)", () => {
        const oldHeaders: MetadataHeader = {
            age: { type: "smallint", allowNull: false }
        };

        const newHeaders: MetadataHeader = {
            age: { type: "int", allowNull: false }
        };

        const result = compareMetaData(oldHeaders, newHeaders);
        expect(result.modifyColumns).toEqual({ age: { type: "int", allowNull: false } });
    });

    test("Handles increasing column length", () => {
        const oldHeaders: MetadataHeader = {
            name: { type: "varchar", length: 50, allowNull: false }
        };

        const newHeaders: MetadataHeader = {
            name: { type: "varchar", length: 100, allowNull: false }
        };

        const result = compareMetaData(oldHeaders, newHeaders);
        expect(result.modifyColumns).toEqual({ name: { type: "varchar", length: 100, allowNull: false } });
    });

    test("Handles NOT NULL to NULL conversion", () => {
        const oldHeaders: MetadataHeader = {
            email: { type: "varchar", length: 255, allowNull: false }
        };

        const newHeaders: MetadataHeader = {
            email: { type: "varchar", length: 255, allowNull: true }
        };

        const result = compareMetaData(oldHeaders, newHeaders);
        expect(result.nullableColumns).toEqual(["email"]);
    });

    test("Handles unique constraint removal", () => {
        const oldHeaders: MetadataHeader = {
            username: { type: "varchar", length: 100, unique: true, allowNull: false }
        };

        const newHeaders: MetadataHeader = {
            username: { type: "varchar", length: 100, unique: false, allowNull: false }
        };

        const result = compareMetaData(oldHeaders, newHeaders);
        expect(result.noLongerUnique).toEqual(["username"]);
    });

    test("Handles safe type conversion and length merging", () => {
        const oldHeaders: MetadataHeader = {
            price: { type: "smallint", length: 5 }
        };

        const newHeaders: MetadataHeader = {
            price: { type: "int", length: 10 }
        };

        const result = compareMetaData(oldHeaders, newHeaders);
        expect(result.modifyColumns).toEqual({ price: { type: "int", length: 10 } });
    });
});

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Complex Compare Headers Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;
        let dialectConfig: DialectConfig;

        beforeAll(() => {
            db = Database.create(config);
            dialectConfig = db.getDialectConfig();
        });

        test("Handles merging decimal lengths correctly", () => {
            const oldHeaders: MetadataHeader = {
                amount: { type: "decimal", length: 8, decimal: 4 }
            };

            const newHeaders: MetadataHeader = {
                amount: { type: "decimal", length: 15, decimal: 2 }
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.modifyColumns).toEqual({ amount: { type: "decimal", length: 17, decimal: 4 } });
        });

        test("Removes length for no_length types (e.g., JSON, TEXT)", () => {
            const oldHeaders: MetadataHeader = {
                description: { type: "varchar", length: 255 }
            };

            const newHeaders: MetadataHeader = {
                description: { type: "text" }
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.modifyColumns).toEqual({ description: { type: "text" } });
        });

        test("Handles NOT NULL to NULL conversion in dialect-specific logic", () => {
            const oldHeaders: MetadataHeader = {
                email: { type: "varchar", length: 255, allowNull: false }
            };

            const newHeaders: MetadataHeader = {
                email: { type: "varchar", length: 255, allowNull: true }
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.nullableColumns).toEqual(["email"]);
        });

        test("Handles unique constraint removal with dialect-specific behavior", () => {
            const oldHeaders: MetadataHeader = {
                username: { type: "varchar", length: 100, unique: true, allowNull: false }
            };

            const newHeaders: MetadataHeader = {
                username: { type: "varchar", length: 100, unique: false, allowNull: false }
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.noLongerUnique).toEqual(["username"]);
        });
    });
});

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Primary Key Handling Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;
        let dialectConfig: DialectConfig;

        beforeAll(() => {
            db = Database.create(config);
            dialectConfig = db.getDialectConfig();
        });

        test("Detects added primary key correctly", () => {
            const oldHeaders: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false }
            };

            const newHeaders: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["id"]);
        });

        test("Detects removed primary key but retains a primary key if needed", () => {
            const oldHeaders: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const newHeaders: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false } // Primary key removed
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["id"]); // Ensures `id` is still primary
        });

        test("Detects renamed primary key and updates accordingly", () => {
            const oldHeaders: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const newHeaders: MetadataHeader = {
                uuid: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["uuid"]);
            expect(result.renameColumns).toEqual([{ oldName: "id", newName: "uuid" }]);
        });

        test("Handles transition from single-column primary key to composite primary key", () => {
            const oldHeaders: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const newHeaders: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false },
                email: { type: "varchar", length: 255, allowNull: false, primary: true }
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["id", "email"]);
        });

        test("Handles transition from composite primary key to a single-column primary key", () => {
            const oldHeaders: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true },
                email: { type: "varchar", length: 255, allowNull: false, primary: true }
            };

            const newHeaders: MetadataHeader = {
                email: { type: "varchar", length: 255, allowNull: false, primary: true }
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["id", "email"]);
        });

        test("Handles renamed primary keys", () => {
            const oldHeaders: MetadataHeader = {
                id: { type: "int", length: 11, primary: true, allowNull: false },
                name: { type: "varchar", length: 100, allowNull: false, unique: true },
                created_at: { type: "datetime", allowNull: false },
                email: { type: "varchar", length: 255, allowNull: false, unique: true }
            };
            
            const newHeaders: MetadataHeader = {
                uuid: { type: "int", length: 11, primary: true, allowNull: false },
                name: { type: "varchar", length: 100, allowNull: false, unique: true },
                created_at: { type: "datetime", allowNull: false },
                email: { type: "varchar", length: 255, allowNull: false, unique: true }
            };

            const result = compareMetaData(oldHeaders, newHeaders, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["uuid"]);
            expect(result.renameColumns).toEqual([{ oldName: "id", newName: "uuid" }]);
        })
    });
});
