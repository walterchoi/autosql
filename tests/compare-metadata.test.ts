import { compareMetaData } from "../src/helpers/metadata";
import { DialectConfig, ColumnDefinition, MetadataHeader } from "../src/config/types";
import { DB_CONFIG, Database } from "./utils/testConfig";

describe("compareMetaData", () => {
    test("Detects new columns correctly", () => {
        const oldMetaData: MetadataHeader = {
            id: { type: "int", length: 11, primary: true, allowNull: false }
        };

        const newMetaData: MetadataHeader = {
            id: { type: "int", length: 11, primary: true, allowNull: false },
            new_col: { type: "varchar", length: 100, allowNull: true }
        };

        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.addColumns).toEqual({ new_col: { type: "varchar", length: 100, allowNull: true } });
        expect(result.modifyColumns).toEqual({});
    });

    test("Detects removed columns correctly", () => {
        const oldMetaData: MetadataHeader = {
            old_col: { type: "varchar", length: 100, allowNull: true }
        };

        const newMetaData: MetadataHeader = {};

        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.dropColumns).toEqual(["old_col"]);
    });

    test("Detects renamed columns correctly", () => {
        const oldMetaData: MetadataHeader = {
            old_name: { type: "varchar", length: 100, allowNull: true }
        };

        const newMetaData: MetadataHeader = {
            new_name: { type: "varchar", length: 100, allowNull: true }
        };

        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.renameColumns).toEqual([{ oldName: "old_name", newName: "new_name" }]);
    });

    test("Treats ambiguous renames (multiple columns with identical definitions) as drop+add", () => {
        // col_a and col_b are both dropped; new_x and new_y both have the same definition.
        // Cannot determine which is a rename of which, so both should be drop+add.
        const oldMetaData: MetadataHeader = {
            col_a: { type: "varchar", length: 50, allowNull: false },
            col_b: { type: "varchar", length: 50, allowNull: false }
        };
        const newMetaData: MetadataHeader = {
            new_x: { type: "varchar", length: 50, allowNull: false },
            new_y: { type: "varchar", length: 50, allowNull: false }
        };
        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.renameColumns).toEqual([]);
        expect(result.dropColumns).toEqual(expect.arrayContaining(["col_a", "col_b"]));
        expect(result.addColumns).toHaveProperty("new_x");
        expect(result.addColumns).toHaveProperty("new_y");
    });

    test("Renames unambiguous column even when another column has same definition on one side only", () => {
        // col_a dropped, new_x added — both have definition A. col_b also has definition A but is retained.
        // Only one dropped col matches one added col → unambiguous rename.
        const oldMetaData: MetadataHeader = {
            col_a: { type: "int", length: 11, allowNull: false },
            col_b: { type: "int", length: 11, allowNull: false }
        };
        const newMetaData: MetadataHeader = {
            col_b: { type: "int", length: 11, allowNull: false },
            new_x: { type: "int", length: 11, allowNull: false }
        };
        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.renameColumns).toEqual([{ oldName: "col_a", newName: "new_x" }]);
        expect(result.dropColumns).toEqual([]);
    });

    test("Detects safe type changes (smallint → int)", () => {
        const oldMetaData: MetadataHeader = {
            age: { type: "smallint", allowNull: false }
        };

        const newMetaData: MetadataHeader = {
            age: { type: "int", allowNull: false }
        };

        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.modifyColumns).toEqual({ age: { type: "int", allowNull: false, previousType: "smallint" } });
    });

    test("Handles increasing column length", () => {
        const oldMetaData: MetadataHeader = {
            name: { type: "varchar", length: 50, allowNull: false }
        };

        const newMetaData: MetadataHeader = {
            name: { type: "varchar", length: 100, allowNull: false }
        };

        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.modifyColumns).toEqual({ name: { type: "varchar", length: 100, allowNull: false, previousType: "varchar" } });
    });

    test("Handles NOT NULL to NULL conversion", () => {
        const oldMetaData: MetadataHeader = {
            email: { type: "varchar", length: 255, allowNull: false }
        };

        const newMetaData: MetadataHeader = {
            email: { type: "varchar", length: 255, allowNull: true }
        };

        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.nullableColumns).toEqual(["email"]);
    });

    test("Handles unique constraint removal", () => {
        const oldMetaData: MetadataHeader = {
            username: { type: "varchar", length: 100, unique: true, allowNull: false }
        };

        const newMetaData: MetadataHeader = {
            username: { type: "varchar", length: 100, unique: false, allowNull: false }
        };

        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.noLongerUnique).toEqual(["username"]);
    });

    test("Handles safe type conversion and length merging", () => {
        const oldMetaData: MetadataHeader = {
            price: { type: "smallint", length: 5 }
        };

        const newMetaData: MetadataHeader = {
            price: { type: "int", length: 10 }
        };

        const { changes: result } = compareMetaData(oldMetaData, newMetaData);
        expect(result.modifyColumns).toEqual({ price: { type: "int", length: 10, previousType: "smallint" } });
    });
});

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Complex Compare MetaData Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;
        let dialectConfig: DialectConfig;

        beforeAll(() => {
            db = Database.create(config);
            dialectConfig = db.getDialectConfig();
        });

        test("Handles merging decimal lengths correctly", () => {
            const oldMetaData: MetadataHeader = {
                amount: { type: "decimal", length: 8, decimal: 4 }
            };

            const newMetaData: MetadataHeader = {
                amount: { type: "decimal", length: 15, decimal: 2 }
            };

            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
            expect(result.modifyColumns).toEqual({ amount: { type: "decimal", length: 17, decimal: 4, previousType: "decimal" } });
        });

        test("Removes length for no_length types (e.g., JSON, TEXT)", () => {
            const oldMetaData: MetadataHeader = {
                description: { type: "varchar", length: 255 }
            };

            const newMetaData: MetadataHeader = {
                description: { type: "text" }
            };

            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
            expect(result.modifyColumns).toEqual({ description: { type: "text", previousType: "varchar" } });
        });

        test("Handles NOT NULL to NULL conversion in dialect-specific logic", () => {
            const oldMetaData: MetadataHeader = {
                email: { type: "varchar", length: 255, allowNull: false }
            };

            const newMetaData: MetadataHeader = {
                email: { type: "varchar", length: 255, allowNull: true }
            };

            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
            expect(result.nullableColumns).toEqual(["email"]);
        });

        test("Handles unique constraint removal with dialect-specific behavior", () => {
            const oldMetaData: MetadataHeader = {
                username: { type: "varchar", length: 100, unique: true, allowNull: false }
            };

            const newMetaData: MetadataHeader = {
                username: { type: "varchar", length: 100, unique: false, allowNull: false }
            };

            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
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
            const oldMetaData: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false }
            };

            const newMetaData: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["id"]);
        });

        test("Detects removed primary key but retains a primary key if needed", () => {
            const oldMetaData: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const newMetaData: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false } // Primary key removed
            };
            // THIS ONE
            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
            expect(result.primaryKeyChanges).toEqual([]); // Ensures `id` is still primary
        });

        test("Detects renamed primary key and updates accordingly", () => {
            const oldMetaData: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const newMetaData: MetadataHeader = {
                uuid: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["uuid"]);
            expect(result.renameColumns).toEqual([{ oldName: "id", newName: "uuid" }]);
        });

        test("Handles transition from single-column primary key to composite primary key", () => {
            const oldMetaData: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true }
            };

            const newMetaData: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false },
                email: { type: "varchar", length: 255, allowNull: false, primary: true }
            };

            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["id", "email"]);
        });

        test("Handles removal of a primary key by not allowing the change as it is not additive and would break existing data in the table", () => {
            const oldMetaData: MetadataHeader = {
                id: { type: "int", length: 11, allowNull: false, primary: true },
                email: { type: "varchar", length: 255, allowNull: false, primary: true }
            };

            const newMetaData: MetadataHeader = {
                email: { type: "varchar", length: 255, allowNull: false, primary: true }
            };

            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
            expect(result.primaryKeyChanges).toEqual([]);
        });

        test("Handles renamed primary keys", () => {
            const oldMetaData: MetadataHeader = {
                id: { type: "int", length: 11, primary: true, allowNull: false },
                name: { type: "varchar", length: 100, allowNull: false, unique: true },
                created_at: { type: "datetime", allowNull: false },
                email: { type: "varchar", length: 255, allowNull: false, unique: true }
            };
            
            const newMetaData: MetadataHeader = {
                uuid: { type: "int", length: 11, primary: true, allowNull: false },
                name: { type: "varchar", length: 100, allowNull: false, unique: true },
                created_at: { type: "datetime", allowNull: false },
                email: { type: "varchar", length: 255, allowNull: false, unique: true }
            };

            const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
            expect(result.primaryKeyChanges).toEqual(["uuid"]);
            expect(result.renameColumns).toEqual([{ oldName: "id", newName: "uuid" }]);
        })

        test("Simple text with added column and lower length", () => {

            const oldMetaData: MetadataHeader = {
                id: { type: "int", primary: true, autoIncrement: true, allowNull: false },
                name: { type: "varchar", length: 255, allowNull: false, unique: true }
            };

            const newMetaData: MetadataHeader = {                                                                                                                                                                                                                                                                                                                     
                id: {
                  type: 'boolean',
                  length: 1,
                  allowNull: false,
                  unique: true,
                  index: false,
                  pseudounique: false,
                  primary: false,
                  autoIncrement: false,
                  decimal: 0
                },
                name: {
                  type: 'varchar',
                  length: 5,
                  allowNull: false,
                  unique: true,
                  index: false,
                  pseudounique: false,
                  primary: false,
                  autoIncrement: false,
                  decimal: 0
                },
                email: {
                  type: 'varchar',
                  length: 17,
                  allowNull: false,
                  unique: true,
                  index: false,
                  pseudounique: false,
                  primary: false,
                  autoIncrement: false,
                  decimal: 0
                }
              }
              const { changes: result } = compareMetaData(oldMetaData, newMetaData, dialectConfig);
              expect(result.addColumns.email).toEqual(newMetaData.email)
              expect(result.modifyColumns).toEqual({})
              expect(result.primaryKeyChanges).toEqual([])
        })
    });
});
