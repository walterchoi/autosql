import { DB_CONFIG, Database } from "./utils/testConfig";
import { getMetaData } from "../src/helpers/metadata";
import { MetadataHeader } from "../src/config/types";

Object.values(DB_CONFIG).forEach((config) => {
    describe(`getMetaData Tests for ${config.sqlDialect.toUpperCase()}`, () => {

        test("Extracts metadata for simple dataset", async () => {
            const jsonData = [
                { id: 1, name: "John Doe", email: "john@example.com" },
                { id: 2, name: "Jane Smith", email: "jane@example.com" }
            ];

            const expectedMetadata: MetadataHeader = {
                id: {
                    type: "tinyint",
                    length: 1,
                    allowNull: false,
                    unique: true,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                },
                name: {
                    type: "varchar",
                    length: 10,
                    allowNull: false,
                    unique: true,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                },
                email: {
                    type: "varchar",
                    length: 16,
                    allowNull: false,
                    unique: true,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                }
            };

            const metadata = await getMetaData(config, jsonData);
            expect(metadata).toEqual(expectedMetadata);
        });
        
        test("Handles allowNull correctly", async () => {
            const jsonData = [
                { id: 1, name: "John", email: "john@example.com" },
                { id: 2, name: null, email: "jane@example.com" } // Name is NULL
            ];

            const expectedMetadata: MetadataHeader = {
                id: {
                    type: "tinyint",
                    length: 1,
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                },
                name: {
                    type: "varchar",
                    length: 4, // "John"
                    allowNull: true, // ✅ Because one row has NULL
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                },
                email: {
                    type: "varchar",
                    length: 16,
                    allowNull: false,
                    unique: true, // ✅ All emails are unique
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                }
            };

            const metadata = await getMetaData(config, jsonData);
            expect(metadata).toEqual(expectedMetadata);
        });

        test("Detects unique and pseudo-unique columns", async () => {
            const jsonData = [
                { order_id: 1001, user_id: 1, product_id: 101 },
                { order_id: 1002, user_id: 2, product_id: 102 },
                { order_id: 1003, user_id: 3, product_id: 103 },
                { order_id: 1004, user_id: 4, product_id: 104 },
                { order_id: 1005, user_id: 5, product_id: 105 }
            ];

            const expectedMetadata: MetadataHeader = {
                order_id: {
                    type: "tinyint",
                    length: 4,
                    allowNull: false,
                    unique: true, // ✅ Unique values
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                },
                user_id: {
                    type: "tinyint",
                    length: 1,
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: true, // ✅ Close to unique
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                },
                product_id: {
                    type: "tinyint",
                    length: 3,
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                }
            };

            const metadata = await getMetaData(config, jsonData);
            expect(metadata).toEqual(expectedMetadata);
        });

        /*
        test("Handles multiple data types in the same column", async () => {
            const jsonData = [
                { mixed: 1 },
                { mixed: "Hello" },
                { mixed: 3.14 },
                { mixed: null }
            ];

            const expectedMetadata: MetadataHeader = {
                mixed: {
                    type: "varchar", // ✅ Mixed types default to string
                    length: 5, // ✅ "Hello"
                    allowNull: true,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                }
            };

            const metadata = await getMetaData(config, jsonData);
            expect(metadata).toEqual(expectedMetadata);
        });

        test("Handles boolean values correctly", async () => {
            const jsonData = [
                { user_id: 1, is_active: true },
                { user_id: 2, is_active: false }
            ];

            const expectedMetadata: MetadataHeader = {
                user_id: {
                    type: "tinyint",
                    length: 1,
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                },
                is_active: {
                    type: "boolean",
                    length: 1, // ✅ Stored as 0 or 1
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                }
            };

            const metadata = await getMetaData(config, jsonData);
            expect(metadata).toEqual(expectedMetadata);
        });

        test("Handles composite primary keys", async () => {
            const jsonData = [
                { order_id: 1, product_id: 1001, quantity: 2 },
                { order_id: 1, product_id: 1002, quantity: 1 }
            ];

            const expectedMetadata: MetadataHeader = {
                order_id: {
                    type: "tinyint",
                    length: 1,
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: true, // ✅ Composite primary key
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                },
                product_id: {
                    type: "tinyint",
                    length: 4,
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: true, // ✅ Composite primary key
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                },
                quantity: {
                    type: "tinyint",
                    length: 1,
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    default: undefined,
                    decimal: 0
                }
            };

            const metadata = await getMetaData(config, jsonData);
            expect(metadata).toEqual(expectedMetadata);
        });
        */
    });
});
