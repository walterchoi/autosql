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
                    index: true,
                    pseudounique: false,
                    primary: true,
                    autoIncrement: false,
                    decimal: 0
                },
                name: {
                    type: "varchar",
                    length: 10,
                    allowNull: false,
                    unique: true,
                    index: true,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    decimal: 0
                },
                email: {
                    type: "varchar",
                    length: 16,
                    allowNull: false,
                    unique: true,
                    index: true,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
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
                    unique: true,
                    index: true,
                    pseudounique: false,
                    primary: true,
                    autoIncrement: false,
                    decimal: 0
                },
                name: {
                    type: "varchar",
                    length: 4, // "John"
                    allowNull: true, // ✅ Because one row has NULL
                    unique: true,
                    index: true,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    decimal: 0
                },
                email: {
                    type: "varchar",
                    length: 16,
                    allowNull: false,
                    unique: true, // ✅ All emails are unique
                    index: true,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    decimal: 0
                }
            };

            const metadata = await getMetaData(config, jsonData);
            expect(metadata).toMatchObject(expectedMetadata);
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
                    type: "smallint",
                    length: 4,
                    allowNull: false,
                    unique: true, // ✅ Unique values
                    index: true,
                    pseudounique: false,
                    primary: true,
                    autoIncrement: false,
                    decimal: 0
                },
                user_id: {
                    type: "tinyint",
                    length: 1,
                    allowNull: false,
                    unique: true,
                    index: true,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    decimal: 0
                },
                product_id: {
                    type: "tinyint",
                    length: 3,
                    allowNull: false,
                    unique: true,
                    index: true,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    decimal: 0
                }
            };

            const metadata = await getMetaData(config, jsonData);
            expect(metadata).toMatchObject(expectedMetadata);
        });

        test("Extracts metadata for decimal + varchar columns correctly", async () => {
            const jsonData = [
                { id: 1, nondec: "667307317.17081678" },
                { id: 2, nondec: "661217317.17281618" },
                { id: 3, nondec: "value" },
                { id: 4, nondec: "value" }
            ];

            const expectedMetadata: MetadataHeader = {
                id: {
                    type: "tinyint",
                    length: 1,
                    allowNull: false,
                    unique: true,
                    index: true,
                    pseudounique: false,
                    primary: true,
                    autoIncrement: false,
                    decimal: 0
                },
                nondec: {
                    type: "varchar",
                    length: 16,
                    allowNull: false,
                    unique: false,
                    index: false,
                    pseudounique: false,
                    primary: false,
                    autoIncrement: false,
                    decimal: 0
                }
            };

            const metadata = await getMetaData(config, jsonData);
            expect(metadata).toEqual(expectedMetadata);
        });
    });
});
