import { DB_CONFIG, Database } from "./utils/testConfig";
import { getMetaData } from "../src/helpers/metadata";
import { MetadataHeader, QueryWithParams } from "../src/config/types";

Object.values(DB_CONFIG).forEach((config) => {
    describe(`getMetaData Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
        });

        test("Returns without blank columns", async () => {
            const jsonData = [
                { id: 1, name: "John Doe", email: "john@example.com", age: null },
                { id: 2, name: "Jane Smith", email: "jane@example.com", age: null }
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
            const insertQuery = db.getInsertStatementQuery('table_name', jsonData, metadata) as QueryWithParams;
            expect(insertQuery.query).not.toMatch(/age/i);

            // The params array should have no null or undefined values
            expect(insertQuery.params).not.toContain(null);
            expect(insertQuery.params).not.toContain(undefined);

            // Optionally check params length matches number of inserted columns * rows
            const columnsInQuery = (insertQuery.query.match(/\(([^)]+)\)/) || [])[1]
                .split(",")
                .map(col => col.trim().replace(/[`"'"]/g, ""));

            expect(columnsInQuery).not.toContain("age");
            expect(insertQuery.params?.length).toBe(columnsInQuery.length * jsonData.length);
            expect(metadata).toEqual(expectedMetadata);
        });
    });
});
