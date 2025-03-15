import { compareMetaData } from "../src/helpers/metadata";
import { DialectConfig, ColumnDefinition, MetadataHeader } from "../src/config/types";
import { DB_CONFIG, Database } from "./utils/testConfig";

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Primary Key Handling Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;
        let dialectConfig: DialectConfig;

        beforeAll(() => {
            db = Database.create(config);
            dialectConfig = db.getDialectConfig();
        });

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
