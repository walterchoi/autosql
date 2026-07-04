import { DB_CONFIG, Database } from "./utils/testConfig";
import { getMetaData } from "../src/helpers/metadata";

// Regression test: scientific-notation values must infer as the `exponent` type (which the
// dialect maps translate to a real DOUBLE/NUMERIC column). Previously predictType produced
// "exponential", a name no dialect map knows, so the generated DDL was invalid.

const TABLE = "exponent_test_table";
const DATA = [{ v: "1.23e10" }, { v: "9.9e-5" }, { v: "4.5e8" }, { v: "6.02e23" }];

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Exponent column DDL for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
        });

        afterAll(async () => {
            try {
                await db.runQuery(db.dropTableQuery(TABLE));
            } catch {
                /* table may not exist */
            }
            await db.closeConnection();
        });

        test("scientific-notation values infer as `exponent` and generate valid DDL", async () => {
            const meta = await getMetaData(db.getConfig(), DATA);
            expect(meta.v.type).toBe("exponent");

            const queries = db.createTableQuery(TABLE, meta);
            for (const query of queries) {
                await expect(db.testQuery(query)).resolves.not.toThrow();
            }
        });
    });
});
