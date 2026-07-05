import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// H2: a per-call schema override (autoSQL(table, data, schema)) must apply to EVERY generated
// statement — DDL, staging, and inserts. The AsyncLocalStorage refactor left ~15 DDL/staging/
// index builder wrappers reading the stale this.config.schema, so the table was created in the
// base schema while inserts targeted the override. This proves it lands entirely in the override.

const BASE = "test_schema";
const OTHER = "override_schema";
const TABLE = "schema_override_test";

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);
    const ref = (schema: string) => `${qi(schema)}.${qi(TABLE)}`;

    describe(`per-call schema override for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        const countIn = async (schema: string): Promise<number> => {
            const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(schema)}`, params: [] });
            if (!r.success || !r.results?.length) return -1; // table absent in this schema
            return Number(Object.values(r.results[0])[0]);
        };
        const dropBoth = async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref(OTHER)}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref(BASE)}`, params: [] }).catch(() => {});
        };

        beforeAll(async () => {
            db = Database.create({ ...config, schema: BASE, useStagingInsert: false, useWorkers: false });
            await db.establishConnection();
            await db.runQuery({ query: `CREATE SCHEMA IF NOT EXISTS ${qi(OTHER)}`, params: [] });
            await dropBoth();
        });
        afterAll(async () => {
            await dropBoth();
            await db.closeConnection();
        });

        test("autoSQL(table, data, override) creates + inserts into the override schema, not the base", async () => {
            const res = await db.autoSQL(TABLE, [{ id: 1, name: "a" }, { id: 2, name: "b" }], OTHER);
            expect(res.success).toBe(true);
            expect(await countIn(OTHER)).toBe(2); // everything landed in the override schema
            expect(await countIn(BASE)).toBe(-1); // nothing was created in the base schema
        });
    });
});
