import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// A3 live proof: when loading into a PRE-EXISTING table whose column carries a DDL DEFAULT, a row
// with a NULL/omitted value for that column must NOT store the introspected default EXPRESSION as a
// literal value. The bug: introspection put the catalog `column_default` (e.g. Postgres
// `'active'::character varying`, or `CURRENT_TIMESTAMP`) into `default`, which getInsertValues then
// bound as the row's value — storing the expression string verbatim. Fix: introspected defaults go
// to `ddlDefault`, which the insert path never binds; a missing value becomes NULL (or the DB's own
// default), never the expression.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`introspected default not bound as value (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "introspected_default_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;

        const baseConfig = { ...config, schema: "test_schema", useWorkers: false, addTimestamps: false };
        let admin: Database;

        beforeAll(async () => {
            admin = Database.create(baseConfig);
            await admin.establishConnection();
        });
        afterAll(async () => {
            await dropAll();
            await admin.closeConnection();
        });
        async function dropAll() {
            for (const r of [tempRef, ref]) {
                await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
            }
        }
        const statusOf = async (id: number) => {
            const r = await admin.runQuery({ query: `SELECT ${qi("status")} AS s FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
            return r.results!.length ? (Object.values(r.results![0])[0] as any) : undefined;
        };

        beforeEach(async () => {
            await dropAll();
            // Pre-existing table (as a BYOD customer table would be) with a real DDL default.
            await admin.runQuery({
                query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("status")} VARCHAR(20) DEFAULT 'active')`,
                params: [],
            });
        });

        test("a null/omitted value stores NULL or the DB default — never the DDL expression string", async () => {
            const db = Database.create(baseConfig);
            await db.establishConnection();
            try {
                // Row omits `status`; a second row provides it (normal values must still round-trip).
                const r = await db.autoSQL(TABLE, [{ id: 1 }, { id: 2, status: "shipped" }]);
                expect(r.success).toBe(true);

                const s1 = await statusOf(1);
                // Acceptable: NULL (explicit) or 'active' (DB applied its own default). The bug stored
                // the literal expression, e.g. "'active'::character varying" — assert that never happens.
                expect([null, "active"]).toContain(s1);
                if (typeof s1 === "string") {
                    expect(s1).not.toContain("::");
                    expect(s1).not.toMatch(/character varying/i);
                }

                expect(await statusOf(2)).toBe("shipped");
            } finally {
                await db.closeConnection();
            }
        });
    });
});
