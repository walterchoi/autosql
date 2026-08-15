import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// A5 live proof: if a row fails the main load AND the divert to rejectedRowsTable also fails, the
// load must fail LOUD — never report success while the rows silently vanish. The bug: the bootstrap +
// rejected-rows INSERT results were unchecked (runTransaction returns {success:false}, it doesn't
// throw), so a broken rejects table swallowed the rows and the load still returned success:true.
// Here the rejects table pre-exists with an INCOMPATIBLE shape, so the divert INSERT fails.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect !== "sqlserver") // rejectedRowsTable is rejected up-front on SQL Server
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`rejectedRowsTable divert failure fails loud (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "fail_loud_test";
            const HISTORY = TABLE + "__history";
            const REJECTED = "fail_loud_rejects";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const histRef = `${qi("test_schema")}.${qi(HISTORY)}`;
            const rejRef = `${qi("test_schema")}.${qi(REJECTED)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;

            const baseConfig = {
                ...config, schema: "test_schema", useWorkers: false, addTimestamps: false,
                addHistory: true, historyTables: [TABLE], rejectedRowsTable: REJECTED,
            };
            let admin: Database;

            beforeAll(async () => { admin = Database.create(baseConfig); await admin.establishConnection(); });
            afterAll(async () => { await dropAll(); await admin.closeConnection(); });
            async function dropAll() {
                for (const r of [tempRef, histRef, ref, rejRef]) {
                    await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
                }
            }
            beforeEach(async () => {
                await dropAll();
                await admin.runQuery({
                    query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("val")} INT, CONSTRAINT ${qi("fail_loud_val_nn")} CHECK (${qi("val")} >= 0))`,
                    params: [],
                });
                await admin.runQuery({ query: `INSERT INTO ${ref} (${qi("id")}, ${qi("val")}) VALUES (1, 10)`, params: [] });
                // Pre-create the rejects table with an INCOMPATIBLE shape so the divert INSERT fails.
                // (CREATE TABLE IF NOT EXISTS in the bootstrap will skip it, then the INSERT mismatches.)
                await admin.runQuery({ query: `CREATE TABLE ${rejRef} (${qi("unrelated")} INT)`, params: [] });
            });

            test("a divert-write failure surfaces as success:false, not a silent drop", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    // id=2 val=-5 violates the CHECK → per-PK merge fails → divert to the broken rejects table.
                    const r = await db.autoSQL(TABLE, [{ id: 1, val: 15 }, { id: 2, val: -5 }]);
                    expect(r.success).toBe(false);
                    expect(String(r.error)).toMatch(/could not be written to rejectedRowsTable/i);
                } finally {
                    await db.closeConnection();
                }
            });
        });
    });
