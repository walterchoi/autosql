import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// db.preview(table, data, …) is a DRY RUN: it returns what an autoSQL load WOULD do (inferred schema,
// create/alter decision, exact DDL, blocked changes) WITHOUT writing. The load-bearing assertions here
// are the integrity ones — after a preview, a new table still does not exist and an existing table's
// columns are byte-identical.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect === "pgsql" || config.sqlDialect === "mysql")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`preview / dry-run (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "preview_test";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
            const baseConfig = { ...config, schema: "test_schema", useWorkers: false, addTimestamps: false };
            let admin: Database;

            beforeAll(async () => { admin = Database.create(baseConfig); await admin.establishConnection(); });
            afterAll(async () => { await dropAll(); await admin.closeConnection(); });
            async function dropAll() {
                for (const r of [tempRef, ref]) {
                    await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
                }
            }
            beforeEach(dropAll);

            const columns = async (): Promise<string[]> => {
                const r = await admin.runQuery({
                    query: `SELECT column_name FROM information_schema.columns WHERE table_schema = 'test_schema' AND table_name = '${TABLE}' ORDER BY column_name`,
                    params: [],
                });
                return (r.results ?? []).map((row: any) => String(Object.values(row)[0]).toLowerCase());
            };

            test("NEW table → action:create + CREATE ddl, and NOTHING is written", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const p = await db.preview(TABLE, [{ id: 1, name: "a" }, { id: 2, name: "b" }]);
                    expect(p.tables).toHaveLength(1);
                    expect(p.tables[0].action).toBe("create");
                    expect(p.tables[0].currentSchema).toBeNull();
                    expect(p.tables[0].ddl.some((q) => /CREATE TABLE/i.test(q))).toBe(true);
                    expect(Object.keys(p.tables[0].inferredSchema)).toEqual(expect.arrayContaining(["id", "name"]));
                    expect(p.rowCount).toBe(2);
                    // Integrity: the table was NOT created.
                    expect(await columns()).toHaveLength(0);
                } finally { await db.closeConnection(); }
            });

            test("EXISTING table → action:alter + ALTER ddl, and the table is UNCHANGED", async () => {
                await admin.runQuery({ query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("name")} VARCHAR(50))`, params: [] });
                const before = await columns();

                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const p = await db.preview(TABLE, [{ id: 1, name: "a", extra: "new column" }]);
                    expect(p.tables[0].action).toBe("alter");
                    expect(p.tables[0].currentSchema).not.toBeNull();
                    expect(p.tables[0].changes).not.toBeNull();
                    expect(Object.keys(p.tables[0].changes!.addColumns)).toContain("extra");
                    expect(p.tables[0].ddl.some((q) => /ALTER TABLE/i.test(q))).toBe(true);
                    // Integrity: no column was actually added.
                    expect(await columns()).toEqual(before);
                    expect(await columns()).not.toContain("extra");
                } finally { await db.closeConnection(); }
            });

            test("no-change data → action:noop", async () => {
                await admin.runQuery({ query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("name")} VARCHAR(50))`, params: [] });
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const p = await db.preview(TABLE, [{ id: 1, name: "a" }]);
                    expect(p.tables[0].action).toBe("noop");
                } finally { await db.closeConnection(); }
            });

            test("reports the effective numberFormat and flags blocked changes", async () => {
                // A column present in the table but absent from the data would be dropped — blocked
                // unless deleteColumns is set.
                await admin.runQuery({ query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("obsolete")} INT)`, params: [] });
                const db = Database.create({ ...baseConfig, numberFormat: "EU" });
                await db.establishConnection();
                try {
                    const p = await db.preview(TABLE, [{ id: 1 }]);
                    expect(p.numberFormat).toEqual({ thousands: ".", decimal: "," });
                    expect(p.tables[0].blockedChanges.some((w) => /obsolete/i.test(w) && /deleteColumns/i.test(w))).toBe(true);
                    // Integrity: the column is still there.
                    expect(await columns()).toContain("obsolete");
                } finally { await db.closeConnection(); }
            });
        });
    });
