import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live regression for inferAdditionalUniques: false (found by Sprout, 2026-10-03). A shared table keyed
// PRIMARY KEY (conn, id) also got UNIQUE(id), UNIQUE(name) and UNIQUE(conn), because each column was
// 100% distinct within one batch. MySQL's ON DUPLICATE KEY UPDATE fires on any unique index, so
// connection B's id = r1 overwrote connection A's; Postgres failed with a unique violation instead.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect === "pgsql" || config.sqlDialect === "mysql")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`inferUnique / inferAdditionalUniques (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "infer_unique_test";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
            const defaultConfig = { ...config, schema: "test_schema", useWorkers: false, addTimestamps: false };
            const baseConfig = { ...defaultConfig, inferAdditionalUniques: false };
            let db: Database;

            beforeAll(async () => { db = Database.create(baseConfig); await db.establishConnection(); });
            afterAll(async () => { await dropAll(); await db.closeConnection(); });
            async function dropAll() {
                for (const r of [tempRef, ref]) {
                    await db.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
                }
            }
            beforeEach(dropAll);

            const uniqueConstraintCount = async (): Promise<number> => {
                const r = await db.runQuery({
                    query: `SELECT COUNT(*) AS n FROM information_schema.table_constraints WHERE table_schema = 'test_schema' AND table_name = '${TABLE}' AND constraint_type = 'UNIQUE'`,
                    params: [],
                });
                return Number(Object.values(r.results![0])[0]);
            };
            const rowCount = async (): Promise<number> => {
                const r = await db.runQuery({ query: `SELECT COUNT(*) AS n FROM ${ref}`, params: [] });
                return Number(Object.values(r.results![0])[0]);
            };
            const liveMeta = async () => (await (db as any).autoSQLHandler.fetchTableMetadata(TABLE)).currentMetaData;

            test("composite key: the same id under two connections gives two rows", async () => {
                const PK = ["conn", "id"];
                expect((await db.autoSQL(TABLE, [{ conn: "A", id: "r1", name: "Acme" }], undefined, PK)).success).toBe(true);
                expect((await db.autoSQL(TABLE, [{ conn: "B", id: "r1", name: "Acme" }], undefined, PK)).success).toBe(true);
                expect(await rowCount()).toBe(2);
                expect(await uniqueConstraintCount()).toBe(0);

                // An ordinary next load (repeated name and conn) still goes through.
                expect((await db.autoSQL(TABLE, [{ conn: "A", id: "r2", name: "Acme" }], undefined, PK)).success).toBe(true);
                expect(await rowCount()).toBe(3);

                const r = await db.runQuery({ query: `SELECT ${qi("conn")} AS c FROM ${ref} WHERE ${qi("id")} = 'r1' ORDER BY ${qi("conn")}`, params: [] });
                expect((r.results ?? []).map((row: any) => row.c)).toEqual(["A", "B"]);
            });

            test("defaults are unchanged: a primary key still gets inferred uniques beside it", async () => {
                const dflt = Database.create(defaultConfig);
                await dflt.establishConnection();
                try {
                    expect((await dflt.autoSQL(TABLE, [{ conn: "A", id: "r1", name: "Acme" }], undefined, ["conn", "id"])).success).toBe(true);
                    expect(await uniqueConstraintCount()).toBeGreaterThan(0);
                } finally { await dflt.closeConnection(); }
            });

            test("preview with a primary key shows the same CREATE: PK only, no UNIQUE", async () => {
                const p = await db.preview(TABLE, [{ conn: "A", id: "r1", name: "Acme" }], undefined, ["conn", "id"]);
                expect(p.tables[0].action).toBe("create");
                const ddl = p.tables[0].ddl.join("\n");
                expect(ddl).toMatch(/PRIMARY KEY/i);
                expect(ddl).not.toMatch(/UNIQUE\s*\(/i); // \( so the table name "infer_unique_test" doesn't match
            });

            test("existing table: a newly distinct column gets no UNIQUE on ALTER", async () => {
                // code repeats → not unique at CREATE.
                expect((await db.autoSQL(TABLE, [{ id: 1, code: "x" }, { id: 2, code: "x" }])).success).toBe(true);
                const before = await uniqueConstraintCount();

                // This batch: code is 100% distinct and `extra` is a brand-new, 100% distinct column.
                expect((await db.autoSQL(TABLE, [{ id: 3, code: "y", extra: "e1" }, { id: 4, code: "z", extra: "e2" }])).success).toBe(true);
                expect(await uniqueConstraintCount()).toBe(before);
                const meta = await liveMeta();
                expect(meta.code.unique).toBe(false);
                expect(meta.extra.unique).toBe(false);

                // So a later batch repeating those values still loads.
                expect((await db.autoSQL(TABLE, [{ id: 5, code: "y", extra: "e1" }])).success).toBe(true);
                expect(await rowCount()).toBe(5);
            });

            test("existing table: a unique already on the table is kept", async () => {
                // Keyless first load → email inferred unique (unchanged default for a new keyless table).
                expect((await db.autoSQL(TABLE, [{ id: 1, email: "a@x.io" }, { id: 2, email: "b@x.io" }])).success).toBe(true);
                expect((await liveMeta()).email.unique).toBe(true);
                const before = await uniqueConstraintCount();
                expect(before).toBeGreaterThan(0);

                expect((await db.autoSQL(TABLE, [{ id: 3, email: "c@x.io" }, { id: 4, email: "d@x.io" }])).success).toBe(true);
                expect((await liveMeta()).email.unique).toBe(true);
                expect(await uniqueConstraintCount()).toBe(before);
            });

            test("inferUnique: false on a new keyless table creates no UNIQUE", async () => {
                const noInfer = Database.create({ ...baseConfig, inferUnique: false });
                await noInfer.establishConnection();
                try {
                    expect((await noInfer.autoSQL(TABLE, [{ id: 1, email: "a@x.io" }, { id: 2, email: "b@x.io" }])).success).toBe(true);
                    expect(await uniqueConstraintCount()).toBe(0);
                } finally { await noInfer.closeConnection(); }
            });
        });
    });
