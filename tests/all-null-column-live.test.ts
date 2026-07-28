import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// R12 (live): a new column that arrives with no data yet (all null) must not error and must not be
// mis-typed. It is deferred — created, correctly typed, when a later batch carries data. This also
// covers R13: an integer column added via ALTER on Postgres must not carry a display width
// (`smallint(3)` is a syntax error there).

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`all-null / deferred new column (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "all_null_column_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false, excludeBlankColumns: false });
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        });

        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        const notesType = async () => {
            const r = await db.runQuery({
                query: `SELECT data_type AS t FROM information_schema.columns WHERE table_schema='test_schema' AND table_name='${TABLE}' AND column_name='notes'`,
                params: [],
            });
            return r.results!.length ? String(Object.values(r.results![0])[0]).toLowerCase() : "(absent)";
        };

        test("an all-null new column is deferred, then created with the correct type when data arrives", async () => {
            expect((await db.autoSQL(TABLE, [{ id: 1000, name: "a" }, { id: 1001, name: "b" }])).success).toBe(true);

            // `notes` arrives entirely null → deferred (not created, no error, no guessed type).
            const r2 = await db.autoSQL(TABLE, [{ id: 1000, name: "a", notes: null }, { id: 1002, name: "c", notes: null }]);
            expect(r2.success).toBe(true); // pre-fix (MySQL): syntax error (varchar with no length)
            expect(await notesType()).toBe("(absent)"); // deferred, not materialised as varchar

            // A later batch carries integer data → the column is created as an integer, not varchar
            // (R13: on Postgres the ALTER must render a bare `smallint`, no display width).
            const r3 = await db.autoSQL(TABLE, [{ id: 1000, name: "a", notes: 500 }, { id: 1003, name: "d", notes: 600 }]);
            expect(r3.success).toBe(true);
            expect(await notesType()).toBe("smallint");

            const v = await db.runQuery({ query: `SELECT ${qi("notes")} AS n FROM ${ref} WHERE ${qi("id")} = 1000`, params: [] });
            expect(Number(Object.values(v.results![0])[0])).toBe(500);
        });
    });
});
