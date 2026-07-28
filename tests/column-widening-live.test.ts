import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// R1 (live): widening an existing column on re-ingest must succeed. Postgres previously failed
// with `syntax error at or near "DEFAULT"` because the ALTER consolidated sub-actions wrongly.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`column widening on re-ingest (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "widening_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false });
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        });

        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        const noteLength = async () => {
            const r = await db.runQuery({
                query: `SELECT character_maximum_length AS l FROM information_schema.columns WHERE table_schema='test_schema' AND table_name='${TABLE}' AND column_name='note'`,
                params: [],
            });
            return Number(Object.values(r.results![0])[0]);
        };

        // ids are >255 (smallint/int range) on purpose: a <=255 int infers as MySQL tinyint, which
        // round-trips as boolean (a separate bug, R10) and would confound this widening test.
        test("a longer value widens the column and re-ingest succeeds", async () => {
            const r1 = await db.autoSQL(TABLE, [
                { id: 1000, note: "short" },
                { id: 1001, note: "tiny" },
            ]);
            expect(r1.success).toBe(true);
            const before = await noteLength();

            const r2 = await db.autoSQL(TABLE, [
                { id: 1000, note: "a much much longer note value than before, widening the column" },
                { id: 1001, note: "tiny" },
            ]);
            expect(r2.success).toBe(true); // pre-fix (PG): syntax error at or near "DEFAULT"
            expect(await noteLength()).toBeGreaterThan(before);

            // The widened value round-trips intact.
            const r = await db.runQuery({ query: `SELECT ${qi("note")} AS n FROM ${ref} WHERE ${qi("id")} = 1000`, params: [] });
            expect(String(Object.values(r.results![0])[0])).toContain("widening the column");
        });
    });
});
