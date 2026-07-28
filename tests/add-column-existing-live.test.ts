import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// R11 (live): adding a new column to an existing, populated table must succeed on both dialects
// (Postgres previously failed a NOT NULL add with "column contains null values"). The new column is
// nullable so pre-existing rows are left NULL rather than fabricated. Also checks D-B: autoSQL
// returns its resolved schema (incl. managed dwh_* columns).

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`add column to existing table (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "add_column_existing_test";
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

        test("a new non-nullable column is added nullable; existing rows stay NULL", async () => {
            const r1 = await db.autoSQL(TABLE, [{ id: 1000, name: "a" }, { id: 1001, name: "b" }]);
            expect(r1.success).toBe(true);
            // D-B: the result carries the resolved schema, incl. managed dwh_* columns.
            expect(r1.metaData).toBeDefined();
            expect(r1.metaData!.id).toBeDefined();
            expect(r1.metaData!.dwh_created_at).toBeDefined();

            // `status` is present in every row here, so inference marks it NOT NULL — but the table
            // already has rows, so it must be added nullable.
            const r2 = await db.autoSQL(TABLE, [
                { id: 1000, name: "a", status: "active" },
                { id: 1002, name: "c", status: "new" },
            ]);
            expect(r2.success).toBe(true); // pre-fix (PG): column "status" contains null values

            const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            expect(Number(Object.values(c.results![0])[0])).toBe(3); // 1000 upserted, 1001, 1002

            // The pre-existing row 1001 has no status → NULL (not fabricated).
            const nullCount = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref} WHERE ${qi("status")} IS NULL`, params: [] });
            expect(Number(Object.values(nullCount.results![0])[0])).toBe(1);
            // The upserted row got its value.
            const s = await db.runQuery({ query: `SELECT ${qi("status")} AS s FROM ${ref} WHERE ${qi("id")} = 1000`, params: [] });
            expect(String(Object.values(s.results![0])[0])).toBe("active");
        });
    });
});
