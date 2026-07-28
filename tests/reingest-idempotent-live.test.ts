import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Durable guard: re-ingesting identical data must be schema-idempotent — no column's inferred type
// may drift on the second load. A drift means the type doesn't round-trip through introspection
// (the R10 class: tinyint↔boolean, but this covers small int, boolean, varchar, decimal, date), and
// causes a spurious — sometimes destructive — re-ALTER on every ingest.

const DATA = [
    { id: 5, active: true, name: "alpha", amount: 12.50, event_date: "2026-03-15" },
    { id: 6, active: false, name: "beta", amount: 99.99, event_date: "2026-04-20" },
    { id: 7, active: true, name: "gamma", amount: 5.00, event_date: "2026-05-10" },
];

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`re-ingest schema idempotency (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "reingest_idempotent_test";
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

        const columnTypes = async () => {
            const { currentMetaData } = await (db as any).autoSQLHandler.fetchTableMetadata(TABLE);
            const map: Record<string, string | null> = {};
            for (const [col, def] of Object.entries(currentMetaData as Record<string, any>)) {
                map[col] = def.type;
            }
            return map;
        };

        test("no column type drifts on an identical re-ingest", async () => {
            expect((await db.autoSQL(TABLE, DATA)).success).toBe(true);
            const before = await columnTypes();

            expect((await db.autoSQL(TABLE, DATA)).success).toBe(true); // identical
            const after = await columnTypes();

            expect(after).toEqual(before); // no re-typing ALTER — every inferred type round-trips
            const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            expect(Number(Object.values(c.results![0])[0])).toBe(3);
        });
    });
});
