import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";
import { MetadataHeader } from "../src/config/types";

// A-4 provided-schema (assumeSchema) fast path, end to end. The discriminating case: declare
// `id: bigint` on data whose id values are 0/1 — inference would mis-type that as boolean (R3),
// but the provided schema is authoritative, so the column is created/loaded as bigint. Also covers
// first-run CREATE-from-spec and steady-state re-ingest (upsert, no schema thrash).

const SCHEMA: MetadataHeader = {
    id: { type: "bigint", primary: true, allowNull: false },
    flag: { type: "boolean" },                 // genuinely boolean
    name: { type: "varchar", length: 100 },
};
const DATA = [
    { id: 100, flag: true, name: "alpha" },
    { id: 101, flag: false, name: "beta" },
    { id: 0, flag: true, name: "zero" },       // id in {0,1} — the boolean-trap trigger
    { id: 1, flag: false, name: "one" },
];

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`assumeSchema (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "assume_schema_test";
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

        const count = async () => {
            const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            return Number(Object.values(r.results![0])[0]);
        };
        const columnType = async (col: string) => {
            const r = await db.runQuery({
                query: `SELECT data_type AS t FROM information_schema.columns WHERE table_schema = 'test_schema' AND table_name = '${TABLE}' AND column_name = '${col}'`,
                params: [],
            });
            return String(Object.values(r.results![0])[0]).toLowerCase();
        };

        test("first run creates the table from the provided schema and loads it", async () => {
            const res = await db.autoSQL(TABLE, DATA, undefined, undefined, { assumeSchema: SCHEMA });
            expect(res.success).toBe(true);
            expect(await count()).toBe(4);
        });

        test("id declared bigint is stored as bigint, not boolean (inference-skip + R3)", async () => {
            expect(await columnType("id")).toBe("bigint");
            // Functional proof too: an integer comparison on id works (a boolean id would throw on PG).
            const r = await db.runQuery({ query: `SELECT ${qi("name")} AS n FROM ${ref} WHERE ${qi("id")} = 1`, params: [] });
            expect(String(Object.values(r.results![0])[0])).toBe("one");
        });

        test("re-ingesting with the same provided schema upserts without a schema change", async () => {
            const edited = DATA.map(r => (r.id === 100 ? { ...r, name: "ALPHA" } : r));
            const res = await db.autoSQL(TABLE, edited, undefined, undefined, { assumeSchema: SCHEMA });
            expect(res.success).toBe(true);
            expect(await count()).toBe(4);           // upsert, not append
            expect(await columnType("id")).toBe("bigint"); // type unchanged
            const r = await db.runQuery({ query: `SELECT ${qi("name")} AS n FROM ${ref} WHERE ${qi("id")} = 100`, params: [] });
            expect(String(Object.values(r.results![0])[0])).toBe("ALPHA");
        });
    });
});
