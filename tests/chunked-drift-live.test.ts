import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// autoSQLChunked locks the FIRST chunk's inferred types and reuses them. A later chunk can carry a
// value the locked column can't hold (e.g. ids 1..100 -> tinyint, then id 128 overflows). The chunked
// path now re-infers each chunk against the locked schema and WIDENS (ALTER + update the lock) when a
// chunk needs it — so monotonically-growing keys/values no longer overflow, while a no-drift chunk
// still skips the DDL.

const mk = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, i) => ({ id: from + i, name: "n" + ((from + i) % 7) }));

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`autoSQLChunked schema-drift widening (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "chunk_drift_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        let db: Database;
        const colType = async (col: string) => {
            const { currentMetaData } = await (db as any).autoSQLHandler.fetchTableMetadata(TABLE);
            return (currentMetaData as any)?.[col]?.type ?? null;
        };

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false });
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        });
        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        test("a later chunk with a larger id widens the locked type instead of overflowing", async () => {
            // Chunk 1 (ids 1..100) infers tinyint; chunks 2/3 carry ids the locked tinyint can't hold.
            async function* gen() {
                yield mk(1, 100);
                yield mk(101, 200);
                yield mk(201, 300);
            }
            const res = await (db as any).autoSQLHandler.autoSQLChunked(TABLE, gen(), "test_schema");
            expect(res.success).toBe(true);

            const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            expect(Number(Object.values(c.results![0])[0])).toBe(300); // no rows lost to overflow

            // The id column was widened past tinyint to fit id 128..300.
            expect(["smallint", "int", "integer", "bigint", "int2", "int4"]).toContain(await colType("id"));

            // Values are intact (e.g. the ones that would have overflowed tinyint).
            const r = await db.runQuery({ query: `SELECT ${qi("id")} AS id FROM ${ref} WHERE ${qi("id")} = 300`, params: [] });
            expect(Number((r.results![0] as any).id)).toBe(300);
        });
    });
});
