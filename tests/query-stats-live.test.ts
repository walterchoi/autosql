import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";
import { QueryStats } from "../src/config/types";

// Runtime instrumentation: every autoSQL load returns per-phase timings + throughput on
// QueryResult.stats and, if a stats sink is configured, calls logger.stats once with the same object.
// This is what a pipeline forwards to its observability / stats store.

// id starts at 1000 so the first chunk (for the chunked test) already infers a wide-enough integer
// type — the chunked path locks the first chunk's schema, and ids 1..100 would infer tinyint and then
// overflow on a later chunk's larger id.
const ROWS = Array.from({ length: 300 }, (_, i) => ({ id: 1000 + i, name: "n" + (i % 7), amount: (i + 1) * 1.5, active: i % 2 === 0 }));

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`autoSQL run stats (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "query_stats_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
        let db: Database;
        const collected: QueryStats[] = [];

        beforeAll(async () => {
            db = Database.create({
                ...config,
                schema: "test_schema",
                useWorkers: false,
                logger: { stats: (s: QueryStats) => collected.push(s) },
            });
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        });
        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        test("returns per-phase timings + throughput on the result, and calls logger.stats once", async () => {
            const res = await db.autoSQL(TABLE, ROWS);
            expect(res.success).toBe(true);

            // On the result envelope
            const s = res.stats!;
            expect(s).toBeDefined();
            expect(s.table).toBe(TABLE);
            expect(s.rows).toBe(300);
            expect(s.affectedRows).toBeGreaterThan(0);
            expect(s.durationMs).toBeGreaterThan(0);
            expect(s.rowsPerSecond).toBeGreaterThan(0);
            // Phases that ran on a fresh table: prepare (inference), configure (CREATE), load.
            expect(s.phases.prepare).toBeGreaterThanOrEqual(0);
            expect(s.phases.configure).toBeGreaterThanOrEqual(0);
            expect(s.phases.load).toBeGreaterThanOrEqual(0);
            expect(typeof s.staged).toBe("boolean");
            expect(typeof s.bulkLoad).toBe("boolean");
            // The three phase timings should sum to roughly (≤) the total duration.
            const phaseSum = (s.phases.prepare ?? 0) + (s.phases.configure ?? 0) + (s.phases.load ?? 0);
            expect(phaseSum).toBeLessThanOrEqual(s.durationMs + 50); // small slack for other steps

            // The sink was called once with the same stats.
            expect(collected).toHaveLength(1);
            expect(collected[0]).toEqual(s);
        });

        test("autoSQLChunked aggregates stats across chunks (rows summed, load accumulated)", async () => {
            const CHUNK_TABLE = "query_stats_chunked_test";
            const cref = `${qi("test_schema")}.${qi(CHUNK_TABLE)}`;
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${qi("test_schema")}.${qi("temp_staging__" + CHUNK_TABLE)}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${cref}`, params: [] }).catch(() => {});
            collected.length = 0;

            async function* gen() {
                yield ROWS.slice(0, 100);
                yield ROWS.slice(100, 200);
                yield ROWS.slice(200, 300);
            }
            const res = await (db as any).autoSQLHandler.autoSQLChunked(CHUNK_TABLE, gen(), "test_schema");
            expect(res.success).toBe(true);

            const s = res.stats!;
            expect(s.rows).toBe(300);                              // summed across the 3 chunks
            expect(s.phases.prepare).toBeGreaterThanOrEqual(0);    // inference on the first chunk only
            expect(s.phases.configure).toBeGreaterThanOrEqual(0);  // DDL on the first chunk only
            expect(s.phases.load).toBeGreaterThanOrEqual(0);       // accumulated over all chunks
            expect(s.rowsPerSecond).toBeGreaterThan(0);
            expect(collected).toHaveLength(1);
            expect(collected[0]).toEqual(s);

            const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${cref}`, params: [] });
            expect(Number(Object.values(c.results![0])[0])).toBe(300);
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${cref}`, params: [] }).catch(() => {});
        });
    });
});
