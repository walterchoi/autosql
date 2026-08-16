import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live proof that dataset-level number-format CONSENSUS flows end-to-end into stored data. The
// resolved separators are overlaid on getConfig() via AsyncLocalStorage (same mechanism as the
// per-operation schema override), so inference AND load-time sqlize — on the main thread and in
// workers — see them. Key arbiter: a column that is ambiguous on its own ("1,234") is stored
// correctly because a DECISIVE sibling column ("1,234,567") resolves the whole dataset.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect === "pgsql" || config.sqlDialect === "mysql")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`number-format consensus end-to-end (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "nf_consensus_test";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
            const streamPrefix = `${qi("test_schema")}.`;
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

            const read = async (col: string, id: number) => {
                const r = await admin.runQuery({ query: `SELECT ${qi(col)} AS v FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
                return r.results!.length ? Object.values(r.results![0])[0] : null;
            };

            test("a DECISIVE sibling column resolves an ambiguous one (no config) — amt '1,234' stored as 1234", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [
                        { id: 1, amt: "1,234", total: "1,234,567" }, // total is decisive US → dataset is US
                        { id: 2, amt: "5,678", total: "2,345,678" },
                    ]);
                    expect(r.success).toBe(true);
                    expect(Number(await read("amt", 1))).toBe(1234);
                    expect(Number(await read("total", 1))).toBe(1234567);
                    expect(Number(await read("amt", 2))).toBe(5678);
                } finally {
                    await db.closeConnection();
                }
            });

            test("EU resolves the same way — amt '1.234' stored as 1234 via a decisive '1.234.567'", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [
                        { id: 1, amt: "1.234", total: "1.234.567" },
                        { id: 2, amt: "5.678", total: "2.345.678" },
                    ]);
                    expect(r.success).toBe(true);
                    expect(Number(await read("amt", 1))).toBe(1234);
                    expect(Number(await read("total", 1))).toBe(1234567);
                } finally {
                    await db.closeConnection();
                }
            });

            test("consensus also holds under useWorkers:true (overlay reaches the worker load path)", async () => {
                const db = Database.create({ ...baseConfig, useWorkers: true });
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [
                        { id: 1, amt: "1,234", total: "1,234,567" },
                        { id: 2, amt: "5,678", total: "2,345,678" },
                    ]);
                    expect(r.success).toBe(true);
                    expect(Number(await read("amt", 1))).toBe(1234);
                } finally {
                    await db.closeConnection();
                }
            });

            test("conflict (US and EU both present) → warns and does NOT auto-resolve", async () => {
                const warnings: string[] = [];
                const db = Database.create({ ...baseConfig, logger: { warn: (m: string) => warnings.push(m) } });
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [
                        { id: 1, a: "1,234,567" }, // decisive US
                        { id: 2, b: "1.234.567" }, // decisive EU → contradiction
                    ]);
                    expect(r.success).toBe(true);
                    expect(warnings.some((w) => /conflicting number formats/i.test(w))).toBe(true);
                } finally {
                    await db.closeConnection();
                }
            });

            test("A24c warning still surfaces under useWorkers:true (inference is main-thread) — #50 flag", async () => {
                const warnings: string[] = [];
                const db = Database.create({ ...baseConfig, useWorkers: true, logger: { warn: (m: string) => warnings.push(m) } });
                await db.establishConnection();
                try {
                    // Only ambiguous values → no consensus → default + A24c per-column warning.
                    const r = await db.autoSQL(TABLE, [{ id: 1, amt: "1,234" }, { id: 2, amt: "5,678" }]);
                    expect(r.success).toBe(true);
                    expect(warnings.some((w) => w.includes('"amt"'))).toBe(true);
                } finally {
                    await db.closeConnection();
                }
            });

            test("chunked: the first chunk's decisive column locks the format for the whole load", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    async function* chunks() {
                        yield [{ id: 1, amt: "1,234", total: "1,234,567" }]; // first chunk decides US
                        yield [{ id: 2, amt: "5,678", total: "2,345,678" }];
                    }
                    const r = await db.autoSQLChunked(TABLE, chunks());
                    expect(r.success).toBe(true);
                    expect(Number(await read("amt", 1))).toBe(1234);
                    expect(Number(await read("amt", 2))).toBe(5678);
                } finally {
                    await db.closeConnection();
                }
            });

            // NOTE: no streaming test here — the stream path stores values via DB CAST / raw parameter
            // binding (never sqlize/normalizeNumber), so separators (consensus OR numberFormat) don't
            // apply to streams. That's a pre-existing limitation tracked as a roadmap follow-up.
        });
    });
