import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live proof that the `sourceTimeZone` override flows end-to-end through autoSQL into stored data:
// a zoneless datetime declared to be in a source zone is stored as the corresponding UTC instant.
// Values are read back as TEXT (dialect cast) to bypass driver JS-Date conversion, which — under the
// suite's non-UTC process zone — would otherwise muddy the comparison.

// Per-dialect: how to create a naive-datetime column, and how to read it back as a stable string.
const DIALECT: Record<string, { colType: string; asText: (col: string) => string }> = {
    pgsql: { colType: "TIMESTAMP", asText: (c) => `to_char(${c}, 'YYYY-MM-DD HH24:MI:SS')` },
    mysql: { colType: "DATETIME", asText: (c) => `DATE_FORMAT(${c}, '%Y-%m-%d %H:%i:%s')` },
};

Object.values(DB_CONFIG)
    .filter((config) => DIALECT[config.sqlDialect])
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);
        const d = DIALECT[config.sqlDialect];

        describe(`sourceTimeZone end-to-end (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "source_tz_test";
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
            beforeEach(async () => {
                await dropAll();
                await admin.runQuery({ query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("ts")} ${d.colType})`, params: [] });
            });
            const tsText = async (id: number) => {
                const r = await admin.runQuery({ query: `SELECT ${d.asText(qi("ts"))} AS s FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
                return r.results!.length ? String(Object.values(r.results![0])[0]) : null;
            };

            test("a zoneless value declared as America/New_York is stored as its UTC instant", async () => {
                const db = Database.create({ ...baseConfig, sourceTimeZone: "America/New_York" });
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [
                        { id: 1, ts: "2024-01-15 12:00:00" }, // EST −5  → 17:00 UTC
                        { id: 2, ts: "2024-07-15 12:00:00" }, // EDT −4  → 16:00 UTC (DST)
                    ]);
                    expect(r.success).toBe(true);
                    expect(await tsText(1)).toBe("2024-01-15 17:00:00");
                    expect(await tsText(2)).toBe("2024-07-15 16:00:00");
                } finally {
                    await db.closeConnection();
                }
            });

            test("without sourceTimeZone the same value is stored verbatim (wall-clock preserved)", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [{ id: 1, ts: "2024-01-15 12:00:00" }]);
                    expect(r.success).toBe(true);
                    expect(await tsText(1)).toBe("2024-01-15 12:00:00");
                } finally {
                    await db.closeConnection();
                }
            });
        });
    });
