import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live proof that `numberFormat` flows end-to-end through autoSQL into stored data — not just into
// inferred column shape. Under EU, a lone-dot 3-digit value ("1.234") is a THOUSANDS-grouped integer
// (1234), so the column is created INT and the row lands as 1234. This is the arbiter test: if the
// resolved separators failed to reach the load path, the value would corrupt to 1 (int) or 1.234.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect === "pgsql" || config.sqlDialect === "mysql")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`numberFormat end-to-end (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "number_format_test";
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
            beforeEach(dropAll);

            const readAmt = async (id: number) => {
                const r = await admin.runQuery({ query: `SELECT ${qi("amt")} AS a FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
                return r.results!.length ? Object.values(r.results![0])[0] : null;
            };

            test("numberFormat:'EU' stores a lone-dot 3-digit value as a grouped integer (1.234 -> 1234)", async () => {
                const db = Database.create({ ...baseConfig, numberFormat: "EU" });
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [
                        { id: 1, amt: "1.234" },
                        { id: 2, amt: "5.678" },
                    ]);
                    expect(r.success).toBe(true);
                    expect(Number(await readAmt(1))).toBe(1234);
                    expect(Number(await readAmt(2))).toBe(5678);
                } finally {
                    await db.closeConnection();
                }
            });

            test("without numberFormat the same value stays a decimal (1.234) — the knob is what flips it", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [{ id: 1, amt: "1.234" }]);
                    expect(r.success).toBe(true);
                    expect(Number(await readAmt(1))).toBeCloseTo(1.234, 3);
                } finally {
                    await db.closeConnection();
                }
            });
        });
    });
