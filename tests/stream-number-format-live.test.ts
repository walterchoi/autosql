import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live proof that separators reach the STREAMING path — now that the per-row fallback sqlizes.
// A stream stores via the DB CAST bulk merge (which rejects a grouped "1,234"/"1.234") and then
// degrades to the per-row fallback, which now normalizes the value. Covers numberFormat, dataset
// consensus, AND the drift-sensitive transforms (decimal half-up rounding, datetime) through per-row.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect === "pgsql" || config.sqlDialect === "mysql")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);
        const asText = config.sqlDialect === "pgsql"
            ? (c: string) => `to_char(${c}, 'YYYY-MM-DD HH24:MI:SS')`
            : (c: string) => `DATE_FORMAT(${c}, '%Y-%m-%d %H:%i:%s')`;

        describe(`streaming number-format via per-row (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "stream_nf_test";
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

            const read = async (col: string, id: number) => {
                const r = await admin.runQuery({ query: `SELECT ${qi(col)} AS v FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
                return r.results!.length ? Object.values(r.results![0])[0] : null;
            };
            const dtText = async (id: number) => {
                const r = await admin.runQuery({ query: `SELECT ${asText(qi("dt"))} AS s FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
                return r.results!.length ? String(Object.values(r.results![0])[0]) : null;
            };

            test("numberFormat:'EU' — a stream normalizes number, decimal (rounded), and datetime via per-row", async () => {
                const db = Database.create({ ...baseConfig, numberFormat: "EU", decimalMaxLength: 2 });
                await db.establishConnection();
                try {
                    const handle = await db.openStream(TABLE, "test_schema", ["id"]);
                    await handle.write([
                        { id: 1, amt: "1.234", price: "1.234,567", dt: "2024-01-15 12:00:00" }, // EU: 1234 / 1234.567 / dt
                        { id: 2, amt: "5.678", price: "2.500,5", dt: "2024-06-30 23:59:59" },
                    ]);
                    const r = await handle.end();
                    expect(r.success).toBe(true);
                    expect(Number(await read("amt", 1))).toBe(1234);           // grouped integer
                    expect(Number(await read("price", 1))).toBeCloseTo(1234.57, 2); // EU decimal, half-up to cap 2
                    expect(await dtText(1)).toBe("2024-01-15 12:00:00");       // datetime normalized, wall-clock preserved
                } finally {
                    await db.closeConnection();
                }
            });

            test("consensus — a decisive sibling resolves an ambiguous stream value ('1,234' -> 1234)", async () => {
                const db = Database.create(baseConfig); // no numberFormat — consensus infers from the staged rows
                await db.establishConnection();
                try {
                    const handle = await db.openStream(TABLE, "test_schema", ["id"]);
                    await handle.write([
                        { id: 1, amt: "1,234", total: "1,234,567" }, // total is decisive US
                        { id: 2, amt: "5,678", total: "2,345,678" },
                    ]);
                    const r = await handle.end();
                    expect(r.success).toBe(true);
                    expect(Number(await read("amt", 1))).toBe(1234);
                    expect(Number(await read("total", 1))).toBe(1234567);
                } finally {
                    await db.closeConnection();
                }
            });
        });
    });
