import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live proof that the per-row degradation fallback NORMALIZES values (sqlize), like the bulk path,
// instead of binding them raw. A CHECK violation forces the batch to degrade to per-row; a good row
// in the same batch carries a locale-formatted number. Before the fix it was bound raw ("1.234")
// and rejected by an INT column; now it is sqlized (numberFormat EU -> 1234) and lands.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect === "pgsql" || config.sqlDialect === "mysql")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`per-row degradation normalizes values (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "perrow_norm_test";
            const REJECTED = "perrow_norm_rejected";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const rejRef = `${qi("test_schema")}.${qi(REJECTED)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
            const baseConfig = {
                ...config, schema: "test_schema", useWorkers: false, addTimestamps: false,
                useStagingInsert: false, streamMaxRetries: 1,
            };
            let admin: Database;

            beforeAll(async () => { admin = Database.create(baseConfig); await admin.establishConnection(); });
            afterAll(async () => { await dropAll(); await admin.closeConnection(); });
            async function dropAll() {
                for (const r of [tempRef, ref, rejRef]) {
                    await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
                }
            }
            beforeEach(async () => {
                await dropAll();
                // amount has a CHECK so a bad row fails the whole batch insert (inference can't "fix" it),
                // forcing the per-row degradation path. val is a plain INT that must receive 1234.
                await admin.runQuery({
                    query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("amount")} INT, ${qi("val")} INT, CONSTRAINT ${qi("amt_nonneg")} CHECK (${qi("amount")} >= 0))`,
                    params: [],
                });
            });

            const valOf = async (id: number) => {
                const r = await admin.runQuery({ query: `SELECT ${qi("val")} AS v FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
                return r.results!.length ? Object.values(r.results![0])[0] : null;
            };
            const rowCount = async (t: string) => {
                const r = await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM ${t}`, params: [] });
                return Number(Object.values(r.results![0])[0]);
            };

            test("a good row's locale number is sqlized on the per-row fallback (EU '1.234' -> 1234)", async () => {
                const db = Database.create({ ...baseConfig, numberFormat: "EU", rejectedRowsTable: REJECTED });
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [
                        { id: 2, amount: -5, val: "9.999" }, // violates CHECK -> diverted
                        { id: 3, amount: 20, val: "1.234" }, // good -> lands via per-row, EU 1.234 = 1234
                    ]);
                    expect(r.success).toBe(true);       // degraded gracefully
                    expect(Number(await valOf(3))).toBe(1234); // sqlized on the per-row path, not bound raw
                    expect(await valOf(2)).toBeNull();  // bad row not stored
                    expect(await rowCount(rejRef)).toBe(1); // bad row diverted
                } finally {
                    await db.closeConnection();
                }
            });
        });
    });
