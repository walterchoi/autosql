import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Real-world risk: a BIGINT id near int64's range exceeds JS's safe-integer limit
// (Number.MAX_SAFE_INTEGER = 2^53-1). Without mysql2's supportBigNumbers, the driver handed back a
// lossy JS Number for any such value — 9223372036854775807 AND 9223372036854775806 both silently
// become 9223372036854776000, which doesn't just corrupt the stored value, it collapses two
// genuinely distinct primary keys onto the same (wrong) number. pg/mssql's drivers already return
// these as exact strings by default; this proves mysql now matches them.

// A real, confirmed collision under lossy float64 rounding — two DISTINCT real int64 values that
// both round to 9223372036854776000 as a JS Number.
const MAX_INT64 = "9223372036854775807";
const MAX_INT64_MINUS_1 = "9223372036854775806";

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`BIGINT precision (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "bigint_precision_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false });
            await db.establishConnection();
        });
        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        test("two distinct near-int64-max values round-trip exactly through a raw BIGINT column, not collapsed onto the same rounded number", async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.runQuery({
                query: `CREATE TABLE ${ref} (${qi("id")} BIGINT PRIMARY KEY, ${qi("label")} VARCHAR(20))`,
                params: [],
            });
            const placeholders = config.sqlDialect === "mysql" ? ["?", "?"] : ["$1", "$2"];
            await db.runQuery({
                query: `INSERT INTO ${ref} (${qi("id")}, ${qi("label")}) VALUES (${placeholders[0]}, 'a')`,
                params: [MAX_INT64],
            });
            await db.runQuery({
                query: `INSERT INTO ${ref} (${qi("id")}, ${qi("label")}) VALUES (${placeholders[0]}, 'b')`,
                params: [MAX_INT64_MINUS_1],
            });

            const r = await db.runQuery({ query: `SELECT ${qi("id")} AS id, ${qi("label")} AS label FROM ${ref} ORDER BY ${qi("label")}`, params: [] });
            expect(r.success).toBe(true);
            expect(r.results).toHaveLength(2); // NOT collapsed into one row by a PK collision

            const ids = r.results!.map((row: any) => String(row.id));
            expect(ids).toEqual([MAX_INT64, MAX_INT64_MINUS_1]); // each exact, not rounded to 9223372036854776000
        });

        test("autoSQL's own insert path infers bigint for a real int64-range value and round-trips it exactly", async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`, params: [] }).catch(() => {});

            const res = await db.autoSQL(TABLE, [
                { id: MAX_INT64, name: "a" },
                { id: MAX_INT64_MINUS_1, name: "b" },
            ], undefined, ["id"]);
            expect(res.success).toBe(true);

            const r = await db.runQuery({ query: `SELECT ${qi("id")} AS id FROM ${ref} ORDER BY ${qi("id")} DESC`, params: [] });
            expect(r.results).toHaveLength(2); // both rows landed — no PK-collision overwrite
            expect(r.results!.map((row: any) => String(row.id))).toEqual([MAX_INT64, MAX_INT64_MINUS_1]);
        });

        test("a small bigint value is unaffected (still round-trips correctly)", async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.runQuery({
                query: `CREATE TABLE ${ref} (${qi("id")} BIGINT PRIMARY KEY)`,
                params: [],
            });
            const placeholder = config.sqlDialect === "mysql" ? "?" : "$1";
            await db.runQuery({ query: `INSERT INTO ${ref} (${qi("id")}) VALUES (${placeholder})`, params: ["42"] });
            const r = await db.runQuery({ query: `SELECT ${qi("id")} AS id FROM ${ref}`, params: [] });
            expect(String(r.results![0].id)).toBe("42");
        });
    });
});
