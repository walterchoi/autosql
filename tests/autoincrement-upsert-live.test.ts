import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Regression guard: ingesting explicit values into a table whose primary key is a real
// AUTO_INCREMENT / SERIAL column must still upsert (not append). Introspection marks such a
// column autoIncrement:true, so excluding auto_increment columns from inserts unconditionally
// would drop the id, break the ON DUPLICATE/ON CONFLICT match, and duplicate rows. Exclusion is
// therefore gated on `surrogateKey`. surrogateKey is NOT set here (default off).

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`auto_increment PK upsert (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "autoinc_upsert_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            // addTimestamps off: the table is created manually without autosql's dwh_* columns,
            // so this isolates auto_increment-id upsert from the unrelated timestamp/staging path.
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false, addTimestamps: false });
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            const createSql = config.sqlDialect === "mysql"
                ? `CREATE TABLE ${ref} (${qi("id")} INT AUTO_INCREMENT PRIMARY KEY, ${qi("name")} VARCHAR(50))`
                : `CREATE TABLE ${ref} (${qi("id")} SERIAL PRIMARY KEY, ${qi("name")} VARCHAR(50))`;
            await db.runQuery({ query: createSql, params: [] });
        });

        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        const rowCount = async () => {
            const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            return Number(Object.values(r.results![0])[0]);
        };
        const nameOf = async (id: number) => {
            const r = await db.runQuery({ query: `SELECT ${qi("name")} AS n FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
            return r.results!.length ? String(Object.values(r.results![0])[0]) : null;
        };

        test("supplying explicit ids upserts existing rows instead of appending", async () => {
            const r1 = await db.autoSQL(TABLE, [{ id: 1, name: "a" }, { id: 2, name: "b" }]);
            expect(r1.success).toBe(true);
            expect(await rowCount()).toBe(2);

            const r2 = await db.autoSQL(TABLE, [{ id: 1, name: "A" }, { id: 2, name: "B" }]);
            expect(r2.success).toBe(true);
            expect(await rowCount()).toBe(2);        // upsert, not append
            expect(await nameOf(1)).toBe("A");       // existing row updated in place
            expect(await nameOf(2)).toBe("B");
        });
    });
});
