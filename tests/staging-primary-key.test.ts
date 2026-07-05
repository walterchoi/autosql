import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";
import { AlterTableChanges, MetadataHeader, QueryInput } from "../src/config/types";

// Staging temp tables (CREATE TABLE AS SELECT — columns only, no keys) were run through the
// normal alter path, so with updatePrimaryKey:true a DROP/ADD PRIMARY KEY was emitted on a
// keyless table and errored. Since useStagingInsert defaults true, this hard-failed autoSQL on
// the first fresh table for anyone who set updatePrimaryKey:true. PK reconciliation is now
// skipped for staging tables (keyed off the staging-name prefix) — the real target is untouched.

const sql = (q: QueryInput): string => (typeof q === "string" ? q : q.query);
const changes = (): AlterTableChanges => ({
    addColumns: {}, modifyColumns: {}, dropColumns: [], renameColumns: [],
    nullableColumns: [], noLongerUnique: [], primaryKeyChanges: ["id"],
});
const PK_RECON = /DROP PRIMARY KEY|DROP CONSTRAINT|ADD PRIMARY KEY/i;

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`staging primary-key reconciliation for ${config.sqlDialect.toUpperCase()}`, () => {
        test("staging table skips PK reconciliation; real table keeps it (emitted SQL)", async () => {
            const db = Database.create({ ...config, updatePrimaryKey: true }) as any;
            const newHeaders: MetadataHeader = { id: { type: "int", primary: true } };

            const stagingSql = (await db.getAlterTableQuery("temp_staging__users", changes(), newHeaders)).map(sql).join(" | ");
            const realSql = (await db.getAlterTableQuery("users", changes(), newHeaders)).map(sql).join(" | ");

            expect(stagingSql).not.toMatch(PK_RECON);      // staging: no DROP/ADD primary key
            expect(realSql).toMatch(PK_RECON);              // real target: reconciliation untouched
        });

        test("live: autoSQL with updatePrimaryKey:true + default staging succeeds on a fresh table", async () => {
            const TABLE = "staging_pk_test";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const db = Database.create({ ...config, schema: "test_schema", updatePrimaryKey: true }); // staging is default-on
            await db.establishConnection();
            try {
                await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
                const res = await db.autoSQL(TABLE, [{ id: 100, name: "a", score: 9 }, { id: 200, name: "b", score: 8 }], undefined);
                expect(res.success).toBe(true); // pre-fix: "Can't DROP 'PRIMARY'" / constraint does not exist
                const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
                expect(Number(Object.values(r.results![0])[0])).toBe(2);
            } finally {
                await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
                await db.closeConnection();
            }
        });
    });
});
