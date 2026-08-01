import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Regression guard: the non-streaming DIRECT path (useStagingInsert:false) must honour
// `config.insertType`. It previously ignored it for the bulk batch (autoInsertData defaulted
// straight to "UPDATE"), so `insertType: "INSERT"` silently upserted a duplicate primary key instead
// of failing — while the per-row fallback DID honour it (an inconsistency found during the item-1
// direct-path degradation work). Now a duplicate PK under "INSERT" genuinely errors; the default
// "UPDATE" still upserts.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`direct-path insertType honouring (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "direct_inserttype_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;

        const baseConfig = {
            ...config,
            schema: "test_schema",
            useWorkers: false,
            addTimestamps: false,
            useStagingInsert: false,
            primaryKey: ["id"],
        };

        let admin: Database;
        beforeAll(async () => {
            admin = Database.create(baseConfig);
            await admin.establishConnection();
        });
        afterAll(async () => {
            await admin.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await admin.closeConnection();
        });
        beforeEach(async () => {
            await admin.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        });

        const nameOf = async (id: number) => {
            const r = await admin.runQuery({ query: `SELECT ${qi("name")} AS n FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
            return r.results!.length ? String(Object.values(r.results![0])[0]) : null;
        };

        test('insertType:"INSERT" makes a duplicate primary key fail (not silently upsert)', async () => {
            const db = Database.create({ ...baseConfig, insertType: "INSERT" });
            await db.establishConnection();
            try {
                expect((await db.autoSQL(TABLE, [{ id: 1, name: "a" }])).success).toBe(true);

                // Same PK again under INSERT → the batch errors and fails loud (no rejectedRowsTable),
                // and the existing row keeps its original value (it was NOT upserted).
                const r = await db.autoSQL(TABLE, [{ id: 1, name: "b" }]);
                expect(r.success).toBe(false);
                expect(await nameOf(1)).toBe("a");
            } finally {
                await db.closeConnection();
            }
        });

        test('the default insertType:"UPDATE" still upserts a duplicate primary key', async () => {
            const db = Database.create({ ...baseConfig, insertType: "UPDATE" });
            await db.establishConnection();
            try {
                expect((await db.autoSQL(TABLE, [{ id: 1, name: "a" }])).success).toBe(true);

                const r = await db.autoSQL(TABLE, [{ id: 1, name: "b" }]);
                expect(r.success).toBe(true);
                expect(await nameOf(1)).toBe("b"); // upserted
            } finally {
                await db.closeConnection();
            }
        });
    });
});
