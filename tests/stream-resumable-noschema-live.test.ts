import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";
import { buildResumableStagingTableName } from "../src/helpers/streamHelpers";

// Adopt must work when no explicit `schema` is configured. The staging table then lands in the
// dialect's default schema (Postgres `public`), while the information_schema lookup that decides
// `adopted` falls back to `config.database` — which never matches on Postgres. If that mismatch
// exists, `adopted` is permanently false: the caller resets its resume point and re-sends pages it
// already staged, and because CREATE TABLE IF NOT EXISTS no-ops, staging silently accumulates
// duplicates.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect !== "sqlserver")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`resumable staging without an explicit schema (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "stream_resumable_noschema_test";
            const SESSION = "conn-noschema";
            const stagingName = buildResumableStagingTableName(TABLE, "autosql_stream__", SESSION);

            const makeDb = () => Database.create({ ...config, useWorkers: false }); // no `schema`
            let db: Database;

            const cleanup = async () => {
                for (const n of [stagingName, "temp_staging__" + TABLE, TABLE]) {
                    await db.runQuery({ query: `DROP TABLE IF EXISTS ${qi(n)}`, params: [] }).catch(() => {});
                }
            };

            beforeAll(async () => { db = makeDb(); await db.establishConnection(); await cleanup(); });
            afterAll(async () => { await cleanup(); await db.closeConnection(); });

            test("a second open adopts the suspended table instead of silently restarting", async () => {
                const first: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                expect(first.adopted).toBe(false);
                await first.write([{ id: 1, name: "a" }]);
                await first.suspend();

                const second: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                expect(second.adopted).toBe(true);
                await second.abort();
            });
        });
    });
