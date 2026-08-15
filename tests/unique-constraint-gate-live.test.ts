import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// A10 live proof: autosql must not silently drop a UNIQUE constraint (incl. a user-defined one) just
// because a batch's data collides with it. Default (`dropUniqueConstraints` off) → the constraint is
// KEPT and the load fails loud on the collision; opt-in → the constraint is dropped and the load
// proceeds. The teeth: assert the constraint STILL EXISTS in the catalog after the flag-off load.

// Per-dialect: count NON-primary unique constraints on the table.
const UNIQUE_COUNT: Record<string, (s: string, t: string) => string> = {
    mysql: (s, t) => `SELECT COUNT(DISTINCT INDEX_NAME) c FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='${s}' AND TABLE_NAME='${t}' AND NON_UNIQUE=0 AND INDEX_NAME <> 'PRIMARY'`,
    pgsql: (s, t) => `SELECT COUNT(*) c FROM pg_constraint WHERE conrelid = '${s}.${t}'::regclass AND contype = 'u'`,
};

Object.values(DB_CONFIG)
    .filter((config) => UNIQUE_COUNT[config.sqlDialect])
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`unique-constraint auto-drop gate (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "unique_gate_test";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
            const base = { ...config, schema: "test_schema", useWorkers: false, addTimestamps: false };
            let admin: Database;

            beforeAll(async () => { admin = Database.create(base); await admin.establishConnection(); });
            afterAll(async () => { await dropAll(); await admin.closeConnection(); });
            async function dropAll() {
                for (const r of [tempRef, ref]) await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
            }
            const uniqueCount = async () => {
                const r = await admin.runQuery({ query: UNIQUE_COUNT[config.sqlDialect]("test_schema", TABLE), params: [] });
                return Number(Object.values(r.results![0])[0]);
            };
            beforeEach(async () => {
                await dropAll();
                await admin.runQuery({ query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("code")} VARCHAR(20), CONSTRAINT ${qi("uq_gate_code")} UNIQUE (${qi("code")}))`, params: [] });
                await admin.runQuery({ query: `INSERT INTO ${ref} (${qi("id")}, ${qi("code")}) VALUES (1, 'A')`, params: [] });
            });

            test("default (off): a colliding batch KEEPS the unique constraint and warns (no silent drop)", async () => {
                expect(await uniqueCount()).toBe(1);
                const warnings: string[] = [];
                const db = Database.create({ ...base, logger: { warn: (m: string) => warnings.push(m) } });
                await db.establishConnection();
                try {
                    // id=2 is a new PK but code='A' collides with the existing row on the UNIQUE.
                    await db.autoSQL(TABLE, [{ id: 2, code: "A" }]);
                    // TEETH: the constraint is still in the catalog — NOT silently dropped. (Whether the
                    // load then fails loud (Postgres: ON CONFLICT pk only) or upserts on the secondary
                    // unique (MySQL: ON DUPLICATE KEY) is the separate A11 cross-dialect divergence.)
                    expect(await uniqueCount()).toBe(1);
                    expect(warnings.some((w) => /dropUniqueConstraints is off/i.test(w))).toBe(true);
                } finally {
                    await db.closeConnection();
                }
            });

            test("opt-in (true): the constraint is dropped and the colliding row lands", async () => {
                const db = Database.create({ ...base, dropUniqueConstraints: true });
                await db.establishConnection();
                try {
                    const r = await db.autoSQL(TABLE, [{ id: 2, code: "A" }]);
                    expect(r.success).toBe(true);
                    expect(await uniqueCount()).toBe(0);           // constraint dropped (opted in)
                    const cnt = await admin.runQuery({ query: `SELECT COUNT(*) c FROM ${ref}`, params: [] });
                    expect(Number(Object.values(cnt.results![0])[0])).toBe(2);
                } finally {
                    await db.closeConnection();
                }
            });
        });
    });
