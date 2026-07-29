import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// R8 (live, MySQL only): a pre-existing 3-byte utf8/utf8mb3 column rejects 4-byte characters (emoji)
// regardless of the pinned connection charset. The opt-in `upgradeCharset` converts such columns to
// utf8mb4 before insert. Verifies: the problem exists (baseline fails), the migration fixes it
// (emoji round-trips), it works with an INDEXED text column (the CONVERT key-length hazard), and it
// is convergent (a second ingest triggers no further conversion).

const mysqlConfig = Object.values(DB_CONFIG).find((c) => c.sqlDialect === "mysql");

const EMOJI = "party 🎉 中文 café";

(mysqlConfig ? describe : describe.skip)("R8 charset upgrade (live, MySQL)", () => {
    const qi = (n: string) => escapeIdentifier(n, "mysql");
    let db: Database;

    const ref = (t: string) => `${qi("test_schema")}.${qi(t)}`;

    // Create an externally-shaped utf8mb3 table with an INDEXED varchar (exercises the CONVERT
    // key-length path). autosql did not create this, so its columns are 3-byte.
    const createLegacyTable = async (table: string) => {
        await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref("temp_staging__" + table)}`, params: [] }).catch(() => {});
        await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref(table)}`, params: [] }).catch(() => {});
        await db.runQuery({
            query:
                `CREATE TABLE ${ref(table)} (` +
                `  ${qi("id")} INT PRIMARY KEY, ` +
                `  ${qi("name")} VARCHAR(100) CHARACTER SET utf8mb3, ` +
                `  INDEX ${qi("idx_name")} (${qi("name")})` +
                `) CHARACTER SET utf8mb3;`,
            params: [],
        });
    };

    const columnCharset = async (table: string, column: string): Promise<string | null> => {
        const r = await db.runQuery({
            query:
                `SELECT CHARACTER_SET_NAME AS cs FROM information_schema.COLUMNS ` +
                `WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
            params: ["test_schema", table, column],
        });
        return (r.results?.[0] as any)?.cs ?? null;
    };

    beforeAll(async () => {
        db = Database.create({ ...mysqlConfig!, schema: "test_schema", useWorkers: false });
        await db.establishConnection();
    });

    afterAll(async () => {
        for (const t of ["charset_baseline_test", "charset_upgrade_test"]) {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref("temp_staging__" + t)}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref(t)}`, params: [] }).catch(() => {});
        }
        await db.closeConnection();
    });

    test("baseline: without the flag, a utf8mb3 column rejects 4-byte characters", async () => {
        const TABLE = "charset_baseline_test";
        await createLegacyTable(TABLE);
        // No upgradeCharset — the column stays utf8mb3, so the emoji insert fails at the wire.
        const res = await db.autoSQL(TABLE, [{ id: 1, name: EMOJI }]);
        expect(res.success).toBe(false);
        expect(await columnCharset(TABLE, "name")).toBe("utf8mb3"); // unchanged
    });

    test("with upgradeCharset: the column is migrated and emoji round-trips; convergent on re-ingest", async () => {
        const TABLE = "charset_upgrade_test";
        await createLegacyTable(TABLE);
        expect(await columnCharset(TABLE, "name")).toBe("utf8mb3");

        const upgradeDb = Database.create({ ...mysqlConfig!, schema: "test_schema", useWorkers: false, upgradeCharset: true });
        await upgradeDb.establishConnection();
        try {
            const res = await upgradeDb.autoSQL(TABLE, [{ id: 1, name: EMOJI }]);
            expect(res.success).toBe(true);

            // The indexed varchar column is now utf8mb4 (CONVERT succeeded despite the index).
            expect(await columnCharset(TABLE, "name")).toBe("utf8mb4");

            const r = await db.runQuery({ query: `SELECT ${qi("name")} AS name FROM ${ref(TABLE)} WHERE ${qi("id")} = 1`, params: [] });
            expect((r.results?.[0] as any)?.name).toBe(EMOJI); // 4-byte characters stored intact

            // Convergent: the column already matches, so no further CONVERT is generated.
            expect(await (upgradeDb as any).getCharsetUpgradeQueries(TABLE)).toEqual([]);

            // And a second ingest still succeeds (idempotent).
            expect((await upgradeDb.autoSQL(TABLE, [{ id: 2, name: "second 🚀" }])).success).toBe(true);
            const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(TABLE)}`, params: [] });
            expect(Number(Object.values(c.results![0])[0])).toBe(2);
        } finally {
            await upgradeDb.closeConnection();
        }
    });
});
