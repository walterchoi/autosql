import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live proof for the ZERO-WINDOW staging-path degradation WITH row-level history (decisions.md D-N).
// On the default staging path, when a merge fails and the user opted in (rejectedRowsTable) with
// addHistory, autosql retries per primary key and — crucially — runs each PK's before-image capture
// and its merge in ONE transaction, so history and data commit or roll back together. A diverted row
// leaves neither a data change nor a history before-image; there is no window in which a spurious
// before-image could persist.
//
// Two tests:
//  1. Outcome (natural CHECK violation): id=1→valid lands with its before-image; id=2→CHECK-violating
//     diverts with NO before-image; real table and rejectedRowsTable are correct.
//  2. Discriminating control (the one that proves the transaction boundary): force the per-PK MERGE to
//     fail while the before-image query is untouched (would succeed on its own). If before-image and
//     merge share a transaction, the failing merge rolls the before-image back → history is empty. If
//     they were separate transactions (the regression this rebuild prevents), the before-image would
//     persist → history would be non-empty → this test fails.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`atomic staging degradation + history (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "staging_degradation_test";
        const HISTORY = TABLE + "__history";
        const REJECTED = "staging_degradation_rejected";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const histRef = `${qi("test_schema")}.${qi(HISTORY)}`;
        const rejRef = `${qi("test_schema")}.${qi(REJECTED)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;

        const baseConfig = {
            ...config,
            schema: "test_schema",
            useWorkers: false,
            addTimestamps: false,
            addHistory: true,
            historyTables: [TABLE],
            rejectedRowsTable: REJECTED,
        };

        let admin: Database;

        beforeAll(async () => {
            admin = Database.create(baseConfig);
            await admin.establishConnection();
        });
        afterAll(async () => {
            await dropAll();
            await admin.closeConnection();
        });
        beforeEach(async () => {
            await dropAll();
            await admin.runQuery({
                query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("val")} INT, CONSTRAINT ${qi("val_nonneg")} CHECK (${qi("val")} >= 0))`,
                params: [],
            });
            await admin.runQuery({ query: `INSERT INTO ${ref} (${qi("id")}, ${qi("val")}) VALUES (1, 10), (2, 20)`, params: [] });
        });

        async function dropAll() {
            for (const r of [tempRef, histRef, ref, rejRef]) {
                await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
            }
        }

        const valOf = async (id: number) => {
            const r = await admin.runQuery({ query: `SELECT ${qi("val")} AS v FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
            return r.results!.length ? Number(Object.values(r.results![0])[0]) : null;
        };
        const rowCount = async (tableRef: string) => {
            const r = await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM ${tableRef}`, params: [] });
            return Number(Object.values(r.results![0])[0]);
        };
        const historyIds = async () => {
            const r = await admin.runQuery({ query: `SELECT ${qi("id")} AS id, ${qi("val")} AS val FROM ${histRef} ORDER BY ${qi("id")}`, params: [] });
            return (r.results ?? []).map((row: any) => ({ id: Number(row.id), val: Number(row.val) }));
        };

        test("diverts the CHECK-violating row with no before-image; the good row lands with its before-image", async () => {
            const db = Database.create(baseConfig);
            await db.establishConnection();
            try {
                const r = await db.autoSQL(TABLE, [{ id: 1, val: 15 }, { id: 2, val: -5 }]);
                expect(r.success).toBe(true);

                // Real table: id=1 updated (10→15), id=2 unchanged (its merge rolled back + diverted).
                expect(await valOf(1)).toBe(15);
                expect(await valOf(2)).toBe(20);

                // The diverted row is captured.
                expect(await rowCount(rejRef)).toBe(1);

                // History holds ONLY the before-image of the row that actually changed (id=1, old val 10).
                // id=2 never merged, so it has no before-image — captured and rolled back atomically.
                expect(await historyIds()).toEqual([{ id: 1, val: 10 }]);
            } finally {
                await db.closeConnection();
            }
        });

        test("discriminating: a per-PK merge failure rolls back its before-image (proves single transaction)", async () => {
            const db = Database.create(baseConfig);
            await db.establishConnection();
            // Force EVERY staging merge to fail (whole-table attempt AND each per-PK merge), while the
            // before-image query is left untouched. A failing SELECT against a missing table aborts the
            // transaction it runs in.
            const failing = { query: `SELECT * FROM ${qi("test_schema")}.${qi("__no_such_table_xyz__")}`, params: [] as any[] };
            const mergeSpy = jest.spyOn(db, "getInsertFromStagingQuery").mockReturnValue(failing);
            try {
                // id=1 exists (val 10); the update to 15 differs, so the before-image WOULD insert a row
                // for id=1 if it ran in its own transaction.
                const r = await db.autoSQL(TABLE, [{ id: 1, val: 15 }]);
                expect(r.success).toBe(true); // degraded: the row diverted

                // The merge never applied, so id=1 is unchanged and diverted...
                expect(await valOf(1)).toBe(10);
                expect(await rowCount(rejRef)).toBe(1);

                // ...and because the before-image shared the failing merge's transaction, it rolled back
                // too: history is EMPTY. (Separate transactions would have left id=1's before-image here.)
                expect(await historyIds()).toEqual([]);
            } finally {
                mergeSpy.mockRestore();
                await db.closeConnection();
            }
        });
    });
});
