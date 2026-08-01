import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live proof for staging-path per-row degradation WITH row-level history (decisions.md D-N / Option
// B). On the default (atomic) staging path, when a merge fails and the user opted in
// (rejectedRowsTable), autosql retries per-row and diverts unrecoverable rows — and must then
// COMPENSATE row-level history: insertHistory records a before-image for every changing row BEFORE
// the merge, so a diverted row (which never actually changed the real table) would otherwise leave a
// spurious before-image. The compensating delete removes exactly this run's before-images for the
// rejected rows (keyed on the engine-supplied dwh_as_at + PK), never touching a prior load's history.
//
// Scenario: pre-existing rows id=1 (val 10) and id=2 (val 20). A staged upsert sets id=1→15 (valid)
// and id=2→-5 (violates a CHECK). The atomic merge fails and rolls back → per-row: id=1 lands, id=2
// diverts. Expected end state: id=1 updated, id=2 unchanged, id=2 in rejectedRowsTable, and history
// holds id=1's before-image but NOT id=2's. The negative-control test disables the compensating
// delete and asserts id=2's spurious before-image survives — proving the delete is load-bearing.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`staging-path degradation + history (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "staging_degradation_test";
        const HISTORY = TABLE + "__history";
        const REJECTED = "staging_degradation_rejected";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const histRef = `${qi("test_schema")}.${qi(HISTORY)}`;
        const rejRef = `${qi("test_schema")}.${qi(REJECTED)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;

        // Default staging path (useStagingInsert left true); addTimestamps off keeps the table/history
        // to id/val; streamMaxRetries:1 makes the CHECK-violating row divert in one per-row round.
        const baseConfig = {
            ...config,
            schema: "test_schema",
            useWorkers: false,
            addTimestamps: false,
            addHistory: true,
            historyTables: [TABLE],
            streamMaxRetries: 1,
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
            const r = await admin.runQuery({ query: `SELECT ${qi("id")} AS id FROM ${histRef} ORDER BY ${qi("id")}`, params: [] });
            return (r.results ?? []).map((row: any) => Number(row.id));
        };

        const runDegradedLoad = async (db: Database) =>
            db.autoSQL(TABLE, [{ id: 1, val: 15 }, { id: 2, val: -5 }]);

        test("diverts the CHECK-violating row and compensates history (no spurious before-image)", async () => {
            const db = Database.create({ ...baseConfig, rejectedRowsTable: REJECTED });
            await db.establishConnection();
            try {
                const r = await runDegradedLoad(db);
                expect(r.success).toBe(true); // degraded, not a hard failure

                // Real table: id=1 updated (10→15), id=2 unchanged (its merge rolled back + diverted).
                expect(await valOf(1)).toBe(15);
                expect(await valOf(2)).toBe(20);

                // The diverted row is captured.
                expect(await rowCount(rejRef)).toBe(1);
                const rej = await admin.runQuery({ query: `SELECT ${qi("raw_data")} AS raw_data FROM ${rejRef}`, params: [] });
                const rawVal = Object.values(rej.results![0])[0];
                const parsed = typeof rawVal === "string" ? JSON.parse(rawVal) : rawVal;
                expect(Number(parsed.id)).toBe(2);

                // History holds the before-image of the row that actually changed (id=1) and NOT the
                // diverted row (id=2) — the compensation removed id=2's spurious before-image.
                expect(await historyIds()).toEqual([1]);
            } finally {
                await db.closeConnection();
            }
        });

        test("negative control: without the compensating delete, the diverted row's before-image survives", async () => {
            const db = Database.create({ ...baseConfig, rejectedRowsTable: REJECTED });
            await db.establishConnection();
            // Neutralise ONLY the compensating delete (return a harmless no-op), leaving everything
            // else identical — proving that delete is what removes id=2's spurious before-image.
            const delSpy = jest.spyOn(db, "getDeleteHistoryRowsQuery").mockReturnValue({ query: "SELECT 1", params: [] });
            try {
                const r = await runDegradedLoad(db);
                expect(r.success).toBe(true);

                // Same divert outcome...
                expect(await valOf(1)).toBe(15);
                expect(await valOf(2)).toBe(20);
                expect(await rowCount(rejRef)).toBe(1);
                expect(delSpy).toHaveBeenCalled(); // the compensation path DID run (just neutralised)

                // ...but now id=2's before-image is left behind — the bug the compensation prevents.
                expect(await historyIds()).toEqual([1, 2]);
            } finally {
                delSpy.mockRestore();
                await db.closeConnection();
            }
        });
    });
});
