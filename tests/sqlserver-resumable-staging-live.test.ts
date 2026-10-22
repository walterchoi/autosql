import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";
import { buildResumableStagingTableName } from "../src/helpers/streamHelpers";

// Resumable (session-keyed) stream staging on SQL Server.
//
// The cross-dialect resumable tests iterate DB_CONFIG, which carries no sqlserver entry, so their
// `!== "sqlserver"` filter is a no-op and SQL Server gets no coverage from them. SQL Server reached
// full streaming parity in 2.5.0, so the resumable path needs its own live test with the hardcoded
// config the other SS live suites use.
//
// The interesting part here is `adopted`: it is decided by an information_schema lookup whose schema
// falls back to `config.database` when no schema is set. On SQL Server that would be `master` rather
// than `dbo`, so this pins the behaviour down on the dialect where the fallback is most obviously
// wrong.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");
const ref = (t: string) => `${qi("test_schema")}.${qi(t)}`;

describe("SQL Server resumable stream staging (live)", () => {
    const TABLE = "ss_stream_resumable";
    const SESSION = "ss-conn-7";
    const stagingName = buildResumableStagingTableName(TABLE, "autosql_stream__", SESSION);

    let db: Database;
    const drop = async (t: string) => { await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref(t)}`, params: [] }).catch(() => {}); };

    beforeAll(async () => {
        db = Database.create(CONFIG);
        await db.establishConnection();
        await db.runQuery(db.getCreateSchemaQuery("test_schema"));
    });
    beforeEach(async () => { for (const t of [stagingName, "temp_staging__" + TABLE, TABLE]) await drop(t); });
    afterAll(async () => {
        for (const t of [stagingName, "temp_staging__" + TABLE, TABLE]) await drop(t);
        await db.closeConnection();
    });

    test("a separate handler adopts a suspended session's rows and merges all of them", async () => {
        const first: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
        expect(first.resumable).toBe(true);
        expect(first.adopted).toBe(false);
        await first.write([{ id: 1, name: "a" }]);
        await first.write([{ id: 2, name: "b" }]);
        await first.suspend();

        // Survived the run that created it.
        const staged = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(stagingName)}`, params: [] });
        expect(Number(Object.values(staged.results![0])[0])).toBe(2);

        // A genuinely separate connection + handler, as a later invocation would be.
        const db2 = Database.create(CONFIG);
        await db2.establishConnection();
        try {
            const second: any = await db2.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
            expect(second.adopted).toBe(true);
            await second.write([{ id: 3, name: "c" }]);
            expect((await second.end()).success).toBe(true);
        } finally {
            await db2.closeConnection();
        }

        const rows = await db.runQuery({ query: `SELECT id FROM ${ref(TABLE)} ORDER BY id`, params: [] });
        expect(rows.results!.map((r: any) => Number(r.id))).toEqual([1, 2, 3]);
    });

    test("a keyless stream on the same target does not destroy the resumable table", async () => {
        const keyed: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
        await keyed.write([{ id: 10, name: "keep" }]);
        await keyed.suspend();

        const plain: any = await db.openStream(TABLE); // runs orphan cleanup on the same target
        await plain.abort();

        const still = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(stagingName)}`, params: [] });
        expect(still.success).toBe(true);
        expect(Number(Object.values(still.results![0])[0])).toBe(1);
    });

    test("a drifted source widens the adopted staging table (T-SQL ADD takes no COLUMN keyword)", async () => {
        const first: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
        await first.write([{ id: 1, name: "a" }]);
        await first.suspend();

        const second: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
        expect(second.adopted).toBe(true);
        await second.write([{ id: 2, name: "b", extra: "new" }]);
        expect(second.widenedColumns).toEqual(["extra"]);
        expect((await second.end()).success).toBe(true);

        const rows = await db.runQuery({ query: `SELECT id, extra FROM ${ref(TABLE)} ORDER BY id`, params: [] });
        expect(rows.results!.map((r: any) => Number(r.id))).toEqual([1, 2]);
        expect(rows.results![0].extra).toBeNull();
        expect(rows.results![1].extra).toBe("new");
    });
});
