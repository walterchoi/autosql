import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Regression: a small-integer column must round-trip through the streaming merge on both dialects.
// Postgres has no tinyint/mediumint, so the stream-merge cast maps them to smallint/integer; a
// missing mapping left a tinyint column uncast and the text→smallint INSERT was rejected. This was
// masked while a bare 0/1 inferred as boolean; now that 0/1 is a small integer (R3), the merge must
// cast it. See streamHelpers `pgCast`.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`stream small-integer round-trip (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "stream_small_int_test";
        const ref = `${qi(config.schema as string)}.${qi(TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
            await db.runQuery(db.dropTableQuery(TABLE)).catch(() => {});
        });
        afterAll(async () => {
            await db.runQuery(db.dropTableQuery(TABLE)).catch(() => {});
            await db.closeConnection();
        });

        test("small integers (including 0/1) stream and merge without a cast error", async () => {
            const handle = await db.openStream(TABLE);
            await handle.write([
                { id: 1, qty: 0, note: "a" },
                { id: 2, qty: 1, note: "b" },
                { id: 3, qty: 200, note: "c" },
            ]);
            const res = await handle.end();
            expect(res.success).toBe(true);

            const rows = await db.runQuery({
                query: `SELECT ${qi("id")} AS id, ${qi("qty")} AS qty FROM ${ref} ORDER BY ${qi("id")}`,
                params: [],
            });
            const byId: Record<number, number> = {};
            for (const r of rows.results!) byId[Number((r as any).id)] = Number((r as any).qty);
            // Values must be preserved as integers (not collapsed to 0/1 as a boolean would).
            expect(byId[1]).toBe(0);
            expect(byId[2]).toBe(1);
            expect(byId[3]).toBe(200);
        });
    });
});
