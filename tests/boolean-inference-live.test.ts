import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// R3 (live): a bare 0/1 now types as integer, not boolean. Two things must hold on a real DB:
//  1. LOSSLESS + CONVERGENT upgrade — a column that was created boolean (from true/false) and
//     later receives 0/1 converts boolean→int once, preserving values (true→1, false→0), and a
//     second identical ingest produces no further schema change.
//  2. The `booleanColumns` hint creates a real boolean column from 0/1 data that round-trips.

const INT_TYPES = ["tinyint", "smallint", "int", "integer", "bigint", "int2", "int4", "int8"];

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`R3 boolean inference (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        const columnType = async (table: string, column: string): Promise<string | null> => {
            const { currentMetaData } = await (db as any).autoSQLHandler.fetchTableMetadata(table);
            return (currentMetaData as Record<string, any>)?.[column]?.type ?? null;
        };

        const cleanup = async (table: string) => {
            const ref = `${qi("test_schema")}.${qi(table)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + table)}`;
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        };

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false });
            await db.establishConnection();
            await cleanup("bool_upgrade_test");
            await cleanup("bool_hint_test");
        });

        afterAll(async () => {
            await cleanup("bool_upgrade_test");
            await cleanup("bool_hint_test");
            await db.closeConnection();
        });

        test("boolean→int upgrade is lossless and convergent", async () => {
            const TABLE = "bool_upgrade_test";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;

            // v1: literal true/false → a genuine boolean column. id 12 is created but never
            // re-sent, so its value only changes via the column-wide conversion (not an upsert).
            expect((await db.autoSQL(TABLE, [
                { id: 10, active: true },
                { id: 11, active: false },
                { id: 12, active: true },
            ])).success).toBe(true);
            expect(await columnType(TABLE, "active")).toBe("boolean");

            // v2: same column now arrives as 0/1 → inferred int → boolean→int conversion.
            expect((await db.autoSQL(TABLE, [
                { id: 10, active: 1 },
                { id: 11, active: 0 },
            ])).success).toBe(true);

            const afterType = await columnType(TABLE, "active");
            expect(INT_TYPES).toContain(afterType); // no longer boolean

            const rows = await db.runQuery({
                query: `SELECT ${qi("id")} AS id, ${qi("active")} AS active FROM ${ref} ORDER BY ${qi("id")}`,
                params: [],
            });
            const byId: Record<number, any> = {};
            for (const r of rows.results!) byId[Number((r as any).id)] = (r as any).active;
            // Lossless: id 12 (true, never re-sent) became 1 via the column conversion.
            expect(Number(byId[12])).toBe(1);
            expect(Number(byId[10])).toBe(1);
            expect(Number(byId[11])).toBe(0);

            // Convergent: an identical 0/1 re-ingest does not re-alter the (now int) column.
            expect((await db.autoSQL(TABLE, [
                { id: 10, active: 1 },
                { id: 11, active: 0 },
            ])).success).toBe(true);
            expect(await columnType(TABLE, "active")).toBe(afterType);
        });

        test("booleanColumns hint creates a boolean column from 0/1 that round-trips", async () => {
            const TABLE = "bool_hint_test";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const hintDb = Database.create({
                ...config, schema: "test_schema", useWorkers: false, booleanColumns: ["flag"],
            });
            await hintDb.establishConnection();
            try {
                // Mix numeric and string flag forms — CSV/text sources deliver "true"/"false".
                expect((await hintDb.autoSQL(TABLE, [
                    { id: 1, flag: 1 },
                    { id: 2, flag: 0 },
                    { id: 3, flag: "true" },
                    { id: 4, flag: "false" },
                    { id: 5, flag: "TRUE" },
                ])).success).toBe(true);

                const { currentMetaData } = await (hintDb as any).autoSQLHandler.fetchTableMetadata(TABLE);
                expect((currentMetaData as any).flag.type).toBe("boolean");

                const rows = await hintDb.runQuery({
                    query: `SELECT ${qi("id")} AS id, ${qi("flag")} AS flag FROM ${ref} ORDER BY ${qi("id")}`,
                    params: [],
                });
                const byId: Record<number, any> = {};
                for (const r of rows.results!) byId[Number((r as any).id)] = (r as any).flag;
                // Truthiness normalises PG (true/false) and MySQL (1/0) back to the flag value.
                expect(Boolean(byId[1])).toBe(true);
                expect(Boolean(byId[2])).toBe(false);
                expect(Boolean(byId[3])).toBe(true);
                expect(Boolean(byId[4])).toBe(false);
                expect(Boolean(byId[5])).toBe(true);
            } finally {
                await hintDb.closeConnection();
            }
        });

        test("plain inference of literal true/false round-trips (no hint)", async () => {
            const TABLE = "bool_plain_test";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            await cleanup(TABLE);
            try {
                // No booleanColumns hint — string "true"/"false" infers boolean via the regex, and
                // the value must still store correctly (the MySQL TINYINT(1) rejection this guards).
                expect((await db.autoSQL(TABLE, [
                    { id: 1, active: "true" },
                    { id: 2, active: "false" },
                    { id: 3, active: "TRUE" },
                ])).success).toBe(true);
                expect(await columnType(TABLE, "active")).toBe("boolean");

                const rows = await db.runQuery({
                    query: `SELECT ${qi("id")} AS id, ${qi("active")} AS active FROM ${ref} ORDER BY ${qi("id")}`,
                    params: [],
                });
                const byId: Record<number, any> = {};
                for (const r of rows.results!) byId[Number((r as any).id)] = (r as any).active;
                expect(Boolean(byId[1])).toBe(true);
                expect(Boolean(byId[2])).toBe(false);
                expect(Boolean(byId[3])).toBe(true);
            } finally {
                await cleanup(TABLE);
            }
        });
    });
});
