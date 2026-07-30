import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live guarantee: number-like fields fed as STRINGS keep their exact representation end to end — the
// leading zero does not "fall off" and a > bigint value is not rounded. Guards the real-world contract
// (and the native-number inference fast path, which must never touch string inputs) against future
// changes. Fields are supplied as strings on purpose; `id` is the integer key.

const ROWS = [
    { id: 1, zip: "07030", account: "00012345", phone: "0412345678", big_id: "12345678901234567890" },
    { id: 2, zip: "02139", account: "00999999", phone: "0400000000", big_id: "99999999999999999999" },
    { id: 3, zip: "10001", account: "01234567", phone: "0411111111", big_id: "10000000000000000001" },
];

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`number-like fidelity round-trip (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "numlike_fidelity_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        let db: Database;

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false });
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        });
        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        test("string number-like columns are typed as text and round-trip exactly (leading zeros intact)", async () => {
            expect((await db.autoSQL(TABLE, ROWS)).success).toBe(true);

            // The number-like columns must be text, not a numeric type.
            const { currentMetaData } = await (db as any).autoSQLHandler.fetchTableMetadata(TABLE);
            const textTypes = ["varchar", "text", "mediumtext", "longtext"];
            for (const col of ["zip", "account", "phone", "big_id"]) {
                expect(textTypes).toContain((currentMetaData as any)[col].type);
            }

            const r = await db.runQuery({
                query: `SELECT ${qi("zip")} AS zip, ${qi("account")} AS account, ${qi("phone")} AS phone, ${qi("big_id")} AS big_id FROM ${ref} WHERE ${qi("id")} = 1`,
                params: [],
            });
            const row = r.results![0] as any;
            expect(row.zip).toBe("07030");                     // leading zero preserved
            expect(row.account).toBe("00012345");              // leading zeros preserved
            expect(row.phone).toBe("0412345678");              // leading zero preserved
            expect(row.big_id).toBe("12345678901234567890");   // 20 digits, not rounded
        });
    });
});
