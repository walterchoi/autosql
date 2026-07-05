import {
    buildCreateStreamStagingTableQuery,
    buildInsertIntoStreamStagingQuery,
    buildMergeFromStreamQuery,
    buildDropStreamStagingTableQuery,
} from "../src/helpers/streamHelpers";
import { escapeIdentifier } from "../src/db/utils/escape";
import { DB_CONFIG, Database } from "./utils/testConfig";
import { DatabaseConfig, MetadataHeader, QueryInput } from "../src/config/types";

const sql = (qi: QueryInput): string => (typeof qi === "string" ? qi : qi.query);

// H1: the streaming builders previously interpolated caller JSON keys / table names with bare
// backticks/quotes and no quote-doubling — an identifier break-out injection reachable from the
// public db.openStream() -> write()/end() path.

describe("stream builders escape interpolated identifiers (unit / emitted SQL)", () => {
    (["mysql", "pgsql"] as const).forEach((dialect) => {
        const evil = dialect === "mysql"
            ? "a`, ADD COLUMN evil TEXT, ADD COLUMN `b"
            : 'a", ADD COLUMN evil TEXT, ADD COLUMN "b';
        const rawBreakout = dialect === "mysql" ? "`a`, ADD COLUMN evil" : '"a", ADD COLUMN evil';
        const config = { sqlDialect: dialect, schema: "s" } as DatabaseConfig;

        test(`${dialect}: create/insert/merge/drop escape a malicious column & table name`, () => {
            const esc = escapeIdentifier(evil, dialect);

            const create = sql(buildCreateStreamStagingTableQuery("stg", [evil], config));
            const insert = sql(buildInsertIntoStreamStagingQuery("stg", [evil], [{ [evil]: "x" }], config));
            const meta: MetadataHeader = { [evil]: { type: "varchar", length: 10 } };
            const merge = sql(buildMergeFromStreamQuery(evil, "stg", meta, "INSERT", config));
            const drop = sql(buildDropStreamStagingTableQuery(evil, config));

            for (const q of [create, insert, merge, drop]) {
                expect(q).toContain(esc);            // fully-escaped (quote doubled)
                expect(q).not.toContain(rawBreakout); // the raw break-out never appears
            }
        });
    });
});

describe("stream round-trip with a column name that requires escaping (live DB)", () => {
    Object.values(DB_CONFIG).forEach((config) => {
        const isMysql = config.sqlDialect === "mysql";
        const TABLE = "stream_escape_test";
        const weirdCol = isMysql ? "a`b" : 'a"b'; // contains the dialect quote char
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);
        const tbl = `${qi(config.schema as string)}.${qi(TABLE)}`;

        describe(`${config.sqlDialect}`, () => {
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

            test("streams a row whose column name contains a quote character", async () => {
                const handle = await db.openStream(TABLE);
                await handle.write([{ id: 1, [weirdCol]: "hello" }]);
                const res = await handle.end();
                expect(res.success).toBe(true); // old raw-interpolation code errors here

                const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${tbl}`, params: [] });
                expect(Number(Object.values(r.results![0])[0])).toBe(1);
            });
        });
    });
});
