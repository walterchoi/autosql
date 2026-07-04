import { DB_CONFIG, Database } from "./utils/testConfig";
import { MetadataHeader } from "../src/config/types";

// Real-DB proof that runTransaction is atomic. These tests fail against the old
// implementation (BEGIN/DML/COMMIT ran on different pooled connections, so a "rolled back"
// statement had actually auto-committed on its own connection).

const TABLE = "txn_atomicity_test";
const META: MetadataHeader = {
    id: { type: "int", length: 11, primary: true, allowNull: false },
    name: { type: "varchar", length: 50, allowNull: false },
};

Object.values(DB_CONFIG).forEach((config) => {
    const isMysql = config.sqlDialect === "mysql";
    const schema = config.schema as string;
    const qi = (n: string) => (isMysql ? `\`${n}\`` : `"${n}"`);
    const tbl = `${qi(schema)}.${qi(TABLE)}`;
    const ph = (i: number) => (isMysql ? "?" : `$${i}`);
    const insertRow = (id: number, name: string) => ({
        query: `INSERT INTO ${tbl} (${qi("id")}, ${qi("name")}) VALUES (${ph(1)}, ${ph(2)})`,
        params: [id, name],
    });

    describe(`Transaction atomicity for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        const rowCount = async (): Promise<number> => {
            const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${tbl}`, params: [] });
            return Number(Object.values(r.results![0])[0]);
        };

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
            await db.runQuery(db.dropTableQuery(TABLE)).catch(() => {});
            await db.runTransaction(db.createTableQuery(TABLE, META));
        });

        afterAll(async () => {
            await db.runQuery(db.dropTableQuery(TABLE)).catch(() => {});
            await db.closeConnection();
        });

        beforeEach(async () => {
            await db.runQuery({ query: `DELETE FROM ${tbl}`, params: [] });
        });

        test("a committed transaction persists its rows", async () => {
            const result = await db.runTransaction([insertRow(2, "carol")]);
            expect(result.success).toBe(true);
            expect(await rowCount()).toBe(1);
        });

        test("a failing statement rolls back the whole transaction", async () => {
            // Second insert violates the primary key; the first insert must NOT survive.
            const result = await db.runTransaction([insertRow(1, "alice"), insertRow(1, "bob")]);
            expect(result.success).toBe(false);
            expect(await rowCount()).toBe(0);
        });

        if (!isMysql) {
            test("PostgreSQL: a failing ALTER rolls back a prior ALTER in the same transaction", async () => {
                const addCol = { query: `ALTER TABLE ${tbl} ADD COLUMN col_new int`, params: [] };
                const result = await db.runTransaction([addCol, addCol]); // duplicate column → error
                expect(result.success).toBe(false);
                const check = await db.runQuery({
                    query: `SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = 'col_new'`,
                    params: [schema, TABLE],
                });
                expect(check.results!.length).toBe(0);
            });
        }
    });
});
