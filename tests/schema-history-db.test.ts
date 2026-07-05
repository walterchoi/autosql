import { DB_CONFIG, Database } from "./utils/testConfig";
import { bootstrapSchemaHistoryTable, recordMigrationStart, recordMigrationSuccess } from "../src/helpers/schemaHistory";
import { MetadataHeader } from "../src/config/types";

// Real-DB proof that recordMigrationStart returns a usable id. On MySQL the id came from a
// separate `SELECT LAST_INSERT_ID()` runQuery that ran on a *different* pooled connection
// (LAST_INSERT_ID is connection-scoped), so the id could be 0 and the history row was left
// stuck at 'pending' with no drift baseline. It now runs in one connection-pinned transaction.

const TABLE = "schema_history_id_test";
const PREV: MetadataHeader = { id: { type: "int" } };
const NEXT: MetadataHeader = { id: { type: "int" }, name: { type: "varchar", length: 20 } };

Object.values(DB_CONFIG).forEach((config) => {
    const isMysql = config.sqlDialect === "mysql";
    const schema = config.schema as string;
    const qi = (n: string) => (isMysql ? `\`${n}\`` : `"${n}"`);
    const hist = `${qi(schema)}.${qi("autosql_schema_history")}`;
    const ph = (i: number) => (isMysql ? "?" : `$${i}`);

    describe(`Schema-history record id for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        const cleanup = () =>
            db.runQuery({ query: `DELETE FROM ${hist} WHERE ${qi("table_name")} = ${ph(1)}`, params: [TABLE] }).catch(() => {});
        const statusOf = async (id: number) => {
            const r = await db.runQuery({ query: `SELECT ${qi("status")} FROM ${hist} WHERE ${qi("id")} = ${ph(1)}`, params: [id] });
            return r.results?.[0]?.status;
        };

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
            await bootstrapSchemaHistoryTable(db);
            await cleanup();
        });

        afterAll(async () => {
            await cleanup();
            await db.closeConnection();
        });

        test("returns a usable id and the row transitions pending -> applied", async () => {
            const id = await recordMigrationStart(db, TABLE, PREV, { note: "test" });
            expect(id).toBeGreaterThan(0);
            const rid = id!;
            expect(await statusOf(rid)).toBe("pending");

            await recordMigrationSuccess(db, rid, NEXT);
            expect(await statusOf(rid)).toBe("applied");
        });
    });
});
