import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// End-to-end verification of the reported pain: 4-byte emoji and non-ASCII scripts must
// round-trip through insert + read-back intact (this exercises the pinned connection charset),
// and with `sanitizeInvalidChars` a value containing bytes the DB cannot store (NUL, lone
// surrogate) must insert cleaned instead of throwing. Requires `npm run db:up`.

const NUL = String.fromCharCode(0);
// id is 100 (outside the boolean 0/1 range) so it is inferred as an integer, not boolean.
const ID = 100;
const EMOJI_ROW = { id: ID, note: "Hello 😀 世界 café Ñoño Привет 🚀" };

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`encoding round-trip (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;
        const T_EMOJI = "encoding_emoji_test";
        const T_SANITIZE = "encoding_sanitize_test";
        const refEmoji = `${qi("test_schema")}.${qi(T_EMOJI)}`;
        const refSan = `${qi("test_schema")}.${qi(T_SANITIZE)}`;

        beforeAll(async () => {
            db = Database.create({ ...config, schema: "test_schema", useWorkers: false });
            await db.establishConnection();
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${refEmoji}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${refSan}`, params: [] }).catch(() => {});
        });

        afterAll(async () => {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${refEmoji}`, params: [] }).catch(() => {});
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${refSan}`, params: [] }).catch(() => {});
            await db.closeConnection();
        });

        const readNote = async (ref: string) => {
            const r = await db.runQuery({ query: `SELECT ${qi("note")} AS n FROM ${ref} WHERE ${qi("id")} = ${ID}`, params: [] });
            return String(Object.values(r.results![0])[0]);
        };

        test("emoji and non-ASCII scripts round-trip intact", async () => {
            const res = await db.autoSQL(T_EMOJI, [EMOJI_ROW]);
            expect(res.success).toBe(true); // pre-fix MySQL: "Incorrect string value: '\\xF0\\x9F...'"
            expect(await readNote(refEmoji)).toBe(EMOJI_ROW.note);
        });

        test("sanitizeInvalidChars lets a NUL-containing value insert (cleaned)", async () => {
            const sdb = Database.create({ ...config, schema: "test_schema", useWorkers: false, sanitizeInvalidChars: true });
            await sdb.establishConnection();
            try {
                const res = await sdb.autoSQL(T_SANITIZE, [{ id: ID, note: `clean${NUL}ed \uD800end` }]);
                expect(res.success).toBe(true); // pre-fix Postgres: "invalid byte sequence" / NUL error
                const stored = await readNote(refSan);
                expect(stored).not.toContain(NUL);   // NUL stripped
                expect(stored.startsWith("cleaned ")).toBe(true);
            } finally {
                await sdb.closeConnection();
            }
        });
    });
});
