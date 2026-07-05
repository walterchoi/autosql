import { Database } from "../src/db/database";

// runWithSchema replaces the old updateSchema(this.config.schema) mutation. Concurrent
// schema-scoped operations on one Database instance must each see their own schema across
// await points, without corrupting the shared instance config.

describe("per-operation schema context", () => {
    const makeDb = () =>
        Database.create({ sqlDialect: "mysql", host: "h", user: "u", password: "p", database: "d", schema: "base" }) as any;

    test("getConfig reflects the configured schema outside any context", () => {
        const db = makeDb();
        expect(db.getConfig().schema).toBe("base");
    });

    test("concurrent contexts with different schemas stay isolated across awaits", async () => {
        const db = makeDb();
        const results = await Promise.all([
            db.runWithSchema("schema_a", async () => {
                await new Promise((r) => setTimeout(r, 20));
                return db.getConfig().schema;
            }),
            db.runWithSchema("schema_b", async () => {
                await new Promise((r) => setTimeout(r, 5));
                return db.getConfig().schema;
            }),
        ]);
        expect(results).toEqual(["schema_a", "schema_b"]);
    });

    test("the shared instance config is never mutated by a scoped operation", async () => {
        const db = makeDb();
        await db.runWithSchema("other", async () => {
            expect(db.getConfig().schema).toBe("other");
        });
        expect(db.getConfig().schema).toBe("base");
    });

    test("a falsy schema runs against the configured schema", async () => {
        const db = makeDb();
        const seen = await db.runWithSchema(undefined, async () => db.getConfig().schema);
        expect(seen).toBe("base");
    });
});
