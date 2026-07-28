import { DB_CONFIG, Database } from "./utils/testConfig";

// R5: the driver connection-pool size was hardcoded to 5 (MySQL connectionLimit / Postgres max),
// which bottlenecks parallel/worker loads. It is now configurable via `connectionLimit` (default 5).
// The pool is created lazily, so this does not require a live query.

Object.values(DB_CONFIG).forEach((config) => {
    describe(`configurable connection pool (${config.sqlDialect.toUpperCase()}) — R5`, () => {
        test("honours a custom connectionLimit", async () => {
            const db = Database.create({ ...config, connectionLimit: 12 });
            await db.establishConnection();
            try {
                expect(db.getMaxConnections()).toBe(12);
            } finally {
                await db.closeConnection();
            }
        });

        test("defaults to 5 when not set", async () => {
            const db = Database.create({ ...config });
            await db.establishConnection();
            try {
                expect(db.getMaxConnections()).toBe(5);
            } finally {
                await db.closeConnection();
            }
        });
    });
});
