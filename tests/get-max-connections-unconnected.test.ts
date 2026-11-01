import { Database } from "../src/db/database";

// getMaxConnections' fallback chain for when `this.connection` is genuinely absent — the exact
// case the no-explicit-any work flagged as the reason these use a structural type instead of the
// driver's own (possibly overconfident) one. Every existing test calls this AFTER
// establishConnection(), so the "fields genuinely absent" branch was never exercised.

describe("getMaxConnections before establishConnection() — connection is null", () => {
    test("mysql falls back to 5", () => {
        const db = Database.create({ sqlDialect: "mysql", host: "h", user: "u", password: "p", database: "d" });
        expect(db.getMaxConnections()).toBe(5);
    });

    test("pgsql falls back to 5", () => {
        const db = Database.create({ sqlDialect: "pgsql", host: "h", user: "u", password: "p", database: "d" });
        expect(db.getMaxConnections()).toBe(5);
    });

    test("sqlserver falls back to config.connectionLimit when set, even with no connection", () => {
        const db = Database.create({ sqlDialect: "sqlserver", host: "h", user: "u", password: "p", database: "d", connectionLimit: 20 });
        expect(db.getMaxConnections()).toBe(20);
    });

    test("sqlserver falls back to 5 when neither connection nor connectionLimit is set", () => {
        const db = Database.create({ sqlDialect: "sqlserver", host: "h", user: "u", password: "p", database: "d" });
        expect(db.getMaxConnections()).toBe(5);
    });
});
