import { Database } from "../src/db/database";

// T1 — cross-dialect conformance matrix. Same input, asserted against each dialect's emitted SQL, so a
// behavioural divergence (an A11/A12-class bug) fails here before a user migrating between destinations
// hits it. Pure SQL generation — no live database. Where a divergence is intentional, it is asserted AS
// the documented behaviour (see the no-PK case).
const mk = (d: string) => Database.create({ sqlDialect: d as any, host: "h", user: "u", password: "p", database: "d", schema: "s" }) as any;
const DIALECTS = ["mysql", "pgsql", "sqlserver"];

describe("cross-dialect conformance (T1)", () => {
    describe("CREATE TABLE maps inference types to each dialect's server type", () => {
        const header: any = {
            id: { type: "int", primary: true },
            name: { type: "varchar", length: 50 },
            amt: { type: "decimal", length: 10, decimal: 2 },
            flag: { type: "boolean" },
            ts: { type: "datetime" },
        };
        const createSql = (d: string) => mk(d).getCreateTableQuery("t", header).map((q: any) => q.query).join("\n");

        test("MySQL", () => {
            const s = createSql("mysql");
            expect(s).toMatch(/varchar\(50\)/i);
            expect(s).toMatch(/decimal\(10\s*,\s*2\)/i);
            expect(s).toMatch(/\btinyint\b/i);      // boolean -> tinyint
        });
        test("Postgres", () => {
            const s = createSql("pgsql");
            expect(s).toMatch(/varchar\(50\)/i);
            expect(s).toMatch(/(decimal|numeric)\(10\s*,\s*2\)/i);
            expect(s).toMatch(/\bboolean\b/i);
            expect(s).toMatch(/timestamp/i);        // datetime -> timestamp
        });
        test("SQL Server", () => {
            const s = createSql("sqlserver");
            expect(s).toMatch(/nvarchar\(50\)/i);   // varchar -> nvarchar (Unicode)
            expect(s).toMatch(/decimal\(10\s*,\s*2\)/i);
            expect(s).toMatch(/\bbit\b/i);          // boolean -> bit
            expect(s).toMatch(/datetime2/i);        // datetime -> datetime2
        });
    });

    describe("upsert (UPDATE mode) with updatable columns updates on conflict everywhere", () => {
        const header: any = { id: { type: "int", primary: true }, v: { type: "int" } };
        const rows = [{ id: 1, v: 2 }];
        test("MySQL", () => {
            expect(mk("mysql").getInsertStatementQuery("t", rows, header, "UPDATE").query)
                .toMatch(/ON DUPLICATE KEY UPDATE `v` = VALUES\(`v`\)/i);
        });
        test("Postgres", () => {
            expect(mk("pgsql").getInsertStatementQuery("t", rows, header, "UPDATE").query)
                .toMatch(/ON CONFLICT \("id"\) DO UPDATE SET "v" = EXCLUDED\."v"/i);
        });
        test("SQL Server", () => {
            expect(mk("sqlserver").getInsertStatementQuery("t", rows, header, "UPDATE").query)
                .toMatch(/WHEN MATCHED THEN UPDATE SET target\.\[v\] = source\.\[v\]/i);
        });
    });

    describe("plain INSERT mode emits no upsert clause on any dialect", () => {
        const header: any = { id: { type: "int", primary: true }, v: { type: "int" } };
        test.each(DIALECTS)("%s", (d) => {
            const { query } = mk(d).getInsertStatementQuery("t", [{ id: 1, v: 2 }], header, "INSERT");
            expect(query).not.toMatch(/ON DUPLICATE KEY|ON CONFLICT|WHEN MATCHED/i);
        });
    });

    describe("UPDATE with no primary key — documented divergence (not silent)", () => {
        const header: any = { a: { type: "int" }, b: { type: "int" } }; // no primary key
        const rows = [{ a: 1, b: 2 }];
        test("Postgres throws (requires a primary key)", () => {
            expect(() => mk("pgsql").getInsertStatementQuery("t", rows, header, "UPDATE")).toThrow(/primary key/i);
        });
        test("MySQL and SQL Server fall back without throwing (documented)", () => {
            expect(() => mk("mysql").getInsertStatementQuery("t", rows, header, "UPDATE")).not.toThrow();
            expect(() => mk("sqlserver").getInsertStatementQuery("t", rows, header, "UPDATE")).not.toThrow();
        });
    });
});
