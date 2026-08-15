import { Database } from "../src/db/database";
import { AlterTableChanges } from "../src/config/types";

// A9: the Postgres adapter injected literal COMMIT;/BEGIN; around a primary-key change, splitting one
// logical migration into three transactions — a failure after the first COMMIT left the table with no
// primary key (durably) while the caller got success:false. Postgres DDL is transactional, so the
// drop-PK + alters + add-PK must all run in the single transaction runTransaction provides.
describe("Postgres PK change is a single transaction (A9)", () => {
    test("getAlterTableQuery emits no embedded COMMIT;/BEGIN; around the PK change", async () => {
        const db: any = Database.create({
            sqlDialect: "pgsql", schema: "test_schema", updatePrimaryKey: true,
            host: "h", user: "u", password: "p", database: "d",
        });
        const changes: AlterTableChanges = {
            addColumns: {}, modifyColumns: {}, dropColumns: [], renameColumns: [],
            nullableColumns: [], noLongerUnique: [], primaryKeyChanges: ["id"],
        };
        const queries = await db.getAlterTableQuery("mytable", changes);
        const allSql = queries.map((q: any) => q.query).join("\n");

        expect(allSql).not.toMatch(/\bCOMMIT\b/i);
        expect(allSql).not.toMatch(/\bBEGIN\b/i);
        // It still drops and re-adds the primary key — just within one transaction.
        expect(allSql).toMatch(/DROP CONSTRAINT|DROP PRIMARY KEY/i);
        expect(allSql).toMatch(/ADD (CONSTRAINT|PRIMARY KEY)/i);
    });
});
