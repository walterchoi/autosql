import { Database } from "../src/db/database";
import type { AlterTableChanges, DatabaseConfig, MetadataHeader, QueryInput } from "../src/config/types";

// A table that never had a primary key at all (a keyless object whose spec is later corrected to
// declare one — the actual shape of a `updatePrimaryKey: true` first-correction) must not have its
// PK *dropped* before being added: MySQL's bare `DROP PRIMARY KEY` errors outright when the table has
// no key to drop (Postgres' `DROP CONSTRAINT IF EXISTS` and SQL Server's dynamic lookup already
// tolerate this; MySQL did not). `primaryKeyExistedBefore: false` must suppress the DROP on every
// dialect, while still emitting the ADD.

const changesWith = (over: Partial<AlterTableChanges>): AlterTableChanges => ({
    addColumns: {}, modifyColumns: {}, dropColumns: [], renameColumns: [],
    nullableColumns: [], noLongerUnique: [], primaryKeyChanges: [], ...over,
});

function makeDb(dialect: "mysql" | "pgsql" | "sqlserver", updatePrimaryKey: boolean) {
    const config: DatabaseConfig = {
        sqlDialect: dialect, schema: "test_schema", updatePrimaryKey,
        host: "h", user: "u", password: "p", database: "d",
    };
    return Database.create(config) as unknown as {
        getAlterTableQuery: (table: string, changesOrOldHeaders: AlterTableChanges | MetadataHeader, newHeaders?: MetadataHeader) => Promise<QueryInput[]>;
        getConfig: () => DatabaseConfig;
    };
}

const sqlOf = (q: QueryInput): string => typeof q === "string" ? q : q.query;

describe.each(["mysql", "pgsql", "sqlserver"] as const)("Widening a keyless table's primary key (%s)", (dialect) => {
    test("suppresses DROP PRIMARY KEY when the table never had one", async () => {
        const db = makeDb(dialect, true);
        const queries = await db.getAlterTableQuery(
            "session_report",
            changesWith({ primaryKeyChanges: ["date", "channel"], primaryKeyExistedBefore: false })
        );
        const allSql = queries.map((q) => sqlOf(q)).join("\n");

        expect(allSql).not.toMatch(/DROP (PRIMARY KEY|CONSTRAINT)/i);
        expect(allSql).toMatch(/ADD (CONSTRAINT|PRIMARY KEY)/i);
    });

    test("still drops the old key first when one already existed", async () => {
        const db = makeDb(dialect, true);
        const queries = await db.getAlterTableQuery(
            "orders",
            changesWith({ primaryKeyChanges: ["id", "tenant_id"], primaryKeyExistedBefore: true })
        );
        const allSql = queries.map((q) => sqlOf(q)).join("\n");

        expect(allSql).toMatch(/DROP (PRIMARY KEY|CONSTRAINT)/i);
        expect(allSql).toMatch(/ADD (CONSTRAINT|PRIMARY KEY)/i);
    });

    test("defaults to dropping first when primaryKeyExistedBefore is omitted (back-compat)", async () => {
        const db = makeDb(dialect, true);
        const queries = await db.getAlterTableQuery(
            "orders",
            changesWith({ primaryKeyChanges: ["id"] })
        );
        const allSql = queries.map((q) => sqlOf(q)).join("\n");

        expect(allSql).toMatch(/DROP (PRIMARY KEY|CONSTRAINT)/i);
    });

    test("caches the widened primary flag only when updatePrimaryKey actually applies the change", async () => {
        const oldHeaders: MetadataHeader = {
            id: { type: "int", length: 11, allowNull: false, primary: true },
            tenant_id: { type: "int", length: 11, allowNull: false }
        };
        const newHeaders: MetadataHeader = {
            id: { type: "int", length: 11, allowNull: false, primary: true },
            tenant_id: { type: "int", length: 11, allowNull: false, primary: true }
        };

        const applied = makeDb(dialect, true);
        await applied.getAlterTableQuery("orders", oldHeaders, newHeaders);
        expect(applied.getConfig().metaData?.orders?.tenant_id.primary).toBe(true);

        // Off by default: the ALTER is blocked, so the live table still has the old key — the cache
        // must keep matching what's actually on the table, not what was merely computed.
        const blocked = makeDb(dialect, false);
        await blocked.getAlterTableQuery("orders", oldHeaders, newHeaders);
        expect(blocked.getConfig().metaData?.orders?.tenant_id.primary).toBeFalsy();
    });
});
