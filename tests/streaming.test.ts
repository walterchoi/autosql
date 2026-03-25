import { AutoSQLStreamHandle } from "../src/db/autosql";
import { buildStreamStagingTableName, isAutosqlStreamTable, buildMergeFromStreamQuery } from "../src/helpers/streamHelpers";
import { MetadataHeader, DatabaseConfig, QueryWithParams } from "../src/config/types";
import { mysqlConfig } from "../src/db/config/mysqlConfig";
import { pgsqlConfig } from "../src/db/config/pgsqlConfig";

// ---------------------------------------------------------------------------
// Stream staging table naming
// ---------------------------------------------------------------------------
describe("stream staging table naming", () => {
    test("buildStreamStagingTableName includes prefix, table, and runId", () => {
        expect(buildStreamStagingTableName("users", "autosql_stream__", "a1b2c3d4"))
            .toBe("autosql_stream__users__a1b2c3d4");
    });

    test("isAutosqlStreamTable identifies correctly formed names", () => {
        expect(isAutosqlStreamTable("autosql_stream__users__a1b2c3d4", "users", "autosql_stream__")).toBe(true);
    });

    test("isAutosqlStreamTable rejects wrong prefix", () => {
        expect(isAutosqlStreamTable("other__users__a1b2c3d4", "users", "autosql_stream__")).toBe(false);
    });

    test("isAutosqlStreamTable rejects suffix that is not 8 hex chars", () => {
        expect(isAutosqlStreamTable("autosql_stream__users__ZZZZZZZZ", "users", "autosql_stream__")).toBe(false);
        expect(isAutosqlStreamTable("autosql_stream__users__a1b2c3", "users", "autosql_stream__")).toBe(false);
        expect(isAutosqlStreamTable("autosql_stream__users__a1b2c3d4e5", "users", "autosql_stream__")).toBe(false);
    });

    test("isAutosqlStreamTable rejects wrong table name", () => {
        expect(isAutosqlStreamTable("autosql_stream__orders__a1b2c3d4", "users", "autosql_stream__")).toBe(false);
    });
});

// ---------------------------------------------------------------------------
// Merge query builder
// ---------------------------------------------------------------------------
const META: MetadataHeader = {
    id:   { type: "int",     primary: true,  allowNull: false },
    name: { type: "varchar", allowNull: false, length: 100 },
    score: { type: "decimal", allowNull: true, length: 10, decimal: 2 },
};

const MYSQL_CONF: DatabaseConfig = { sqlDialect: "mysql" };
const PGSQL_CONF: DatabaseConfig = { sqlDialect: "pgsql" };

describe("buildMergeFromStreamQuery — MySQL", () => {
    test("casts int column with CAST(col AS SIGNED)", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "INSERT", MYSQL_CONF) as QueryWithParams;
        expect(q.query).toContain("CAST(`id` AS SIGNED)");
    });

    test("casts decimal column correctly", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "INSERT", MYSQL_CONF) as QueryWithParams;
        expect(q.query).toContain("CAST(`score` AS DECIMAL(10,2))");
    });

    test("varchar column has no cast", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "INSERT", MYSQL_CONF) as QueryWithParams;
        expect(q.query).toContain("`name`");
        expect(q.query).not.toContain("CAST(`name`");
    });

    test("generates ON DUPLICATE KEY UPDATE for insertType UPDATE", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "UPDATE", MYSQL_CONF) as QueryWithParams;
        expect(q.query).toContain("ON DUPLICATE KEY UPDATE");
        expect(q.query).not.toContain("`id` = VALUES(`id`)"); // primary key excluded
        expect(q.query).toContain("`name` = VALUES(`name`)");
    });

    test("no ON DUPLICATE KEY UPDATE for insertType INSERT", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "INSERT", MYSQL_CONF) as QueryWithParams;
        expect(q.query).not.toContain("ON DUPLICATE KEY UPDATE");
    });

    test("params array is empty (no parameterized values)", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "UPDATE", MYSQL_CONF) as QueryWithParams;
        expect(q.params).toEqual([]);
    });
});

describe("buildMergeFromStreamQuery — PostgreSQL", () => {
    test("casts int column with ::INTEGER", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "INSERT", PGSQL_CONF) as QueryWithParams;
        expect(q.query).toContain(`"id"::INTEGER`);
    });

    test("casts decimal column with ::DECIMAL", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "INSERT", PGSQL_CONF) as QueryWithParams;
        expect(q.query).toContain(`"score"::DECIMAL(10,2)`);
    });

    test("generates ON CONFLICT DO UPDATE for insertType UPDATE", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "UPDATE", PGSQL_CONF) as QueryWithParams;
        expect(q.query).toContain("ON CONFLICT");
        expect(q.query).toContain("DO UPDATE SET");
        expect(q.query).toContain(`"name" = EXCLUDED."name"`);
    });

    test("params array is empty", () => {
        const q = buildMergeFromStreamQuery("users", "staging_users__abc", META, "UPDATE", PGSQL_CONF) as QueryWithParams;
        expect(q.params).toEqual([]);
    });
});

// ---------------------------------------------------------------------------
// AutoSQLStreamHandle flow (mocked db)
// ---------------------------------------------------------------------------
const OK_RESULT = { start: new Date(), end: new Date(), duration: 0, success: true, affectedRows: 1, results: [] };

function makeDb(configOverrides: Partial<DatabaseConfig> = {}) {
    return {
        getConfig: () => ({
            sqlDialect: "mysql" as const,
            useSchemaLock: false,
            schemaHistory: false,
            useStagingInsert: false,
            safeMode: false,
            insertType: "UPDATE" as const,
            insertStack: 100,
            stagingPrefix: "temp_staging__",
            historyTableSuffix: "__history",
            streamMaxRetries: 3,
            streamingStagingPrefix: "autosql_stream__",
            ...configOverrides,
        }),
        runQuery: jest.fn().mockResolvedValue(OK_RESULT),
        runTransaction: jest.fn().mockResolvedValue(OK_RESULT),
        getDialectConfig: jest.fn().mockReturnValue(mysqlConfig),
        getInsertStatementQuery: jest.fn().mockReturnValue({ query: "INSERT INTO t VALUES (?)", params: [1] }),
        log: jest.fn(), warn: jest.fn(), error: jest.fn(),
        acquireSchemaLock: jest.fn(), releaseSchemaLock: jest.fn(),
        getTableMetaData: jest.fn().mockResolvedValue(null),
    };
}

function makeHandler(db: any) {
    return {
        fetchTableMetadata: jest.fn().mockResolvedValue({ currentMetaData: null, tableExists: false }),
        configureTables: jest.fn().mockResolvedValue([OK_RESULT]),
    };
}

describe("AutoSQLStreamHandle", () => {
    test("write() with empty chunk is a no-op", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = new AutoSQLStreamHandle(handler as any, db as any, "users", "autosql_stream__users__abc12345", undefined, undefined, undefined);
        await handle.write([]);
        expect(db.runTransaction).not.toHaveBeenCalled();
    });

    test("write() after end() throws", async () => {
        const db = makeDb();
        const handle = new AutoSQLStreamHandle(makeHandler(db) as any, db as any, "users", "autosql_stream__users__abc12345", undefined, undefined, undefined);
        // Mark as ended by calling abort
        await handle.abort();
        await expect(handle.write([{ id: 1 }])).rejects.toThrow("write() called after end()/abort()");
    });

    test("abort() drops staging table if created", async () => {
        const db = makeDb();
        const handle = new AutoSQLStreamHandle(makeHandler(db) as any, db as any, "users", "autosql_stream__users__abc12345", undefined, undefined, undefined);
        // Simulate staging created
        (handle as any).stagingCreated = true;
        await handle.abort();
        expect(db.runTransaction).toHaveBeenCalledWith(
            expect.arrayContaining([expect.objectContaining({ query: expect.stringContaining("DROP TABLE") })])
        );
    });

    test("abort() is a no-op when staging was never created", async () => {
        const db = makeDb();
        const handle = new AutoSQLStreamHandle(makeHandler(db) as any, db as any, "users", "autosql_stream__users__abc12345", undefined, undefined, undefined);
        await handle.abort();
        expect(db.runTransaction).not.toHaveBeenCalled();
    });
});
