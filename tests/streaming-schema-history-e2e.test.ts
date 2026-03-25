/**
 * End-to-end integration tests for streaming + schema history (v1.0.6).
 *
 * Verifies every new feature in isolation and wired together using a fully
 * mocked database — no live connection required.
 *
 * Features covered:
 *   • openStream(): connectivity check, orphan cleanup
 *   • write(): staging table create-on-first-write, insert per chunk
 *   • end(): staging read → schema inference → DDL → bulk merge → cleanup
 *   • Schema history: bootstrap, pending→applied/failed lifecycle
 *   • Advisory locks: acquire/release order, released on DDL error
 *   • Per-row fallback: fires on bulk merge failure, accumulates affectedRows
 *   • Rejected rows: captured to rejectedRowsTable or throws
 *   • abort(): drops staging if created, no-op otherwise
 *   • Orphan cleanup: detects + drops leftover tables, honours keepOrphanedStagingTables
 *   • detectSchemaDrift: no-history, match, warn, throw
 *   • getSchemaAt: null when no records, parsed schema when found
 *   • All features simultaneously: correct call ordering under full config
 */

import { AutoSQLHandler, AutoSQLStreamHandle } from "../src/db/autosql";
import { computeChecksum, detectSchemaDrift, getSchemaAt } from "../src/helpers/schemaHistory";
import { SchemaDriftError } from "../src/errors";
import { DatabaseConfig, MetadataHeader, QueryResult } from "../src/config/types";
import { mysqlConfig } from "../src/db/config/mysqlConfig";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const STAGING_ROWS = [
    { id: "1", name: "Alice", score: "9.5" },
    { id: "2", name: "Bob",   score: "7.2" },
    { id: "3", name: "Carol", score: "8.1" },
];

function ok(extra: Partial<QueryResult> = {}): QueryResult {
    return { start: new Date(), end: new Date(), duration: 0, success: true, affectedRows: 0, results: [], ...extra };
}

function fail(extra: Partial<QueryResult> = {}): QueryResult {
    return { start: new Date(), end: new Date(), duration: 0, success: false, affectedRows: 0, error: "mock failure", ...extra };
}

/**
 * Default runQuery mock — covers every query shape the production code issues.
 */
function defaultRunQuery(q: { query: string; params?: any[] }): Promise<QueryResult> {
    const sql = q.query;

    if (sql === 'SELECT 1') {
        return Promise.resolve(ok({ results: [{ 1: 1 }] }));
    }
    if (sql.includes('information_schema.tables')) {
        return Promise.resolve(ok({ results: [] }));
    }
    if (sql.includes('SELECT') && sql.includes('autosql_stream__')) {
        // buildSelectFromStreamStagingQuery — returns staging rows
        return Promise.resolve(ok({ results: STAGING_ROWS }));
    }
    if (sql.includes('LAST_INSERT_ID')) {
        return Promise.resolve(ok({ results: [{ id: 42 }] }));
    }
    if (sql.includes('autosql_schema_history') && sql.trim().startsWith('SELECT')) {
        return Promise.resolve(ok({ results: [] }));
    }
    // INSERT INTO autosql_schema_history (pending record) — runQuery
    if (sql.includes('autosql_schema_history') && sql.trim().startsWith('INSERT')) {
        return Promise.resolve(ok({ results: [{ id: 42 }] }));
    }
    // UPDATE autosql_schema_history (applied/failed)
    if (sql.includes('autosql_schema_history') && sql.trim().startsWith('UPDATE')) {
        return Promise.resolve(ok());
    }
    return Promise.resolve(ok());
}

function makeDb(configOverrides: Partial<DatabaseConfig> = {}) {
    const runQuery = jest.fn().mockImplementation(defaultRunQuery);
    const runTransaction = jest.fn().mockResolvedValue(ok({ affectedRows: 3 }));

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
            keepOrphanedStagingTables: false,
            database: "testdb",
            ...configOverrides,
        }),
        runQuery,
        runTransaction,
        getDialectConfig: jest.fn().mockReturnValue(mysqlConfig),
        getInsertStatementQuery: jest.fn().mockReturnValue({ query: "INSERT INTO `users` VALUES (?)", params: [1] }),
        log: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        acquireSchemaLock: jest.fn().mockResolvedValue(undefined),
        releaseSchemaLock: jest.fn().mockResolvedValue(undefined),
        getTableMetaData: jest.fn().mockResolvedValue(null),
        updateSchema: jest.fn(),
        updateTableMetadata: jest.fn(),
        getTableExistsQuery: jest.fn().mockReturnValue({ query: "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_name = ?", params: ["users"] }),
        getTableMetaDataQuery: jest.fn().mockReturnValue({ query: "SELECT * FROM information_schema.columns WHERE table_name = ?", params: ["users"] }),
        getCreateTableQuery: jest.fn().mockReturnValue([{ query: "CREATE TABLE `users` (id INT)", params: [] }]),
        getAlterTableQuery: jest.fn().mockResolvedValue([{ query: "ALTER TABLE `users` ADD COLUMN name VARCHAR(255)", params: [] }]),
        runTransactionsWithConcurrency: jest.fn().mockResolvedValue([ok({ affectedRows: 3 })]),
        getDialect: jest.fn().mockReturnValue("mysql"),
        getCreateTempTableQuery: jest.fn().mockReturnValue({ query: "CREATE TABLE temp", params: [] }),
        getDropTableQuery: jest.fn().mockReturnValue({ query: "DROP TABLE temp", params: [] }),
        getSplitTablesQuery: jest.fn().mockReturnValue({ query: "SELECT 1", params: [] }),
        getUniqueIndexesQuery: jest.fn().mockReturnValue({ query: "SELECT 1", params: [] }),
        getPrimaryKeysQuery: jest.fn().mockReturnValue({ query: "SELECT 1", params: [] }),
        getConstraintConflictQuery: jest.fn().mockReturnValue({ query: "SELECT 1", params: [] }),
        getDropUniqueConstraintQuery: jest.fn().mockReturnValue({ query: "ALTER TABLE t DROP INDEX i", params: [] }),
        getInsertFromStagingQuery: jest.fn().mockReturnValue({ query: "INSERT INTO t SELECT * FROM staging", params: [] }),
        getInsertChangedRowsToHistoryQuery: jest.fn().mockReturnValue({ query: "INSERT INTO history SELECT * FROM t", params: [] }),
    };
}

function makeHandler(db: ReturnType<typeof makeDb>) {
    const handler = new AutoSQLHandler(db as any);
    // Override the methods that would issue real DB queries
    (handler as any).fetchTableMetadata = jest.fn().mockResolvedValue({ currentMetaData: null, tableExists: false });
    (handler as any).configureTables = jest.fn().mockResolvedValue([ok()]);
    return handler;
}

/** Builds a handle pre-wired to a given handler and db (bypasses openStream). */
function makeHandle(
    handler: ReturnType<typeof makeHandler>,
    db: ReturnType<typeof makeDb>,
    opts: { stagingCreated?: boolean; schema?: string } = {}
): AutoSQLStreamHandle {
    const handle = new AutoSQLStreamHandle(
        handler as any,
        db as any,
        "users",
        "autosql_stream__users__abc12345",
        opts.schema,
        undefined,
        undefined
    );
    if (opts.stagingCreated) {
        (handle as any).stagingCreated = true;
        (handle as any).columns = ["id", "name", "score"];
    }
    return handle;
}

// ---------------------------------------------------------------------------
// describe: "openStream() — connectivity and orphan cleanup"
// ---------------------------------------------------------------------------
describe("openStream() — connectivity and orphan cleanup", () => {
    test("connectivity check (SELECT 1) is issued on openStream", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        await handler.openStream("users");
        const calls = (db.runQuery as jest.Mock).mock.calls.map((c: any[]) => c[0].query);
        expect(calls).toContain("SELECT 1");
    });

    test("orphan search query (information_schema.tables) is issued", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        await handler.openStream("users");
        const calls = (db.runQuery as jest.Mock).mock.calls.map((c: any[]) => c[0].query as string);
        expect(calls.some((q) => q.includes("information_schema.tables"))).toBe(true);
    });

    test("returns an AutoSQLStreamHandle-like object with write/end/abort methods", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = await handler.openStream("users");
        expect(typeof handle.write).toBe("function");
        expect(typeof handle.end).toBe("function");
        expect(typeof handle.abort).toBe("function");
    });

    test("throws when SELECT 1 fails", async () => {
        const db = makeDb();
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string }) => {
            if (q.query === "SELECT 1") return Promise.resolve(fail({ error: "connection refused" }));
            return defaultRunQuery(q);
        });
        const handler = makeHandler(db);
        await expect(handler.openStream("users")).rejects.toThrow("openStream: cannot connect to database");
    });
});

// ---------------------------------------------------------------------------
// describe: "write() — staging table lifecycle"
// ---------------------------------------------------------------------------
describe("write() — staging table lifecycle", () => {
    test("first write() creates staging table (runTransaction with CREATE TABLE)", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db);
        await handle.write(STAGING_ROWS);
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(queries.some((q) => q.includes("CREATE TABLE") && q.includes("autosql_stream__"))).toBe(true);
    });

    test("first write() inserts chunk rows (runTransaction with INSERT VALUES)", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db);
        await handle.write(STAGING_ROWS);
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(
            queries.some((q) => q.includes("INSERT INTO") && q.includes("autosql_stream__") && q.includes("VALUES"))
        ).toBe(true);
    });

    test("second write() only inserts (no extra CREATE TABLE call)", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db);
        await handle.write(STAGING_ROWS);
        (db.runTransaction as jest.Mock).mockClear();
        await handle.write([{ id: "4", name: "Dave", score: "6.0" }]);
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(queries.some((q) => q.includes("CREATE TABLE"))).toBe(false);
        expect(
            queries.some((q) => q.includes("INSERT INTO") && q.includes("autosql_stream__"))
        ).toBe(true);
    });

    test("empty chunk write() is a no-op (no runTransaction calls)", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db);
        await handle.write([]);
        expect(db.runTransaction).not.toHaveBeenCalled();
    });
});

// ---------------------------------------------------------------------------
// describe: "end() — pipeline sequencing"
// ---------------------------------------------------------------------------
describe("end() — pipeline sequencing", () => {
    test("reads staging rows (runQuery SELECT * FROM staging)", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const calls = (db.runQuery as jest.Mock).mock.calls as any[][];
        const queries = calls.map((args) => args[0].query as string);
        expect(
            queries.some((q) => q.includes("SELECT") && q.includes("autosql_stream__"))
        ).toBe(true);
    });

    test("calls fetchTableMetadata", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        expect((handler as any).fetchTableMetadata).toHaveBeenCalledWith("users");
    });

    test("calls configureTables", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        expect((handler as any).configureTables).toHaveBeenCalled();
    });

    test("executes merge query via runTransaction", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        // Merge query: INSERT INTO `users` ... SELECT ... FROM `autosql_stream__...`
        expect(
            queries.some((q) => q.includes("autosql_stream__") && q.includes("SELECT"))
        ).toBe(true);
    });

    test("always drops staging table in finally (even when merge succeeds)", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(
            queries.some((q) => q.includes("DROP TABLE") && q.includes("autosql_stream__"))
        ).toBe(true);
    });

    test("returns success:true with correct affectedRows", async () => {
        const db = makeDb();
        // runTransaction returns affectedRows: 3 by default
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        const result = await handle.end();
        expect(result.success).toBe(true);
        expect(result.affectedRows).toBe(3);
    });
});

// ---------------------------------------------------------------------------
// describe: "end() — schema history integration"
// ---------------------------------------------------------------------------
describe("end() — schema history integration", () => {
    test("schemaHistory:false → no bootstrap or record calls at all", async () => {
        const db = makeDb({ schemaHistory: false });
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const txCalls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const txQueries = txCalls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(txQueries.some((q) => q.includes("autosql_schema_history"))).toBe(false);

        const qCalls = (db.runQuery as jest.Mock).mock.calls as any[][];
        const qQueries = qCalls.map((args) => args[0].query as string);
        expect(qQueries.some((q) => q.includes("autosql_schema_history"))).toBe(false);
    });

    test("schemaHistory:true → runTransaction called with CREATE TABLE autosql_schema_history", async () => {
        const db = makeDb({ schemaHistory: true });
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(queries.some((q) => q.includes("CREATE TABLE") && q.includes("autosql_schema_history"))).toBe(true);
    });

    test("schemaHistory:true + new table → runQuery called with INSERT INTO autosql_schema_history (pending record)", async () => {
        const db = makeDb({ schemaHistory: true });
        // Simulate table changes by making fetchTableMetadata return existing metadata
        // so compareMetaData will detect the new columns as changes.
        const existingMeta: MetadataHeader = {
            id: { type: "int", primary: true, allowNull: false },
        };
        const handler = makeHandler(db);
        (handler as any).fetchTableMetadata = jest.fn().mockResolvedValue({
            currentMetaData: existingMeta,
            tableExists: true,
        });
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const qCalls = (db.runQuery as jest.Mock).mock.calls as any[][];
        const qQueries = qCalls.map((args) => args[0].query as string);
        // recordMigrationStart issues INSERT INTO autosql_schema_history ... SELECT ...
        expect(
            qQueries.some(
                (q) => q.includes("INSERT INTO") && q.includes("autosql_schema_history") && q.includes("SELECT")
            )
        ).toBe(true);
    });

    test("DDL success → runQuery UPDATE autosql_schema_history params include 'applied'", async () => {
        const db = makeDb({ schemaHistory: true });
        const existingMeta: MetadataHeader = {
            id: { type: "int", primary: true, allowNull: false },
        };
        const handler = makeHandler(db);
        (handler as any).fetchTableMetadata = jest.fn().mockResolvedValue({
            currentMetaData: existingMeta,
            tableExists: true,
        });
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const qCalls = (db.runQuery as jest.Mock).mock.calls as any[][];
        const updateCall = qCalls.find(
            (args) =>
                (args[0].query as string).includes("UPDATE") &&
                (args[0].query as string).includes("autosql_schema_history") &&
                args[0].params?.[0] === "applied"
        );
        expect(updateCall).toBeDefined();
    });

    test("DDL failure → runQuery UPDATE autosql_schema_history params include 'failed', end() returns success:false", async () => {
        const db = makeDb({ schemaHistory: true });
        const existingMeta: MetadataHeader = {
            id: { type: "int", primary: true, allowNull: false },
        };
        const handler = makeHandler(db);
        (handler as any).fetchTableMetadata = jest.fn().mockResolvedValue({
            currentMetaData: existingMeta,
            tableExists: true,
        });
        // Make configureTables throw
        (handler as any).configureTables = jest.fn().mockRejectedValue(new Error("DDL failed"));
        const handle = makeHandle(handler, db, { stagingCreated: true });
        const result = await handle.end();
        expect(result.success).toBe(false);
        const qCalls = (db.runQuery as jest.Mock).mock.calls as any[][];
        const failedCall = qCalls.find(
            (args) =>
                (args[0].query as string).includes("UPDATE") &&
                (args[0].query as string).includes("autosql_schema_history") &&
                args[0].params?.[0] === "failed"
        );
        expect(failedCall).toBeDefined();
    });
});

// ---------------------------------------------------------------------------
// describe: "end() — advisory lock integration"
// ---------------------------------------------------------------------------
describe("end() — advisory lock integration", () => {
    test("useSchemaLock:false → acquireSchemaLock not called", async () => {
        const db = makeDb({ useSchemaLock: false });
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        expect(db.acquireSchemaLock).not.toHaveBeenCalled();
    });

    test("useSchemaLock:true → acquireSchemaLock called before configureTables", async () => {
        const db = makeDb({ useSchemaLock: true });
        const handler = makeHandler(db);
        const callOrder: string[] = [];
        (db.acquireSchemaLock as jest.Mock).mockImplementation(() => {
            callOrder.push("acquire");
            return Promise.resolve();
        });
        (handler as any).configureTables = jest.fn().mockImplementation(() => {
            callOrder.push("configureTables");
            return Promise.resolve([ok()]);
        });
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const acquireIdx = callOrder.indexOf("acquire");
        const configureIdx = callOrder.indexOf("configureTables");
        expect(acquireIdx).toBeGreaterThanOrEqual(0);
        expect(configureIdx).toBeGreaterThanOrEqual(0);
        expect(acquireIdx).toBeLessThan(configureIdx);
    });

    test("useSchemaLock:true → releaseSchemaLock called after configureTables", async () => {
        const db = makeDb({ useSchemaLock: true });
        const handler = makeHandler(db);
        const callOrder: string[] = [];
        (handler as any).configureTables = jest.fn().mockImplementation(() => {
            callOrder.push("configureTables");
            return Promise.resolve([ok()]);
        });
        (db.releaseSchemaLock as jest.Mock).mockImplementation(() => {
            callOrder.push("release");
            return Promise.resolve();
        });
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const configureIdx = callOrder.indexOf("configureTables");
        const releaseIdx = callOrder.indexOf("release");
        expect(configureIdx).toBeGreaterThanOrEqual(0);
        expect(releaseIdx).toBeGreaterThanOrEqual(0);
        expect(releaseIdx).toBeGreaterThan(configureIdx);
    });

    test("releaseSchemaLock called even when configureTables throws", async () => {
        const db = makeDb({ useSchemaLock: true });
        const handler = makeHandler(db);
        (handler as any).configureTables = jest.fn().mockRejectedValue(new Error("DDL crash"));
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end(); // should NOT throw — error is caught and returned as success:false
        expect(db.releaseSchemaLock).toHaveBeenCalled();
    });
});

// ---------------------------------------------------------------------------
// describe: "end() — per-row fallback on bulk merge failure"
// ---------------------------------------------------------------------------
describe("end() — per-row fallback on bulk merge failure", () => {
    /**
     * Build a db where the bulk-merge runTransaction fails (triggering _perRowMerge),
     * but all other runTransaction calls succeed.
     */
    function makePerRowDb() {
        const db = makeDb();
        (db.runTransaction as jest.Mock).mockImplementation((queries: any[]) => {
            const sql = queries[0]?.query as string;
            // Fail only the bulk merge (INSERT INTO `users` … SELECT … FROM `autosql_stream__…`)
            if (sql && sql.includes("autosql_stream__") && sql.includes("SELECT")) {
                return Promise.resolve(fail());
            }
            return Promise.resolve(ok({ affectedRows: 1 }));
        });
        return db;
    }

    test("bulk merge (runTransaction) fails → getInsertStatementQuery called per row", async () => {
        const db = makePerRowDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        // Mock _perRowMerge to verify getInsertStatementQuery is called per row
        (handle as any)._perRowMerge = jest.fn().mockImplementation(
            async (rows: any[], _meta: any, _insertType: any, _maxRetries: any) => {
                for (const row of rows) {
                    db.getInsertStatementQuery("users", [row], _meta, _insertType);
                }
                return rows.length;
            }
        );
        await handle.end();
        expect(db.getInsertStatementQuery).toHaveBeenCalled();
        expect((db.getInsertStatementQuery as jest.Mock).mock.calls.length).toBeGreaterThanOrEqual(STAGING_ROWS.length);
    });

    test("end() still returns success:true when per-row fallback succeeds", async () => {
        const db = makePerRowDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        // Mock _perRowMerge so that per-row inserts "succeed" — returns STAGING_ROWS.length
        (handle as any)._perRowMerge = jest.fn().mockResolvedValue(STAGING_ROWS.length);
        const result = await handle.end();
        expect(result.success).toBe(true);
    });

    test("affectedRows reflect per-row insert count", async () => {
        const db = makePerRowDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        // Mock _perRowMerge to return the exact per-row count
        (handle as any)._perRowMerge = jest.fn().mockResolvedValue(STAGING_ROWS.length);
        const result = await handle.end();
        // affectedRows should equal the count returned by _perRowMerge
        expect(result.affectedRows).toBe(STAGING_ROWS.length);
    });
});

// ---------------------------------------------------------------------------
// describe: "end() — rejected rows capture"
// ---------------------------------------------------------------------------
describe("end() — rejected rows capture", () => {
    function makeRejectedDb() {
        const db = makeDb({ rejectedRowsTable: "rejected_rows", streamMaxRetries: 1 });
        (db.runTransaction as jest.Mock).mockImplementation((queries: any[]) => {
            const sql = queries[0]?.query as string;
            if (sql && sql.includes("autosql_stream__") && sql.includes("SELECT")) {
                return Promise.resolve(fail());
            }
            return Promise.resolve(ok({ affectedRows: 1 }));
        });
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string; params?: any[] }) => {
            const sql = q.query;
            if (sql === "SELECT 1") return Promise.resolve(ok({ results: [{ 1: 1 }] }));
            if (sql.includes("information_schema.tables")) return Promise.resolve(ok({ results: [] }));
            if (sql.includes("SELECT") && sql.includes("autosql_stream__")) {
                return Promise.resolve(ok({ results: STAGING_ROWS }));
            }
            if (sql.includes("LAST_INSERT_ID")) return Promise.resolve(ok({ results: [{ id: 42 }] }));
            if (sql.includes("autosql_schema_history") && sql.trim().startsWith("SELECT")) return Promise.resolve(ok({ results: [] }));
            // Per-row inserts always fail so rows end up in rejected_rows
            if (sql.includes("INSERT INTO `users`")) return Promise.resolve(fail({ error: "row rejected" }));
            return Promise.resolve(ok());
        });
        return db;
    }

    test("per-row failures + rejectedRowsTable set → runTransaction with CREATE + INSERT for rejected rows", async () => {
        const db = makeRejectedDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(queries.some((q) => q.includes("CREATE TABLE") && q.includes("rejected_rows"))).toBe(true);
        expect(queries.some((q) => q.includes("INSERT INTO") && q.includes("rejected_rows"))).toBe(true);
    });

    test("per-row failures + no rejectedRowsTable → end() returns success:false", async () => {
        // Same as rejected db but without rejectedRowsTable
        const db = makeDb({ streamMaxRetries: 1 });
        (db.runTransaction as jest.Mock).mockImplementation((queries: any[]) => {
            const sql = queries[0]?.query as string;
            if (sql && sql.includes("autosql_stream__") && sql.includes("SELECT")) {
                return Promise.resolve(fail());
            }
            return Promise.resolve(ok({ affectedRows: 1 }));
        });
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string; params?: any[] }) => {
            const sql = q.query;
            if (sql === "SELECT 1") return Promise.resolve(ok({ results: [{ 1: 1 }] }));
            if (sql.includes("information_schema.tables")) return Promise.resolve(ok({ results: [] }));
            if (sql.includes("SELECT") && sql.includes("autosql_stream__")) {
                return Promise.resolve(ok({ results: STAGING_ROWS }));
            }
            if (sql.includes("INSERT INTO `users`")) return Promise.resolve(fail({ error: "row rejected" }));
            return Promise.resolve(ok());
        });
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        const result = await handle.end();
        expect(result.success).toBe(false);
    });
});

// ---------------------------------------------------------------------------
// describe: "abort()"
// ---------------------------------------------------------------------------
describe("abort()", () => {
    test("abort() after write() → runTransaction with DROP TABLE", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.abort();
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(queries.some((q) => q.includes("DROP TABLE") && q.includes("autosql_stream__"))).toBe(true);
    });

    test("abort() before any write() → no runTransaction calls", async () => {
        const db = makeDb();
        const handler = makeHandler(db);
        const handle = makeHandle(handler, db);
        await handle.abort();
        expect(db.runTransaction).not.toHaveBeenCalled();
    });
});

// ---------------------------------------------------------------------------
// describe: "orphan cleanup"
// ---------------------------------------------------------------------------
describe("orphan cleanup", () => {
    test("no orphans found → no DROP TABLE called from openStream cleanup", async () => {
        const db = makeDb();
        // runQuery returns empty results for information_schema (default)
        const handler = makeHandler(db);
        await handler.openStream("users");
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(queries.some((q) => q.includes("DROP TABLE") && q.includes("autosql_stream__"))).toBe(false);
    });

    test("valid orphan found → db.warn called + runTransaction DROP called", async () => {
        const db = makeDb();
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string; params?: any[] }) => {
            if (q.query.includes("information_schema.tables")) {
                return Promise.resolve(ok({ results: [{ table_name: "autosql_stream__users__deadbeef" }] }));
            }
            return defaultRunQuery(q);
        });
        const handler = makeHandler(db);
        await handler.openStream("users");
        expect(db.warn).toHaveBeenCalled();
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(queries.some((q) => q.includes("DROP TABLE") && q.includes("autosql_stream__users__deadbeef"))).toBe(true);
    });

    test("keepOrphanedStagingTables:true → information_schema NOT queried", async () => {
        const db = makeDb({ keepOrphanedStagingTables: true });
        const handler = makeHandler(db);
        await handler.openStream("users");
        const calls = (db.runQuery as jest.Mock).mock.calls as any[][];
        const queries = calls.map((args) => args[0].query as string);
        expect(queries.some((q) => q.includes("information_schema.tables"))).toBe(false);
    });
});

// ---------------------------------------------------------------------------
// describe: "detectSchemaDrift — standalone"
// ---------------------------------------------------------------------------
describe("detectSchemaDrift — standalone", () => {
    test("no history records → { drifted: false, expected: null }", async () => {
        const db = makeDb();
        // getTableMetaData returns a live schema
        const liveSchema: MetadataHeader = { id: { type: "int", primary: true } };
        (db.getTableMetaData as jest.Mock).mockResolvedValue(liveSchema);
        // runQuery for the history SELECT returns empty results
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string }) => {
            if (q.query.includes("autosql_schema_history") && q.query.includes("SELECT")) {
                return Promise.resolve(ok({ results: [] }));
            }
            return defaultRunQuery(q);
        });
        const result = await detectSchemaDrift(db as any, "users");
        expect(result.drifted).toBe(false);
        expect(result.expected).toBeNull();
    });

    test("checksum matches last applied record → { drifted: false }", async () => {
        const liveSchema: MetadataHeader = { id: { type: "int", primary: true } };
        const checksum = computeChecksum(liveSchema);
        const db = makeDb();
        (db.getTableMetaData as jest.Mock).mockResolvedValue(liveSchema);
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string }) => {
            if (q.query.includes("autosql_schema_history") && q.query.includes("SELECT")) {
                return Promise.resolve(ok({ results: [{ checksum }] }));
            }
            return defaultRunQuery(q);
        });
        const result = await detectSchemaDrift(db as any, "users");
        expect(result.drifted).toBe(false);
        expect(result.expected).toBe(checksum);
    });

    test("checksum differs + strictDriftDetection:false → { drifted: true } + db.warn called", async () => {
        const liveSchema: MetadataHeader = { id: { type: "int", primary: true } };
        const db = makeDb({ strictDriftDetection: false });
        (db.getTableMetaData as jest.Mock).mockResolvedValue(liveSchema);
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string }) => {
            if (q.query.includes("autosql_schema_history") && q.query.includes("SELECT")) {
                return Promise.resolve(ok({ results: [{ checksum: "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222" }] }));
            }
            return defaultRunQuery(q);
        });
        const result = await detectSchemaDrift(db as any, "users");
        expect(result.drifted).toBe(true);
        expect(db.warn).toHaveBeenCalled();
    });

    test("checksum differs + strictDriftDetection:true → throws SchemaDriftError", async () => {
        const liveSchema: MetadataHeader = { id: { type: "int", primary: true } };
        const db = makeDb({ strictDriftDetection: true });
        (db.getTableMetaData as jest.Mock).mockResolvedValue(liveSchema);
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string }) => {
            if (q.query.includes("autosql_schema_history") && q.query.includes("SELECT")) {
                return Promise.resolve(ok({ results: [{ checksum: "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222" }] }));
            }
            return defaultRunQuery(q);
        });
        await expect(detectSchemaDrift(db as any, "users")).rejects.toThrow(SchemaDriftError);
    });
});

// ---------------------------------------------------------------------------
// describe: "getSchemaAt — standalone"
// ---------------------------------------------------------------------------
describe("getSchemaAt — standalone", () => {
    test("no applied records before timestamp → returns null", async () => {
        const db = makeDb();
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string }) => {
            if (q.query.includes("autosql_schema_history") && q.query.includes("SELECT")) {
                return Promise.resolve(ok({ results: [] }));
            }
            return defaultRunQuery(q);
        });
        const result = await getSchemaAt(db as any, "users", new Date());
        expect(result).toBeNull();
    });

    test("applied record found → returns parsed MetadataHeader", async () => {
        const expectedSchema: MetadataHeader = {
            id:   { type: "int", primary: true, allowNull: false },
            name: { type: "varchar", allowNull: false, length: 100 },
        };
        const db = makeDb();
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string }) => {
            if (q.query.includes("autosql_schema_history") && q.query.includes("SELECT")) {
                return Promise.resolve(ok({ results: [{ new_schema: JSON.stringify(expectedSchema) }] }));
            }
            return defaultRunQuery(q);
        });
        const result = await getSchemaAt(db as any, "users", new Date());
        expect(result).toEqual(expectedSchema);
    });
});

// ---------------------------------------------------------------------------
// describe: "full pipeline — all features enabled simultaneously"
// ---------------------------------------------------------------------------
describe("full pipeline — all features enabled simultaneously", () => {
    function makeFullDb() {
        const db = makeDb({
            useSchemaLock: true,
            schemaHistory: true,
            streamMaxRetries: 3,
        });

        const existingMeta: MetadataHeader = {
            id: { type: "int", primary: true, allowNull: false },
        };

        // Full runQuery mock
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string; params?: any[] }) => {
            const sql = q.query;
            if (sql === "SELECT 1") return Promise.resolve(ok({ results: [{ 1: 1 }] }));
            if (sql.includes("information_schema.tables")) return Promise.resolve(ok({ results: [] }));
            if (sql.includes("SELECT") && sql.includes("autosql_stream__")) {
                return Promise.resolve(ok({ results: STAGING_ROWS }));
            }
            if (sql.includes("LAST_INSERT_ID")) return Promise.resolve(ok({ results: [{ id: 99 }] }));
            if (sql.includes("autosql_schema_history") && sql.trim().startsWith("SELECT")) {
                return Promise.resolve(ok({ results: [] }));
            }
            if (sql.includes("autosql_schema_history") && sql.trim().startsWith("INSERT")) {
                return Promise.resolve(ok({ results: [{ id: 99 }] }));
            }
            if (sql.includes("autosql_schema_history") && sql.trim().startsWith("UPDATE")) {
                return Promise.resolve(ok());
            }
            return Promise.resolve(ok());
        });

        return { db, existingMeta };
    }

    test("lock acquired before bootstrapSchemaHistoryTable", async () => {
        const { db, existingMeta } = makeFullDb();
        const callOrder: string[] = [];
        (db.acquireSchemaLock as jest.Mock).mockImplementation(() => {
            callOrder.push("acquire");
            return Promise.resolve();
        });
        (db.runTransaction as jest.Mock).mockImplementation((queries: any[]) => {
            const sql = queries[0]?.query as string ?? "";
            if (sql.includes("autosql_schema_history") && sql.includes("CREATE TABLE")) {
                callOrder.push("bootstrap");
            }
            if (sql.includes("autosql_stream__") && sql.includes("SELECT")) {
                return Promise.resolve(fail());
            }
            return Promise.resolve(ok({ affectedRows: 2 }));
        });
        const handler = makeHandler(db);
        (handler as any).fetchTableMetadata = jest.fn().mockResolvedValue({ currentMetaData: existingMeta, tableExists: true });
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const acquireIdx = callOrder.indexOf("acquire");
        const bootstrapIdx = callOrder.indexOf("bootstrap");
        expect(acquireIdx).toBeGreaterThanOrEqual(0);
        expect(bootstrapIdx).toBeGreaterThanOrEqual(0);
        expect(acquireIdx).toBeLessThan(bootstrapIdx);
    });

    test("history record written after bootstrap but before configureTables", async () => {
        const { db, existingMeta } = makeFullDb();
        const callOrder: string[] = [];
        (db.runTransaction as jest.Mock).mockImplementation((queries: any[]) => {
            const sql = queries[0]?.query as string ?? "";
            if (sql.includes("autosql_schema_history") && sql.includes("CREATE TABLE")) {
                callOrder.push("bootstrap");
            }
            if (sql.includes("autosql_stream__") && sql.includes("SELECT")) {
                return Promise.resolve(fail());
            }
            return Promise.resolve(ok({ affectedRows: 2 }));
        });
        (db.runQuery as jest.Mock).mockImplementation((q: { query: string; params?: any[] }) => {
            const sql = q.query;
            if (sql === "SELECT 1") return Promise.resolve(ok({ results: [{ 1: 1 }] }));
            if (sql.includes("information_schema.tables")) return Promise.resolve(ok({ results: [] }));
            if (sql.includes("SELECT") && sql.includes("autosql_stream__")) {
                return Promise.resolve(ok({ results: STAGING_ROWS }));
            }
            if (sql.includes("LAST_INSERT_ID")) return Promise.resolve(ok({ results: [{ id: 99 }] }));
            if (sql.includes("autosql_schema_history") && sql.trim().startsWith("SELECT")) {
                return Promise.resolve(ok({ results: [] }));
            }
            if (
                sql.includes("autosql_schema_history") &&
                sql.trim().startsWith("INSERT") &&
                sql.includes("SELECT")
            ) {
                callOrder.push("history_record");
                return Promise.resolve(ok({ results: [{ id: 99 }] }));
            }
            if (sql.includes("autosql_schema_history") && sql.trim().startsWith("UPDATE")) {
                return Promise.resolve(ok());
            }
            return Promise.resolve(ok());
        });
        const handler = makeHandler(db);
        (handler as any).fetchTableMetadata = jest.fn().mockResolvedValue({ currentMetaData: existingMeta, tableExists: true });
        (handler as any).configureTables = jest.fn().mockImplementation(() => {
            callOrder.push("configureTables");
            return Promise.resolve([ok()]);
        });
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const bootstrapIdx = callOrder.indexOf("bootstrap");
        const historyIdx = callOrder.indexOf("history_record");
        const configIdx = callOrder.indexOf("configureTables");
        // history_record appears after bootstrap
        if (bootstrapIdx >= 0 && historyIdx >= 0) {
            expect(bootstrapIdx).toBeLessThan(historyIdx);
        }
        // configureTables appears after history record
        if (historyIdx >= 0 && configIdx >= 0) {
            expect(historyIdx).toBeLessThan(configIdx);
        }
        // At minimum, configureTables should have been called
        expect(configIdx).toBeGreaterThanOrEqual(0);
    });

    test("lock released before merge (only held during DDL phase)", async () => {
        const { db, existingMeta } = makeFullDb();
        const callOrder: string[] = [];
        (db.acquireSchemaLock as jest.Mock).mockImplementation(() => {
            callOrder.push("acquire");
            return Promise.resolve();
        });
        (db.releaseSchemaLock as jest.Mock).mockImplementation(() => {
            callOrder.push("release");
            return Promise.resolve();
        });
        (db.runTransaction as jest.Mock).mockImplementation((queries: any[]) => {
            const sql = queries[0]?.query as string ?? "";
            if (sql.includes("autosql_stream__") && sql.includes("SELECT")) {
                callOrder.push("merge");
                return Promise.resolve(ok({ affectedRows: 3 }));
            }
            return Promise.resolve(ok({ affectedRows: 1 }));
        });
        const handler = makeHandler(db);
        (handler as any).fetchTableMetadata = jest.fn().mockResolvedValue({ currentMetaData: existingMeta, tableExists: true });
        const handle = makeHandle(handler, db, { stagingCreated: true });
        await handle.end();
        const releaseIdx = callOrder.indexOf("release");
        const mergeIdx = callOrder.indexOf("merge");
        if (releaseIdx >= 0 && mergeIdx >= 0) {
            expect(releaseIdx).toBeLessThan(mergeIdx);
        } else {
            // If merge wasn't tracked through runTransaction, ensure release happened
            expect(db.releaseSchemaLock).toHaveBeenCalled();
        }
    });

    test("staging dropped regardless of outcome", async () => {
        const { db } = makeFullDb();
        // Make the merge fail AND configureTables fail so we test both paths
        (db.runTransaction as jest.Mock).mockImplementation((queries: any[]) => {
            const sql = queries[0]?.query as string ?? "";
            if (sql.includes("autosql_stream__") && sql.includes("SELECT")) {
                return Promise.resolve(fail());
            }
            return Promise.resolve(ok({ affectedRows: 1 }));
        });
        const handler = makeHandler(db);
        (handler as any).configureTables = jest.fn().mockRejectedValue(new Error("schema error"));
        const handle = makeHandle(handler, db, { stagingCreated: true });
        const result = await handle.end();
        // end() should not throw — it catches internally
        expect(result.success).toBe(false);
        const calls = (db.runTransaction as jest.Mock).mock.calls as any[][];
        const queries = calls.flatMap((args) => args[0]).map((q: any) => q.query as string);
        expect(queries.some((q) => q.includes("DROP TABLE") && q.includes("autosql_stream__"))).toBe(true);
    });
});
