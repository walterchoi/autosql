/**
 * Tests for autoSQLChunked.
 *
 * Because autoSQLChunked orchestrates several async methods on AutoSQLHandler
 * that require a live database connection, we test it by mocking the internal
 * methods — verifying call counts and sequencing rather than SQL output.
 */
import { AutoSQLHandler } from "../src/db/autosql";
import { InsertInput, QueryResult } from "../src/config/types";
import { Database } from "../src/db/database";
import { mysqlConfig } from "../src/db/config/mysqlConfig";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Minimal passing QueryResult */
const OK: QueryResult = {
    start: new Date(),
    end: new Date(),
    duration: 0,
    success: true,
    affectedRows: 1,
    results: []
};

/** Build a minimal fake InsertInput */
function makeInsertInput(table: string, data: Record<string, any>[]): InsertInput {
    return {
        table,
        data,
        metaData: { id: { type: "int", allowNull: false } },
        previousMetaData: null,
        comparedMetaData: undefined,
        stagingPrefix: "temp_staging__",
        historyTableSuffix: "__history"
    };
}

/** Async generator that yields each array in `chunks` */
async function* makeChunks(chunks: Record<string, any>[][]): AsyncIterable<Record<string, any>[]> {
    for (const chunk of chunks) {
        yield chunk;
    }
}

// ---------------------------------------------------------------------------
// Build a minimal mock Database
// ---------------------------------------------------------------------------

function buildMockDb(overrides: Partial<Database> = {}): Database {
    return {
        runWithSchema: (_s: any, fn: any) => fn(),
        runWithSeparators: (_sep: any, fn: any) => fn(),
        getConfig: () => ({
            sqlDialect: "mysql" as const,
            useWorkers: false,
            useStagingInsert: false,
            safeMode: false,
            useSchemaLock: false,
            schemaLockTimeout: 30,
            staging: "temp_staging__",
            historyTableSuffix: "__history",
            insertStack: 100,
            insertType: "UPDATE" as const,
        } as any),
        // Subsequent chunks re-infer to detect schema drift; they compare against the locked schema
        // using the dialect config.
        getDialectConfig: () => mysqlConfig,
        updateSchema: jest.fn(),
        log: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        acquireSchemaLock: jest.fn().mockResolvedValue(undefined),
        releaseSchemaLock: jest.fn().mockResolvedValue(undefined),
        ...overrides
    } as unknown as Database;
}

// ---------------------------------------------------------------------------
// Tests: basic chunked flow (non-staging)
// ---------------------------------------------------------------------------

describe("autoSQLChunked — basic flow (useStagingInsert: false)", () => {
    let handler: AutoSQLHandler;
    let mockDb: Database;

    beforeEach(() => {
        mockDb = buildMockDb();
        handler = new AutoSQLHandler(mockDb as any);

        // Stub internal methods
        (handler as any).prepareInsertData = jest.fn().mockResolvedValue([
            makeInsertInput("users", [{ id: 1 }])
        ]);
        (handler as any).extractNestedInputs = jest.fn().mockResolvedValue([]);
        (handler as any).configureTables = jest.fn().mockResolvedValue([OK]);
        (handler as any).insertData = jest.fn().mockResolvedValue([OK]);
    });

    test("returns success: true for a single chunk", async () => {
        const result = await handler.autoSQLChunked("users", makeChunks([[{ id: 1 }]]));
        expect(result.success).toBe(true);
    });

    test("calls prepareInsertData and configureTables exactly once for multiple chunks", async () => {
        await handler.autoSQLChunked(
            "users",
            makeChunks([[{ id: 1 }], [{ id: 2 }], [{ id: 3 }]])
        );
        expect((handler as any).prepareInsertData).toHaveBeenCalledTimes(1);
        expect((handler as any).configureTables).toHaveBeenCalledTimes(1);
    });

    test("calls insertData once per chunk", async () => {
        await handler.autoSQLChunked(
            "users",
            makeChunks([[{ id: 1 }], [{ id: 2 }], [{ id: 3 }]])
        );
        expect((handler as any).insertData).toHaveBeenCalledTimes(3);
    });

    test("skips empty chunks", async () => {
        await handler.autoSQLChunked(
            "users",
            makeChunks([[], [{ id: 1 }], []])
        );
        expect((handler as any).prepareInsertData).toHaveBeenCalledTimes(1);
        expect((handler as any).insertData).toHaveBeenCalledTimes(1);
    });

    test("returns success with 0 affectedRows for an empty iterable", async () => {
        const result = await handler.autoSQLChunked("users", makeChunks([]));
        expect(result.success).toBe(true);
        expect(result.affectedRows).toBe(0);
    });

    test("subsequent chunks reuse locked schema, not the first chunk's data", async () => {
        const chunk2 = [{ id: 2 }];
        let secondCallInput: InsertInput[] | undefined;

        (handler as any).insertData = jest.fn().mockImplementation(async (inputs: InsertInput[]) => {
            if ((handler as any).insertData.mock.calls.length === 2) {
                secondCallInput = inputs;
            }
            return [OK];
        });

        await handler.autoSQLChunked(
            "users",
            makeChunks([[{ id: 1 }], chunk2])
        );

        // The second insertData call should carry chunk2 as its data
        expect(secondCallInput![0].data).toBe(chunk2);
    });

    test("accumulates affectedRows across chunks", async () => {
        (handler as any).insertData = jest.fn().mockResolvedValue([{ ...OK, affectedRows: 5 }]);
        const result = await handler.autoSQLChunked(
            "users",
            makeChunks([[{ id: 1 }], [{ id: 2 }]])
        );
        expect(result.affectedRows).toBe(10); // 5 per chunk × 2 chunks
    });

    test("returns success: false with error message when prepareInsertData throws", async () => {
        (handler as any).prepareInsertData = jest.fn().mockRejectedValue(new Error("db error"));
        const result = await handler.autoSQLChunked("users", makeChunks([[{ id: 1 }]]));
        expect(result.success).toBe(false);
        expect(result.error).toContain("db error");
    });

    test("returns success: false when insertData throws on a subsequent chunk", async () => {
        let callCount = 0;
        (handler as any).insertData = jest.fn().mockImplementation(async () => {
            callCount++;
            if (callCount === 2) throw new Error("insert failed on chunk 2");
            return [OK];
        });

        const result = await handler.autoSQLChunked(
            "users",
            makeChunks([[{ id: 1 }], [{ id: 2 }]])
        );
        expect(result.success).toBe(false);
        expect(result.error).toContain("insert failed on chunk 2");
    });

    test("forwards early termination to the source chunks iterator (cursor/stream cleanup)", async () => {
        // The load fails on the 2nd chunk, abandoning the source iterator mid-stream. Since the
        // first-chunk peek takes over manual iteration, the source's own `return()` (its cleanup hook)
        // must still be invoked — otherwise a DB-cursor/stream-backed `chunks` would leak.
        let calls = 0;
        (handler as any).insertData = jest.fn().mockImplementation(async () => {
            if (++calls === 2) throw new Error("boom on chunk 2");
            return [OK];
        });

        const ret = jest.fn(async () => ({ value: undefined, done: true }));
        const data = [[{ id: 1 }], [{ id: 2 }], [{ id: 3 }]];
        const source: AsyncIterable<Record<string, any>[]> = {
            [Symbol.asyncIterator]() {
                let i = 0;
                return {
                    next: async () => (i < data.length ? { value: data[i++], done: false } : { value: undefined, done: true }),
                    return: ret,
                } as AsyncIterator<Record<string, any>[]>;
            },
        };

        const result = await handler.autoSQLChunked("users", source);
        expect(result.success).toBe(false);
        expect(ret).toHaveBeenCalled(); // source cleanup ran despite the early abort
    });
});

// ---------------------------------------------------------------------------
// Tests: advisory lock integration in autoSQLChunked
// ---------------------------------------------------------------------------

describe("autoSQLChunked — advisory lock sequencing", () => {
    let handler: AutoSQLHandler;
    let mockDb: Database;

    beforeEach(() => {
        mockDb = buildMockDb({
            getConfig: () => ({
                sqlDialect: "mysql" as const,
                useWorkers: false,
                useStagingInsert: false,
                safeMode: false,
                useSchemaLock: true,
                schemaLockTimeout: 10,
                staging: "temp_staging__",
                historyTableSuffix: "__history",
                insertStack: 100,
                insertType: "UPDATE" as const,
            } as any)
        });
        handler = new AutoSQLHandler(mockDb as any);

        (handler as any).prepareInsertData = jest.fn().mockResolvedValue([
            makeInsertInput("users", [{ id: 1 }])
        ]);
        (handler as any).extractNestedInputs = jest.fn().mockResolvedValue([]);
        (handler as any).configureTables = jest.fn().mockResolvedValue([OK]);
        (handler as any).insertData = jest.fn().mockResolvedValue([OK]);
    });

    test("acquireSchemaLock is called once before first chunk DDL", async () => {
        await handler.autoSQLChunked("users", makeChunks([[{ id: 1 }], [{ id: 2 }]]));
        expect(mockDb.acquireSchemaLock).toHaveBeenCalledTimes(1);
        expect(mockDb.acquireSchemaLock).toHaveBeenCalledWith("users", 10);
    });

    test("releaseSchemaLock is called once after first chunk DDL", async () => {
        await handler.autoSQLChunked("users", makeChunks([[{ id: 1 }], [{ id: 2 }]]));
        expect(mockDb.releaseSchemaLock).toHaveBeenCalledTimes(1);
        expect(mockDb.releaseSchemaLock).toHaveBeenCalledWith("users");
    });

    test("releaseSchemaLock is called even when configureTables throws", async () => {
        (handler as any).configureTables = jest.fn().mockRejectedValue(new Error("DDL failed"));
        await handler.autoSQLChunked("users", makeChunks([[{ id: 1 }]]));
        expect(mockDb.releaseSchemaLock).toHaveBeenCalledTimes(1);
    });

    test("lock is not held during insertData (released before insert)", async () => {
        const callOrder: string[] = [];
        (mockDb.releaseSchemaLock as jest.Mock).mockImplementation(async () => {
            callOrder.push("release");
        });
        (handler as any).insertData = jest.fn().mockImplementation(async () => {
            callOrder.push("insert");
            return [OK];
        });

        await handler.autoSQLChunked("users", makeChunks([[{ id: 1 }]]));
        expect(callOrder.indexOf("release")).toBeLessThan(callOrder.indexOf("insert"));
    });
});

// ---------------------------------------------------------------------------
// Tests: autoSQLChunked does not call acquireSchemaLock when useSchemaLock: false
// ---------------------------------------------------------------------------

describe("autoSQLChunked — lock skipped when useSchemaLock: false", () => {
    test("acquireSchemaLock is NOT called when useSchemaLock is false", async () => {
        const mockDb = buildMockDb(); // useSchemaLock: false
        const handler = new AutoSQLHandler(mockDb as any);

        (handler as any).prepareInsertData = jest.fn().mockResolvedValue([
            makeInsertInput("users", [{ id: 1 }])
        ]);
        (handler as any).extractNestedInputs = jest.fn().mockResolvedValue([]);
        (handler as any).configureTables = jest.fn().mockResolvedValue([OK]);
        (handler as any).insertData = jest.fn().mockResolvedValue([OK]);

        await handler.autoSQLChunked("users", makeChunks([[{ id: 1 }]]));
        expect(mockDb.acquireSchemaLock).not.toHaveBeenCalled();
        expect(mockDb.releaseSchemaLock).not.toHaveBeenCalled();
    });
});
