import { resolve } from "path";
import WorkerPool, { stripNonCloneable } from "../src/workers/workerPool";

const CRASH_WORKER = resolve(process.cwd(), "tests", "fixtures", "crashWorker.js");

// A8: the config handed to a worker goes through structured clone, which cannot serialise functions or
// live handles. A configured `logger` (or a live sshClient/sshStream) threw DataCloneError synchronously
// and crashed the default-on worker path. The pool now strips those fields before spawning.
describe("worker config is cloneable (A8)", () => {
    test("stripNonCloneable removes functions/live handles, keeps everything else", () => {
        const cfg = {
            sqlDialect: "pgsql", host: "h", port: 5432, maxWorkers: 4,
            logger: { warn: () => {} }, sshClient: {}, sshStream: {},
        };
        const s = stripNonCloneable(cfg);
        expect(s).toEqual({ sqlDialect: "pgsql", host: "h", port: 5432, maxWorkers: 4 });
        expect(s.logger).toBeUndefined();
        expect(s.sshClient).toBeUndefined();
        expect(s.sshStream).toBeUndefined();
    });

    test("a pool spawns and runs a task even when the config carries a logger (no DataCloneError)", async () => {
        // Pre-fix, constructing the Worker with { workerData: { dbConfig: { logger } } } throws synchronously.
        const pool = new WorkerPool(2, { sqlDialect: "pgsql", host: "x", logger: console }, CRASH_WORKER);
        try {
            const r = await pool.runTask("ok", "hello");
            expect(r.success).toBe(true);
            expect(r.result).toBe("hello");
        } finally {
            await pool.close();
        }
    });

    // A14: close() asks each worker to close its DB connections (graceful) before terminating, so pooled
    // server-side connections aren't leaked. The fixture acks the handshake, so close resolves promptly.
    test("close() completes the graceful-shutdown handshake", async () => {
        const pool = new WorkerPool(2, { sqlDialect: "pgsql", host: "x" }, CRASH_WORKER);
        await pool.runTask("ok", "a");
        const start = Date.now();
        await pool.close(2000);
        expect(Date.now() - start).toBeLessThan(1500); // acked, didn't wait out the grace timeout
    });
});
