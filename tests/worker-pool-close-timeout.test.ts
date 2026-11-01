import { resolve } from "path";
import WorkerPool from "../src/workers/workerPool";

// WorkerPool.close()'s gracefulTerminate: the bounded timeout (A14) that stops a wedged worker
// (one that never acks __closeConnection__) from hanging teardown forever. Every existing close()
// test uses crashWorker.js, which always acks close promptly — the entire reason the timeout
// exists had zero coverage.

const WEDGED_WORKER = resolve(process.cwd(), "tests", "fixtures", "wedgedCloseWorker.js");

function withDeadline<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, reject) => {
      const t = setTimeout(() => reject(new Error(`TIMED OUT (${label}) after ${ms}ms`)), ms);
      if (typeof t.unref === "function") t.unref();
    }),
  ]);
}

describe("WorkerPool.close() — graceful-termination timeout for a wedged worker", () => {
    test("close() resolves within the grace bound even when the worker never acks __closeConnection__", async () => {
        const pool = new WorkerPool(1, {}, WEDGED_WORKER);
        // Prime a real worker thread (close() only terminates workers that exist).
        const ok = await withDeadline(pool.runTask("ok", "primed"), 5000, "prime");
        expect(ok.success).toBe(true);

        const graceMs = 300;
        const start = Date.now();
        await withDeadline(pool.close(graceMs), graceMs + 2000, "close");
        const elapsed = Date.now() - start;

        // Resolved via the timeout branch, not an instant ack — bounded close, not a hang.
        expect(elapsed).toBeGreaterThanOrEqual(graceMs - 50);
        expect(elapsed).toBeLessThan(graceMs + 2000);
    });

    test("multiple wedged workers all terminate within one shared grace bound (not graceMs each)", async () => {
        const pool = new WorkerPool(3, {}, WEDGED_WORKER);
        await withDeadline(
            Promise.all([pool.runTask("ok", 1), pool.runTask("ok", 2), pool.runTask("ok", 3)]),
            5000,
            "prime 3"
        );

        const graceMs = 300;
        const start = Date.now();
        await withDeadline(pool.close(graceMs), graceMs + 2000, "close 3");
        const elapsed = Date.now() - start;

        // All 3 terminate in parallel (Promise.all in close()), so this is still ~one graceMs
        // window, not 3x it.
        expect(elapsed).toBeLessThan(graceMs + 2000);
    });
});
