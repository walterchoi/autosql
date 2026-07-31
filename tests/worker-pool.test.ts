import { resolve } from "path";
import WorkerPool from "../src/workers/workerPool";

// Resolve the crash fixture from the repo root (jest runs with cwd = rootDir),
// avoiding any __dirname/ESM ambiguity in the transformed test module.
const CRASH_WORKER = resolve(process.cwd(), "tests", "fixtures", "crashWorker.js");

// A regression here is a HANG, not a wrong value. Race every awaited task against
// a deadline so a reintroduced hang FAILS fast instead of stalling for the full
// jest testTimeout.
function withDeadline<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, reject) => {
      const t = setTimeout(
        () => reject(new Error(`TIMED OUT (${label}) after ${ms}ms — WorkerPool hung (F4 regression)`)),
        ms
      );
      if (typeof t.unref === "function") t.unref();
    }),
  ]);
}

describe("WorkerPool — F4: a dead/wedged worker must never hang the pool", () => {
  it("healthy tasks still complete and return their results", async () => {
    const pool = new WorkerPool(2, {}, CRASH_WORKER);
    try {
      const results = await withDeadline(
        Promise.all([pool.runTask("ok", "a"), pool.runTask("ok", "b"), pool.runTask("ok", "c")]),
        5000,
        "healthy"
      );
      expect(results.map((r: any) => r.success)).toEqual([true, true, true]);
      expect(results.map((r: any) => r.result)).toEqual(["a", "b", "c"]);
    } finally {
      pool.close();
    }
  });

  it("returns a failure envelope (does not hang) when the in-flight worker dies", async () => {
    const pool = new WorkerPool(1, {}, CRASH_WORKER);
    try {
      const result = await withDeadline(pool.runTask("crash", { id: 1 }), 5000, "single crash");
      expect(result.success).toBe(false);
      expect(typeof result.error).toBe("string");
    } finally {
      pool.close();
    }
  });

  it("surfaces an uncaught worker error as a failure envelope", async () => {
    const pool = new WorkerPool(1, {}, CRASH_WORKER);
    try {
      const result = await withDeadline(pool.runTask("throw", {}), 5000, "throw");
      expect(result.success).toBe(false);
      expect(String(result.error)).toContain("boom");
    } finally {
      pool.close();
    }
  });

  it("fails every task when more tasks than workers are queued and the only worker dies", async () => {
    // 1 worker, 3 tasks submitted together: task 0 crashes the worker; tasks 1 & 2
    // sit in the queue and must be drained as failures rather than hang.
    const pool = new WorkerPool(1, {}, CRASH_WORKER);
    try {
      const results = await withDeadline(
        Promise.all([
          pool.runTask("crash", { id: 0 }),
          pool.runTask("crash", { id: 1 }),
          pool.runTask("crash", { id: 2 }),
        ]),
        5000,
        "queued crash"
      );
      expect(results).toHaveLength(3);
      expect(results.every((r: any) => r.success === false)).toBe(true);
    } finally {
      pool.close();
    }
  });

  it("fails fast for a task submitted AFTER every worker has already died", async () => {
    // This is the WorkerHelper path: the next task is dispatched from the previous
    // task's .then, i.e. after the crash already emptied the pool. Without the
    // runTask fast-fail guard this task would queue against a dead pool and hang.
    const pool = new WorkerPool(1, {}, CRASH_WORKER);
    try {
      const first = await withDeadline(pool.runTask("crash", { id: 0 }), 5000, "prime crash");
      expect(first.success).toBe(false);

      const afterDeath = await withDeadline(pool.runTask("ok", { id: 1 }), 5000, "post-death submit");
      expect(afterDeath.success).toBe(false);
    } finally {
      pool.close();
    }
  });

  it("times out an alive-but-wedged worker when workerTaskTimeout is set", async () => {
    // workerTaskTimeout is in SECONDS.
    const pool = new WorkerPool(1, { workerTaskTimeout: 0.3 }, CRASH_WORKER);
    try {
      const result = await withDeadline(pool.runTask("hang", {}), 5000, "timeout");
      expect(result.success).toBe(false);
      expect(String(result.error)).toMatch(/timed out/i);
    } finally {
      pool.close();
    }
  });
});
