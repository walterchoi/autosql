import { Worker } from "worker_threads";
import { resolve } from "path";
import { existsSync } from "fs";

// The config is handed to a worker via `workerData`, which goes through structured clone — that CANNOT
// serialise functions or live handles. Strip them (A8): a configured `logger` (function) or a live
// `sshClient`/`sshStream` would throw DataCloneError synchronously and crash the default-on worker
// path. The worker re-derives what it needs from `sshConfig` and simply runs without the custom logger.
export function stripNonCloneable(config: any): any {
  if (!config || typeof config !== "object") return config;
  const { logger, sshClient, sshStream, ...cloneable } = config;
  return cloneable;
}

interface QueuedTask {
  method: string;
  params: any;
  resolve: (value: any) => void;
}

interface PendingEntry {
  resolve: (value: any) => void;
  timer?: NodeJS.Timeout;
}

class WorkerPool {
  private workers: Worker[] = [];
  private idleWorkers: Worker[] = [];
  private pendingTasks: QueuedTask[] = [];
  private workerPending: Map<Worker, PendingEntry> = new Map();
  private workerFile: string;
  private taskTimeoutMs: number;
  private closed = false;
  private dbConfig: any;

  // `workerFile` override is for tests only (point the pool at a crash fixture);
  // production always resolves the compiled worker next to this module.
  constructor(size: number, dbConfig: any, workerFile?: string) {
    // Sanitise ONCE up front so every spawned worker gets a cloneable config (A8).
    this.dbConfig = stripNonCloneable(dbConfig);
    this.workerFile = workerFile ?? resolve(__dirname, "worker.js");

    // Per-task timeout (config is in SECONDS -> ms). 0/undefined disables it.
    // The no-hang guarantee does NOT depend on this: worker death is always caught
    // by the 'error'/'exit' handlers. The timeout only guards an alive-but-wedged
    // worker (e.g. a DB call that never returns). Off by default so a legitimately
    // long-running batch is never spuriously failed.
    const timeoutSec = Number(this.dbConfig?.workerTaskTimeout) || 0;
    this.taskTimeoutMs = timeoutSec > 0 ? timeoutSec * 1000 : 0;

    if (!existsSync(this.workerFile)) {
      throw new Error(
        `WORKER_UNAVAILABLE: compiled worker not found at ${this.workerFile}. ` +
        `Run the TypeScript compiler first, or set useWorkers: false to skip worker threads.`
      );
    }

    for (let i = 0; i < size; i++) {
      this.spawnWorker();
    }
  }

  private spawnWorker(): void {
    const worker = new Worker(this.workerFile, {
      workerData: { dbConfig: this.dbConfig }
    });

    worker.on("message", (msg) => {
      // Ignore the graceful-shutdown ack — it's handled by gracefulTerminate, not a task result (A14).
      if (msg && msg.__closed__) return;
      // Task finished normally: settle it, then hand this worker its next task.
      this.settleWorker(worker, msg);
      this.assignNextTask(worker);
    });

    worker.on("error", (err) => {
      // Uncaught error inside the worker. Node emits 'exit' immediately after;
      // failWorker is idempotent so the follow-up 'exit' is a harmless no-op.
      this.failWorker(worker, err?.message || String(err));
    });

    worker.on("exit", (code) => {
      if (this.closed) return; // expected teardown from close()
      // Worker stopped without returning a result — terminate(), OOM, a native
      // crash, or process.exit inside the worker. Without this handler the task's
      // resolver would never fire and WorkerHelper.run would hang the whole load.
      this.failWorker(worker, `Worker stopped unexpectedly (exit code ${code}) before returning a result`);
    });

    this.workers.push(worker);
    this.idleWorkers.push(worker);
  }

  /**
   * Resolve a worker's in-flight task exactly once and clear its timeout.
   * Returns true if there was a pending task to settle (idempotent thereafter).
   */
  private settleWorker(worker: Worker, value: any): boolean {
    const entry = this.workerPending.get(worker);
    if (!entry) return false;
    this.workerPending.delete(worker);
    if (entry.timer) clearTimeout(entry.timer);
    entry.resolve(value);
    return true;
  }

  /** Give a now-free worker the next queued task, or park it as idle. */
  private assignNextTask(worker: Worker): void {
    if (this.closed || !this.workers.includes(worker)) return;
    const nextTask = this.pendingTasks.shift();
    if (nextTask) {
      this.dispatch(worker, nextTask);
    } else {
      this.idleWorkers.push(worker);
    }
  }

  private dispatch(worker: Worker, task: QueuedTask): void {
    let timer: NodeJS.Timeout | undefined;
    if (this.taskTimeoutMs > 0) {
      timer = setTimeout(() => this.handleTimeout(worker), this.taskTimeoutMs);
      // Don't let the timeout alone keep the process alive.
      if (typeof timer.unref === "function") timer.unref();
    }
    this.workerPending.set(worker, { resolve: task.resolve, timer });
    worker.postMessage({ method: task.method, params: task.params });
  }

  private handleTimeout(worker: Worker): void {
    // Only act if this worker still owns the task we timed (it may have just replied).
    if (!this.workerPending.has(worker)) return;
    // Fail the wedged task and tear the worker down. failWorker settles it and
    // drops it from rotation; terminate() there fires 'exit' (idempotent).
    this.failWorker(worker, `Worker task timed out after ${this.taskTimeoutMs} ms`);
  }

  /**
   * A worker died (or timed out). Settle its in-flight task with a failure
   * envelope, drop it from rotation, and — if no workers remain — fail every
   * queued task so a caller can never hang waiting on a dead pool. Idempotent:
   * safe to call for both the 'error' and the following 'exit'.
   */
  private failWorker(worker: Worker, reason: string): void {
    // Settle whatever this worker was running (no-op if already settled).
    this.settleWorker(worker, { success: false, error: reason });
    // Drop it from rotation.
    this.workers = this.workers.filter((w) => w !== worker);
    this.idleWorkers = this.idleWorkers.filter((w) => w !== worker);
    // Ensure the OS thread is gone (no-op if it already exited).
    worker.terminate().catch(() => {});

    // Pool is now empty: nothing will ever drain the queue, so fail it all rather
    // than leave those resolvers hanging forever.
    if (this.workers.length === 0 && this.pendingTasks.length > 0) {
      const orphaned = this.pendingTasks.splice(0, this.pendingTasks.length);
      for (const task of orphaned) {
        task.resolve({ success: false, error: reason });
      }
    }
  }

  runTask(method: string, params: any): Promise<any> {
    return new Promise((resolve) => {
      // Every worker has already died: fail fast instead of queueing a task that
      // nothing is left to pick up (this is the path WorkerHelper hits when it
      // dispatches the next task after a crash settled the previous one).
      if (this.workers.length === 0) {
        resolve({ success: false, error: "Worker pool has no live workers" });
        return;
      }
      const idleWorker = this.idleWorkers.pop();
      if (idleWorker) {
        this.dispatch(idleWorker, { method, params, resolve });
      } else {
        this.pendingTasks.push({ method, params, resolve });
      }
    });
  }

  async close(graceMs = 2000) {
    this.closed = true;
    // Clear any outstanding task timers so they can't fire during/after teardown.
    for (const entry of this.workerPending.values()) {
      if (entry.timer) clearTimeout(entry.timer);
    }
    this.workerPending.clear();
    const workers = this.workers;
    this.workers = [];
    this.idleWorkers = [];
    // Ask each worker to close its DB connections first, then terminate — so pooled server-side
    // connections are released gracefully instead of leaked on an abrupt kill (A14). Bounded, so a
    // wedged worker can't hang teardown. (Crash/timeout paths in failWorker still terminate abruptly.)
    await Promise.all(workers.map((w) => this.gracefulTerminate(w, graceMs)));
  }

  private gracefulTerminate(worker: Worker, graceMs: number): Promise<void> {
    return new Promise<void>((resolveDone) => {
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        worker.terminate().catch(() => {});
        resolveDone();
      };
      const timer = setTimeout(finish, graceMs);
      if (typeof timer.unref === "function") timer.unref();
      worker.once("message", (msg: any) => {
        if (msg && msg.__closed__) { clearTimeout(timer); finish(); }
      });
      try {
        worker.postMessage({ method: "__closeConnection__", params: [] });
      } catch {
        finish();
      }
    });
  }
}

export default WorkerPool;
