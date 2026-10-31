// Shared shape for the worker_threads IPC between WorkerPool (parent) and worker.ts (the thread).
// `params`/`result` are genuinely arbitrary — whatever the invoked AutoSQLHandler method takes and
// returns — same reasoning as DataRow in config/types.ts.

export interface WorkerTaskMessage {
    method: string;
    params: unknown;
}

// The outcome of an actual task (never the __closeConnection__ ack below) — what runTask()'s
// Promise resolves to, and what WorkerPool's own bookkeeping (settleWorker, QueuedTask,
// PendingEntry) passes around.
export type WorkerTaskResult =
    | { success: true; result: unknown }
    | { success: false; error: string; errorCode?: string };

// Anything worker.ts can actually postMessage: a task outcome, or the __closeConnection__ shutdown
// ack. Only the raw `worker.on("message", ...)` handler sees this broader type — it filters the
// ack out immediately, so everything downstream of that filter uses WorkerTaskResult instead.
export type WorkerResultMessage = WorkerTaskResult | { __closed__: true };
