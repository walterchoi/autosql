import { parentPort, workerData } from "worker_threads";
import { Database } from "../db/database";
import { driverErrorCode } from "../db/database";
import type { AutoSQLHandler } from "../db/autosql";
import type { WorkerTaskMessage, WorkerResultMessage } from "./workerTypes";

// This IIFE is the whole worker_thread entry script, run once at startup; every path inside
// reports its outcome via parentPort.postMessage. Synchronous top to bottom (the real async work
// lives in the per-message handler below), so no `async`/`void` wrapper is needed here — a
// synchronous throw is still caught by the try/catch right below.
(() => {
  try {
    if (!workerData || !workerData.dbConfig) {
      throw new Error("workerData is missing or invalid! Ensure it's passed correctly.");
    }

    const { dbConfig } = workerData;
    const db: Database = Database.create(dbConfig);
    const autoSQL = db.autoSQLHandler;

    // Listener must return void, not a Promise (no-misused-promises) — the inner async body
    // already catches every path below and reports it via postMessage, so nothing can reject out
    // of this handler for a caller to miss.
    parentPort?.on("message", (task: WorkerTaskMessage) => void (async () => {
        try {
            const { method, params } = task;

            if (method === "__closeConnection__") {
                // Graceful shutdown (A14): close DB connections before the pool terminates the thread
                // so pooled server-side connections aren't leaked. Pool waits (bounded) for the ack.
                try { await db.closeConnection(); } catch { /* best effort */ }
                const closedMsg: WorkerResultMessage = { __closed__: true };
                parentPort?.postMessage(closedMsg);
                return;
            }

            const normalizedParams = Array.isArray(params) ? params : [params];

            let resultMsg: WorkerResultMessage;
            if (method === "test") {
                const randomTimeInMs = Math.random() * 500;
                const startTime = new Date();
                const result = `${startTime.toISOString()}, ${String(params)}`
                await new Promise(resolve => setTimeout(resolve, randomTimeInMs));
                resultMsg = { success: true, result };
            } else if (typeof autoSQL[method as keyof AutoSQLHandler] === "function") {
                const result = await (autoSQL[method as keyof AutoSQLHandler] as (...args: unknown[]) => unknown)(...normalizedParams);
                resultMsg = { success: true, result };
            } else {
                throw new Error(`Invalid method: ${method}`);
            }
            parentPort?.postMessage(resultMsg);
        } catch (error) {
            const errorMsg: WorkerResultMessage = {
                success: false,
                error: error instanceof Error ? error.message : String(error),
                errorCode: driverErrorCode(error),
            };
            parentPort?.postMessage(errorMsg);
        }
    })());

    } catch (error) {
        const errorMsg: WorkerResultMessage = {
            success: false,
            error: error instanceof Error ? error.message : String(error),
            errorCode: driverErrorCode(error),
        };
        parentPort?.postMessage(errorMsg);
    }
})();