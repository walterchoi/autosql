import { parentPort, workerData } from "worker_threads";
import { Database } from "../db/database";
import type { AutoSQLHandler } from "../db/autosql";

// Fire-and-forget: this IIFE is the whole worker_thread entry script. Nothing in this file awaits
// it (there is no caller to await it against) — every path inside already reports its outcome via
// parentPort.postMessage, so an unhandled rejection here would mean the IIFE's own try/catch failed.
void (async () => {
  try {
    // Ensure workerData is received properly
    if (!workerData || !workerData.dbConfig) {
      throw new Error("workerData is missing or invalid! Ensure it's passed correctly.");
    }

    // Extract database config, method, and params
    const { dbConfig } = workerData;
    let db : Database
    db = Database.create(dbConfig);
    const autoSQL = db.autoSQLHandler as AutoSQLHandler;

    // Listener must return void, not a Promise (no-misused-promises) — the inner async body
    // already catches every path below and reports it via postMessage, so nothing can reject out
    // of this handler for a caller to miss.
    parentPort?.on("message", (task) => void (async () => {
        try {
            const { method, params } = task;

            if (method === "__closeConnection__") {
                // Graceful shutdown (A14): close DB connections before the pool terminates the thread
                // so pooled server-side connections aren't leaked. Pool waits (bounded) for the ack.
                try { await db.closeConnection(); } catch { /* best effort */ }
                parentPort?.postMessage({ __closed__: true });
                return;
            }

            const normalizedParams = Array.isArray(params) ? params : [params];

            if (method === "test") {
                const randomTimeInMs = Math.random() * 500;
                const startTime = new Date();
                const result = `${startTime}, ${params}`
                await new Promise(resolve => setTimeout(resolve, randomTimeInMs));
                parentPort?.postMessage({ success: true, result: result });
            } else if (typeof autoSQL[method as keyof AutoSQLHandler] === "function") {
                const result = await (autoSQL[method as keyof AutoSQLHandler] as Function)(...normalizedParams);
                parentPort?.postMessage({ success: true, result });
            } else {
                throw new Error(`Invalid method: ${method}`);
            }
        } catch (error) {
            parentPort?.postMessage({
                success: false,
                error: error instanceof Error ? error.message : String(error),
                errorCode: (error as any)?.code != null ? String((error as any).code) : undefined,
            });
        }
    })());

    } catch (error) {
        parentPort?.postMessage({
        success: false,
        error: error instanceof Error ? error.message : String(error),
        errorCode: (error as any)?.code != null ? String((error as any).code) : undefined,
        });
    }
})();