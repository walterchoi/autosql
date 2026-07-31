// Plain-JS worker fixture for the WorkerPool unit tests (F4 — dead worker must
// not hang the pool). A real Worker thread loads this file directly (no ts-jest
// transform), so it must be runnable Node CommonJS. WorkerPool is pointed at this
// file via its test-only `workerFile` constructor override.
const { parentPort } = require("worker_threads");

parentPort.on("message", (task) => {
  const { method, params } = task;
  switch (method) {
    case "ok":
      // Normal completion — echoes params back so healthy work is verifiable.
      parentPort.postMessage({ success: true, result: params });
      break;
    case "crash":
      // Die mid-task with no reply and NO 'error' event — only 'exit' fires.
      // This is the nastiest F4 case: without an 'exit' handler the resolver hangs.
      process.exit(1);
      break;
    case "throw":
      // Uncaught synchronous throw -> parent 'error' event, then 'exit'.
      throw new Error("worker boom");
    case "hang":
      // Never reply -> exercises the per-task timeout.
      break;
    default:
      parentPort.postMessage({ success: false, error: "unknown method " + method });
  }
});
