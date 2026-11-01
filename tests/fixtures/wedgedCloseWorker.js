// Worker fixture for WorkerPool.close()'s graceful-termination TIMEOUT branch: acks ordinary
// tasks normally, but deliberately never acks __closeConnection__ — the opposite of
// crashWorker.js, which always acks close promptly. Without the timeout, close() would hang
// forever waiting for an ack this worker will never send.
const { parentPort } = require("worker_threads");

parentPort.on("message", (task) => {
  const { method, params } = task;
  switch (method) {
    case "__closeConnection__":
      // Deliberately never reply — exercises gracefulTerminate's setTimeout(finish, graceMs).
      break;
    case "ok":
      parentPort.postMessage({ success: true, result: params });
      break;
    default:
      parentPort.postMessage({ success: false, error: "unknown method " + method });
  }
});
