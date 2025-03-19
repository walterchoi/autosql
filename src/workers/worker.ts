import { parentPort, workerData } from "worker_threads";

(async () => {
  try {
    // ✅ Ensure workerData is received properly
    if (!workerData || !workerData.dbConfig) {
      throw new Error("workerData is missing or invalid! Ensure it's passed correctly.");
    }

    // Extract database config, method, and params
    const { dbConfig } = workerData;

    console.log(`Worker started with dbConfig: ${JSON.stringify(dbConfig)}`);

    parentPort?.on("message", async (task) => {
      const { method, params } = task;

      if (method === "test") {
        const randomTimeInMs = Math.random() * 500;
        await new Promise(resolve => setTimeout(resolve, randomTimeInMs)); // Simulate async delay
        parentPort?.postMessage({ success: true, result: params });
        return;
      }

      throw new Error(`Invalid method: ${method}`);
    });

  } catch (error) {
    parentPort?.postMessage({
      success: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
})();
