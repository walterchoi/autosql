import { parentPort, workerData } from "worker_threads";
import { Database } from "../db/database";
import { AutoSQLHandler } from "../db/autosql";

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
        let db : Database
        db = Database.create(dbConfig);

        const { method, params } = task;

        if (method === "test") {
            const randomTimeInMs = Math.random() * 500;
            const startTime = db.startDate
            const result = `${startTime}, ${params}`
            await new Promise(resolve => setTimeout(resolve, randomTimeInMs)); // Simulate async delay
            parentPort?.postMessage({ success: true, result: result });
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
