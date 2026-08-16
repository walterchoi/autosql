import WorkerPool from "./workerPool";
import { DatabaseConfig } from "../config/types";
import { defaults } from "../config/defaults";

class WorkerHelper {
  static async run(dbConfig: DatabaseConfig, method: string, paramsArray: any[], workerSize: number = dbConfig.maxWorkers ?? defaults.maxWorkers) {
    // Never spawn more workers than there are tasks (A8): configuring a single table shouldn't stand up
    // an 8-thread pool, each with its own DB connection pool.
    workerSize = Math.max(1, Math.min(workerSize, paramsArray.length || 1));
    const workerPool = new WorkerPool(workerSize, dbConfig);

    const workerPromises: Promise<any>[] = [];
    let activeWorkers = 0;
    let taskIndex = 0;

    return new Promise((resolve) => {
      const results: any[] = [];

      const processNextTask = () => {
        if (taskIndex >= paramsArray.length) {
          if (activeWorkers === 0) {
            resolve(results);
            // Fire-and-forget graceful teardown (closes each worker's DB connections, then terminates).
            workerPool.close().catch(() => {});
          }
          return;
        }

        if (activeWorkers < workerSize) {
          const currentTaskIndex = taskIndex++;
          const params = paramsArray[currentTaskIndex];

          activeWorkers++;
          const workerPromise = workerPool.runTask(method, params).then((result) => {
            results[currentTaskIndex] = result;
            activeWorkers--;
            processNextTask();
            return result;
          });

          workerPromises.push(workerPromise);
        }
      };

      for (let i = 0; i < workerSize && i < paramsArray.length; i++) {
        processNextTask();
      }
    });
  }
}

export default WorkerHelper;
