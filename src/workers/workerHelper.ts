import WorkerPool from "./workerPool";
import { DatabaseConfig } from "../config/types";
import { defaults } from "../config/defaults";

class WorkerHelper {
  static async run(dbConfig: DatabaseConfig, method: string, paramsArray: any[], workerSize: number = defaults.maxWorkers) {
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
            workerPool.close();
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
