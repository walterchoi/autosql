import WorkerPool from "./workerPool";
import { DatabaseConfig } from "../config/types";
import { defaults } from "../config/defaults";

class WorkerHelper {
  private static workerPool: WorkerPool | null = null;

  static async run(dbConfig: DatabaseConfig, method: string, paramsArray: any[], workerSize: number = defaults.maxWorkers) {
    // ✅ Initialize worker pool only once
    if (!this.workerPool) {
      this.workerPool = new WorkerPool(workerSize, dbConfig);
    }

    const workerPromises: Promise<any>[] = [];
    let activeWorkers = 0;
    let taskIndex = 0;
    
    return new Promise((resolve) => {
      const results: any[] = [];

      const processNextTask = () => {
        if (taskIndex >= paramsArray.length) {
          // ✅ Check if there are no active workers & no remaining tasks
          if (activeWorkers === 0) {
            console.log(`\n✅ All Workers Completed, completed: ${taskIndex - 1} tasks ✅`);
            resolve(results);
            this.closePool();
          }
          return;
        }

        if (activeWorkers < workerSize) {
          // ✅ Assign a task to an available worker
          const currentTaskIndex = taskIndex++;
          const params = paramsArray[currentTaskIndex];

          activeWorkers++;
          const workerPromise = this.workerPool!.runTask(method, params).then((result) => {
            results[currentTaskIndex] = result;
            activeWorkers--;
            processNextTask(); // Assign next task when a worker becomes available
            return result;
          });

          workerPromises.push(workerPromise);
        }
      };

      // ✅ Start the initial batch of tasks
      for (let i = 0; i < workerSize && i < paramsArray.length; i++) {
        processNextTask();
      }
    });
  }

  private static closePool() {
    if (this.workerPool) {
      this.workerPool.close();
      this.workerPool = null;
    }
  }
}

export default WorkerHelper;
