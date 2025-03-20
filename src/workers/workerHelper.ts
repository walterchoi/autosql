import WorkerPool from "./workerPool";
import { DatabaseConfig } from "../config/types";
import { defaults } from "../config/defaults";

class WorkerHelper {
  private static workerPool: WorkerPool | null = null;

  static async run(dbConfig: DatabaseConfig, method: string, paramsArray: any[][], workerSize: number = defaults.maxWorkers) {
    // ✅ Initialize worker pool only once
    if (!this.workerPool) {
      this.workerPool = new WorkerPool(workerSize, dbConfig);
    }

    // ✅ Assign each worker 1 task from paramsArray
    const workerPromises: Promise<any>[] = paramsArray.map((params, index) =>
      this.workerPool!.runTask(method, params).then((result) => {
        return result;
      })
    );

    // ✅ Wait for all workers to complete their tasks
    const results = await Promise.all(workerPromises);

    // ✅ Close the worker pool after execution
    this.closePool();

    return results;
  }

  private static closePool() {
    if (this.workerPool) {
      this.workerPool.close();
      this.workerPool = null;
    }
  }
}

export default WorkerHelper;
