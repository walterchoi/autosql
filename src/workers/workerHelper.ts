import WorkerPool from "./workerPool";
import type { DatabaseConfig } from "../config/types";
import type { WorkerTaskResult } from "./workerTypes";
import { DEFAULTS } from "../config/defaults";

// A namespace of related helpers, not a data constant (was `class X { static ... }`; see
// no-extraneous-class).
// eslint-disable-next-line @typescript-eslint/naming-convention
const WorkerHelper = {
  async run(dbConfig: DatabaseConfig, method: string, paramsArray: unknown[], workerSize: number = dbConfig.maxWorkers ?? DEFAULTS.maxWorkers): Promise<WorkerTaskResult[]> {
    // Nothing to dispatch. Without this, the loop below never runs for an empty array (its
    // bound is `i < paramsArray.length`), so processNextTask — the only place that resolves the
    // returned promise — is never called, and run() hangs forever instead of resolving to [].
    if (paramsArray.length === 0) return [];

    // Never spawn more workers than there are tasks (A8): configuring a single table shouldn't stand up
    // an 8-thread pool, each with its own DB connection pool.
    workerSize = Math.max(1, Math.min(workerSize, paramsArray.length || 1));
    const workerPool = new WorkerPool(workerSize, dbConfig);

    const workerPromises: Promise<WorkerTaskResult>[] = [];
    let activeWorkers = 0;
    let taskIndex = 0;

    return new Promise((resolve) => {
      const results: WorkerTaskResult[] = [];

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
};

export default WorkerHelper;
