import { Worker } from "worker_threads";
import { resolve } from "path";
import { existsSync } from "fs";

class WorkerPool {
  private workers: Worker[] = [];
  private idleWorkers: Worker[] = [];
  private pendingTasks: { method: string; params: any; resolve: (value: any) => void }[] = [];
  private workerPending: Map<Worker, (value: any) => void> = new Map();
  private workerFile: string;

  constructor(size: number, private dbConfig: any) {
    this.workerFile = resolve(__dirname, "worker.js");

    if (!existsSync(this.workerFile)) {
      throw new Error(
        `WORKER_UNAVAILABLE: compiled worker not found at ${this.workerFile}. ` +
        `Run the TypeScript compiler first, or set useWorkers: false to skip worker threads.`
      );
    }

    for (let i = 0; i < size; i++) {
      const worker = new Worker(this.workerFile, {
        workerData: { dbConfig }
      });

      worker.on("message", (msg) => {
        const pendingResolve = this.workerPending.get(worker);
        if (pendingResolve) {
          this.workerPending.delete(worker);
          pendingResolve(msg);
        }

        const nextTask = this.pendingTasks.shift();
        if (nextTask) {
          this.workerPending.set(worker, nextTask.resolve);
          worker.postMessage({ method: nextTask.method, params: nextTask.params });
        } else {
          this.idleWorkers.push(worker);
        }
      });

      worker.on("error", (err) => {
        const pendingResolve = this.workerPending.get(worker);
        if (pendingResolve) {
          this.workerPending.delete(worker);
          pendingResolve({ success: false, error: err.message });
        }
        // Drain any remaining queued tasks for this worker with an error result
        const nextTask = this.pendingTasks.shift();
        if (nextTask) {
          nextTask.resolve({ success: false, error: err.message });
        }
      });

      this.workers.push(worker);
      this.idleWorkers.push(worker);
    }
  }

  runTask(method: string, params: any): Promise<any> {
    return new Promise((resolve) => {
      const idleWorker = this.idleWorkers.pop();
      if (idleWorker) {
        this.workerPending.set(idleWorker, resolve);
        idleWorker.postMessage({ method, params });
      } else {
        this.pendingTasks.push({ method, params, resolve });
      }
    });
  }

  close() {
    this.workers.forEach((worker) => worker.terminate());
  }
}

export default WorkerPool;
