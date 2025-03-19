import { Worker } from "worker_threads";
import { resolve } from "path";

class WorkerPool {
  private workers: Worker[] = [];
  private queue: { resolve: (value: any) => void; task: any }[] = [];
  private workerFile: string;

  constructor(size: number, private dbConfig: any) {
    // Ensure the correct worker file path
    this.workerFile = resolve(__dirname, "worker.js"); // Match compiled file

    for (let i = 0; i < size; i++) {
      // ✅ Pass `workerData` to each worker when created
      const worker = new Worker(this.workerFile, {
        workerData: { dbConfig } // Ensure each worker starts with dbConfig
      });

      worker.on("message", (msg) => {
        const queuedItem = this.queue.shift();
        if (queuedItem) queuedItem.resolve(msg);
      });

      this.workers.push(worker);
    }
  }

  runTask(method: string, params: any[]): Promise<any> {
    return new Promise((resolve) => {
      this.queue.push({ resolve, task: { method, params } });

      const worker = this.workers.pop();
      if (worker) {
        // ✅ Send `method` and `params` to the worker
        worker.postMessage({ method, params });
        this.workers.push(worker);
      }
    });
  }

  close() {
    this.workers.forEach((worker) => worker.terminate());
  }
}

export default WorkerPool;