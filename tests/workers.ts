import WorkerPool from "../src/workers/workerPool"; // Ensure .js for ESM
import { DB_CONFIG } from "./utils/testConfig"; // Ensure .js for ESM

async function runWorkerTests() {
    const dbConfig = Object.values(DB_CONFIG)[0]; // Pick one DB config
    const pool = new WorkerPool(10); // Create a pool with 10 workers

    console.log("Starting worker tests...");

    const workerPromises: Promise<any>[] = [];

    for (let i = 1; i <= 10; i++) {
        const params = [`Worker-${i}`, `Task-${i}`];

        const workerPromise = pool.runTask(dbConfig, "test", params).then((result) => {
            console.log(`Worker ${i} completed:`, result);
            return result;
        });

        workerPromises.push(workerPromise);
    }

    // Wait for all workers to finish
    const results = await Promise.all(workerPromises);

    // Log the results
    console.log("\n✅ All Workers Completed ✅");
    results.forEach((result, index) => {
        console.log(`Worker ${index + 1} Result:`, result);
    });

    // Close the worker pool
    pool.close();
}

runWorkerTests();
