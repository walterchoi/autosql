jest.mock("../src/workers/workerPool");
import WorkerPool from "../src/workers/workerPool";
import WorkerHelper from "../src/workers/workerHelper";
import type { DatabaseConfig } from "../src/config/types";

// WorkerHelper.run's own clamping/queue-draining logic, mocked at the WorkerPool boundary so no
// real worker threads are needed — fast, and lets us assert the exact constructor args and
// completion-order-independent result alignment that matter here. Never called directly by any
// existing test; only informally approximated via raw WorkerPool calls elsewhere.

const CONFIG: DatabaseConfig = { sqlDialect: "mysql" };
const mockWorkerPool = WorkerPool as unknown as jest.Mock;

beforeEach(() => mockWorkerPool.mockClear());

describe("WorkerHelper.run — workerSize clamping", () => {
    test("clamps workerSize down to paramsArray.length when fewer tasks than requested workers", async () => {
        mockWorkerPool.mockImplementation(function (this: any) {
            this.runTask = jest.fn().mockResolvedValue({ success: true, result: "x" });
            this.close = jest.fn().mockResolvedValue(undefined);
        });

        await WorkerHelper.run(CONFIG, "m", ["a", "b"], 10); // 10 workers requested, only 2 tasks

        expect(mockWorkerPool).toHaveBeenCalledTimes(1);
        expect(mockWorkerPool.mock.calls[0][0]).toBe(2); // clamped
    });

    test("never goes below 1 worker even when workerSize is requested as 0", async () => {
        mockWorkerPool.mockImplementation(function (this: any) {
            this.runTask = jest.fn().mockResolvedValue({ success: true, result: "x" });
            this.close = jest.fn().mockResolvedValue(undefined);
        });

        await WorkerHelper.run(CONFIG, "m", ["a", "b", "c"], 0);

        expect(mockWorkerPool.mock.calls[0][0]).toBe(1);
    });

    test("an empty paramsArray resolves to [] immediately and never constructs a WorkerPool at all", async () => {
        const result = await WorkerHelper.run(CONFIG, "m", [], 5);
        expect(result).toEqual([]);
        expect(mockWorkerPool).not.toHaveBeenCalled();
    });
});

describe("WorkerHelper.run — result alignment under out-of-order completion", () => {
    test("results land at the correct index regardless of which task's worker finishes first", async () => {
        // Task 0 is the SLOWEST, task 2 the FASTEST — if results were pushed in completion
        // order instead of assigned by index, this would come back shuffled.
        const delays: Record<string, number> = { a: 30, b: 15, c: 0 };
        mockWorkerPool.mockImplementation(function (this: any) {
            this.runTask = jest.fn((_method: string, param: string) =>
                new Promise((res) => setTimeout(() => res({ success: true, result: param.toUpperCase() }), delays[param]))
            );
            this.close = jest.fn().mockResolvedValue(undefined);
        });

        const results = await WorkerHelper.run(CONFIG, "m", ["a", "b", "c"], 3);

        expect(results.map((r: any) => r.result)).toEqual(["A", "B", "C"]); // input order, not completion order
    });

    test("a queue larger than the worker count drains every task, still in input order", async () => {
        mockWorkerPool.mockImplementation(function (this: any) {
            this.runTask = jest.fn((_method: string, param: string) => Promise.resolve({ success: true, result: param }));
            this.close = jest.fn().mockResolvedValue(undefined);
        });

        const results = await WorkerHelper.run(CONFIG, "m", ["a", "b", "c", "d", "e"], 2); // 2 workers, 5 tasks

        expect(results.map((r: any) => r.result)).toEqual(["a", "b", "c", "d", "e"]);
    });
});
