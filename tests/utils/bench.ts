/**
 * Tiny benchmarking helper for the performance regression suite.
 *
 * Philosophy: absolute timings vary with hardware and (for live tests) the database, so we do NOT
 * assert hard millisecond thresholds as the primary guard — those flake. Instead:
 *   - `scalingRatio` lets a test assert an operation stays ~linear (catches an O(n²) regression
 *     independent of machine speed: 10× the data should be well under ~20× the time, never ~100×).
 *   - `bench` reports median/min/max + throughput so trends are visible in CI logs.
 *   - Generous absolute ceilings are used only as a coarse backstop against catastrophic slowdowns.
 */

export interface BenchResult {
    label: string;
    medianMs: number;
    minMs: number;
    maxMs: number;
    runs: number;
}

/** Time an async fn: `warmup` untimed runs (JIT/caches), then `iterations` timed runs; report median. */
export async function bench(
    label: string,
    fn: () => Promise<void> | void,
    opts: { iterations?: number; warmup?: number } = {},
): Promise<BenchResult> {
    const iterations = opts.iterations ?? 3;
    const warmup = opts.warmup ?? 1;
    for (let i = 0; i < warmup; i++) await fn();
    const times: number[] = [];
    for (let i = 0; i < iterations; i++) {
        const t0 = process.hrtime.bigint();
        await fn();
        const t1 = process.hrtime.bigint();
        times.push(Number(t1 - t0) / 1e6);
    }
    times.sort((a, b) => a - b);
    const medianMs = times[Math.floor(times.length / 2)];
    return { label, medianMs, minMs: times[0], maxMs: times[times.length - 1], runs: iterations };
}

/** Console line for CI visibility. `units` (e.g. row count) adds a throughput figure. */
export function reportBench(r: BenchResult, units?: number): void {
    const thr = units ? `  (${Math.round((units / r.medianMs) * 1000).toLocaleString()} rows/s)` : "";
    // eslint-disable-next-line no-console
    console.log(`  ⏱  ${r.label}: median ${r.medianMs.toFixed(1)}ms  [min ${r.minMs.toFixed(1)} / max ${r.maxMs.toFixed(1)}, n=${r.runs}]${thr}`);
}

/**
 * Ratio of per-op time at the larger size vs the smaller size, normalised by the size multiple.
 * ~1.0 means linear scaling. A value that balloons (e.g. >2) signals superlinear (algorithmic)
 * regression regardless of absolute machine speed. Returns { ratio, normalized }.
 */
export function scalingRatio(smallMs: number, smallN: number, largeMs: number, largeN: number): { ratio: number; normalized: number } {
    const ratio = largeMs / smallMs;
    const sizeMultiple = largeN / smallN;
    return { ratio, normalized: ratio / sizeMultiple };
}
