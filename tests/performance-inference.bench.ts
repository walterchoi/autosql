import { getMetaData, getDataHeaders } from "../src/helpers/metadata";
import { DatabaseConfig } from "../src/config/types";
import { makeRows } from "./utils/fakeData";
import { bench, reportBench, scalingRatio } from "./utils/bench";

// Opt-in performance regression guard for the inference hot path (pure CPU, no database — the most
// stable thing to benchmark). Run with `npm run bench`. Primary assertion is a SCALING invariant:
// 10× the rows must stay well under ~20× the time, so an accidental O(n²) regression trips regardless
// of machine speed. Absolute ceilings are a coarse backstop. Timings are logged for trend visibility.

const CONFIG: DatabaseConfig = { sqlDialect: "pgsql", autoIndexing: true };

describe("bench: inference (CPU-only regression guard)", () => {
    test("getMetaData scales ~linearly with row count (no O(n²) regression)", async () => {
        const SMALL = 2_000;
        const LARGE = 20_000; // 10× SMALL
        const small = makeRows(SMALL);
        const large = makeRows(LARGE);

        const rSmall = await bench(`inference ${SMALL} rows`, async () => { await getMetaData(CONFIG, small); }, { warmup: 1, iterations: 2 });
        const rLarge = await bench(`inference ${LARGE} rows`, async () => { await getMetaData(CONFIG, large); }, { warmup: 1, iterations: 2 });
        reportBench(rSmall, SMALL);
        reportBench(rLarge, LARGE);

        const { ratio, normalized } = scalingRatio(rSmall.medianMs, SMALL, rLarge.medianMs, LARGE);
        // eslint-disable-next-line no-console
        console.log(`  ↳ 10× rows took ${ratio.toFixed(1)}× time (normalized ${normalized.toFixed(2)}; ~1.0 = linear)`);
        expect(normalized).toBeLessThan(3); // linear≈1, quadratic≈10 — generous, catches algorithmic regressions
        expect(rLarge.medianMs).toBeLessThan(10_000); // coarse catastrophic-slowdown backstop
    });

    // NOTE (finding): the per-value bottleneck is `sqlize` + uniqueSet insert + `Buffer.byteLength`,
    // NOT predictType's regex — so the native fast-path is a small (single-digit %) win, within timing
    // noise, and is NOT a reliable pass/fail guard. Report throughput as a trend metric only; assert a
    // generous absolute ceiling. The real inference-throughput win lives in the per-value work above.
    test("native-typed inference throughput (report + coarse ceiling)", async () => {
        const N = 8_000;
        const nativeRows = makeRows(N);
        const r = await bench("infer native-typed", async () => { await getDataHeaders(nativeRows, CONFIG); }, { warmup: 1, iterations: 2 });
        reportBench(r, N);
        expect(r.medianMs).toBeLessThan(10_000); // coarse catastrophic-slowdown backstop
    });
});
