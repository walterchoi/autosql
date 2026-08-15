import { sqlize, validateConfig } from "../src/helpers/utilities";
import { pgsqlConfig } from "../src/db/config/pgsqlConfig";
import { mysqlConfig } from "../src/db/config/mysqlConfig";
import { sqlServerConfig } from "../src/db/config/sqlServerConfig";
import { DatabaseConfig } from "../src/config/types";

// sqlize() produces the exact literal that gets parameter-bound and stored. Two silent-corruption
// bugs lived here (both invisible to UTC-only, default-cap CI):
//   A1 — timezone-naive datetimes were parsed via `new Date()` (LOCAL zone) then re-emitted as UTC,
//        shifting the wall-clock by the host offset on any non-UTC machine.
//   A4 — decimal rounding truncated downward instead of rounding half-up (currency bias).
// This suite pins the stored VALUE, not just inferred column shape.

// --- Regression teeth: these tests only prove A1 is fixed if the process is NOT in UTC. jest.config.js
// forces Australia/Sydney; assert it took, so a UTC harness fails loud instead of false-greening. ---
describe("harness sanity", () => {
    test("tests run in a non-UTC timezone (else the A1 date tests have no teeth)", () => {
        expect(new Date("2024-01-01T00:00:00").getTimezoneOffset()).not.toBe(0);
    });
});

describe("A1 — sqlize does NOT shift timezone-naive datetimes by the host offset", () => {
    // Same expected literal on every host, regardless of process TZ.
    test.each([
        ["zoneless space",      "2024-01-15 12:00:00",       "2024-01-15 12:00:00"],
        ["zoneless ISO T",      "2024-01-15T12:00:00",       "2024-01-15 12:00:00"],
        ["UTC Z (no shift)",    "2024-01-15T10:30:00Z",      "2024-01-15 10:30:00"],
        ["explicit +02:00 → UTC", "2024-01-15T10:30:00+02:00", "2024-01-15 08:30:00"],
        ["fractional + Z",      "2024-06-30T23:59:59.123Z",  "2024-06-30 23:59:59"],
    ])("%s", (_label, input, expected) => {
        expect(sqlize(input, "datetime", pgsqlConfig)).toBe(expected);
    });

    test("date column keeps the wall-clock DAY even for a zoned input (no cross-midnight shift)", () => {
        // 02:00+05:00 is the previous day in UTC — converting would silently store 2024-01-14.
        expect(sqlize("2024-01-15T02:00:00+05:00", "date", pgsqlConfig)).toBe("2024-01-15");
        expect(sqlize("2024-01-15", "date", pgsqlConfig)).toBe("2024-01-15");
    });

    test("ASP.NET /Date(ms)/ stays an absolute UTC instant", () => {
        // 1704067200000 = 2024-01-01T00:00:00.000Z
        expect(sqlize("/Date(1704067200000)/", "datetime", pgsqlConfig)).toBe("2024-01-01 00:00:00");
    });

    test("consistent across dialects (mysql / sqlserver normalise the same wall-clock)", () => {
        expect(sqlize("2024-01-15 12:00:00", "datetime", mysqlConfig)).toBe("2024-01-15 12:00:00");
        // SQL Server keeps sub-second precision (its rule only strips 4+ fractional digits).
        expect(sqlize("2024-01-15T12:00:00", "datetime", sqlServerConfig)).toBe("2024-01-15 12:00:00");
    });
});

describe("A4 — sqlize rounds decimals half-up (away from zero), exactly, not by truncation", () => {
    const cap = (n: number): DatabaseConfig => ({ sqlDialect: "pgsql", decimalMaxLength: n });

    test.each([
        ["2.675 → 2.68 (was 2.67)", "2.675", 2, "2.68"],
        ["1.005 → 1.01 (was 1.00)", "1.005", 2, "1.01"],
        ["1.999 → 2.00 (carry)",    "1.999", 2, "2.00"],
        ["9.999 → 10.00 (grow int)", "9.999", 2, "10.00"],
        ["2.674 → 2.67 (truncate)", "2.674", 2, "2.67"],
        ["negative half-away",      "-2.675", 2, "-2.68"],
        ["within cap unchanged",    "2.5", 2, "2.5"],
    ])("%s", (_label, input, scale, expected) => {
        expect(sqlize(input, "decimal", pgsqlConfig, cap(scale))).toBe(expected);
    });

    test("exact at large magnitude (no float mantissa loss) — supersedes the D-G precision>15 truncation", () => {
        // 15-digit integer part + rounding at scale 2: float math would corrupt this; string carry is exact.
        expect(sqlize("123456789012345.678", "decimal", pgsqlConfig, cap(2))).toBe("123456789012345.68");
        // scale 18 (>15): old code truncated here; now it rounds correctly.
        expect(sqlize("1.1234567890123456785", "decimal", pgsqlConfig, cap(18))).toBe("1.123456789012345679");
    });

    test("default (no decimalMaxLength) still does NOT round values that fit the dialect ceiling (D-G intent intact)", () => {
        expect(sqlize("3.14159265358979323846", "decimal", pgsqlConfig)).toBe("3.14159265358979323846");
    });
});

describe("sourceTimeZone — opt-in: interpret zoneless datetimes in a declared zone, store UTC", () => {
    // These assertions use EXPLICIT IANA zones via Intl, so their results are host-independent: the
    // suite runs under Australia/Sydney yet a NY source zone still yields NY→UTC. That is the proof
    // the feature works for any region, not just where the process happens to run.
    const src = (zone: string) => ({ sqlDialect: "pgsql" as const, sourceTimeZone: zone });

    test("New York winter (EST, −5) → UTC", () => {
        expect(sqlize("2024-01-15 12:00:00", "datetime", pgsqlConfig, src("America/New_York"))).toBe("2024-01-15 17:00:00");
    });
    test("New York summer (EDT, −4, DST) → UTC — proves DST is handled, not a fixed offset", () => {
        expect(sqlize("2024-07-15 12:00:00", "datetime", pgsqlConfig, src("America/New_York"))).toBe("2024-07-15 16:00:00");
    });
    test("Southern-hemisphere DST too: Sydney January (AEDT, +11) → UTC", () => {
        expect(sqlize("2024-01-15 12:00:00", "datetime", pgsqlConfig, src("Australia/Sydney"))).toBe("2024-01-15 01:00:00");
    });
    test("Sydney winter (AEST, +10, no DST) → UTC", () => {
        expect(sqlize("2024-06-15 12:00:00", "datetime", pgsqlConfig, src("Australia/Sydney"))).toBe("2024-06-15 02:00:00");
    });

    test("a zone-qualified input IGNORES sourceTimeZone (it is already an absolute instant)", () => {
        expect(sqlize("2024-01-15T10:30:00Z", "datetime", pgsqlConfig, src("America/New_York"))).toBe("2024-01-15 10:30:00");
        expect(sqlize("2024-01-15T10:30:00+02:00", "datetime", pgsqlConfig, src("America/New_York"))).toBe("2024-01-15 08:30:00");
    });

    test("date and time columns are NEVER shifted, even with sourceTimeZone set (no cross-boundary drift)", () => {
        expect(sqlize("2024-01-15", "date", pgsqlConfig, src("America/New_York"))).toBe("2024-01-15");
        expect(sqlize("12:00:00", "time", pgsqlConfig, src("America/New_York"))).toBe("12:00:00");
    });

    test('sourceTimeZone "UTC" is a no-op (wall-clock == instant)', () => {
        expect(sqlize("2024-01-15 12:00:00", "datetime", pgsqlConfig, src("UTC"))).toBe("2024-01-15 12:00:00");
    });

    test("an invalid zone fails LOUD at validateConfig (not silently per row)", () => {
        expect(() => validateConfig({ sqlDialect: "pgsql", sourceTimeZone: "Not/AZone" })).toThrow(/Invalid sourceTimeZone/);
        expect(() => validateConfig({ sqlDialect: "pgsql", sourceTimeZone: "America/New_York" })).not.toThrow();
    });
});
