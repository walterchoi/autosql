import { predictIndexes } from "../src/helpers/keys";
import type { MetadataHeader, DataRow } from "../src/config/types";

// A GA4-session_report-shaped batch: property_id/date are constant TODAY (one property, one day)
// but must always be part of the key once a second property/day lands in the same table. `sessions`
// stands in for "some column that happens to be fully unique in THIS small batch" (a metric, a
// row-level id) — exactly the shape that tricks the old single-key fallback.
function ga4ShapedBatch(): { headers: MetadataHeader; data: DataRow[] } {
    const headers: MetadataHeader = {
        property_id: { type: "varchar", length: 100, allowNull: false, categorical: true },
        date: { type: "date", allowNull: false, categorical: true },
        channel: { type: "varchar", length: 100, allowNull: false, categorical: true },
        deviceCategory: { type: "varchar", length: 100, allowNull: false, categorical: true },
        sessions: { type: "int", length: 11, allowNull: false, unique: true },
    };
    const channels = ["Direct", "Organic", "Paid", "Referral", "Social"];
    const devices = ["desktop", "mobile", "tablet"];
    const data: DataRow[] = [];
    let i = 0;
    for (const ch of channels) {
        for (const dv of devices) {
            data.push({ property_id: "123456789", date: "2026-11-11", channel: ch, deviceCategory: dv, sessions: 10 + i });
            i++;
        }
    }
    return { headers, data };
}

const primaryColumnsOf = (result: MetadataHeader): string[] =>
    Object.entries(result).filter(([, c]) => c.primary).map(([name]) => name).sort();

describe("predictIndexes — partial-key hints and preferCompositeKeys", () => {
    test("a primaryKey hint with non-unique columns is never diluted by the single-key fallback", () => {
        const { headers, data } = ga4ShapedBatch();

        const result = predictIndexes(headers, undefined, ["property_id", "date"], data);

        // Before the fix this picked (property_id, date, sessions) — a metric landed in the key
        // because `sessions` happened to be unique in this batch, and the single-key fallback has no
        // notion of the hint. The real composite search below it never ran.
        expect(primaryColumnsOf(result)).toEqual(["channel", "date", "deviceCategory", "property_id"].sort());
    });

    test("without a hint, today's default ordering is unchanged (single-key fallback still wins)", () => {
        const { headers, data } = ga4ShapedBatch();

        const result = predictIndexes(headers, undefined, undefined, data);

        // No hint was given at all, so the pre-existing behaviour must not change by default.
        expect(primaryColumnsOf(result)).toEqual(["sessions"]);
    });

    test("preferCompositeKeys opts an unhinted caller into the same composite-first search", () => {
        const { headers, data } = ga4ShapedBatch();

        const result = predictIndexes(headers, undefined, undefined, data, undefined, true);

        expect(primaryColumnsOf(result)).toEqual(["channel", "deviceCategory"].sort());
    });

    test("preferCompositeKeys still falls back to the single-key heuristic if no composite is found", () => {
        const headers: MetadataHeader = {
            id: { type: "int", length: 11, allowNull: false, unique: true },
            note: { type: "varchar", length: 100, allowNull: true },
        };
        const data: DataRow[] = [{ id: 1, note: "a" }, { id: 2, note: "a" }];

        const result = predictIndexes(headers, undefined, undefined, data, undefined, true);

        // Composite-first found nothing (no composite candidates at all) — a table shouldn't end up
        // keyless just because preferCompositeKeys is on; it falls back the way today's default does.
        expect(primaryColumnsOf(result)).toEqual(["id"]);
    });
});
