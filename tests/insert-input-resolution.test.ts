import { resolveStatementInput, resolveStagingInput, resolveHistoryInput } from "../src/db/queryBuilders/shared/insertBuilderShared";
import type { InsertInput, MetadataHeader } from "../src/config/types";

// 3.0.0 fix (no-non-null-assertion): calling the table-name overload of
// getInsertStatementQuery/getInsertFromStagingQuery/getInsertChangedRowsToHistoryQuery without
// metaData used to reach a raw TypeError from Object.keys(undefined) further down. These three
// functions are where that got a clear, named guard instead — direct unit coverage, since nothing
// calls them with a bare table name and no metaData today (which is exactly why the gap existed).

const META: MetadataHeader = { id: { type: "int", allowNull: false } };
const INPUT: InsertInput = { table: "t", data: [{ id: 1 }], metaData: META, previousMetaData: null, comparedMetaData: undefined };

describe("resolveStatementInput", () => {
    test("table name + no metaData throws a clear, named error", () => {
        expect(() => resolveStatementInput("t", [{ id: 1 }], undefined))
            .toThrow(/resolveStatementInput: metaData is required when called with a table name/);
    });

    test("table name + no data throws the original, unrelated message (not the metaData one)", () => {
        expect(() => resolveStatementInput("t", undefined, META))
            .toThrow(/No data provided for insert into table "t"/);
    });

    test("an InsertInput object never hits either check, regardless of metaData", () => {
        expect(() => resolveStatementInput(INPUT)).not.toThrow();
    });

    test("table name + data + metaData resolves normally", () => {
        const r = resolveStatementInput("t", [{ id: 1 }], META);
        expect(r.table).toBe("t");
        expect(r.header).toBe(META);
    });
});

describe("resolveStagingInput", () => {
    test("table name + no metaData throws a clear, named error", () => {
        expect(() => resolveStagingInput("t", undefined)).toThrow(/resolveStagingInput: metaData is required when called with a table name/);
    });

    test("an InsertInput object never hits the check", () => {
        expect(() => resolveStagingInput(INPUT)).not.toThrow();
    });

    test("table name + metaData resolves normally", () => {
        expect(() => resolveStagingInput("t", META)).not.toThrow();
    });
});

describe("resolveHistoryInput", () => {
    test("table name + no metaData throws a clear, named error", () => {
        expect(() => resolveHistoryInput("t", undefined)).toThrow(/resolveHistoryInput: metaData is required when called with a table name/);
    });

    test("an InsertInput object never hits the check", () => {
        expect(() => resolveHistoryInput(INPUT)).not.toThrow();
    });

    test("table name + metaData resolves normally", () => {
        expect(() => resolveHistoryInput("t", META)).not.toThrow();
    });
});
