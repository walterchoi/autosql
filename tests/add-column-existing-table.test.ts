import { compareMetaData } from "../src/helpers/metadata";
import { mysqlConfig } from "../src/db/config/mysqlConfig";
import { MetadataHeader } from "../src/config/types";

// R11 (unit): a column added to an EXISTING table must be emitted as nullable — existing rows have
// no value for it. A column that can back-fill (calculated timestamp, or explicit default) keeps
// NOT NULL. See decisions.md D-A.

const col = (o: Partial<any> = {}) => ({ type: "varchar", allowNull: false, ...o });

describe("compareMetaData: new columns on an existing table (R11)", () => {
    test("a new NOT NULL inferred column becomes nullable", () => {
        const oldH: MetadataHeader = { id: col({ type: "int", primary: true }) } as any;
        const newH: MetadataHeader = {
            id: col({ type: "int", primary: true }),
            extra: col({ type: "varchar", allowNull: false }),
        } as any;
        const { changes } = compareMetaData(oldH, newH, mysqlConfig);
        expect(changes.addColumns.extra).toBeDefined();
        expect(changes.addColumns.extra.allowNull).toBe(true); // forced nullable for existing rows
    });

    test("a new column with an explicit default keeps NOT NULL (it can back-fill)", () => {
        const oldH: MetadataHeader = { id: col({ type: "int", primary: true }) } as any;
        const newH: MetadataHeader = {
            id: col({ type: "int", primary: true }),
            status: col({ type: "varchar", allowNull: false, default: "'active'" }),
        } as any;
        const { changes } = compareMetaData(oldH, newH, mysqlConfig);
        expect(changes.addColumns.status.allowNull).toBe(false);
    });

    test("a new calculated column keeps NOT NULL (back-fills via default)", () => {
        const oldH: MetadataHeader = { id: col({ type: "int", primary: true }) } as any;
        const newH: MetadataHeader = {
            id: col({ type: "int", primary: true }),
            dwh_created_at: col({ type: "datetime", allowNull: false, calculated: true }),
        } as any;
        const { changes } = compareMetaData(oldH, newH, mysqlConfig);
        expect(changes.addColumns.dwh_created_at.allowNull).toBe(false);
    });

    test("a fresh CREATE (no old headers) keeps inferred NOT NULL", () => {
        const newH: MetadataHeader = { id: col({ type: "int", allowNull: false }) } as any;
        const { changes, updatedMetaData } = compareMetaData(null, newH, mysqlConfig);
        expect(changes.addColumns).toEqual({}); // create path: nothing is an "add"
        expect(updatedMetaData.id.allowNull).toBe(false);
    });
});
