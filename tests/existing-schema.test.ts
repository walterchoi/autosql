import { DB_CONFIG, Database } from "./utils/testConfig";
import { MetadataHeader } from "../src/config/types";

// N1 / v1b: when the caller supplies `existingSchema`, AutoSQL trusts it and skips live introspection
// of the target table — the main per-run DB round-trip for a stable pipeline. Proven here by running
// handleMetadata on an UNCONNECTED Database with a fetchTableMetadata spy: it must complete without
// ever touching the DB.

describe("existingSchema skips introspection (unit)", () => {
    const existing: MetadataHeader = {
        id: { type: "int", primary: true, allowNull: false },
        name: { type: "varchar", length: 50, allowNull: false },
    };

    test("handleMetadata does not call fetchTableMetadata when existingSchema is supplied", async () => {
        const db = Database.create({ ...DB_CONFIG.mysql, addTimestamps: false }); // never connected
        const handler = (db as any).autoSQLHandler;
        const spy = jest.spyOn(handler, "fetchTableMetadata");

        const res = await handler.handleMetadata("t", [{ id: 1000, name: "a" }], undefined, { existingSchema: existing });

        expect(spy).not.toHaveBeenCalled();   // no introspection round-trip
        expect(res.currentMetaData).toBe(existing); // the provided schema is used as the baseline
    });

    test("handleMetadata DOES introspect when existingSchema is omitted (control)", async () => {
        const db = Database.create({ ...DB_CONFIG.mysql, addTimestamps: false });
        const handler = (db as any).autoSQLHandler;
        const spy = jest.spyOn(handler, "fetchTableMetadata").mockResolvedValue({ currentMetaData: null, tableExists: false });

        await handler.handleMetadata("t", [{ id: 1000, name: "a" }], undefined, undefined);

        expect(spy).toHaveBeenCalledTimes(1); // falls back to live introspection
    });
});
