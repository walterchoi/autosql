import { ensureTimestamps } from "../src/helpers/timestamps";
import { MetadataHeader, supportedDialects } from "../src/config/types";

describe("ensureTimestamps", () => {
  it("does nothing if addTimestamps is false", () => {
    const dbConfig = { sqlDialect: "mysql" as supportedDialects, addTimestamps: false };
    const metadata: MetadataHeader = {
      id: { type: "int", primary: true },
      name: { type: "varchar", length: 255 }
    };

    const result = ensureTimestamps(dbConfig, metadata);
    expect(result).toEqual(metadata); // Should return unchanged metadata
  });

  it("adds missing timestamp columns if addTimestamps is true", () => {
    const dbConfig = { sqlDialect: "mysql" as supportedDialects, addTimestamps: true };
    const metadata: MetadataHeader = {
      id: { type: "int", primary: true },
      name: { type: "varchar", length: 255 }
    };

    const result = ensureTimestamps(dbConfig, metadata);

    expect(result).toHaveProperty("dwh_created_at");
    expect(result).toHaveProperty("dwh_modified_at");
    expect(result).toHaveProperty("dwh_loaded_at");

    expect(result["dwh_created_at"]).toMatchObject({
      type: "datetime",
      allowNull: false,
      calculated: true,
      updatedCalculated: false,
    });

    expect(result["dwh_modified_at"]).toMatchObject({
      type: "datetime",
      allowNull: true,
      calculated: true,
      updatedCalculated: true
    });

    expect(result["dwh_loaded_at"]).toMatchObject({
      type: "datetime",
      allowNull: true,
      calculated: true,
      updatedCalculated: true
    });
  });

  it("does not add timestamps if they already exist", () => {
    const dbConfig = { sqlDialect: "mysql" as supportedDialects, addTimestamps: true };
    const metadata: MetadataHeader = {
      id: { type: "int", primary: true },
      created_at: { type: "datetime", allowNull: false },
      updatedon: { type: "datetime", allowNull: true },
      dw_timestamp: { type: "datetime", allowNull: true }
    };

    const result = ensureTimestamps(dbConfig, metadata);

    // Should NOT add duplicates (using equivalent existing timestamps)
    expect(result).not.toHaveProperty("dwh_created_at"); // 'created_at' already exists
    expect(result).not.toHaveProperty("dwh_modified_at"); // 'updatedon' already exists
    expect(result).not.toHaveProperty("dwh_loaded_at"); // 'dw_timestamp' already exists

    expect(Object.keys(result)).toEqual(["id", "created_at", "updatedon", "dw_timestamp"]);
  });

  it("adds missing timestamps even if some exist", () => {
    const dbConfig = { sqlDialect: "mysql" as supportedDialects, addTimestamps: true };
    const metadata: MetadataHeader = {
      id: { type: "int", primary: true },
      created_at: { type: "datetime", allowNull: false } // Equivalent to 'dwh_created_at'
    };

    const result = ensureTimestamps(dbConfig, metadata);

    expect(result).not.toHaveProperty("dwh_created_at"); // Already exists as 'created_at'
    expect(result).toHaveProperty("dwh_modified_at");
    expect(result).toHaveProperty("dwh_loaded_at");
  });
});
