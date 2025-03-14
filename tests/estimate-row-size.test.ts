import { estimateRowSize } from "../src/helpers/utilities";
import { MetadataHeader } from "../src/config/types";

describe("estimateRowSize", () => {
  it("calculates row size correctly for basic columns", () => {
    const metadata: MetadataHeader = {
      id: { type: "int", primary: true },
      name: { type: "varchar", length: 255 },
      age: { type: "smallint" },
      is_active: { type: "boolean" }
    };

    const result = estimateRowSize(metadata, "mysql");
    expect(result.rowSize).toBeGreaterThan(0);
    expect(result.exceedsLimit).toBe(false);
  });

  it("handles variable-length text fields correctly", () => {
    const metadata: MetadataHeader = {
      description: { type: "text" }
    };

    const result = estimateRowSize(metadata, "mysql");
    expect(result.rowSize).toBeLessThan(16 * 1024); // Should only count pointer size
    expect(result.exceedsLimit).toBe(false);
  });

  it("detects when row size exceeds MySQL limit", () => {
    const metadata: MetadataHeader = {
      id: { type: "int", primary: true },
      large_text: { type: "varchar", length: 16428 } // Too large for a single row
    };

    const result = estimateRowSize(metadata, "mysql");
    expect(result.exceedsLimit).toBe(true);
  });

  it("detects when row size exceeds PostgreSQL limit", () => {
    const metadata: MetadataHeader = {
      col1: { type: "varchar", length: 4000 },
      col2: { type: "varchar", length: 4000 },
      col3: { type: "varchar", length: 4000 }
    };

    const result = estimateRowSize(metadata, "pgsql");
    expect(result.exceedsLimit).toBe(true);
  });

  it("correctly accounts for NULL flags and indexing overhead", () => {
    const metadata: MetadataHeader = {
      id: { type: "int", primary: true },
      name: { type: "varchar", length: 100, allowNull: true },
      email: { type: "varchar", length: 200, unique: true }
    };

    const result = estimateRowSize(metadata, "mysql");
    expect(result.rowSize).toBeGreaterThan(0);
    expect(result.rowSize).toBeLessThan(16 * 1024);
  });

  it("accounts for composite primary keys properly", () => {
    const metadata: MetadataHeader = {
      order_id: { type: "int", primary: true },
      user_id: { type: "int", primary: true }
    };

    const result = estimateRowSize(metadata, "mysql");
    expect(result.exceedsLimit).toBe(false);
  });
});
