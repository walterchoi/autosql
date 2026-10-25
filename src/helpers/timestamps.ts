import type { DatabaseConfig, MetadataHeader } from "../config/types";
import { normalizeName } from "../helpers/utilities";
import { CREATED_TIMESTAMP_NAMES, MODIFIED_TIMESTAMP_NAMES, DWH_LOADED_TIMESTAMP_NAMES } from "../config/defaults";

export function ensureTimestamps(dbConfig: DatabaseConfig, metaData: MetadataHeader, startDate: Date = new Date()): MetadataHeader {
    if (!dbConfig.addTimestamps) {
      return metaData; // RETURN if timestamps are not required
    }

    const normalizedCreatedTimestampNames = CREATED_TIMESTAMP_NAMES.map(normalizeName);
    const normalizedModifiedTimestampNames = MODIFIED_TIMESTAMP_NAMES.map(normalizeName);
    const normalizedDwhLoadedTimestampNames = DWH_LOADED_TIMESTAMP_NAMES.map(normalizeName);

    const normalizedColumns = Object.keys(metaData).map(normalizeName);

    // Check if columns already exist
    const hasCreatedAt = normalizedColumns.some(col =>
      normalizedCreatedTimestampNames.includes(col)
    );

    const hasModifiedAt = normalizedColumns.some(col =>
      normalizedModifiedTimestampNames.includes(col)
    );

    const hasLoadedAt = normalizedColumns.some(col =>
      normalizedDwhLoadedTimestampNames.includes(col)
    );
  
    // Add missing timestamp columns
    if (!hasCreatedAt) {
      metaData["dwh_created_at"] = {
        type: "datetime",
        allowNull: false,
        calculated: true,
        updatedCalculated: false,
        calculatedDefault: startDate.toISOString()
      };
    }
  
    if (!hasModifiedAt) {
      metaData["dwh_modified_at"] = {
        type: "datetime",
        allowNull: true,
        calculated: true,
        updatedCalculated: true,
        calculatedDefault: startDate.toISOString()
      };
    }
  
    if (!hasLoadedAt) {
      metaData["dwh_loaded_at"] = {
        type: "datetime",
        allowNull: true,
        calculated: true,
        updatedCalculated: true,
        calculatedDefault: startDate.toISOString()
      };
    }
  
    return metaData;
  }