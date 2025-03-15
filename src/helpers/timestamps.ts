import { DatabaseConfig, MetadataHeader } from "../config/types";
import { CREATED_TIMESTAMP_NAMES, MODIFIED_TIMESTAMP_NAMES, DWH_LOADED_TIMESTAMP_NAMES } from "../config/defaults";

export function ensureTimestamps(dbConfig: DatabaseConfig, metaData: MetadataHeader): MetadataHeader {
    if (!dbConfig.addTimestamps) {
      return metaData; // RETURN if timestamps are not required
    }
  
    // Check if columns already exist
    const hasCreatedAt = Object.keys(metaData).some((col) =>
      CREATED_TIMESTAMP_NAMES.includes(col)
    );
    const hasModifiedAt = Object.keys(metaData).some((col) =>
      MODIFIED_TIMESTAMP_NAMES.includes(col)
    );
    const hasLoadedAt = Object.keys(metaData).some((col) =>
      DWH_LOADED_TIMESTAMP_NAMES.includes(col)
    );
  
    // Add missing timestamp columns
    if (!hasCreatedAt) {
      metaData["dwh_created_at"] = {
        type: "datetime",
        allowNull: false,
        calculated: true,
      };
    }
  
    if (!hasModifiedAt) {
      metaData["dwh_modified_at"] = {
        type: "datetime",
        allowNull: true,
        calculated: true,
        updatedCalculated: true,
      };
    }
  
    if (!hasLoadedAt) {
      metaData["dwh_loaded_at"] = {
        type: "datetime",
        allowNull: true,
        calculated: true,
        updatedCalculated: true,
      };
    }
  
    return metaData;
  }