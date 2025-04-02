import { DatabaseConfig, MetadataHeader } from "../config/types";
import { normalizeName } from "../helpers/utilities";
import { CREATED_TIMESTAMP_NAMES, MODIFIED_TIMESTAMP_NAMES, DWH_LOADED_TIMESTAMP_NAMES } from "../config/defaults";

export function ensureTimestamps(dbConfig: DatabaseConfig, metaData: MetadataHeader, startDate: Date = new Date()): MetadataHeader {
    if (!dbConfig.addTimestamps) {
      return metaData; // RETURN if timestamps are not required
    }

    const NORMALIZED_CREATED_TIMESTAMP_NAMES = CREATED_TIMESTAMP_NAMES.map(normalizeName);
    const NORMALIZED_MODIFIED_TIMESTAMP_NAMES = MODIFIED_TIMESTAMP_NAMES.map(normalizeName);
    const NORMALIZED_DWH_LOADED_TIMESTAMP_NAMES = DWH_LOADED_TIMESTAMP_NAMES.map(normalizeName);

    const normalizedColumns = Object.keys(metaData).map(normalizeName);
  
    // Check if columns already exist
    const hasCreatedAt = normalizedColumns.some(col =>
      NORMALIZED_CREATED_TIMESTAMP_NAMES.includes(col)
    );
    
    const hasModifiedAt = normalizedColumns.some(col =>
      NORMALIZED_MODIFIED_TIMESTAMP_NAMES.includes(col)
    );
    
    const hasLoadedAt = normalizedColumns.some(col =>
      NORMALIZED_DWH_LOADED_TIMESTAMP_NAMES.includes(col)
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