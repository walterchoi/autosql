import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { InsertResult, MetadataHeader, AlterTableChanges } from "../config/types";
import { getMetaData, compareMetaData } from "../helpers/metadata";
import { parseDatabaseMetaData } from "../helpers/utilities";

export class AutoSQLHandler {
    private db: MySQLDatabase | PostgresDatabase;

    constructor(dbInstance: MySQLDatabase | PostgresDatabase) {
        this.db = dbInstance;
    }

    async execute(table: string, data: Record<string, any>[]): Promise<InsertResult> {
        const currentMetaDataQuery = this.db.getTableMetaDataQuery(this.db.getConfig().schema || this.db.getConfig().database || "", table)
        const currentMetaDataResults = await this.db.runQuery(currentMetaDataQuery);
        const currentMetaData = parseDatabaseMetaData(currentMetaDataResults, this.db.getDialectConfig())
        const newMetaData = await getMetaData(this.db.getConfig(), data);
        let mergedMetaData: AlterTableChanges;
        if(currentMetaData) {
            mergedMetaData = compareMetaData(currentMetaData, newMetaData, this.db.getDialectConfig())
        } else {
            
        }
        
        const start = this.db.startDate;
        const affectedRows = 0
        const end = new Date;
        return {
            start,
            end,
            duration: end.getTime() - start.getTime(),
            affectedRows
        };
    }
}