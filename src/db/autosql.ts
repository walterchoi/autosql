import { MySQLDatabase } from "./mysql";
import { PostgresDatabase } from "./pgsql";
import { InsertResult } from "../config/types";

export class AutoSQLHandler {
    private db: MySQLDatabase | PostgresDatabase;

    constructor(dbInstance: MySQLDatabase | PostgresDatabase) {
        this.db = dbInstance;
    }

    async execute(table: string, data: Record<string, any>[]): Promise<InsertResult> {

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