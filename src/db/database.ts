import { Pool } from "mysql2/promise";
import { Pool as PgPool } from "pg";
import { sqlDialectConfig } from "../config/sqldialect";

export abstract class Database {
    protected connection: Pool | PgPool | null = null;
    protected config: any;

    constructor(config: any) {
        this.config = config;
    }

    abstract connect(): Promise<void>;
    abstract runQuery(query: string, params?: any[]): Promise<any>;

    async validateDatabase(): Promise<void> {
        if (!this.connection) await this.connect();
        console.log("Database connection validated.");
    }

    async validateQuery(query: string): Promise<void> {
        console.log(`Validating Query: ${query}`);
    }

    async runSqlQuery(query: string): Promise<any> {
        if (!this.connection) await this.connect();
        return this.runQuery(query);
    }
}
