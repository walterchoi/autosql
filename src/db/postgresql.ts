import { Pool } from "pg";
import { Database } from "./database";

export class PostgreSQL extends Database {
    async connect(): Promise<void> {
        this.connection = new Pool({
            host: this.config.host,
            user: this.config.username,
            password: this.config.password,
            database: this.config.database,
            max: 25
        });
    }

    async runQuery(query: string, params?: any[]): Promise<any> {
        if (!this.connection) await this.connect();
        const client = await (this.connection as Pool).connect();
        const result = await client.query(query, params);
        client.release();
        return result.rows;
    }
}
