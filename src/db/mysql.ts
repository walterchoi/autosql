import mysql, { Pool } from "mysql2/promise";
import { Database } from "./database";

export class MySQL extends Database {
    async connect(): Promise<void> {
        this.connection = await mysql.createPool({
            host: this.config.host,
            user: this.config.username,
            password: this.config.password,
            database: this.config.database,
            connectionLimit: 20,
        });
    }

    async runQuery(query: string, params?: any[]): Promise<any> {
        if (!this.connection) await this.connect();
        const [rows] = await (this.connection as Pool).execute(query, params);
        return rows;
    }
}
