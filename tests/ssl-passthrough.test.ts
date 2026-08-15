// Unit coverage for the TLS/`ssl` passthrough into the connection pools (BYOD — SDH-598). The
// drivers are mocked so we can inspect the pool config without a real TLS host (a live verified-TLS
// connect needs an external TLS DB and is env-guarded, per 09-byod-connection-tls.md). Verifies the
// per-driver normalization: mysql2 rejects `ssl: true`, so it must become `{}`; pg takes `true`
// as-is; and an omitted `ssl` leaves NO `ssl` key (byte-for-byte back-compat).

jest.mock("mysql2/promise", () => ({ createPool: jest.fn(() => ({ pool: { config: {} } })) }));
jest.mock("pg", () => ({ Pool: jest.fn(() => ({ options: {} })) }));

import mysql from "mysql2/promise";
import pg from "pg";
import { Database } from "../src/db/database";

const mysqlCreatePool = mysql.createPool as unknown as jest.Mock;
const PgPool = (pg as any).Pool as jest.Mock;

const base = { host: "h", user: "u", password: "p", database: "d" };
const poolCfgMysql = async (ssl?: any) => {
    mysqlCreatePool.mockClear();
    const db: any = Database.create({ sqlDialect: "mysql", ...base, ...(ssl !== undefined ? { ssl } : {}) });
    await db.establishDatabaseConnection();
    return mysqlCreatePool.mock.calls[0][0];
};
const poolCfgPg = async (ssl?: any) => {
    PgPool.mockClear();
    const db: any = Database.create({ sqlDialect: "pgsql", ...base, ...(ssl !== undefined ? { ssl } : {}) });
    await db.establishDatabaseConnection();
    return PgPool.mock.calls[0][0];
};

describe("ssl passthrough", () => {
    describe("MySQL (mysql2 rejects ssl:true → normalise to {})", () => {
        test("ssl:true → ssl:{}", async () => {
            expect((await poolCfgMysql(true)).ssl).toEqual({});
        });
        test("ssl object is passed through as-is", async () => {
            const ssl = { ca: "PEM", rejectUnauthorized: true, servername: "db.example.com" };
            expect((await poolCfgMysql(ssl)).ssl).toEqual(ssl);
        });
        test("omitted → no ssl key (back-compat)", async () => {
            expect("ssl" in (await poolCfgMysql(undefined))).toBe(false);
        });
    });

    describe("Postgres (pg accepts ssl:true and the object directly)", () => {
        test("ssl:true → ssl:true", async () => {
            expect((await poolCfgPg(true)).ssl).toBe(true);
        });
        test("ssl object is passed through as-is", async () => {
            const ssl = { ca: "PEM", rejectUnauthorized: true };
            expect((await poolCfgPg(ssl)).ssl).toEqual(ssl);
        });
        test("omitted → no ssl key (back-compat)", async () => {
            expect("ssl" in (await poolCfgPg(undefined))).toBe(false);
        });
    });

    test("rejectUnauthorized:false warns that verification is disabled", async () => {
        const warn = jest.fn();
        const db: any = Database.create({ sqlDialect: "pgsql", ...base, ssl: { rejectUnauthorized: false }, logger: { warn } });
        await db.establishDatabaseConnection();
        expect(warn).toHaveBeenCalledWith(expect.stringMatching(/verification is DISABLED/i));
    });
});
