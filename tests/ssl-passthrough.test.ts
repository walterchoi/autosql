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

    // §5 — `ssl: false` is treated the same as omitting it (plaintext), NOT passed to the driver.
    describe("§5 ssl:false == omit (no ssl key)", () => {
        test("mysql: ssl:false → no ssl key", async () => {
            expect("ssl" in (await poolCfgMysql(false))).toBe(false);
        });
        test("pgsql: ssl:false → no ssl key", async () => {
            expect("ssl" in (await poolCfgPg(false))).toBe(false);
        });
    });

    // §6/§7 — the "verification disabled" warning fires exactly when rejectUnauthorized:false, on BOTH
    // drivers, and is silent otherwise.
    const warnFor = async (dialect: "mysql" | "pgsql", ssl: any) => {
        const warn = jest.fn();
        const db: any = Database.create({ sqlDialect: dialect, ...base, ...(ssl !== undefined ? { ssl } : {}), logger: { warn } });
        await db.establishDatabaseConnection();
        return warn;
    };
    describe("§6/§7 rejectUnauthorized:false warning", () => {
        test("mysql warns when rejectUnauthorized:false", async () => {
            expect(await warnFor("mysql", { rejectUnauthorized: false })).toHaveBeenCalledWith(expect.stringMatching(/verification is DISABLED/i));
        });
        for (const [label, ssl] of [["ssl:true", true], ["verify object", { rejectUnauthorized: true }], ["omitted", undefined], ["ssl:false", false]] as const) {
            test(`does NOT warn for ${label} (mysql & pgsql)`, async () => {
                expect(await warnFor("mysql", ssl)).not.toHaveBeenCalledWith(expect.stringMatching(/verification is DISABLED/i));
                expect(await warnFor("pgsql", ssl)).not.toHaveBeenCalledWith(expect.stringMatching(/verification is DISABLED/i));
            });
        }
    });

    // §9 — ssl + an SSH tunnel are mutually exclusive ("pick one path"); warn when both are set.
    test("§9 warns when both ssl and sshStream are set", async () => {
        const warn = jest.fn();
        const db: any = Database.create({ sqlDialect: "mysql", ...base, ssl: true, sshStream: {} as any, logger: { warn } });
        await db.establishDatabaseConnection();
        expect(warn).toHaveBeenCalledWith(expect.stringMatching(/both `ssl` and `sshConfig`/i));
    });
});

// §8 — SQL Server maps `ssl` onto tedious's options (this is NOT a driver-object passthrough). The
// mapping is a pure function of config.ssl, so no live server or mock is needed.
describe("§8 SQL Server ssl → tedious options mapping", () => {
    const opts = (ssl?: any) => (Database.create({ sqlDialect: "sqlserver", host: "h", user: "u", password: "p", database: "d", ...(ssl !== undefined ? { ssl } : {}) }) as any).sqlServerTlsOptions();

    test("omitted → local default (encrypt off, trust self-signed)", () => {
        expect(opts(undefined)).toEqual({ encrypt: false, trustServerCertificate: true });
    });
    test("false → same as omitted", () => {
        expect(opts(false)).toEqual({ encrypt: false, trustServerCertificate: true });
    });
    test("true → encrypt on, verify against system CAs", () => {
        expect(opts(true)).toEqual({ encrypt: true, trustServerCertificate: false });
    });
    test("{ca, rejectUnauthorized:true} → encrypt on, verify, ca in cryptoCredentialsDetails", () => {
        expect(opts({ ca: "PEM", rejectUnauthorized: true })).toEqual({
            encrypt: true, trustServerCertificate: false, cryptoCredentialsDetails: { ca: "PEM" },
        });
    });
    test("{rejectUnauthorized:false} → encrypt on, trustServerCertificate (skip verify)", () => {
        expect(opts({ rejectUnauthorized: false })).toEqual({ encrypt: true, trustServerCertificate: true });
    });
    test("mutual TLS + SNI map into cryptoCredentialsDetails", () => {
        expect(opts({ cert: "C", key: "K", servername: "db.example.com" })).toEqual({
            encrypt: true, trustServerCertificate: false, cryptoCredentialsDetails: { cert: "C", key: "K", servername: "db.example.com" },
        });
    });
});
