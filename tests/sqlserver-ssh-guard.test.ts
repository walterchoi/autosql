import { validateConfig } from "../src/helpers/utilities";
import { DatabaseConfig } from "../src/config/types";

// SSH tunnelling on SQL Server was silently a no-op: the base establishConnection opens the tunnel
// for every dialect, but only mysql.ts and pgsql.ts pass the resulting stream to their driver, so the
// mssql pool connected DIRECTLY while the caller believed the traffic was tunnelled. Refused at
// config time, before a useless tunnel is opened on the bastion.

const base = (over: Partial<DatabaseConfig> = {}): DatabaseConfig =>
    ({ sqlDialect: "sqlserver", host: "h", user: "u", password: "p", database: "d", ...over } as DatabaseConfig);

describe("SQL Server SSH guard", () => {
    test("sshConfig on SQL Server is refused, naming the reason", () => {
        expect(() => validateConfig(base({ sshConfig: { host: "bastion", username: "u" } as any })))
            .toThrow(/not supported on SQL Server/i);
        expect(() => validateConfig(base({ sshConfig: { host: "bastion", username: "u" } as any })))
            .toThrow(/NOT through the tunnel/);
    });

    test("a pre-opened sshStream is refused too", () => {
        expect(() => validateConfig(base({ sshStream: {} as any }))).toThrow(/not supported on SQL Server/i);
    });

    test("SQL Server without ssh is unaffected", () => {
        expect(() => validateConfig(base())).not.toThrow();
        expect(() => validateConfig(base({ ssl: true }))).not.toThrow();
    });

    test("ssh stays supported on mysql and pgsql", () => {
        for (const sqlDialect of ["mysql", "pgsql"] as const) {
            expect(() => validateConfig(base({ sqlDialect, sshConfig: { host: "b", username: "u" } as any })))
                .not.toThrow();
        }
    });
});
