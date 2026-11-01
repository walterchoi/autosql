jest.mock("../src/helpers/ssh", () => ({ setSSH: jest.fn() }));
import { setSSH } from "../src/helpers/ssh";
import { Database } from "../src/db/database";

// establishConnection's retry loop opens a fresh SSH tunnel per attempt (sshConfig is set).
// On a failed attempt it must close THAT attempt's tunnel before retrying, so a retry/final-throw
// doesn't leak an SSH connection per failed attempt. Zero coverage — every other retry test uses
// a plain TCP config, no sshConfig at all.

function makeDb() {
    return Database.create({
        sqlDialect: "mysql", host: "h", user: "u", password: "p", database: "d",
        sshConfig: { username: "x", host: "bastion", destination_address: "db", destination_port: 3306 } as any,
    }) as any;
}

beforeEach(() => (setSSH as jest.Mock).mockReset());

describe("establishConnection — SSH tunnel cleanup between retry attempts", () => {
    test("a failed attempt's tunnel is closed and cleared before the next attempt opens its own", async () => {
        const endCalls: string[] = [];
        let sshCall = 0;
        (setSSH as jest.Mock).mockImplementation(async () => {
            sshCall++;
            const label = `tunnel-${sshCall}`;
            return { stream: {}, sshClient: { end: () => endCalls.push(label) } };
        });

        const db = makeDb();
        let attempts = 0;
        db.establishDatabaseConnection = jest.fn().mockImplementation(async () => {
            attempts++;
            if (attempts === 1) throw new Error("ECONNRESET"); // non-permanent, no .code set
        });

        await db.establishConnection();

        expect(attempts).toBe(2);
        expect(setSSH).toHaveBeenCalledTimes(2); // a fresh tunnel opened per attempt
        expect(endCalls).toEqual(["tunnel-1"]); // attempt 1's tunnel closed; attempt 2's (successful) tunnel left open
        expect(db.config.sshClient).toBeDefined(); // the surviving (2nd) tunnel's client, not cleared
        expect(db.config.sshStream).toBeDefined();
    });

    test("a permanent error still closes that attempt's tunnel before re-throwing", async () => {
        const end = jest.fn();
        (setSSH as jest.Mock).mockResolvedValue({ stream: {}, sshClient: { end } });

        const db = makeDb();
        db.getPermanentErrors = async () => ["ER_ACCESS_DENIED"];
        db.establishDatabaseConnection = jest.fn().mockImplementation(async () => {
            throw Object.assign(new Error("Access denied"), { code: "ER_ACCESS_DENIED" });
        });

        await expect(db.establishConnection()).rejects.toThrow("Access denied");

        expect(end).toHaveBeenCalled();
        expect(db.config.sshClient).toBeUndefined();
        expect(db.config.sshStream).toBeUndefined();
    });

    test("a tunnel that's already closed (end() throws) doesn't block the retry", async () => {
        (setSSH as jest.Mock).mockResolvedValue({
            stream: {}, sshClient: { end: () => { throw new Error("already closed"); } },
        });

        const db = makeDb();
        let attempts = 0;
        db.establishDatabaseConnection = jest.fn().mockImplementation(async () => {
            attempts++;
            if (attempts === 1) throw new Error("ECONNRESET");
        });

        await db.establishConnection(); // must not reject because end() itself threw
        expect(attempts).toBe(2);
    });
});
