import { Database } from "../src/db/database";

// closeConnection's {success:false} branch (the driver's .end() throwing) had zero coverage for
// any dialect — every existing test only exercises the happy path. The finally block's cleanup
// (connection/sshClient/sshStream cleared) must still run even on failure.

function makeDb() {
    return Database.create({ sqlDialect: "mysql", host: "h", user: "u", password: "p", database: "d" }) as any;
}

describe("Database.closeConnection", () => {
    test("no connection: resolves {success:true} immediately, nothing to close", async () => {
        const db = makeDb();
        const result = await db.closeConnection();
        expect(result).toEqual({ success: true });
    });

    test("a successful close clears connection/sshClient/sshStream", async () => {
        const db = makeDb();
        const end = jest.fn().mockResolvedValue(undefined);
        db.connection = { end };
        const sshEnd = jest.fn();
        db.config.sshClient = { end: sshEnd };
        db.config.sshStream = {};

        const result = await db.closeConnection();

        expect(result).toEqual({ success: true });
        expect(end).toHaveBeenCalled();
        expect(sshEnd).toHaveBeenCalled();
        expect(db.connection).toBeUndefined();
        expect(db.config.sshClient).toBeUndefined();
        expect(db.config.sshStream).toBeUndefined();
    });

    test("the driver's end() throwing an Error surfaces {success:false, error}, and still clears state", async () => {
        const db = makeDb();
        db.connection = { end: jest.fn().mockRejectedValue(new Error("connection reset")) };

        const result = await db.closeConnection();

        expect(result).toEqual({ success: false, error: "connection reset" });
        expect(db.connection).toBeUndefined(); // finally still ran
    });

    test("outlier: end() rejecting with a non-Error value reports \"Unknown error\"", async () => {
        const db = makeDb();
        db.connection = { end: jest.fn().mockRejectedValue("just a string") };

        const result = await db.closeConnection();

        expect(result).toEqual({ success: false, error: "Unknown error" });
    });

    test("an sshClient.end() failure after a successful driver close is also reported as a failure, not swallowed", async () => {
        const db = makeDb();
        db.connection = { end: jest.fn().mockResolvedValue(undefined) };
        db.config.sshClient = { end: jest.fn().mockImplementation(() => { throw new Error("tunnel already closed"); }) };

        const result = await db.closeConnection();

        expect(result).toEqual({ success: false, error: "tunnel already closed" });
        expect(db.connection).toBeUndefined();
        expect(db.config.sshClient).toBeUndefined(); // still cleared despite the failure
    });
});
