import fs from "fs/promises";
import os from "os";
import path from "path";

// setSSH's synchronous, network-free validation: the missing-username throw, and the
// private_key_path -> file-read fallback. The actual connect() needs a live SSH server (an
// integration concern, not a unit gap) — ssh2's Client is stubbed here specifically to avoid
// that, capturing what connect() was called with instead of actually connecting.

(globalThis as any).__capturedSshConnectConfigs = [];

jest.mock("ssh2", () => {
    class FakeClient {
        private errCb?: (err: Error) => void;
        on(event: string, cb: (err: Error) => void) {
            if (event === "error") this.errCb = cb;
            return this;
        }
        connect(cfg: unknown) {
            (globalThis as any).__capturedSshConnectConfigs.push(cfg);
            // Reject quickly instead of actually connecting — this test only cares what connect()
            // was called WITH, not the real handshake.
            process.nextTick(() => this.errCb?.(new Error("mock: no real connection attempted")));
        }
    }
    return { Client: FakeClient };
});

// Must load AFTER the ssh2 mock above is registered.
const { setSSH } = require("../src/helpers/ssh");

beforeEach(() => { (globalThis as any).__capturedSshConnectConfigs = []; });

describe("setSSH — synchronous validation", () => {
    test("a missing username throws before any connection attempt", async () => {
        await expect(setSSH({ host: "h", destination_address: "d", destination_port: 1 } as any))
            .rejects.toThrow("No SSH username provided in sshKeys config.");
        expect((globalThis as any).__capturedSshConnectConfigs).toHaveLength(0); // never reached connect()
    });

    test("outlier: an entirely falsy sshKeys also throws the username error, not a TypeError", async () => {
        await expect(setSSH(undefined as any)).rejects.toThrow("No SSH username provided in sshKeys config.");
    });

    test("private_key_path resolves into the connect config when private_key isn't set directly", async () => {
        const tmpFile = path.join(os.tmpdir(), `autosql-ssh-key-test-${Date.now()}.pem`);
        await fs.writeFile(tmpFile, "FAKE-PRIVATE-KEY-CONTENTS");
        try {
            await expect(
                setSSH({
                    username: "u", host: "h", destination_address: "d", destination_port: 1,
                    private_key_path: tmpFile,
                } as any)
            ).rejects.toThrow(); // the mock Client always rejects — only the config matters here

            const configs = (globalThis as any).__capturedSshConnectConfigs;
            expect(configs).toHaveLength(1);
            expect(configs[0].privateKey).toBe("FAKE-PRIVATE-KEY-CONTENTS");
        } finally {
            await fs.unlink(tmpFile).catch(() => {});
        }
    });

    test("private_key (direct) takes priority — private_key_path is never read when it's set", async () => {
        const tmpFile = path.join(os.tmpdir(), `autosql-ssh-key-unused-${Date.now()}.pem`);
        await fs.writeFile(tmpFile, "SHOULD-NOT-BE-READ");
        try {
            await expect(
                setSSH({
                    username: "u", host: "h", destination_address: "d", destination_port: 1,
                    private_key: "DIRECT-KEY-CONTENTS", private_key_path: tmpFile,
                } as any)
            ).rejects.toThrow();

            const configs = (globalThis as any).__capturedSshConnectConfigs;
            expect(configs[0].privateKey).toBe("DIRECT-KEY-CONTENTS");
        } finally {
            await fs.unlink(tmpFile).catch(() => {});
        }
    });
});
