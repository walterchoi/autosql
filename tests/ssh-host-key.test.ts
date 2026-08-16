import crypto from "crypto";
import { sshHostKeyFingerprint, fingerprintMatches } from "../src/helpers/ssh";

// A7: the SSH tunnel now verifies the bastion's host key against a pinned fingerprint (ssh2 does NO
// verification by default → MITM-able). Test the verifier as a pure function; no live handshake needed.
describe("SSH host-key fingerprint verification (A7)", () => {
    const key = Buffer.from("a-fake-host-key-blob-for-testing");
    // Independent OpenSSH-format reference: SHA256 of the key blob, base64, padding stripped.
    const ref = "SHA256:" + crypto.createHash("sha256").update(key).digest("base64").replace(/=+$/, "");

    test("produces the OpenSSH SHA256:base64 form (not hex, no padding)", () => {
        const fp = sshHostKeyFingerprint(key);
        expect(fp).toBe(ref);
        expect(fp).toMatch(/^SHA256:[A-Za-z0-9+/]+$/); // base64 charset, no '=' padding, not hex
    });

    test("matches the correct fingerprint (tolerating the SHA256: prefix and padding)", () => {
        expect(fingerprintMatches(key, ref)).toBe(true);
        expect(fingerprintMatches(key, ref.replace(/^SHA256:/, ""))).toBe(true);
        expect(fingerprintMatches(key, ref + "==")).toBe(true);
    });

    test("rejects a wrong fingerprint", () => {
        const other = "SHA256:" + crypto.createHash("sha256").update(Buffer.from("different")).digest("base64").replace(/=+$/, "");
        expect(fingerprintMatches(key, other)).toBe(false);
        expect(fingerprintMatches(key, "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")).toBe(false);
    });
});
