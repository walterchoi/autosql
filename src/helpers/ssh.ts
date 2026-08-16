import { readFile } from "fs/promises";
import crypto from "crypto";
import type { Client, ClientChannel } from "ssh2";
import type { SSHKeys, DatabaseConfig } from "../config/types";

// OpenSSH-format host-key fingerprint: "SHA256:" + base64(sha256(keyblob)) with padding stripped —
// the exact form `ssh-keyscan host | ssh-keygen -lf -` prints, so a user can supply what they can
// actually obtain. `key` is the raw host-key buffer ssh2 hands the hostVerifier.
export function sshHostKeyFingerprint(key: Buffer): string {
    return "SHA256:" + crypto.createHash("sha256").update(key).digest("base64").replace(/=+$/, "");
}

// Compare a presented host key to an expected fingerprint. Tolerant of the optional "SHA256:" prefix
// and trailing base64 padding on the expected value; base64 is case-sensitive so the digest itself is
// NOT lower-cased.
export function fingerprintMatches(key: Buffer, expected: string): boolean {
    const norm = (fp: string) => fp.trim().replace(/^SHA256:/i, "").replace(/=+$/, "");
    return norm(sshHostKeyFingerprint(key)) === norm(expected);
}

export async function setSSH(sshKeys: SSHKeys, logger?: DatabaseConfig["logger"]): Promise<{ stream: ClientChannel; sshClient: Client }> {
  let SSHClient: typeof import("ssh2").Client;

  try {
    SSHClient = require("ssh2").Client;
  } catch (err) {
    throw new Error(
      `SSH tunnel config specified but 'ssh2' is not installed. Please run: npm install ssh2`
    );
  }

  if (!sshKeys || !sshKeys.username) {
    throw new Error("No SSH username provided in sshKeys config.");
  }

  // Read the private key into a LOCAL — do not mutate the caller's sshKeys object (it may be reused).
  let privateKey = sshKeys.private_key;
  if (sshKeys.private_key_path && !privateKey) {
    privateKey = await readFile(sshKeys.private_key_path, "utf-8");
  }

  const warn = logger?.warn ?? ((m: string) => console.warn(m));

  const sshConfig: any = {
    host: sshKeys.host,
    port: sshKeys.port,
    username: sshKeys.username,
    readyTimeout: sshKeys.timeout ?? 10000,
    ...(sshKeys.password && { password: sshKeys.password }),
    ...(privateKey && { privateKey }),
    ...(sshKeys.debug && {
      debug: (msg: string) => {
        if (msg.includes("Outgoing") || msg.includes("Client")) {
          // Route SSH debug frames through the configured logger, not straight to console.
          (logger?.log ?? ((m: string) => console.log(m)))(msg);
        }
      }
    })
  };

  // Host-key verification (A7). ssh2 performs NO host-key verification unless a `hostVerifier` is
  // supplied — it accepts whatever key the server presents, so the tunnel that exists to protect the
  // DB credentials in transit is silently MITM-able. When the caller pins a fingerprint, verify against
  // it and refuse a mismatch; otherwise warn loudly that verification is off (parity with the TLS
  // `rejectUnauthorized: false` warning).
  if (sshKeys.hostFingerprint) {
    sshConfig.hostVerifier = (key: Buffer): boolean => {
      const ok = fingerprintMatches(key, sshKeys.hostFingerprint!);
      if (!ok) {
        warn(`SSH host-key verification FAILED for ${sshKeys.host}: presented key ${sshHostKeyFingerprint(key)} does not match the pinned hostFingerprint. Refusing the tunnel.`);
      }
      return ok;
    };
  } else {
    warn(`SSH tunnel to ${sshKeys.host} is connecting WITHOUT host-key verification (no 'hostFingerprint' set). The bastion's identity is not checked, so the tunnel — and the database credentials it carries — are susceptible to a man-in-the-middle. Set sshKeys.hostFingerprint (e.g. from 'ssh-keyscan -t ed25519 <host> | ssh-keygen -lf -') to verify.`);
  }

  const sshClient = new SSHClient();

  // ✅ Wrap ssh connection & forwardOut into a single promise
  const stream: ClientChannel = await new Promise((resolve, reject) => {
    sshClient
      .on("ready", () => {
        sshClient.forwardOut(
          sshKeys.source_address ?? "127.0.0.1",
          sshKeys.source_port ?? 0,
          sshKeys.destination_address,
          sshKeys.destination_port,
          (err, stream) => {
            if (err) return reject(err);

            stream.on("close", () => sshClient.end());
            resolve(stream);
          }
        );
      })
      .on("error", reject)
      .connect(sshConfig);
  });

  return { stream, sshClient };
}
