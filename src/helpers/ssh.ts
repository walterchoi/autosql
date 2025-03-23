import { readFile } from "fs/promises";
import type { Client, ClientChannel } from "ssh2";
import type { SSHKeys } from "../config/types";

export async function setSSH(sshKeys: SSHKeys): Promise<{ stream: ClientChannel; sshClient: Client }> {
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

  if (sshKeys.private_key_path && !sshKeys.private_key) {
    sshKeys.private_key = await readFile(sshKeys.private_key_path, "utf-8");
  }

  const sshConfig: any = {
    host: sshKeys.host,
    port: sshKeys.port,
    username: sshKeys.username,
    readyTimeout: sshKeys.timeout ?? 10000,
    ...(sshKeys.password && { password: sshKeys.password }),
    ...(sshKeys.private_key && { privateKey: sshKeys.private_key }),
    ...(sshKeys.debug && {
      debug: (msg: string) => {
        if (msg.includes("Outgoing") || msg.includes("Client")) {
          console.log(msg);
        }
      }
    })
  };

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