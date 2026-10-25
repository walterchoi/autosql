# Security & safe operation

See the [README](../README.md) for a quick start, and [Configuration](configuration.md) for the
full config field reference.

## SSH support

AutoSQL supports SSH tunneling for connecting to remote MySQL or PostgreSQL servers via an
intermediate gateway. Include the SSH configuration inside your `DatabaseConfig` under the
`sshConfig` key — AutoSQL establishes the tunnel automatically when `establishConnection()` is
called.

⚠ **MySQL and Postgres only.** `sshConfig` (or `sshStream`) with `sqlDialect: "sqlserver"`
**throws**, because the `mssql` driver is never handed the tunnel's stream — the connection would
be made directly and not through the tunnel. For SQL Server, reach the server by VPN or peering,
and use `ssl` for encryption in transit.

```ts
const config: DatabaseConfig = {
  ...
  sshConfig: {
    username: 'ssh_user',
    host: 'remote_host',
    port: 22,
    password: 'password',
    private_key: 'PRIVATE_KEY_STRING',
    private_key_path: '/path/to/key.pem',
    source_address: 'localhost',
    source_port: 3306,
    destination_address: 'remote_sql_host',
    destination_port: 3306,
    // Verify the bastion's SSH host key to close the MITM window on the tunnel (recommended).
    // Obtain with: ssh-keyscan -t ed25519 remote_host | ssh-keygen -lf -
    hostFingerprint: 'SHA256:abc123…'
  }
}

const db = Database.create(config);
await db.establishConnection();
// Tunnel is now active and DB connection is routed through it
```

**Host-key verification.** Set `hostFingerprint` to the SSH server's expected OpenSSH fingerprint
(`"SHA256:…"`, prefix/padding optional) and autosql verifies the bastion's host key on connect and
**refuses a mismatch** — otherwise the tunnel would trust any server on that address (a MITM
window, since ssh2 does no verification by default). Omit it and the tunnel still connects but
logs a **loud warning** that its identity is unverified.

## TLS / encrypted connections

Connect over direct TLS (MySQL / Postgres / SQL Server) with the `ssl` config — e.g. a managed
Postgres/RDS that requires SSL, or a customer-hosted MySQL. It's passed through to the driver, so
anything `mysql2` / `pg` accepts works:

```ts
// Verify the server against a CA bundle (recommended for production)
const db = Database.create({
  sqlDialect: 'pgsql', host, user, password, database,
  ssl: { ca: fs.readFileSync('rds-ca-bundle.pem', 'utf8'), rejectUnauthorized: true },
});

// Enable TLS with the driver's default verification
Database.create({ ...config, ssl: true });

// Dev / self-signed only — verification OFF (autosql logs a warning)
Database.create({ ...config, ssl: { rejectUnauthorized: false } });
```

- Omit `ssl` (or `ssl: false`) → unchanged plaintext/driver-default behaviour.
- Pick **one** encrypted path — a direct TLS connection **or** an SSH tunnel (`sshConfig`), not both.
- Works on **all three dialects**. MySQL/Postgres receive `ssl` as the driver's `ssl` object; **SQL Server** maps it automatically onto the `mssql` driver's `encrypt` / `trustServerCertificate` / `cryptoCredentialsDetails` (custom CA, mutual TLS, SNI) options — so the same `{ ca, rejectUnauthorized }` config works everywhere.

## Operating safely (least-privilege / BYOD)

AutoSQL is safe to run inside a database you don't own — e.g. a customer's server where it loads
into a namespace you control. The guarantees:

- **Everything stays inside `config.schema`.** Every object AutoSQL creates — target table, `temp_staging__*`, `*__history`, the schema-history audit table, the rejected-rows table, surrogate/`dwh_*` columns — is qualified with `config.schema` (or a schema option that defaults to it). AutoSQL **never** emits `DROP DATABASE` or `DROP SCHEMA`.
- **Non-destructive by default.** With the defaults `deleteColumns: false` and `updatePrimaryKey: false`, AutoSQL issues no `DROP COLUMN`, no primary-key drop, and no `TRUNCATE` against your table — the only tables it drops are its own `temp_staging__*` staging tables. A blocked column-drop / PK-change is logged, not applied. (There is no `TRUNCATE` anywhere in AutoSQL.)
- **No implicit schema creation.** `autoSQL()` assumes `config.schema` already exists — it does not run `CREATE SCHEMA`/`CREATE DATABASE`. A least-privilege user that can write inside a pre-created schema but has no `CREATE SCHEMA` privilege works fine. (Create the schema yourself first via the exported `createSchema()` only if you hold the privilege.)
- **No `FILE` privilege needed by default.** The default load path is parameterised multi-row `INSERT`. `LOAD DATA LOCAL INFILE` (MySQL) / `COPY` (Postgres) run **only** when you opt into `bulkLoad: true`.
- **Errors identify the missing grant.** A failed query returns the driver's message on `error` **and** its structured code on `errorCode` (mysql2 `code` / Postgres SQLSTATE / SQL Server error number), so you can tell exactly which `GRANT` is missing.
- **Connection budget is respected.** `connectionLimit` caps the pool (set it low — 1–2 — for a small/shared DB), and `closeConnection()` fully drains it, leaving no lingering connections.
- **Identifier limits are enforced up front.** A table/column name that would exceed the dialect's identifier limit (MySQL 64 chars, Postgres 63 bytes, SQL Server 128 chars) throws a clear error at generation time rather than failing mid-load or being silently truncated by Postgres.
