import { DB_CONFIG, Database } from "./utils/testConfig";

// Live proof that the `ssl` passthrough performs a REAL TLS handshake — not just that the option is
// threaded to the driver (that's ssl-passthrough.test.ts). Both Docker DBs present a self-signed
// server cert (MySQL 8+/9 enables TLS by default; the pgsql service is configured with `ssl=on` and a
// generated cert — see docker-compose.yml), so we can prove BOTH acceptance criteria without an
// external host:
//   • TLS actually negotiates (the session reports it is encrypted);
//   • verification is REAL — a self-signed/untrusted cert with rejectUnauthorized:true is REJECTED.

Object.values(DB_CONFIG)
    .filter(c => c.sqlDialect === "mysql" || c.sqlDialect === "pgsql")
    .forEach((config) => {
        describe(`TLS handshake (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            // Dialect-specific "is this session actually encrypted?" probe.
            const sslInUse = async (db: Database): Promise<boolean> => {
                if (config.sqlDialect === "mysql") {
                    const r = await db.runQuery({ query: "SHOW SESSION STATUS LIKE 'Ssl_cipher'", params: [] });
                    if (!r.success || !r.results?.length) return false;
                    const cipher = String((r.results[0] as any).Value ?? Object.values(r.results[0])[1] ?? "");
                    return cipher.length > 0;
                }
                const r = await db.runQuery({ query: "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", params: [] });
                return !!(r.success && r.results?.[0] && Object.values(r.results[0])[0] === true);
            };

            test("rejectUnauthorized:false connects over a real (encrypted) TLS session", async () => {
                const db = Database.create({ ...config, schema: "test_schema", useWorkers: false, ssl: { rejectUnauthorized: false } });
                await db.establishConnection();
                try {
                    expect(await sslInUse(db)).toBe(true);
                } finally {
                    await db.closeConnection();
                }
            });

            test("rejectUnauthorized:true REJECTS the untrusted self-signed cert (verification is real)", async () => {
                const db = Database.create({ ...config, schema: "test_schema", useWorkers: false, ssl: { rejectUnauthorized: true } });
                await db.establishConnection();
                try {
                    const r = await db.runQuery({ query: "SELECT 1 AS one", params: [] });
                    // The handshake fails verification, so the query cannot run — proving verification is NOT skipped.
                    expect(r.success).toBe(false);
                    expect(String(r.error)).toMatch(/ssl|tls|handshake|certificate|self.?signed|depth/i);
                } finally {
                    await db.closeConnection();
                }
            }, 20000);
        });
    });
