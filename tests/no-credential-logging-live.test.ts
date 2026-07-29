import { DB_CONFIG, Database } from "./utils/testConfig";

// R7: the library must never log connection credentials. Regression guard — drive a real
// authentication failure (correct host/port, a sentinel wrong password) and capture every logger
// channel; the password value must not appear in any log/warn/error output. Driver error messages
// may legitimately include the host/user (their convention), but never the secret.

const SENTINEL_PASSWORD = "S3NTINEL_wrong_pw_do_not_log_9x7q";

Object.values(DB_CONFIG).forEach((config) => {
    describe(`no credential logging (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        test("a failed connection never logs the password", async () => {
            const logs: string[] = [];
            const capture = (m: any) => logs.push(String(m));
            const db = Database.create({
                ...config,
                password: SENTINEL_PASSWORD,
                logger: { log: capture, warn: capture, error: capture },
                // Fail fast rather than retrying the bad password many times.
                maxQueryAttempts: 1,
            } as any);

            // Any of these may throw (auth rejected / connection failed) — that's expected. We only
            // care that whatever gets logged along the way does not contain the secret.
            try {
                await db.establishConnection();
                await db.runQuery({ query: "SELECT 1 AS one", params: [] });
            } catch {
                /* expected: authentication failure */
            } finally {
                await db.closeConnection().catch(() => {});
            }

            const joined = logs.join("\n");
            expect(joined).not.toContain(SENTINEL_PASSWORD);
        });
    });
});
