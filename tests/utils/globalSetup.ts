import { DB_CONFIG } from "./testConfig";
import { Database } from "../../src/db/database";

// Jest global preflight (full suite only). Before any live test runs, verify every configured test
// database is actually reachable. If one is not, fail FAST with a single, actionable message instead
// of dozens of tests failing with a cryptic ECONNREFUSED whose driver message is swallowed to an
// empty string (exactly the confusing failure this guards against). A short retry tolerates a DB that
// is still warming up. Disabled for the unit config (`globalSetup: undefined`), which needs no DB.
export default async function globalSetup(): Promise<void> {
    const entries = Object.entries(DB_CONFIG);
    const MAX_ATTEMPTS = 8; // ~ up to 20s per DB before giving up (each probe ~2s + 1s backoff)
    const unreachable: string[] = [];

    for (const [name, config] of entries) {
        let ok = false;
        let lastErr = "";
        for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            const db = Database.create(config);
            try {
                await db.establishConnection();
                const r = await db.runQuery({ query: "SELECT 1 AS ok", params: [] });
                if (r.success) ok = true;
                else lastErr = r.error || "connection failed";
            } catch (e: any) {
                lastErr = e?.code || e?.message || String(e);
            } finally {
                try { await db.closeConnection(); } catch { /* ignore */ }
            }
            if (ok) break;
            if (attempt < MAX_ATTEMPTS) await new Promise((res) => setTimeout(res, 1000));
        }
        if (!ok) {
            unreachable.push(`${name} (${config.sqlDialect} @ ${config.host || "localhost"}:${config.port ?? "?"}) — ${lastErr || "no connection"}`);
        }
    }

    if (unreachable.length) {
        throw new Error(
            `\n\n❌ Database preflight failed — these test databases are not reachable:\n` +
            unreachable.map((u) => `   • ${u}`).join("\n") +
            `\n\nStart them (and wait for healthy), then re-run the suite:\n` +
            `   npm run db:up\n\n` +
            `This check exists so a DB-down run fails clearly here, instead of as dozens of empty\n` +
            `ECONNREFUSED errors deep in the live tests.\n`
        );
    }
}
