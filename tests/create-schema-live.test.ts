import { DB_CONFIG, Database } from "./utils/testConfig";

// createSchema must report a refused CREATE. runQuery returns success:false on a SQL error instead of
// throwing, and createSchema used to ignore it and return success, so a login without create rights
// "created" the schema and the load then failed with a misleading "schema does not exist" (found live
// on Postgres 16, 2026-10-03). Now it throws with the driver code on error.code.

const RESTRICTED_USER = "autosql_nocreate";
const RESTRICTED_PASSWORD = "NoCreate_pw1";
const DENIED_SCHEMA = "autosql_denied_schema";
const NEW_SCHEMA = "autosql_create_schema_test";

const EXPECTED_CODE: Record<string, string> = { mysql: "ER_DBACCESS_DENIED_ERROR", pgsql: "42501" };

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect === "pgsql" || config.sqlDialect === "mysql")
    .forEach((config) => {
        const isMySQL = config.sqlDialect === "mysql";

        describe(`createSchema (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            let admin: Database;

            const run = async (query: string) => {
                const r = await admin.runQuery({ query, params: [] });
                if (!r.success) throw new Error(`setup query failed: ${query}: ${r.error}`);
            };

            beforeAll(async () => {
                admin = Database.create({ ...config, schema: "test_schema" });
                await admin.establishConnection();
                if (isMySQL) {
                    await run(`DROP USER IF EXISTS '${RESTRICTED_USER}'@'%'`);
                    await run(`CREATE USER '${RESTRICTED_USER}'@'%' IDENTIFIED BY '${RESTRICTED_PASSWORD}'`);
                    // Enough to connect with test_schema as the default database, nothing more.
                    await run(`GRANT SELECT ON test_schema.* TO '${RESTRICTED_USER}'@'%'`);
                } else {
                    await run(`DROP ROLE IF EXISTS ${RESTRICTED_USER}`);
                    // A plain login role has no CREATE ON DATABASE (not granted to PUBLIC by default).
                    await run(`CREATE ROLE ${RESTRICTED_USER} LOGIN PASSWORD '${RESTRICTED_PASSWORD}'`);
                }
                await run(`DROP SCHEMA IF EXISTS ${NEW_SCHEMA}`);
            });

            afterAll(async () => {
                await admin.runQuery({ query: `DROP SCHEMA IF EXISTS ${NEW_SCHEMA}`, params: [] });
                await admin.runQuery({ query: isMySQL ? `DROP USER IF EXISTS '${RESTRICTED_USER}'@'%'` : `DROP ROLE IF EXISTS ${RESTRICTED_USER}`, params: [] });
                await admin.closeConnection();
            });

            test("a successful create returns { [schemaName]: true } (plus deprecated success)", async () => {
                expect(await admin.createSchema(NEW_SCHEMA)).toEqual({ [NEW_SCHEMA]: true, success: true });
                expect(await admin.checkSchemaExists(NEW_SCHEMA)).toEqual({ [NEW_SCHEMA]: true });
            });

            test("an existing schema still succeeds (IF NOT EXISTS)", async () => {
                await admin.createSchema(NEW_SCHEMA);
                expect((await admin.createSchema(NEW_SCHEMA))[NEW_SCHEMA]).toBe(true);
            });

            test("a login without create rights rejects with the driver code", async () => {
                const restricted = Database.create({
                    ...config,
                    user: RESTRICTED_USER,
                    password: RESTRICTED_PASSWORD,
                    ...(isMySQL ? { database: "test_schema" } : {}),
                    schema: "test_schema",
                });
                await restricted.establishConnection();
                try {
                    const err: any = await restricted.createSchema(DENIED_SCHEMA).then(
                        () => { throw new Error("createSchema resolved for a login without create rights"); },
                        (e) => e,
                    );
                    expect(err.code).toBe(EXPECTED_CODE[config.sqlDialect]);
                    expect(err.message).toContain(DENIED_SCHEMA);
                    expect(err.message).toContain(EXPECTED_CODE[config.sqlDialect]);
                } finally {
                    await restricted.closeConnection();
                }
                expect(await admin.checkSchemaExists(DENIED_SCHEMA)).toEqual({ [DENIED_SCHEMA]: false });
            });
        });
    });
