import { MySQLIndexQueryBuilder } from "../src/db/queryBuilders/mysql/indexBuilder";
import { PostgresIndexQueryBuilder } from "../src/db/queryBuilders/pgsql/indexBuilder";
import { SqlServerIndexQueryBuilder } from "../src/db/queryBuilders/sqlserver/indexBuilder";
import { escapeIdentifier } from "../src/db/utils/escape";
import type { supportedDialects } from "../src/config/types";

// Characterization test, written BEFORE collapsing the byte-identical getAddPrimaryKeyQuery body
// across all three dialect indexBuilders into a shared helper (see the lint-and-dedup audit, Tier 1).
// Locks down the exact SQL + params each dialect's builder emits today, so the refactor can be
// proven to change nothing.
describe("getAddPrimaryKeyQuery — exact SQL per dialect (pre-dedup characterization)", () => {
    const q = (name: string, dialect: supportedDialects) => escapeIdentifier(name, dialect);

    const builders = [
        { dialect: "mysql" as const, builder: MySQLIndexQueryBuilder },
        { dialect: "pgsql" as const, builder: PostgresIndexQueryBuilder },
        { dialect: "sqlserver" as const, builder: SqlServerIndexQueryBuilder },
    ];

    for (const { dialect, builder } of builders) {
        test(`${dialect}: single-column PK, no schema`, () => {
            const result = builder.getAddPrimaryKeyQuery("my_table", ["id"]);
            expect(result).toEqual({
                query: `ALTER TABLE ${q("my_table", dialect)} ADD PRIMARY KEY (${q("id", dialect)});`,
                params: []
            });
        });

        test(`${dialect}: composite PK, with schema`, () => {
            const result = builder.getAddPrimaryKeyQuery("my_table", ["id", "tenant_id"], "my_schema");
            expect(result).toEqual({
                query: `ALTER TABLE ${q("my_schema", dialect)}.${q("my_table", dialect)} ADD PRIMARY KEY (${q("id", dialect)}, ${q("tenant_id", dialect)});`,
                params: []
            });
        });
    }
});
