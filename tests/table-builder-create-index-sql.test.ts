import { MySQLTableQueryBuilder } from "../src/db/queryBuilders/mysql/tableBuilder";
import { PostgresTableQueryBuilder } from "../src/db/queryBuilders/pgsql/tableBuilder";
import { SqlServerTableQueryBuilder } from "../src/db/queryBuilders/sqlserver/tableBuilder";
import { escapeIdentifier } from "../src/db/utils/escape";
import { generateSafeConstraintName } from "../src/helpers/utilities";
import type { MetadataHeader, QueryInput } from "../src/config/types";
import type { supportedDialects } from "../src/config/types";

const queryText = (q: QueryInput) => typeof q === "string" ? q : q.query;

// Characterization test, written BEFORE collapsing the near-identical "CREATE INDEX per flagged
// column" loop across all three dialect tableBuilders into a shared helper (lint-and-dedup audit,
// Tier 1). Locks down the exact trailing CREATE INDEX statements each dialect emits today for a
// table with two indexed (non-unique, non-primary) columns.
describe("getCreateTableQuery — trailing CREATE INDEX statements per dialect (pre-dedup characterization)", () => {
    const header: MetadataHeader = {
        id: { type: "int", length: 11, primary: true, allowNull: false },
        a: { type: "varchar", length: 50, allowNull: true, index: true } as any,
        b: { type: "varchar", length: 50, allowNull: true, index: true } as any,
    };

    // mysql/pgsql: no schema configured → no prefix. sqlserver: always defaults to "dbo" (no
    // DatabaseConfig.schema override here) — the three dialects genuinely differ on this default.
    function expectedIndexQueries(dialect: supportedDialects, table: string, schemaPrefix = "") {
        const q = (name: string) => escapeIdentifier(name, dialect);
        return ["a", "b"].map((col) => {
            const indexName = generateSafeConstraintName(table, col, "index");
            return `CREATE INDEX ${q(indexName)} ON ${schemaPrefix}${q(table)} (${q(col)});`;
        });
    }

    test("mysql", () => {
        const queries = MySQLTableQueryBuilder.getCreateTableQuery("t", header);
        expect(queries.slice(1).map(q => queryText(q))).toEqual(expectedIndexQueries("mysql", "t"));
    });

    test("pgsql", () => {
        const queries = PostgresTableQueryBuilder.getCreateTableQuery("t", header);
        expect(queries.slice(1).map(q => queryText(q))).toEqual(expectedIndexQueries("pgsql", "t"));
    });

    test("sqlserver", () => {
        const queries = SqlServerTableQueryBuilder.getCreateTableQuery("t", header);
        // sqlserver's CREATE TABLE is a single IF-NOT-EXISTS/BEGIN...END batch at index 0, same shape.
        expect(queries.slice(1).map(q => queryText(q))).toEqual(expectedIndexQueries("sqlserver", "t", `${escapeIdentifier("dbo", "sqlserver")}.`));
    });
});
