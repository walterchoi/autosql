import { MySQLTableQueryBuilder } from "../src/db/queryBuilders/mysql/tableBuilder";
import { PostgresTableQueryBuilder } from "../src/db/queryBuilders/pgsql/tableBuilder";
import { SqlServerTableQueryBuilder } from "../src/db/queryBuilders/sqlserver/tableBuilder";
import type { MetadataHeader, QueryInput } from "../src/config/types";

const sql = (q: QueryInput): string => typeof q === "string" ? q : q.query;

// maxIndexCount (mysql/pgsql: 64, sqlserver: 250) is budgeted PK -> unique -> regular index, each
// slicing what's left in that order, independently implemented in each dialect's tableBuilder.
// Zero coverage anywhere: a table whose unique+regular index count approaches the limit could
// silently drop indexes in the wrong order (or the wrong COUNT) with nothing catching it.

/** `count` unique columns named `${prefix}0`..`${prefix}(count-1)`, in that insertion order. */
function namedColumns(count: number, prefix: string, flag: "unique" | "index"): MetadataHeader {
    const header: MetadataHeader = {};
    for (let i = 0; i < count; i++) {
        header[`${prefix}${i}`] = { type: "int", allowNull: true, [flag]: true };
    }
    return header;
}

describe("index budget capping — MySQL (maxIndexCount: 64)", () => {
    test("PK takes 1 slot, unique keys take the next 63 in order, indexes get none of what's left", () => {
        const headers: MetadataHeader = {
            pk0: { type: "int", allowNull: false, primary: true },
            ...namedColumns(70, "u", "unique"),  // 70 candidates, only 63 slots remain after PK
            ...namedColumns(70, "ix", "index"),  // budget is already 0 by the time these are considered
        };
        const queries = MySQLTableQueryBuilder.getCreateTableQuery("t", headers);
        const createTableSql = sql(queries[0]);

        expect(createTableSql).toContain("PRIMARY KEY (`pk0`)");
        // First 63 unique columns included, by insertion order.
        expect(createTableSql).toContain("UNIQUE(`u0`)");
        expect(createTableSql).toContain("UNIQUE(`u62`)");
        // The 64th+ candidate (budget exhausted) is NOT included.
        expect(createTableSql).not.toContain("UNIQUE(`u63`)");
        expect(createTableSql).not.toContain("UNIQUE(`u69`)");
        // Nothing left for regular indexes at all — zero CREATE INDEX statements.
        const createIndexQueries = queries.slice(1);
        expect(createIndexQueries).toHaveLength(0);
    });

    test("a composite (multi-column) primary key still only costs ONE slot, not one per column", () => {
        const headers: MetadataHeader = {
            pk0: { type: "int", allowNull: false, primary: true },
            pk1: { type: "int", allowNull: false, primary: true },
            ...namedColumns(64, "u", "unique"), // if PK cost 2 slots, only 62 would fit; if 1, all 63 that CAN fit do
        };
        const queries = MySQLTableQueryBuilder.getCreateTableQuery("t", headers);
        const createTableSql = sql(queries[0]);
        expect(createTableSql).toContain("UNIQUE(`u62`)"); // the 63rd unique key (index 62) still fits
        expect(createTableSql).not.toContain("UNIQUE(`u63`)"); // the 64th does not (63 slots total after PK)
    });

    test("under budget: every unique key and every index is included, nothing dropped", () => {
        const headers: MetadataHeader = {
            pk0: { type: "int", allowNull: false, primary: true },
            ...namedColumns(2, "u", "unique"),
            ...namedColumns(2, "ix", "index"),
        };
        const queries = MySQLTableQueryBuilder.getCreateTableQuery("t", headers);
        expect(sql(queries[0])).toContain("UNIQUE(`u0`)");
        expect(sql(queries[0])).toContain("UNIQUE(`u1`)");
        const createIndexQueries = queries.slice(1);
        expect(createIndexQueries).toHaveLength(2);
    });
});

describe("index budget capping — Postgres (maxIndexCount: 64)", () => {
    test("PK takes 1 slot, unique keys take the next 63 in order, indexes get none of what's left", () => {
        const headers: MetadataHeader = {
            pk0: { type: "int", allowNull: false, primary: true },
            ...namedColumns(70, "u", "unique"),
            ...namedColumns(70, "ix", "index"),
        };
        const queries = PostgresTableQueryBuilder.getCreateTableQuery("t", headers);
        const createTableSql = sql(queries[0]);

        expect(createTableSql).toContain('PRIMARY KEY ("pk0")');
        expect(createTableSql).toContain('UNIQUE("u0")');
        expect(createTableSql).toContain('UNIQUE("u62")');
        expect(createTableSql).not.toContain('UNIQUE("u63")');
        expect(createTableSql).not.toContain('UNIQUE("u69")');
        expect(queries.slice(1)).toHaveLength(0);
    });
});

describe("index budget capping — SQL Server (maxIndexCount: 250)", () => {
    test("PK takes 1 slot, unique keys take the next 249 in order, indexes get none of what's left", () => {
        const headers: MetadataHeader = {
            pk0: { type: "int", allowNull: false, primary: true },
            ...namedColumns(252, "u", "unique"), // 252 candidates, only 249 slots remain after PK
            ...namedColumns(5, "ix", "index"),
        };
        const queries = SqlServerTableQueryBuilder.getCreateTableQuery("t", headers);
        const createTableSql = sql(queries[0]);

        expect(createTableSql).toContain("PRIMARY KEY ([pk0])");
        expect(createTableSql).toContain("UNIQUE([u0])");
        expect(createTableSql).toContain("UNIQUE([u248])"); // the 249th (index 248) still fits
        expect(createTableSql).not.toContain("UNIQUE([u249])"); // the 250th candidate does not
        expect(queries.slice(1)).toHaveLength(0);
    });
});
