import { parseDatabaseMetaData } from "../src/helpers/utilities";
import { mysqlConfig } from "../src/db/config/mysqlConfig";
import { MetadataHeader } from "../src/config/types";

// R10: MySQL has no native boolean, so `tinyint(1)` is the boolean convention while a plain
// `tinyint` is a small integer. Introspection previously mapped every `tinyint` to boolean (it read
// only DATA_TYPE), which on re-ingest triggered a destructive boolean→int conversion. It must now
// use COLUMN_TYPE to map only `tinyint(1)` to boolean.

const row = (name: string, dataType: string, columnType: string) => ({
    COLUMN_NAME: name, DATA_TYPE: dataType, COLUMN_TYPE: columnType,
    IS_NULLABLE: "YES", LENGTH: null, EXTRA: "", column_key: "",
});

describe("MySQL tinyint introspection (R10)", () => {
    test("tinyint(1) -> boolean, plain tinyint -> tinyint, int -> int", () => {
        const meta = parseDatabaseMetaData([
            row("flag", "tinyint", "tinyint(1)"),
            row("small_int", "tinyint", "tinyint"),
            row("wide_tinyint", "tinyint", "tinyint(3)"),
            row("count", "int", "int"),
        ], mysqlConfig) as MetadataHeader;

        expect(meta.flag.type).toBe("boolean");        // the boolean convention preserved
        expect(meta.small_int.type).toBe("tinyint");   // small int, NOT boolean (the fix)
        expect(meta.wide_tinyint.type).toBe("tinyint"); // any non-(1) width is an integer
        expect(meta.count.type).toBe("int");
    });
});
