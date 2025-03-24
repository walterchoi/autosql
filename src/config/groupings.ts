export const groupings = {
    "intGroup": ["boolean", "binary", "tinyint", "smallint", "int", "numeric", "bigint"],
    "specialIntGroup": ["decimal", "double", "exponent"],
    "textGroup": ["varchar", "text", "mediumtext", "longtext"],
    "specialTextGroup": ["json"],
    "dateGroup": ["date", "time", "datetime", "datetimetz", 'timestamp', 'timestamptz'],
    "keysGroup": ["tinyint", "smallint", "int", "varchar"]
}

export const isNumeric = (type: string) => ["tinyint", "smallint", "int", "bigint", "numeric", "decimal", "double", "exponent"].includes(type);
export const isInteger = (type: string) => ["tinyint", "smallint", "int", "bigint"].includes(type);
export const isFloating = (type: string) => ["numeric", "decimal", "double", "exponent"].includes(type);
export const isText = (type: string) => ["varchar", "text", "mediumtext", "longtext", "json"].includes(type);
export const isBoolean = (type: string) => type === "boolean";
export const isDate = (type: string) => ["date", "datetime", "datetimetz"].includes(type);
export const isTime = (type: string) => type === "time";