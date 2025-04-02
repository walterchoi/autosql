const safeTypeChanges: Record<string, string[]> = {
    "tinyint": ["smallint", "int", "bigint", "decimal", "varchar"],
    "smallint": ["int", "bigint", "decimal", "varchar"],
    "int": ["bigint", "decimal", "double", "varchar"],
    "bigint": ["decimal", "double", "varchar"],
    "boolean": ["tinyint", "smallint", "int", "varchar", "text"],
    "binary": ["varchar", "text"],
    "decimal": ["double", "varchar"],
    "double": ["varchar"],
    "exponent": ["double", "varchar"],
    "varchar": ["text", "mediumtext", "longtext"],
    "text": ["mediumtext", "longtext"],
    "mediumtext": ["longtext"],
    "json": ["text", "mediumtext", "longtext", "varchar"],
    "datetime": ["datetimetz", "timestamp", "varchar"],
    "datetimetz": ["varchar"],
    "timestamp": ["varchar"],
    "date": ["datetime", "varchar"],
    "time": ["varchar"]
};

export default safeTypeChanges;