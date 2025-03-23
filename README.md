# AutoSQL - Automated SQL Insertions for Modern Data Workflows

![NPM](https://nodei.co/npm/autosql.png)

> **Now rewritten in TypeScript with an entirely new class-based structure!**

## 🚀 AutoSQL — A Smarter Way to Insert Data

AutoSQL is a TypeScript-powered tool that simplifies and automates the SQL insertion process with intelligent schema prediction, safe table handling, batching, and dialect-specific optimisations for MySQL and PostgreSQL.

### 🔧 New in This Version:
- Full **TypeScript** support
- Core logic restructured into a reusable `Database` **class-based architecture**
- Robust error handling and logging support
- Modular utilities for type prediction, metadata inference, batching, and SSH tunneling

---

## 📦 Installation

```bash
npm install autosql
```

---

## 📚 Table of Contents

- [Supported SQL Dialects](#-supported-sql-dialects)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Insert Options](#-insert-options)
- [Core Interfaces](#-core-interfaces-database-and-autosqlhandler)
  - [`Database` Class](#-database-class)
  - [`AutoSQLHandler` Class](#-autosqlhandler-class)
- [Convenience Utilities](#-convenience-utilities)
  - [Type Inference & Normalisation](#-type-inference--normalisation)
  - [Config & Metadata Tools](#-config--metadata-tools)
  - [Metadata Inference & Preparation](#-metadata-inference--preparation)
  - [Insert Planning & Execution](#-insert-planning--execution)

---

## 🧬 Supported SQL Dialects

AutoSQL supports:

- **MySQL** (via `mysql2`)
- **PostgreSQL** (via `pg`)

Optional support for SSH tunneling is available via:

- [`ssh2`](https://www.npmjs.com/package/ssh2)

---

## ⚡ Quick Start

```ts
import { Database } from 'autosql';

const config = {
  sqlDialect: 'mysql',
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'mysql',
  port: 3306
};

const data = [
  { id: 1, name: 'Alice', created_at: '2024-01-01' },
  { id: 2, name: 'Bob', created_at: '2024-01-02' }
];

let db: Database;

db = Database.create(config);
await db.establishConnection();

await db.insertData({ table: 'target_table', data });

await db.closeConnection();
```

AutoSQL will:
- Infer metadata and key structure
- Create or alter the target table
- Batch insert rows
- Handle dialect-specific quirks automatically

---

## ⚙️ Configuration

```ts
export interface DatabaseConfig {
  // Required connection settings
  sqlDialect: 'mysql' | 'pgsql';
  host?: string;
  user?: string;
  password?: string;
  database?: string;
  port?: number;

  // Optional table target
  schema?: string;
  table?: string;

  // Metadata control
  metaData?: { [tableName: string]: MetadataHeader };
  existingMetaData?: { [tableName: string]: MetadataHeader };
  updatePrimaryKey?: boolean;
  primaryKey?: string[];

  // Table creation and charset settings
  engine?: string;
  charset?: string;
  collate?: string;
  encoding?: string;
  addTimestamps?: boolean; // If TRUE, runs function ensureTimestamps as part of AutoSQL function. Which adds a dwh_created_at, dwh_modified_at and dwh_loaded_at timestamp columns that are automatically filled. - defaults to TRUE

  // Type inference controls
  pseudoUnique?: number; // The % of values that must be unique to be considered pseudoUnique. - defaults to 0.9 (90%)
  autoIndexing?: boolean; // Automatically identify and add indexes to tables when altering / creating - defaults to TRUE
  decimalMaxLength?: number; // Automatically round decimals to a maximum of X decimal places - defaults to 10
  maxKeyLength?: number; // Limits indexes / primary keys from using columns that are longer than this length - defaults to 255

  // Sampling controls
  sampling?: number;
  samplingMinimum?: number;

  // Insert strategy
  insertType?: 'UPDATE' | 'INSERT'; // UPDATE automatically replaces non-primary key values with new values that are found
  insertStack?: number; // Maximum number of rows to insert in one query - defaults to 100
  safeMode?: boolean; // Prevent the altering of tables if needed - defaults to FALSE 
  deleteColumns?: boolean; // Drop columns if needed - defaults to FALSE

  // Batching + scaling
  autoSplit?: boolean;
  useWorkers?: boolean;
  maxWorkers?: number;

  // SSH tunneling support
  sshConfig?: SSHKeys;
  sshStream?: ClientChannel | null;
  sshClient?: SSHClient;
}
```

---

## 🧠 Metadata Format

AutoSQL can infer metadata from your data, or you can specify it manually:

```ts
meta_data: [
  {
    created_at: {
      type: 'datetime',
      length: 0,
      allowNull: true,
      default: 'CURRENT_TIMESTAMP',
      index: true
    }
  },
  {
    name: {
      type: 'varchar',
      length: 50,
      allowNull: false,
      unique: true,
      primary: true
    }
  }
]
```

---

## 🔐 SSH Support

```ts
ssh_config: {
  username: 'ssh_user',
  host: 'remote_host',
  port: 22,
  password: 'password',
  private_key: 'PRIVATE_KEY_STRING',
  private_key_path: '/path/to/key.pem',
  source_address: 'localhost',
  source_port: 3306,
  destination_address: 'remote_sql_host',
  destination_port: 3306
}
```

---

## 📑 Insert Options

These control batching and insert behaviour:

- `insertType`: `'UPDATE' | 'INSERT'` — Determines behaviour on duplicate keys. `UPDATE` replaces non-primary key values with the new ones. Defaults to `'INSERT'`.
- `insertStack`: `number` — Maximum number of rows to insert in a single query. Defaults to `100`.
- `safeMode`: `boolean` — If `true`, prevents table alterations during runtime. Defaults to `false`.
- `deleteColumns`: `boolean` — Allows dropping of existing columns when altering tables. Defaults to `false`.
- `autoSplit`: `boolean` — Automatically splits large datasets into smaller chunks if they exceed size limits.
- `useWorkers`: `boolean` — Enables background workers for batch inserts. Useful for scaling large jobs.
- `maxWorkers`: `number` — Maximum number of workers to run in parallel.

---

### 🏁 Core Interfaces: `Database` and `AutoSQLHandler`

These are the primary entry points into AutoSQL's workflow. The `Database` class handles connection management, while `AutoSQLHandler` exposes end-to-end insert automation and table management functions.

```ts
import { Database } from 'autosql';

const db = Database.create(config);
await db.establishConnection();

const result = await db.autoSQL.autoConfigureTable(
  'target_table', // table name
  sampleData,     // raw input data
  null,           // optional existing metadata
  initialMeta     // optional manually defined metadata
);
```

This is the core interface for managing connections, generating queries, and executing inserts.

#### 🔹 Core Methods
- **`getConfig()`** – Returns the full `DatabaseConfig` used to initialise this instance.
- **`updateTableMetadata(table, metaData, type?)`** – Updates stored metadata under the given table key.
- **`updateSchema(schema)`** – Updates the current schema name being used.
- **`getDialect()`** – Returns the SQL dialect (`mysql` or `pgsql`).
- **`establishConnection()`** – Creates and stores a live database connection.
- **`runQuery(queryOrParams)`** – Executes a SQL query or list of queries.
- **`testConnection()`** – Attempts to connect and returns success as boolean.
- **`checkSchemaExists(schemaName)`** – Returns whether the given schema(s) exist.
- **`createSchema(schemaName)`** – Creates the schema if it doesn't exist already.
- **`createTableQuery(table, headers)`** – Returns `QueryInput[]` to create a table.
- **`alterTableQuery(table, oldHeaders, newHeaders)`** – Returns `QueryInput[]` to alter an existing table.
- **`dropTableQuery(table)`** – Returns a `QueryInput` to drop a table.
- **`startTransaction()` / `commit()` / `rollback()`** – Manages manual transaction blocks.
- **`closeConnection()`** – Safely closes the active DB connection.
- **`runTransaction(queries)`** – Runs multiple queries inside a single transaction.
- **`runTransactionsWithConcurrency(queryGroups)`** – Runs multiple query batches in parallel.
- **`getTableMetaData(schema, table)`** – Fetches current metadata from the DB for a given table.

#### 🔸 Static Method
- **`Database.create(config)`** – Returns an instance of either `MySQLDatabase` or `PostgresDatabase` based on config.

#### 🧪 Abstract (Dialect-Specific) Methods
- **`getPermanentErrors()`** – Returns known non-retryable database error codes or patterns.
- **`getDialectConfig()`** – Provides dialect-specific query constraints and formatting rules.
- **`establishDatabaseConnection()`** – Initializes the actual SQL driver connection.
- **`testQuery(query)`** – Runs a query in test mode to validate structure or reachability.
- **`executeQuery(query)`** – Executes a raw SQL command using the established driver.
- **`getCreateSchemaQuery(schemaName)`** – Returns a `CREATE SCHEMA` query for the current dialect.
- **`getCheckSchemaQuery(schemaName)`** – Returns a `SELECT` query to check if schema exists.
- **`getCreateTableQuery(table, headers)`** – Builds a dialect-specific `CREATE TABLE` statement.
- **`getAlterTableQuery(table, changesOrOldMeta, newMeta?)`** – Returns SQL to alter a table structure, either from a diff or two sets of metadata.
- **`getDropTableQuery(table)`** – Produces a `DROP TABLE` statement.
- **`getPrimaryKeysQuery(table)`** – Returns a query to fetch current primary keys.
- **`getForeignKeyConstraintsQuery(table)`** – Returns foreign key references to or from this table.
- **`getViewDependenciesQuery(table)`** – Lists any views that depend on this table.
- **`getDropPrimaryKeyQuery(table)`** – Builds query to remove current primary key.
- **`getAddPrimaryKeyQuery(table, keys)`** – Builds query to add primary key using provided column(s).
- **`getUniqueIndexesQuery(table, column?)`** – Fetches defined unique indexes for a table (and optional column).
- **`getTableExistsQuery(schema, table)`** – Generates a query to verify the existence of a table.
- **`getTableMetaDataQuery(schema, table)`** – Extracts column-level metadata from the DB.
- **`getSplitTablesQuery(table)`** – Returns SQL to list physical table partitions or children (if supported).
- **`getInsertStatementQuery(tableOrInput, data?, metaData?)`** – Produces a full insert query based on inputs.
- **`getMaxConnections()`** – Returns a number representing how many DB connections are safe to use (based on configuration).

Each method is designed to work with the same `Database` instance.

---

### ⚙️ `AutoSQLHandler` Class

This class is accessible via `db.autoSQL` and orchestrates metadata generation, table management, and data insertions using a set of high-level, reusable methods.

- **`autoSQL(table, data, schema?)`** – The all-in-one method that runs metadata prediction, table creation or alteration, and full data insertion. Handles worker concurrency, batch splitting, and uses all helper methods under the hood.
- **`insertData(inputOrTable, data?, metaData?, previousMeta?, comparedMeta?, runQuery = true)`** – Inserts data using the dialect-aware batching engine. Accepts either a combined `InsertInput` or separate arguments.
- **`splitTableData(table, data, metaData)`** – If enabled, splits a large dataset across multiple tables. Returns an array of table-specific insert instructions.
- **`fetchTableMetadata(table)`** – Retrieves existing metadata from the database and returns `{ currentMetaData, tableExists }`.
- **`autoConfigureTable(inputOrTable, data?, currentMeta?, newMeta?, runQuery = true)`** – Smart wrapper that checks if a table needs to be created or altered based on incoming metadata.
- **`autoAlterTable(table, tableChanges, tableExists?, runQuery = true)`** – Alters the table structure using a pre-computed `AlterTableChanges` object.
- **`autoCreateTable(table, newMetaData, tableExists?, runQuery = true)`** – Creates a new table using the provided metadata definition.
- **`getMetaData(config, data)`** – Analyses a sample dataset and returns metadata including types, lengths, and uniqueness predictions.
- **`compareMetaData(oldMeta, newMeta)`** – Compares two metadata structures and returns the necessary changes.

Each of these methods is used internally by `autoSQL` and can be called directly for more granular control.

---

## 🧰 Convenience Utilities

AutoSQL exposes utilities that power `autoSQL` and can be used independently. These include metadata analysis, SQL formatting, batching, config validation, and more.

### 🔍 Type Inference & Normalisation

- **`predictType(value)`** – Predicts SQL-compatible type (`varchar`, `datetime`, `int`, etc.) based on a single input value.
- **`collateTypes(typeSetOrArray)`** – Accepts a `Set` or `Array` of types and returns a single compatible SQL type.
- **`normalizeNumber(input, thousands, decimal)`** – Standardises numeric values to SQL-safe format with optional locale indicators.
- **`calculateColumnLength(column, value, sqlLookup)`** – Dynamically computes and updates column length and decimal precision based on input data.
- **`shuffleArray(array)`** – Randomly reorders an array (used for sampling).
- **`isObject(val)`** – Type-safe check to determine if a value is a non-null object.

### ⚙️ Config & Metadata Tools

- **`validateConfig(config)`** – Validates and merges the provided `DatabaseConfig` with default settings.
- **`mergeColumnLengths(lengthA, lengthB)`** – Chooses the greater length definition between two metadata column states.
- **`setToArray(set)`** – Converts a Set to a regular array.
- **`normalizeKeysArray(keys)`** – Flattens and sanitizes arrays of key strings (e.g., for primary keys).
- **`isValidDataFormat(data)`** – Checks if the input is a valid array of plain objects suitable for inserts.

### 🧠 Metadata Inference & Preparation

- **`initializeMetaData(headers)`** – Constructs a default metadata object from column headers with default flags and null types.
- **`getDataHeaders(data, config)`** – Scans sample data to derive column names and infer initial metadata.
- **`predictIndexes(metaData, maxKeyLength?, primaryKey?, sampleData?)`** – Suggests primary keys, unique constraints, and indexes based on uniqueness, length limits, or configured priorities.
- **`updateColumnType(existingMeta, newValue)`** – Adjusts the type and attributes of a column based on new sample input.

### 📦 Insert Planning & Execution

- **`splitInsertData(data, config)`** – Splits large datasets into batches that meet size and row count constraints.
- **`getInsertValues(metaData, row, dialectConfig)`** – Extracts a single row's values as a SQL-safe array, accounting for dialect-specific formatting.
- **`organizeSplitData(data, splitMetaData)`** – Partitions the dataset by metadata groups for multiple table insert strategies.
- **`organizeSplitTable(table, newMetaData, currentMetaData, dialectConfig)`** – Generates split metadata configurations based on structural divergence.
- **`estimateRowSize(metaData, dialect)`** – Estimates the byte size of a row using provided metadata and flags potential overflows.
- **`parseDatabaseMetaData(rows, dialectConfig?)`** – Transforms SQL column descriptions into AutoSQL-compatible metadata.
- **`tableChangesExist(alterTableChanges)`** – Returns `true` if the proposed table changes indicate schema modification is needed.
- **`isMetaDataHeader(obj)`** – Type guard to check if an object qualifies as a metadata header.
- **`isValidDataFormat(data)`** – Validates that the input is an array of row objects suitable for processing.

---

## 📬 Feedback

This library is under active development. Suggestions, issues, and contributions are welcome.

Contact: **w@walterchoi.com**
