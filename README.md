# AutoSQL - Automated SQL Insertions for Modern Data Workflows

![NPM](https://nodei.co/npm/autosql.png)

> **Now rewritten in TypeScript with an entirely new class-based structure!**

## 🚀 AutoSQL — A Smarter Way to Insert Data

AutoSQL is a TypeScript-powered tool that simplifies and automates the SQL insertion process with intelligent schema prediction, safe table handling, batching, and dialect-specific optimisations for MySQL and PostgreSQL.

## 🌐 Overview

AutoSQL helps engineers and analysts insert structured or semi-structured JSON into SQL databases (MySQL/PostgreSQL) with zero manual schema prep. It's ideal for:

- No-code/low-code tools that export data as raw JSON
- Rapid data warehousing of API responses or flat files
- Auto-generating schemas with correct types, keys, and indexes

It shines in modern ETL workflows where structure is unpredictable but SQL output is needed.

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
- [Configuration](#%EF%B8%8F-configuration)
  - [Metadata Format](#-metadata-format)
  - [SSH Support](#-ssh-support)
- [Insert Options](#-insert-options)
- [Core Interfaces](#-core-classes-database-and-autosqlhandler)
  - [`Database` Class](#%EF%B8%8F-database-class)
  - [`AutoSQLHandler` Class](#%EF%B8%8F-autosqlhandler-class)
- [Convenience Utilities](#-convenience-utilities)
  - [Type Inference & Normalisation](#-type-inference--normalisation)
  - [Config & Metadata Tools](#%EF%B8%8F-config--metadata-tools)
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

// Option 1: Direct insert if schema already exists or is managed externally
await db.autoInsertData({ table: 'target_table', data });

// Option 2: Fully automated schema + insert workflow
await db.autoSQL('target_table', data);

await db.closeConnection();
```

AutoSQL will:
- Infer metadata and key structure
- Create or alter the target table
- Batch insert rows
- Handle dialect-specific quirks automatically
- Automatically manage timestamps and optional history tracking (if configured)

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
  // ALL SETTINGS BELOW HERE ARE OPTIONAL
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

  // Type inference controls
  pseudoUnique?: number; // The % of values that must be unique to be considered pseudoUnique. - defaults to 0.9 (90%)
  autoIndexing?: boolean; // Automatically identify and add indexes to tables when altering / creating - defaults to TRUE
  decimalMaxLength?: number; // Automatically round decimals to a maximum of X decimal places - defaults to 10
  maxKeyLength?: number; // Limits indexes / primary keys from using columns that are longer than this length - defaults to 255
  maxVarcharLength?: number; // Prevents varchar columns from exceeding this length, autoconverts this length of varchar to text columns -- defaults to 1024 characters

  // Sampling controls
  sampling?: number; // If provided data exceeds samplingMinimum rows, we sample this % of values for identifying uniques and column types — defaults to 0, allows values between 0 and 1
  samplingMinimum?: number; // If provided data exceeds this row count, sampling kicks in — defaults to 100

  // Insert strategy
  insertType?: 'UPDATE' | 'INSERT'; // UPDATE automatically replaces non-primary key values with new values that are found
  insertStack?: number; // Maximum number of rows to insert in one query - defaults to 100
  safeMode?: boolean; // Prevent the altering of tables if needed - defaults to false
  deleteColumns?: boolean; // Drop columns if needed - defaults to false

  // Timestamp columns
  addTimestamps?: boolean; // If TRUE, runs function ensureTimestamps as part of AutoSQL function. Which adds a dwh_created_at, dwh_modified_at and dwh_loaded_at timestamp columns that are automatically filled. -- defaults to TRUE

    // Optional advanced insert modes
  useStagingInsert?: boolean; // Enable temporary staging table insert pattern (if supported) -- defaults to TRUE
  addHistory?: boolean; // Automatically duplicate rows into history tables before overwrites -- defaults to FALSE
  historyTables?: string[]; // Names of the tables to have history tracked -- pairs with addHistory above
  autoSplit?: boolean; // Automatically split large datasets (columns) across multiple tables if needed
  addNested?: boolean; // Extracts nested JSON values into separate tables with composite primary keys -- defaults to FALSE
  nestedTables?: string[]; // Nested Table names to apply nested extraction on -- if nesting `columnA` on `tableB`, this would be [`tableB_columnA`]

  // Performance scaling
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
AutoSQL supports SSH tunneling for connecting to remote MySQL or PostgreSQL servers via an intermediate gateway.

Include the SSH configuration inside your `DatabaseConfig` object under the `sshConfig` key. AutoSQL will automatically establish the tunnel when `establishConnection()` is called.

```ts
const config: DatabaseConfig = {
  ...
  sshConfig: {
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
}

  const db = Database.create(config);
  await db.establishConnection();
  // Tunnel is now active and DB connection is routed through it
```

---

## 📑 Insert Options

These control how data is batched, inserted, and optionally how schema alterations are handled.

### Basic Insert Options

- `insertType`: `'UPDATE' | 'INSERT'`  
  Determines behaviour on duplicate keys. `UPDATE` replaces non-primary key values with new ones. Defaults to `'INSERT'`.

- `insertStack`: `number`  
  Maximum number of rows to insert in a single query. Defaults to `100`.

- `safeMode`: `boolean`  
  If `true`, prevents any table alterations during runtime. Defaults to `false`.

- `deleteColumns`: `boolean`  
  Allows dropping of existing columns when altering tables. Defaults to `false`.

---

### ⏱ Timestamp Columns

- `addTimestamps`: `boolean`  
  If `true`, automatically adds and manages the following timestamp columns:  
  - `dwh_created_at`,  
  - `dwh_modified_at`,  
  - `dwh_loaded_at`  
  These are injected and updated during insert operations. Defaults to `true`.
  This will also check a variety of common timestamp columns and will only add the equivalent if they do not exist in the existing data. As an example, modified timestamps will check modified_at, modify_at, modified_date, update_date etc.

---

### 🧪 Advanced Insert Modes

- `useStagingInsert`: `boolean`  
  Enables a staging table strategy where data is first inserted into a temporary table before being merged into the target. Useful for large or high-concurrency environments. Defaults to `true`.

- `addHistory`: `boolean`  
  If enabled, before overwriting rows (in `UPDATE` mode), AutoSQL writes the previous version into a corresponding history table. Defaults to `false`.

- `historyTables`: `string[]`  
  List of table names to track with history inserts. Used in conjunction with `addHistory`.

- `autoSplit`: `boolean`  
  Automatically splits datasets across multiple tables when the row size or column count exceeds allowed limits. Prevents failed inserts due to row size limits. Defaults to `false`

- `addNested`: `boolean`  
If enabled, AutoSQL will extract nested objects or arrays from a field and insert them into a separate table.  
Defaults to `false`.

- `nestedTables`: `string[]`  
  Used in conjunction with `addNested`. Specifies which nested structures should be extracted and written into their own relational tables.  

  **Format:** Each entry should follow the pattern: `"<tableName>_<columnName>"`  

  For each entry:
  - If the dataset includes a table that matches `<tableName>`,
  - And that table contains a column named `<columnName>`,
  - And the column contains a JSON object or an array of JSON objects,
  - AutoSQL will extract the nested structure into a new table named `<tableName>_<columnName>`

  **Behavior:**
  - The new nested table will include the parent row’s primary key (e.g., `row1_id`) to maintain relationships
  - The nested object will define the child table’s schema
  - Arrays will be flattened—each item becomes a separate row in the nested table

### 🧵 Scaling & Workers

- `useWorkers`: `boolean`  
  Enables parallel worker threads for inserting batches. Improves performance with large datasets. Defaults to `true`

- `maxWorkers`: `number`  
  Maximum number of concurrent workers to use during insertion. Must be used with `useWorkers`. Defaults to `8`

## 🏁 Core Classes: `Database` (with AutoSQL Utilities)

The `Database` class is the primary entry point into AutoSQL's workflow. It handles connection management and exposes high-level `autoSQL` methods for automated insertions, table creation, and metadata handling.

```ts
import { Database } from 'autosql';

const db = Database.create(config);
await db.establishConnection();

await db.autoConfigureTable(
  'target_table', // table name
  sampleData,     // raw input data
  null,           // optional existing metadata
  initialMeta     // optional manually defined metadata
);
```

This is the core interface for managing connections, generating queries, and executing inserts.

### ⚙️ `Database` Class

#### 🔸 Static Method
- **`Database.create(config)`** – Returns an instance of either `MySQLDatabase` or `PostgresDatabase` based on config.

#### 🔹 Core Methods
- **`getConfig()`** – Returns the full `DatabaseConfig` used to initialise this instance.
- **`updateSchema(schema: string)`** – Updates the current schema name being used.
- **`getDialect()`** – Returns the SQL dialect (`mysql` or `pgsql`).
- **`establishConnection()`** – Creates and stores a live database connection.
- **`testConnection()`** – Attempts to connect and returns success as a boolean.
- **`runQuery(queryOrParams: QueryInput | QueryInput[])`** – Executes a SQL query or list of queries.
- **`startTransaction()` / `commit()` / `rollback()`** – Manages manual transaction blocks.
- **`runTransaction(queries: QueryInput[])`** – Runs multiple queries inside a single transaction.
- **`runTransactionsWithConcurrency(queryGroups: QueryInput[][])`** – Runs multiple query batches in parallel.
- **`closeConnection()`** – Safely closes the active DB connection.

#### 🔹 Table and Schema Methods
- **`checkSchemaExists(schemaName: string)`** – Returns whether the given schema exists.
- **`createSchema(schemaName: string)`** – Creates the schema if it doesn't exist already.
- **`createTableQuery(table: string, headers: MetadataHeader)`** – Returns `QueryInput[]` to create a table.
- **`alterTableQuery(table: string, oldHeaders: MetadataHeader, newHeaders: MetadataHeader)`** – Returns `QueryInput[]` to alter an existing table.
- **`dropTableQuery(table: string)`** – Returns a `QueryInput` to drop a table.
- **`getTableMetaData(schema: string, table: string)`** – Fetches current metadata from the DB for a given table.

#### 🔹 AutoSQL Methods (Exposed on `db`)

- **`autoSQL(table: string, data: Record<string, any>[], schema?: string, primaryKey?: string[])`**  
  The simplest way to handle everything — metadata inference, schema changes, batching, inserting, history, workers, and nested structures — in one call.  
  Designed for production-ready automation and one-liner ingestion.

- **`autoInsertData(inputOrTable: InsertInput | string, inputData?: Record<string, any>[], inputMetaData?: MetadataHeader, inputPreviousMetaData?: AlterTableChanges | MetadataHeader | null, inputComparedMetaData?: { changes: AlterTableChanges, updatedMetaData: MetadataHeader }, inputRunQuery = true, inputInsertType?: 'UPDATE' | 'INSERT')`**  
  Executes a full insert using the dialect-aware batching engine.  
  If `inputRunQuery` is `true`, queries are executed via `runTransactionsWithConcurrency()`.  
  If `false`, a list of insert queries (`QueryInput[]`) is returned without running them.

- **`autoConfigureTable(inputOrTable: InsertInput | string, data?: Record<string, any>[], currentMeta?: MetadataHeader, newMeta?: MetadataHeader, runQuery = true)`**  
  Determines whether a table should be created or altered based on metadata comparison.  
  If `runQuery` is `true`, schema changes are applied immediately via `runTransactionsWithConcurrency()`.  
  If `false`, queries are returned for inspection.

- **`autoCreateTable(table: string, newMetaData: MetadataHeader, tableExists?: boolean, runQuery = true)`**  
  Creates a new table with the provided metadata.  
  If `runQuery` is `false`, returns the `CREATE TABLE` queries without executing them.

- **`autoAlterTable(table: string, tableChanges: AlterTableChanges, tableExists?: boolean, runQuery = true)`**  
  Alters an existing table using a computed diff.  
  Like above, `runQuery` controls whether to return or execute the queries.

- **`fetchTableMetadata(table: string)`**  
  Looks up metadata for the given table and returns `{ currentMetaData, tableExists }`.  
  Used internally for decisions about schema creation or alteration.

- **`splitTableData(table: string, data: Record<string, any>[], metaData: MetadataHeader)`**  
  If `autoSplit` is enabled, splits a wide dataset across multiple smaller tables.  
  Returns an array of `InsertInput` instructions for multi-table insert execution.

- **`handleMetadata(table: string, data: Record<string, any>[], primaryKey?: string[])`**  
  Combines metadata inference and comparison into one call.  
  Returns an object with:
  - `currentMetaData`: existing table metadata from the DB  
  - `newMetaData`: metadata inferred from new data  
  - `mergedMetaData`: result of merging existing and new metadata  
  - `initialComparedMetaData`: diff result, if any  
  - `changes`: schema changes needed for alignment

- **`getMetaData(config: DatabaseConfig, data: Record<string, any>[], primaryKey?: string[])`**  
  Analyses sample data and returns a metadata map with type, length, nullability, uniqueness, and key suggestions.

- **`compareMetaData(oldMeta: MetadataHeader, newMeta: MetadataHeader)`**  
  Compares two metadata structures and returns:
  - `changes`: an `AlterTableChanges` diff object
  - `updatedMetaData`: the merged metadata structure

Each method is designed to work with the same `Database` instance.

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
