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

- [Supported SQL Dialects](#supported-sql-dialects)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Metadata Format](#metadata-format)
- [SSH Support](#ssh-support)
- [Insert Options](#insert-options)
- [Convenience Utilities](#convenience-utilities)

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
  addTimestamps?: boolean;

  // Type inference controls
  minimumUnique?: number;
  maximumUniqueLength?: number;
  maxNonTextLength?: number;
  pseudoUnique?: number;
  autoIndexing?: boolean;
  decimalMaxLength?: number;
  maxKeyLength?: number;

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

- `max_insert`: max rows per insert (default: 5000)
- `max_insert_size`: max bytes per insert (default: 1MB)
- `insert_stack`: preferred stack size for batching (default: 100)
- `safe_mode`: run all inserts in transactions (default: true)
- `insert_type`: behaviour on duplicate keys (`REPLACE` or `IGNORE`)

---

## 🧰 Convenience Utilities

AutoSQL exposes utilities that power `autoSQL` and can be used independently:

- **`getMetaData(data, config)`** – Generates metadata for a given dataset
- **`autoConfigureTable(config, metaData)`** – Creates or alters tables
- **`insertData(config, data)`** – Runs batched inserts
- **`predictType(value)`** – Predicts SQL type from a JS value
- **`runSQLQuery(config, sql)`** – Run a raw SQL query
- **`validateDatabase(config)`** – Validates a DB connection
- **`validateQuery(config, sql)`** – Validates a SQL query

---

## 📬 Feedback

This library is under active development. Suggestions, issues, and contributions are welcome.

Contact: **w@walterchoi.com**

