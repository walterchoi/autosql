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
interface Config {
  host: string;
  username: string;
  password: string;
  database: string;
  table: string;
  sql_dialect: 'mysql' | 'pgsql';
  meta_data?: ColumnMetadata[];
  primary?: string[];
  ssh_config?: SSHConfig;
  auto_id?: boolean;
  create_table?: boolean;
  safe_mode?: boolean;
  insert_type?: 'REPLACE' | 'IGNORE';
  max_insert?: number;
  max_insert_size?: number;
  insert_stack?: number;
  sampling?: number;
  sampling_minimum?: number;
  pseudo_unique?: number;
  minimum_unique?: number;
  collation?: string;
  wait_for_approval?: boolean;
  locale?: string;
  timezone?: string;
  convert_timezone?: boolean;
  convert_all_timezone?: boolean;
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

