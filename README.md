# AutoSQL - Automated SQL Insertions for Modern Data Workflows

![NPM](https://nodei.co/npm/autosql.png)

## 🚀 AutoSQL — A Smarter Way to Insert Data

AutoSQL is a TypeScript-powered **zero-config ingest layer** for SQL databases. It helps engineers and analysts insert structured or semi-structured JSON into MySQL, PostgreSQL, or SQL Server with no manual schema prep, modelling, or migrations.

Built for modern ELT workflows, AutoSQL automatically infers the right schema — types, keys, indexes — and creates or updates tables on the fly. It's ideal for:

- API responses and flat files used in data warehousing
- No-code/low-code tool exports
- Rapid ingestion pipelines where structure evolves frequently

Unlike traditional ORMs, AutoSQL doesn't require boilerplate models or migration scripts. Just connect, pass in your data, and let AutoSQL handle the rest.

See [CHANGELOG.md](./CHANGELOG.md) for release history.

---

## 📦 Installation

```bash
npm install autosql
```

AutoSQL supports **MySQL** (via `mysql2`), **PostgreSQL** (via `pg`), and **SQL Server / Azure SQL** (via `mssql`) — with full feature parity across all three. The dialect drivers (and `pg-copy-streams`, for Postgres `bulkLoad`) are **optional peer dependencies**: install only the one you use.

```bash
npm install autosql mysql2       # MySQL
npm install autosql pg           # PostgreSQL (+ pg-copy-streams for bulkLoad)
npm install autosql mssql        # SQL Server / Azure SQL
```

SSH tunneling is optional, via [`ssh2`](https://www.npmjs.com/package/ssh2).

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

const db = Database.create(config);
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

For a dry run that reports what a load **would** do without writing anything, see `db.preview(...)` in the [API reference](docs/api.md#preview--dry-run).

---

## What AutoSQL can do

- **Automatic schema inference & evolution** — types, primary/composite keys, unique and pseudo-unique constraints, indexes, locale-aware number parsing, and safe `ALTER TABLE` diffing. See [Configuration](docs/configuration.md).
- **Streaming & large datasets** — `openStream` for chunked writes with resumable staging across runs (cursors included), and `autoSQLChunked` for `AsyncIterable` sources. See [Streaming & large datasets](docs/streaming.md).
- **Schema history, drift detection & multi-writer safety** — an audit trail of every DDL change, with advisory locks to serialize concurrent schema changes. See [Schema history & multi-writer safety](docs/schema-history.md).
- **Security & safe operation** — SSH tunneling, TLS, and least-privilege/BYOD guarantees (nothing outside your configured schema, no destructive DDL by default). See [Security & safe operation](docs/security.md).
- **Graceful degradation** — an opt-in rejected-rows table so one bad row doesn't sink a whole batch, plus run auditing and structured per-run metrics.
- **A full `Database` API and standalone utilities** for metadata inference, query building, and insert planning, usable outside the all-in-one `autoSQL` call. See the [API reference](docs/api.md).

---

## 📚 Documentation

- **[Configuration](docs/configuration.md)** — the full `DatabaseConfig` reference (type inference, insert behavior, number/time-zone handling, scaling, logging) and the `MetadataHeader` format.
- **[API reference](docs/api.md)** — the `Database` class, `preview`/dry-run, and every standalone convenience utility.
- **[Streaming & large datasets](docs/streaming.md)** — `openStream`, resuming across runs, cursors, rejected rows, `autoSQLChunked`.
- **[Schema history & multi-writer safety](docs/schema-history.md)** — DDL audit trail, drift detection, advisory locks.
- **[Security & safe operation](docs/security.md)** — SSH tunneling, TLS, least-privilege/BYOD guarantees.
- **[Local development](docs/local-development.md)** — the Docker Compose setup used for integration tests.

---

## 📬 Feedback

This library is under active development. Suggestions, issues, and contributions are welcome.

Contact: **w@walterchoi.com**
