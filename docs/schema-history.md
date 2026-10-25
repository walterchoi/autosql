# Schema history & multi-writer safety

See the [README](../README.md) for a quick start, and [Configuration](configuration.md) for the
full config field reference.

## Schema history & drift detection

Enable `schemaHistory` to keep a full audit trail of every DDL operation AutoSQL applies.

```ts
const db = Database.create({
  ...,
  schemaHistory: true,
  schemaHistoryTable: 'autosql_schema_history', // default
  detectDrift: true,           // warn if the live schema diverges from the recorded one
  strictDriftDetection: false, // set true to throw SchemaDriftError instead of warning
});
```

AutoSQL creates the history table automatically on first use. Each migration writes a `pending`
record, then updates to `applied`, `failed`, or `rolled_back`.

### Exported functions

```ts
import { detectSchemaDrift, getSchemaAt, computeChecksum } from 'autosql';
import { SchemaDriftError } from 'autosql';

// Check whether the live schema matches the last recorded checksum
const { drifted, expected, actual } = await detectSchemaDrift(db, 'users');

// Reconstruct what the schema looked like at a point in time
const historicSchema = await getSchemaAt(db, 'users', new Date('2025-06-01'));

// Compute the sha256 checksum used internally for drift comparison
const checksum = computeChecksum(metaData);
```

### Error types

- **`SchemaLockTimeoutError`** — thrown when `useSchemaLock: true` and the advisory lock could not be acquired within `schemaLockTimeout` seconds.
- **`SchemaDriftError`** — thrown when `strictDriftDetection: true` and the live schema checksum does not match the last recorded checksum.

Both are exported from the package root:

```ts
import { SchemaLockTimeoutError, SchemaDriftError } from 'autosql';
```

## Multi-writer safety

When multiple processes call `autoSQL` on the same table simultaneously, schema inference and DDL
can race. Enable advisory locks to serialize the DDL phase:

```ts
const db = Database.create({
  ...,
  useSchemaLock: true,
  schemaLockTimeout: 30, // seconds
});
```

- **MySQL** — uses `GET_LOCK('autosql_schema__<table>', timeout)` on a dedicated pool connection.
- **PostgreSQL** — uses `pg_try_advisory_lock(hash(<table>))` polled every 500 ms on a dedicated pool client.
- **SQL Server** — uses `sp_getapplock` on a dedicated transaction.

The lock is held only during schema inference and DDL, then released before any inserts begin —
concurrent inserts are never blocked. If the lock cannot be acquired within the timeout,
`SchemaLockTimeoutError` is thrown.
