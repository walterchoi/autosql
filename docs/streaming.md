# Streaming & large datasets

For large or incremental datasets, use `openStream` to avoid loading everything into memory at
once. Each stream session uses its own isolated staging table so concurrent writes never
interfere. See the [README](../README.md) for a quick start, and [API reference](api.md) for the
`Database` method list.

```ts
const stream = await db.openStream('events', 'my_schema', ['id']);

// Write data in as many chunks as you like
await stream.write(chunk1);
await stream.write(chunk2);

// Merge staged data into the target table, then clean up
const result = await stream.end();
console.log(result.affectedRows);

// Or abandon without merging
await stream.abort();
```

## How it works

1. **`openStream(table, schema?, primaryKey?, opts?)`** — runs a connectivity check and cleans up any orphaned staging tables from previous crashed runs (configurable with `keepOrphanedStagingTables`). Pass `opts.sessionKey` for a staging table that survives between runs — see [Resuming a stream across runs](#resuming-a-stream-across-runs).
2. **`write(chunk)`** — on the first call, creates an all-text (`LONGTEXT` / `TEXT`) staging table unique to this run. Each subsequent call appends rows to it.
3. **`end()`** — reads all staged rows, infers the schema with `getMetaData`, applies any necessary DDL via `configureTables`, then issues a bulk `INSERT … SELECT` with dialect-specific type casts. If the bulk merge fails, a per-row fallback fires — failed rows trigger a schema widening pass before each retry round (up to `streamMaxRetries`). The staging table is then dropped in the `finally` block — except for a resumable stream whose rows did not reach the target, which keeps its table so the run can be retried.
4. **`abort()`** — drops the staging table without merging. Safe to call even if `write()` was never called.
5. **`suspend()`** — *(resumable streams only)* stops without merging **and** without dropping, so a later run can pick the table up. Throws on a non-resumable stream.

## Error handling & the async contract

`openStream()`, `write()`, `end()` and `abort()` each return a promise that **rejects on
failure**, so you must `await` them (or attach a `.catch`). They are **not** fire-and-forget: an
un-awaited `write()` that fails becomes an unhandled promise rejection and its error is lost.

A rejected `write()` leaves this run's staging table in an **indeterminate** state (the chunk may
be partly applied or absent). When a `write()` rejects, take one of two safe paths:

- **retry the same chunk** — `write()` is append-only, so re-sending a chunk after a transient failure is fine; then continue and `end()` as usual; or
- **`abort()`** — drop the staging table and discard the whole run.

Do **not** call `end()` after a failed/un-awaited `write()` expecting the gap to be ignored:
`end()` merges whatever is staged, so a lost chunk becomes missing rows.

## Resuming a stream across runs

By default a stream's staging table belongs to one `openStream` call: the name embeds a generated
run id, and the next call starts a new one. That's right for a load that completes in a single
run, but it means a load too large to finish in one run can never make progress — each attempt
starts over, and the alternative (committing pages straight to the target table) puts partial,
visibly-wrong data in production whenever a run fails.

Pass a **`sessionKey`** to opt into a staging table that survives between runs:

```ts
// Run 1 — gets part way through, then stops without losing what it staged.
const stream = await db.openStream('events', 'my_schema', ['id'], { sessionKey: `conn-${connectionId}` });
if (!stream.adopted) resumePoint = 0;   // nothing carried over — send from the start
await stream.write(page1);
await stream.write(page2);
await stream.suspend();                 // keep the staging table; do not merge

// Run 2 — a later process picks up exactly where run 1 stopped.
const resumed = await db.openStream('events', 'my_schema', ['id'], { sessionKey: `conn-${connectionId}` });
console.log(resumed.adopted);           // true — page1 + page2 are still staged
await resumed.write(page3);
await resumed.end();                    // promotes all three pages at once, then drops staging
```

The key must identify the **load session, not one run of it** — derive it from something that
outlives an attempt, such as a connection or destination id. Derive it from a run or attempt id
and every retry gets a fresh table, so resuming silently becomes restarting. The key is hashed
into the table name, so any string is safe to pass.

⚠ **Always check `handle.adopted` before trusting a resume point you stored yourself.** `false`
means there was no staging table to attach to — first run for this key, or an earlier attempt's
table is gone — so nothing was carried over and you must re-send from the beginning. (A table that
exists but holds no rows yet reports `true`: its column set was adopted.) Trusting a stored
high-water mark against a non-adopted stream silently drops every row before that mark. A resume
point stored elsewhere can also expire independently of the staging table, so the table surviving
is not by itself evidence that your pages are accounted for: reconcile both.

Three terminal states, rather than two:

| | merges staged rows | keeps staging table |
|---|---|---|
| `end()` | ✅ | ❌ (kept only if the rows didn't reach the target, so you can retry) |
| `suspend()` | ❌ | ✅ |
| `abort()` | ❌ | ❌ |

Other behaviour worth knowing:

- **A drifted source widens the staging table.** If your source gains a column part way through a multi-run load, the adopted staging table is altered to fit it (staging is all-text, so there's never a type to widen — only columns to add). Added columns are nullable, so rows staged by earlier runs read `NULL` for them, which is what they were, and `end()` infers over the whole staging table so the column reaches the target through the ordinary DDL path. Columns added this way are listed on `handle.widenedColumns`.

  Widening respects **`safeMode`** — the existing switch for whether autosql performs DDL at all. With `safeMode: true` there is no DDL to widen with, so an unknown column is refused loudly rather than silently dropped. A per-run (keyless) stream also keeps the refusal: its column set came from its own first chunk moments earlier, so an unknown key there is a caller inconsistency, not source drift across runs.
- ⚠ **Retrying `end()` is only safely idempotent under `insertType: 'UPDATE'` with a primary key**, where the merge upserts. Under `'INSERT'`, or with no primary key, a retry re-sends rows that the per-row fallback had already landed before failing, duplicating them in the target. If you can't guarantee a key, treat a failed `end()` as needing reconciliation rather than a blind retry.
- **Inference sees every staged row.** `end()` re-reads the whole staging table, so types and lengths are inferred from the full accumulated dataset — not just the resumed run's chunk.
- **Orphan cleanup leaves session-keyed tables alone**, including other session keys' tables for the same target. Only per-run staging tables are swept.
- ⚠ **One writer per session key.** autosql does not serialise writers; two concurrent streams sharing a key both append and duplicate rows at merge.
- ⚠ **Lifetime is the caller's responsibility for now.** A table that survives between runs also survives a connection being deleted or a sync being abandoned, which is storage in your warehouse that nothing will reclaim. Call `abort()` when you retire a session. A built-in session registry and sweeper is planned.

## Resuming across runs with a cursor

Knowing the staging table survived is not the same as knowing *where to continue*. Pass a
**cursor** with each `write()` and autosql records it alongside the rows, so the resume point
lives in the same table as the data it describes and the two share one lifetime. Nothing external
has to be kept in step.

```ts
const stream = await db.openStream('events', 'my_schema', ['id'], { sessionKey: `conn-${connectionId}` });

// Where did the last run get to?
const { lastCursor, stagedRows, lastStagedAt } = await stream.stagingState();

let page = lastCursor ? pageAfter(lastCursor) : firstPage();
// Re-send the page the previous run died on: it may have landed only part way through.
if (lastCursor) page = lastCursor;

for (; page; page = nextPage(page)) {
  await stream.write(await fetchPage(page), { cursor: page });
  if (outOfTime()) { await stream.suspend(); return; }   // come back next invocation
}
await stream.end();
```

`stagingState()` returns `{ stagedRows, cursors, lastCursor, firstStagedAt, lastStagedAt }`. It
reads the staging table rather than the handle, so it works on a stream whose rows a *different*
process staged — which is the whole point after a lambda dies.

**Re-sending a page is safe, and expected.** This is the same at-least-once contract Singer and
Fivetran use: a run that died part way through a page can't know how much of it landed, so it
re-sends the whole page. autosql collapses the duplicates at merge, keeping the **last-staged**
row for each primary key, so the re-run wins. That's also why a cursor **requires a
`primaryKey`** — without one there is nothing to deduplicate on, so autosql refuses rather than
letting duplicates reach your table.

### The three staging-only columns

autosql adds these to the staging table. They never reach the target:

| column | what it is |
|---|---|
| `_autosql_cursor` | the cursor you passed to `write()`, stored per row |
| `_autosql_staged_at` | when autosql staged the row, from the **database** clock |
| `_autosql_seq` | monotonic per row; the ordering that `stagingState()` and dedup rely on |

`_autosql_` is a **reserved prefix** — a data column using it is refused, since it would collide
with these.

Two things worth knowing about them:

- **`_autosql_seq` is the ordering, not the timestamp.** Cursors are opaque strings autosql can't sort, and a timestamp isn't enough on its own: MySQL `DATETIME` resolves to one second, so two pages staged in the same second would tie. Fivetran's `_fivetran_synced` is the cautionary tale here — it records when a record was *prepared*, not written, and rows don't arrive in its order, which quietly broke everyone who used it as a watermark. Don't order on `_autosql_staged_at`; it's for staleness, not sequence.
- **`_autosql_staged_at` comes from the database clock**, not the process clock, so a load spread across several hosts doesn't interleave their clock skew. It also gives you staleness for free: if `lastStagedAt` is older than you're willing to resume from, `abort()` and start clean rather than appending this week's pages onto last month's.

autosql still owns no part of *your* pagination: it doesn't know what a cursor means, only that
it should keep it next to the rows it arrived with and hand it back.

## Rejected rows

If `rejectedRowsTable` is configured, rows that cannot be merged after all retries are written to
that table instead of throwing:

```ts
const db = Database.create({
  ...,
  rejectedRowsTable: 'autosql_rejected_rows',
  streamMaxRetries: 5,
});
```

This same graceful degradation also applies to the **non-streaming direct-insert path**
(`useStagingInsert: false`): when a batch insert fails, autosql retries the batch's rows one at a
time (widening the schema between rounds) and diverts any that still fail to `rejectedRowsTable`.
Without `rejectedRowsTable` set, a failed batch throws (fail-loud is the default). The default
**staging** path (`useStagingInsert: true`) is unaffected — it stays atomic (all-or-nothing) by
design, so a bad row fails the whole load there.

## Works with advisory locks and schema history

```ts
const db = Database.create({
  ...,
  useSchemaLock: true,      // holds lock only during DDL phase
  schemaHistory: true,      // records a migration entry for any DDL at merge time
});
```

See [Schema history & multi-writer safety](schema-history.md) for details.

## Large-dataset support: `autoSQLChunked`

For datasets too large to hold in memory, use `autoSQLChunked` with any `AsyncIterable`:

```ts
async function* pageRows() {
  let page = 0;
  while (true) {
    const rows = await fetchPage(page++);
    if (rows.length === 0) break;
    yield rows;
  }
}

const result = await db.autoSQLChunked('events', pageRows());
```

The first non-empty chunk runs the full inference + DDL pipeline. All subsequent chunks skip
directly to insert — no repeated schema work. Compatible with `useSchemaLock: true` and
`useStagingInsert: true`.
