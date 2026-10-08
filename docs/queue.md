# queue

`src/queue.ts`: `PgQueue`, the table jobs live in and the connection to it, and `PgRelayInitError`. [Index](index.md)

## `new PgQueue<TName>(tableName, options?)`

```ts
new PgQueue("jobs")                                        // connects with DATABASE_URL
new PgQueue("jobs", { connectionString: "postgres://…" })  // explicit URL
new PgQueue("jobs", { pool })                              // your own pg.Pool
new PgQueue("jobs", { schema: "relay" })                   // table relay.jobs
new PgQueue<"send-email" | "generate-report">("jobs")      // typed job names
```

Constructing a queue does no I/O and doesn't read `DATABASE_URL`, so a module that creates one can be imported anywhere, including at build time.

| Argument | Rule |
|---|---|
| `tableName` | A lowercase unquoted identifier of at most 50 characters: `/^[a-z_][a-z0-9_]{0,49}$/`. Otherwise the constructor throws. |

### `PgQueueOptions`

| Option | Default | Meaning |
|---|---|---|
| `connectionString` | `process.env.DATABASE_URL` | Read when the queue first connects, not at construction. |
| `pool` | none | An existing `pg.Pool` to use instead of opening one. `close()` leaves it open. |
| `schema` | the connection's current schema, usually `public` | The Postgres schema for the table, created by `init()` if missing. A lowercase identifier of at most 63 characters, not starting with `pg_`. Otherwise the constructor throws. |

A separate schema keeps the table away from tools that manage your app's schema, such as Prisma Migrate, which may report a table it didn't create as drift, or drop it on reset. With a schema, every query names `schema.table` directly, whatever the `search_path`. The database user needs permission to create the schema, or it has to exist already.

### Properties

| Property | Meaning |
|---|---|
| `tableName` | As passed |
| `schema` | As passed, or `undefined` |
| `qualifiedTableName` | `"schema.table"`, or `"table"` without a schema: how SQL refers to it |
| `pool` | The queue's `pg.Pool`, opened on first access. Throws `PgRelayInitError` when there's no `pool`, `connectionString` or `DATABASE_URL`. |

## `init()`

```ts
queue.init(): Promise<void>
```

Creates the table and its indexes if they're missing, then checks that an existing table has pg-relay's columns. You rarely need to call it: `writeJob`, `listen`, `getJob`, `listJobs`, `retryJob`, `pruneJobs` and `stats` all call it first.

- It runs once per `PgQueue` instance. Later calls return the same promise, so **a failure stays failed**: construct a new queue to try again.
- It runs in a transaction holding a Postgres advisory lock on the table's name, so many processes can start at once.
- It never changes existing rows.
- It rejects with `PgRelayInitError` when it can't connect, can't create the schema, table or indexes, or finds a table of that name that isn't a pg-relay table (missing columns are named in the message). The original error is on `cause`.

## `getJob`

```ts
queue.getJob<TPayload>(id: string): Promise<JobRecord<TPayload, TName> | null>
```

The job with this id, as a [`JobRecord`](job.md#jobrecordtpayload-tname). `null` when there's none, including for an id that isn't a uuid. `TPayload` only types the result. It isn't checked.

## `listJobs`

```ts
queue.listJobs<TPayload>(filter?: JobListFilter<TName>): Promise<JobRecord<TPayload, TName>[]>
```

The jobs matching every field of the [filter](job.md#joblistfiltertname), newest first (`created_at DESC, id`). It throws if `limit` isn't a whole number ≥ 1 or `offset` isn't a whole number ≥ 0.

```ts
await queue.listJobs({ name: "export", status: ["pending", "running"] })
await queue.listJobs({ payload: { report: { id: "r_123" } } })        // payload contains these fields
await queue.listJobs({
  name: "generate-summary",
  payloadPath: {                                                     // Postgres JSON path
    path: "$.rangeStart >= $from && $.rangeStart < $to",
    vars: { from: from.toISOString(), to: to.toISOString() },        // sent as parameters
  },
})
```

- **`payload`** uses `jsonb @>`: nested objects match on the fields given, and arrays match if they contain the given elements.
- **`payloadPath`** is a [Postgres JSON path](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-SQLJSON-PATH) predicate. Values go in `vars` and are referred to as `$name`. They're never pasted into the SQL. A job whose payload lacks the path's fields doesn't match. An invalid path rejects the call. ISO-8601 UTC strings written the same way compare correctly as strings.
- **Paging:** use `limit` and `offset`, or pass the last job's own `createdAt` as `createdBefore` for the next page. Times compare to the millisecond, so jobs written in the same millisecond as the cursor job are skipped. Page with `offset` if that matters.

### Showing a job's progress

```ts
function describeJob(job: JobRecord): string {
  const total = job.maxRetries + 1
  switch (job.status) {
    case "succeeded": return "Done"
    case "failed":    return `Failed: ${job.lastError}`
    case "running":   return `Running (attempt ${job.attempts}/${total})`
    case "pending":
      if (job.attempts === 0) return "Queued"
      return `Attempt ${job.attempts}/${total} failed (${job.lastError}), retrying at ${job.runAfter.toISOString()}`
  }
}
```

- `pending` with `attempts > 0` is waiting for a retry. `runAfter` is when it's due, and `lastError` says why the last attempt failed.
- `runAfter` is on the database's clock, so a countdown computed with `Date.now()` is off by your server's clock drift.
- A `running` job whose worker died still shows `running` until `lockedUntil` passes and another worker claims it.

## `retryJob`

```ts
queue.retryJob<TPayload>(id: string): Promise<JobRecord<TPayload, TName> | null>
```

Sends a **failed** job back to `pending`, due now, with `attempts` reset to 0 so it gets its full retries again. Use it for a "retry" button.

- It returns the updated job, or `null` when there's no failed job with that id. Pending, running and succeeded jobs are left alone.
- `lastError` is kept for reference. `finishedAt` and the lock columns are cleared.
- It rejects if another unfinished job already holds the same name and `dedupeKey`, and the job stays `failed`.

## `pruneJobs`

```ts
queue.pruneJobs(options: PruneJobsOptions<TName>): Promise<number>
```

Deletes `succeeded` and `failed` jobs that finished more than `olderThanSeconds` ago, optionally only one `status` or `name`, and returns how many were deleted. Pending and running jobs are never deleted. It throws if `olderThanSeconds` isn't a whole number ≥ 0, or if `status` names anything other than `succeeded` or `failed`.

Finished rows otherwise stay forever, and `stats()` gets slower as they pile up. Run it on a schedule:

```ts
await queue.pruneJobs({ olderThanSeconds: 7 * 86_400, status: "succeeded" })
await queue.pruneJobs({ olderThanSeconds: 30 * 86_400 })   // keep failures longer
```

## `stats`

```ts
queue.stats(filter?: JobStatsFilter<TName>): Promise<JobNameStats<TName>[]>
```

Per job name: counts by status, how many pending jobs are due, how long the oldest due job has waited, and how many running jobs have an expired lock. See [`JobNameStats`](job.md#jobstatsfiltertname-jobnamestatstname) for the fields.

```ts
await queue.stats()                                  // every name with rows
await queue.stats({ name: "send-email" })
await queue.stats({ name: ["send-email", "export"] })
```

- **One read-only query.** It takes no locks and never blocks claiming.
- **Only names with rows are returned**, sorted by name. A name never written, or fully pruned, is missing rather than all zeros. `{ name: [] }` returns `[]`.
- **Counts are numbers.** `oldestDueSeconds` is fractional seconds, or `null` when nothing is due.
- **Age is measured from `run_after`**, not `created_at`, so a delayed job or one waiting out a retry backoff isn't counted as late.
- **Cost** grows with unpruned finished rows, because `succeeded` and `failed` count all of them. It's meant to be polled every 15–60 seconds. Prune on a schedule.

What to alert on: `oldestDueSeconds` growing (workers down, stuck or too few), `due` (backlog a worker could claim now), `failed` going up, and `expiredLocks > 0` (workers that died mid-job).

```ts
app.get("/health/queue", async (_req, res) => {
  const stuck = (await queue.stats()).filter((s) => (s.oldestDueSeconds ?? 0) > 300 || s.expiredLocks > 0)
  res.status(stuck.length > 0 ? 503 : 200).json({ stuck })
})
```

## `close()`

```ts
queue.close(): Promise<void>
```

Ends the pool if the queue opened it. A pool passed in as `pool` stays open. It does nothing if the queue never connected. Stop subscribers on the queue first.

## `PgRelayInitError`

```ts
class PgRelayInitError extends Error { name: "PgRelayInitError"; cause?: unknown }
```

Thrown when the queue can't connect, has nothing to connect to, or its table can't be created or isn't a pg-relay table. `listen` and `writeJob` reject with it too, so a worker that awaits `listen` stops before it ever polls.

## The table

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` | `gen_random_uuid()` |
| `name` | `text` | |
| `payload` | `jsonb` | |
| `status` | `text` | `pending`, `running`, `succeeded` or `failed` (checked) |
| `attempts` | `integer` | Runs started, counting the one in progress |
| `max_retries`, `retry_backoff_seconds`, `lock_ttl_seconds` | `integer` | The job's `JobConfig` |
| `run_after` | `timestamptz` | Not claimed before this time |
| `locked_until`, `locked_by` | `timestamptz`, `text` | The current claim |
| `last_error` | `text` | The most recent failure's message |
| `dedupe_key` | `text` | |
| `created_at`, `updated_at`, `finished_at` | `timestamptz` | |

Indexes, named after the table:

| Index | Definition | Used by |
|---|---|---|
| `<table>_pending_idx` | `(name, run_after, created_at) WHERE status = 'pending'` | Claiming due jobs, `stats` |
| `<table>_running_idx` | `(locked_until) WHERE status = 'running'` | Reclaiming expired locks, `stats` |
| `<table>_dedupe_idx` | unique `(name, dedupe_key) WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'running')` | Deduplication |

The columns, status values and indexes are pg-relay's internals, not an API. Read jobs through `getJob`, `listJobs` and `stats` rather than querying the table directly. Requires Postgres 13 or later.
