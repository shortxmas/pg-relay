# pg-relay

A small job queue backed by a Postgres table, for apps that run as two processes:

- the **app runtime** writes jobs with a `Publisher`
- the **worker** listens for jobs with a `Subscriber`, does the work, and reports back whether the job succeeded or failed

There is no broker. A job is a row: publishing inserts it, and the worker claims it with `FOR UPDATE SKIP LOCKED`, keeps it locked while it runs, and marks it finished or schedules a retry.

> **Status:** early development (0.x). The API may change between minor versions.

## Install

```sh
npm install pg-relay pg
```

`pg` (node-postgres 8) and `@types/pg` are peer dependencies, so your app and pg-relay share one copy. npm installs `@types/pg` automatically. Requires Node 18+ and Postgres 13+.

## Usage

Define the queue once, in a file both processes import (e.g. `queue.ts` at the repo root):

```ts
// queue.ts
import { PgQueue, Publisher, Subscriber } from "pg-relay"

const queue = new PgQueue("jobs")   // table name; connects with DATABASE_URL

export const publisher = new Publisher(queue)
export const subscriber = new Subscriber(queue)
```

Constructing these does no I/O and doesn't read `DATABASE_URL`, so the file is safe to import anywhere, including during `next build`. The connection opens the first time a job is written or listened for.

### Writing jobs (app runtime)

```ts
import { publisher } from "./queue"

const { id, deduped } = await publisher.writeJob("geminiAiWorkflow", {
  payload: { reportId: "r_123" },                   // any JSON the worker needs
  config: { maxRetries: 3, lockTtlSeconds: 900 },
  dedupeKey: "report:r_123",                        // optional
})
```

No job names are registered ahead of time. A job is whatever name the publisher writes and the subscriber listens for.

Writing a job inside your own database transaction, so it rolls back with your other writes, is up to each app. `writeJob` always uses the queue's own connection.

### Listening for jobs (worker)

```ts
// worker.ts
import { subscriber } from "./queue"

await subscriber.listen("geminiAiWorkflow", async (job) => {
  const result = await callGemini(job.payload, { signal: job.signal })
  await job.complete()
  // or: await job.fail(error)                    retried after its backoff, if retries are left
  //     await job.fail(error, { retry: false })  failed now
})

for (const signal of ["SIGTERM", "SIGINT"] as const) {
  process.once(signal, async () => {
    await subscriber.stop()   // stops claiming, waits for running handlers
    await subscriber.queue.close()
    process.exit(0)
  })
}
```

`listen` creates the table if needed, then starts polling and resolves. If the table can't be set up, it rejects, so an unhandled rejection stops the worker before it ever polls. pg-relay never installs signal handlers or calls `process.exit` itself.

## Writing jobs

```ts
publisher.writeJob(name: string, conditions?: JobConditions): Promise<{ id: string; deduped: boolean }>

type JobConditions = {
  payload?: JobPayload  // JSON object for the worker; defaults to {}
  config?: JobConfig    // any field left out uses the default
  dedupeKey?: string    // see Deduplication
}
```

### `JobConfig`

| Field | Default | Meaning |
|---|---|---|
| `maxRetries` | `4` | Retries after the first attempt before the job is marked failed. `4` means 5 attempts in total. |
| `retryBackoffSeconds` | `30` | Wait before the first retry. It doubles on every retry after that: 30s, 60s, 120s, 240s. |
| `lockTtlSeconds` | `300` | How long a claimed job stays locked to its worker. The subscriber renews the lock every `lockTtlSeconds / 3` while the handler runs. If the worker dies and the lock runs out, another worker can claim the job. Must be at least 1. |
| `delaySeconds` | `0` | How long after writing before the job may first run, e.g. `86400` for "tomorrow". |

The defaults are exported as `DEFAULT_JOB_CONFIG`. A job's config is stored on its row when it's written, so changing the defaults later doesn't affect jobs already queued.

### Deduplication

While a job with the same name and `dedupeKey` is pending or running, `writeJob` writes nothing and returns that job's id with `deduped: true`. Once it succeeds or fails, the key is free again. Keys are scoped to the job name, and a unique index enforces them, so concurrent writes still produce one job.

## Listening for jobs

```ts
new Subscriber(queue, config?: SubscriberConfig)
subscriber.listen(name: string, handler: (job: ClaimedJob) => unknown, config?: ListenConfig): Promise<void>
subscriber.stop(): Promise<void>
```

| Config | Default | Meaning |
|---|---|---|
| `concurrency` | `1` | Jobs of one name this subscriber runs at once. |
| `pollIntervalSeconds` | `5` | Wait after a poll finds nothing before polling again. After a claimed job, it polls again immediately. |
| `logger` | `console` | Subscriber-wide only. Receives `warn` and `error` calls. |

Config passed to the `Subscriber` applies to every `listen`. Config passed to one `listen` overrides it for that job name. Each `listen` has its own slots, so a slow job name can't starve the others. Listening for the same name twice on one subscriber throws.

### The claimed job

```ts
type ClaimedJob = {
  id: string
  name: string
  payload: JobPayload
  attempts: number          // runs started so far, counting this one
  maxRetries: number
  signal: AbortSignal       // aborts with LockLostError if another worker takes the job over
  complete(): Promise<boolean>
  fail(error: unknown, options?: { retry?: boolean }): Promise<boolean>
}
```

Every job has to be finished with exactly one call to `complete` or `fail`:

| Handler | Result |
|---|---|
| Calls `complete()` | `succeeded` |
| Calls `fail(error)` with retries left | Back to `pending`, due after `retryBackoffSeconds × 2^(attempts−1)` |
| Calls `fail(error)` with none left, or `fail(error, { retry: false })` | `failed` |
| Throws without finishing the job | Same as `fail(error)` |
| Returns without finishing the job | Same as `fail(error)`, with a "returned without calling complete or fail" error |
| Throws after finishing the job | Logged; the outcome stands |
| Calls `complete`/`fail` a second time | That call throws |

`complete` and `fail` return `false` when this worker lost the lock, because it expired and another worker claimed the job. In that case nothing is written and the other worker's claim stands.

### Crashes and retries

- An attempt is counted when a job is claimed, so a job that crashes its worker still uses up its retries.
- When a worker dies, its job's lock expires and the job is claimed again by the normal poll. There's no separate sweep.
- If the attempt that died was the job's last, the job is marked `failed` ("worker stopped during the final attempt") instead of running again.

## Connecting

```ts
new PgQueue("jobs")                                        // DATABASE_URL
new PgQueue("jobs", { connectionString: "postgres://…" })  // explicit URL
new PgQueue("jobs", { pool })                              // your own pg.Pool
```

- `queue.close()` ends the pool only if the queue opened it. A pool you pass in stays open.
- The table name must be a lowercase Postgres identifier of at most 50 characters (`/^[a-z_][a-z0-9_]{0,49}$/`).

## The table

The table is created by `queue.init()`. You rarely need to call `init()` yourself, because the first `writeJob` (and, later, `listen`) calls it.

- Init runs inside a transaction holding a Postgres advisory lock, so the app and the worker can both start at once without colliding.
- It creates the table and indexes if they're missing and leaves existing rows alone.
- It rejects with `PgRelayInitError` if it can't connect, can't create the table, or finds an existing table with that name that isn't a pg-relay table. A worker that awaits it stops before polling.

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` | `gen_random_uuid()` |
| `name` | `text` | Job name |
| `payload` | `jsonb` | |
| `status` | `text` | `pending`, `running`, `succeeded` or `failed` |
| `attempts` | `integer` | Runs started, counting the one in progress |
| `max_retries`, `retry_backoff_seconds`, `lock_ttl_seconds` | `integer` | The job's `JobConfig` |
| `run_after` | `timestamptz` | Not claimed before this time |
| `locked_until`, `locked_by` | `timestamptz`, `text` | The current claim |
| `last_error` | `text` | Most recent failure |
| `dedupe_key` | `text` | See Deduplication |
| `created_at`, `updated_at`, `finished_at` | `timestamptz` | |

Indexes:
- **due pending jobs, per name:** `(name, run_after, created_at) WHERE status = 'pending'`
- **expired locks:** `(locked_until) WHERE status = 'running'`
- **dedupe:** unique `(name, dedupe_key)` for unfinished jobs

All times come from the database's `now()`, so clock differences between processes don't matter.

Requires Postgres 13 or later.
