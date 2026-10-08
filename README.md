# pg-relay

A small job queue backed by a Postgres table, for apps that run as two processes:

- the **app runtime** writes jobs with a `Publisher`
- the **worker** listens for jobs with a `Subscriber`, does the work, and reports back whether the job succeeded or failed

There is no broker. A job is a row: publishing inserts it, and the worker claims it with `FOR UPDATE SKIP LOCKED`, keeps it locked while it runs, and marks it finished or schedules a retry.

> **Status:** early development (0.x). The API may change between minor versions.

## Install

```sh
npm install pg-relay
```

`pg` (node-postgres 8) and `@types/pg` are peer dependencies, so your app and pg-relay share one copy, and the app chooses the `pg` version.
- npm 7+ and pnpm install them for you.
- Yarn doesn't, so add them yourself: `yarn add pg-relay pg @types/pg`.
- If your app imports `pg` itself, list it in your own `package.json` as usual.

Requires Node 18+ and Postgres 13+.

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

To write a job inside your own database transaction, so it commits or rolls back with your other writes, pass `{ tx }`: see [Writing jobs in your own transaction](#writing-jobs-in-your-own-transaction).

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
    await subscriber.stop()   // stops claiming, aborts running jobs' signals, waits for handlers to settle
    await subscriber.queue.close()
    process.exit(0)
  })
}
```

`listen` creates the table if needed, then starts polling and resolves. If the table can't be set up, it rejects, so an unhandled rejection stops the worker before it ever polls. pg-relay never installs signal handlers or calls `process.exit` itself.

### Typed job names

Optionally, list the queue's job names as a union. Nothing changes at runtime: names are only checked when your code compiles.

```ts
// queue.ts
type JobName = "generate-summary" | "send-email"

const queue = new PgQueue<JobName>("jobs")
export const publisher = new Publisher(queue)    // inferred: Publisher<JobName>
export const subscriber = new Subscriber(queue)  // inferred: Subscriber<JobName>
```

- `writeJob` and `listen` only accept those names, so `publisher.writeJob("send-emial")` is a compile error.
- The `name` filters of `listJobs`, `pruneJobs` and `stats` are checked the same way, and the `name` of every returned `JobRecord` and `JobNameStats` is typed as `JobName`.
- Inside `listen("send-email", (job) => …)` and its `onFailed` hook, `job.name` is typed as `"send-email"`.
- Without the type argument, any string works.

## Writing jobs

```ts
publisher.writeJob(name: string, conditions?: JobConditions, options?: { tx?: TransactionClient }): Promise<{ id: string; deduped: boolean }>

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

### Writing jobs in your own transaction

Most jobs exist because something else in the database just changed: a review was approved, so email the customer. The job and the change have to land together. Written on separate connections, either order can go wrong:
- **Job first, then the app's transaction commits.** If the app's transaction rolls back, the job has already been written, and the worker emails the customer about an approval that never happened.
- **App's transaction commits, then the job.** If the process dies or the pool times out in between, the job is lost. Retrying the request usually doesn't help either, because the triggering write is guarded to happen once (`WHERE approved_at IS NULL`).

Pass the transaction you already have as `tx`, and the job commits or rolls back with your other writes. It accepts a node-postgres client with a transaction open on it:

```ts
const client = await pool.connect()
try {
  await client.query("BEGIN")
  await client.query("UPDATE reviews SET status = 'approved' WHERE id = $1", [id])
  await publisher.writeJob("send-email", { payload: { reviewId: id } }, { tx: client })
  await client.query("COMMIT")
} catch (error) {
  await client.query("ROLLBACK")
  throw error
} finally {
  client.release()
}
```

or the `tx` of a Prisma interactive transaction:

```ts
await prisma.$transaction(async (tx) => {
  await tx.review.update({ where: { id }, data: { status: "approved" } })
  await publisher.writeJob("send-email", { payload: { reviewId: id } }, { tx })
})
```

- The worker can't claim the job until you commit. If you roll back, the job never existed.
- Dedupe works inside the transaction: writing the same name and `dedupeKey` twice returns the first job with `deduped: true`. A duplicate is never an error, so it doesn't abort your transaction.
- If another open transaction has written the same key, `writeJob` waits for it. If that transaction commits, you get its job back with `deduped: true`. If it rolls back, your job is written.
- **Isolation level:** under `REPEATABLE READ` or `SERIALIZABLE`, a duplicate key committed by another transaction after yours started raises a serialization failure (`40001`). Retry the transaction, as for any other `40001`. The default, `READ COMMITTED`, never does this.
- **Same database:** the connection must be on the queue's database. pg-relay can't check this.
- The queue still needs its own connection (`DATABASE_URL`, `connectionString` or `pool`): it creates the table there on first use, outside your transaction. The worker and `getJob`/`listJobs` need that connection anyway.
- `tx` must be a single connection. A `pg.Pool` is rejected with a `TypeError`, because each of its queries may run on a different connection, outside your transaction. Prisma's array form, `$transaction([a, b])`, can't run pg-relay's SQL and isn't supported.

## Listening for jobs

```ts
new Subscriber(queue, config?: SubscriberConfig)
subscriber.listen(name: string, handler: (job: ClaimedJob) => unknown, config?: ListenConfig & { onFailed?: (job: FailedJob) => unknown }): Promise<void>
subscriber.stop(options?: { abort?: boolean }): Promise<void>
```

| Config | Default | Meaning |
|---|---|---|
| `concurrency` | `1` | Jobs of one name this subscriber runs at once. |
| `pollIntervalSeconds` | `5` | Wait after a poll finds nothing before polling again. After a claimed job, it polls again immediately. |
| `logger` | `console` | Subscriber-wide only. Receives `warn` and `error` calls. |
| `onFailed` | none | Per `listen` only. Called when a job of that name fails for good; see [When a job fails for good](#when-a-job-fails-for-good). |

Config passed to the `Subscriber` applies to every `listen`. Config passed to one `listen` overrides it for that job name. Each `listen` has its own slots, so a slow job name can't starve the others. Listening for the same name twice on one subscriber throws.

### The claimed job

```ts
type ClaimedJob = {
  id: string
  name: string
  payload: JobPayload
  attempts: number          // runs started so far, counting this one
  maxRetries: number
  signal: AbortSignal       // aborts with LockLostError if another worker takes the job over,
                            // or SubscriberStoppedError when subscriber.stop() runs
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

### When a job fails for good

Most jobs have a record in the app that shows their progress: a report that's "generating", an export that's "in progress". When the job fails for good, that record has to move to a failed state, or the UI waits forever. The handler can't always do this itself: when the worker dies during the job's last attempt, no handler code runs. Pass `onFailed` to `listen` to hear about every failure:

```ts
await subscriber.listen("generate-report", handler, {
  onFailed: async (job) => {
    if(typeof job.payload.reportId !== "string") return
    await db.query(`UPDATE reports SET status = 'failed' WHERE id = $1`, [job.payload.reportId])
  },
})

type FailedJob = {
  id: string
  name: string
  payload: JobPayload   // as stored, not validated
  attempts: number
  maxRetries: number
  lastError: string     // what was stored in last_error
  error?: unknown       // the original error; absent when the worker died during the final attempt
}
```

It's called every time pg-relay moves a job of that name to `failed`:
- the handler calls `fail(error, { retry: false })`
- the handler calls `fail(error)` with no retries left
- the handler throws, or returns without finishing the job, with no retries left
- a claim finds a job whose final attempt was already started by a worker that died. The handler isn't called, and `error` is absent.

How it runs:
- Only when the `failed` write landed. If this worker lost the lock, nothing is written and the hook isn't called. Never for `succeeded`, for a retry back to `pending`, or for a job [released by `stop()`](#stopping-the-worker).
- After the handler has returned or thrown, never at the same time as it.
- In the job's slot: the slot doesn't claim its next job until the hook settles, and `stop()` waits for it.
- If it throws, the error goes to `logger.error` and the job stays `failed`. It isn't retried, so make its writes safe to repeat.
- Once per failure: if `queue.retryJob` resets the job and it fails again, the hook is called again.
- The payload is passed as stored. A malformed payload may be why the job failed, so check it before using it.

The hook is **best effort**. It runs after the `failed` write commits, so a worker that dies in the milliseconds between the two never calls it. If you need certainty, also run a reconciliation sweep that reads `queue.listJobs({ name, status: "failed" })` on a schedule and updates any records still marked in progress.

### Stopping the worker

Workers get stopped all the time: every deploy, scale-down or node drain sends SIGTERM, waits a grace period (30 seconds by default on Kubernetes), then SIGKILLs. A job that calls a model or renders a document can easily run longer than that. `stop()` lets such a job stop cleanly and go straight back to the queue, instead of being killed mid-step and waiting out its lock:

```ts
process.once("SIGTERM", async () => {
  await subscriber.stop()          // aborts every running job.signal with SubscriberStoppedError
  await subscriber.queue.close()
  process.exit(0)
})

await subscriber.listen("generate-report", async (job) => {
  const report = await callModel(input, { signal: job.signal })   // throws as soon as stop() runs
  await saveReport(job.id, report)
  await job.complete()
})
// interrupted by stop() → the job is pending again, due now, with the attempt refunded
```

`stop()` stops claiming, aborts each running job's `signal` with a `SubscriberStoppedError`, and resolves once every handler and `onFailed` hook has settled. The lock keeps being renewed while a handler winds down, so no other worker takes the job in the meantime. Use `job.signal.reason instanceof SubscriberStoppedError` to tell a stop apart from a lost lock (`LockLostError`). A job whose signal already aborted for a lost lock keeps that reason, and nothing is written for it.

After a stop aborts a job, the handler decides what happens:

| Handler | Result |
|---|---|
| Calls `complete()` (finished in time) | `succeeded` |
| Throws, or returns without finishing the job | Released |
| Calls `fail(error)` | Released: a retryable failure after a stop is taken to be the stop |
| Calls `fail(error, { retry: false })` | `failed`, and `onFailed` is called |

A released job goes back to `pending`, due now with no backoff, and the interrupted attempt doesn't count: `attempts` goes back down by one. `lastError` is kept as it was. `onFailed` isn't called, because the job didn't fail. A job interrupted on its final attempt is released too, rather than marked `failed`, so a deploy never uses up a job's retries.

Refunding the attempt can't make a job loop forever. Only `stop()` refunds one. A worker that crashes or runs out of memory never gets that far, so its attempt still counts. A handler that ignores its signal is still waited on, and if SIGKILL comes first the job falls back to the normal path: its lock expires and the attempt counts.

`stop({ abort: false })` stops claiming and waits for running handlers without aborting them, for workers whose jobs are short enough to finish within the grace period. Calling `stop()` again while one is in progress returns the same promise.

## Checking on jobs

To show users where a job is, for example a status badge or a progress page, read it back from the queue. The app and the worker can both do this:

```ts
queue.getJob(id: string): Promise<JobRecord | null>
queue.listJobs(filter?: JobListFilter): Promise<JobRecord[]>

type JobListFilter = {
  name?: string
  status?: JobStatus | JobStatus[]   // e.g. ["pending", "running"]
  dedupeKey?: string
  payload?: JobPayload               // payload contains these fields and values
  payloadPath?: { path: string; vars?: Record<string, JsonValue> }  // JSON path condition
  createdAfter?: Date                // written strictly after
  createdBefore?: Date               // written strictly before
  limit?: number                     // default 100
  offset?: number                    // default 0
}
```

`getJob` returns `null` for an id that doesn't exist, including one that isn't a uuid. `listJobs` returns the jobs matching every field you set, newest first. Both create the table first if needed. A `JobRecord` is the job's row in camelCase: `status`, `attempts`, `maxRetries`, `runAfter`, `lastError`, `finishedAt`, and so on (see [The table](#the-table)).

Everything a UI needs can be worked out from the record:

```ts
import type { JobRecord } from "pg-relay"

function describeJob(job: JobRecord): string {
  const total = job.maxRetries + 1
  switch (job.status) {
    case "succeeded": return "Done"
    case "failed":    return `Failed: ${job.lastError}`
    case "running":   return `Running (attempt ${job.attempts}/${total})`
    case "pending": {
      if (job.attempts === 0) return "Queued"
      const secs = Math.max(0, Math.ceil((job.runAfter.getTime() - Date.now()) / 1000))
      return `Attempt ${job.attempts}/${total} failed (${job.lastError}), retrying in ${secs}s`
    }
  }
}
```

- A `pending` job with `attempts > 0` is waiting to be retried. `runAfter` is when it becomes due, and `lastError` says why the last attempt failed.
- `runAfter` comes from the database's clock, so "retrying in N seconds" is off by however far your server's clock drifts from it.
- A `running` job whose worker died still shows `running` until its lock (`lockedUntil`) expires and another worker claims it.

### Filtering on the payload

`payload` matches jobs whose payload contains the given fields and values, nested objects included:

```ts
await queue.listJobs({ name: "export", payload: { report: { id: "r_123" } } })
```

`payloadPath` takes a [Postgres JSON path](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-SQLJSON-PATH) condition for anything else, such as ranges. Pass values as `vars` and refer to them as `$name`. They're sent as query parameters, never pasted into the SQL:

```ts
// unfinished summaries whose week starts in [from, to)
await queue.listJobs({
  name: "generate-summary",
  status: ["pending", "running", "failed"],
  payloadPath: {
    path: "$.rangeStart >= $from && $.rangeStart < $to",
    vars: { from: from.toISOString(), to: to.toISOString() },
  },
})
```

- ISO-8601 UTC timestamps written the same way (e.g. with `toISOString()`) compare correctly as strings.
- A job whose payload lacks the path's fields doesn't match.
- An invalid path rejects the call.

### Paging

`limit` and `offset` page through results, newest first. `createdAfter` and `createdBefore` take a job's own `createdAt`, so the last job of one page can be the cursor for the next:

```ts
const older = await queue.listJobs({ createdBefore: page.at(-1)!.createdAt, limit: 50 })
```

Times are compared to the millisecond, the precision of a JavaScript `Date`. Jobs written in the same millisecond as the cursor job are skipped. If that matters, page with `offset` instead.

## Retrying and cleaning up

```ts
queue.retryJob(id: string): Promise<JobRecord | null>
queue.pruneJobs(options: { olderThanSeconds: number; status?: "succeeded" | "failed" | ("succeeded" | "failed")[]; name?: string }): Promise<number>
```

`retryJob` sends a **failed** job back to `pending`, due now, with `attempts` reset to 0 so it gets its full retries again. Use it for a "retry" button.
- It returns the updated job, or `null` if there's no failed job with that id. Pending, running and succeeded jobs are left alone.
- `lastError` is kept so you can still see why the job failed.
- It rejects if another unfinished job already holds the same name and `dedupeKey`.

`pruneJobs` deletes `succeeded` and `failed` jobs that finished more than `olderThanSeconds` ago, and returns how many it deleted. Finished jobs otherwise stay in the table forever. `status` and `name` narrow it down. Pending and running jobs are never deleted. Run it on a schedule, for example from a cron or when the worker starts:

```ts
await queue.pruneJobs({ olderThanSeconds: 7 * 86_400, status: "succeeded" })   // keep failures longer
await queue.pruneJobs({ olderThanSeconds: 30 * 86_400 })
```

## Monitoring the queue

Once jobs run in a separate worker, the questions in production are "is the queue keeping up?" and "is anything stuck?". `stats()` answers both in one read-only query, per job name. The app and the worker can both call it:

```ts
queue.stats(filter?: { name?: string | string[] }): Promise<JobNameStats[]>

type JobNameStats = {
  name: string
  pending: number                   // status = 'pending', due or not
  due: number                       // pending with run_after <= now(): what a worker would claim right now
  running: number
  succeeded: number
  failed: number
  oldestDueSeconds: number | null   // how long the oldest due job has waited; null when none are due
  expiredLocks: number              // running jobs whose worker stopped renewing their lock
}
```

```ts
await queue.stats()
// [
//   { name: "generate-report", pending: 1, due: 0, running: 0, succeeded: 88,   failed: 0,
//     oldestDueSeconds: null, expiredLocks: 0 },
//   { name: "send-email",      pending: 3, due: 3, running: 1, succeeded: 4120, failed: 2,
//     oldestDueSeconds: 41.7, expiredLocks: 0 },
// ]
```

What to watch:
- **Queue age** (`oldestDueSeconds`) is the main alerting signal. If it keeps growing, workers are down, stuck or too few, whatever the backlog's size.
- **Backlog** (`due`): jobs a worker could claim right now. `pending` also counts jobs scheduled for later, so a queue holding a thousand jobs for tomorrow is healthy.
- **Failures** (`failed`): alert when it goes up.
- **Dead workers** (`expiredLocks`): running jobs whose lock expired without being renewed. Another worker will claim them again.

Age is measured from a job's `run_after`, not its `created_at`. A job written with `delaySeconds`, or waiting out a retry backoff, isn't late until its `run_after` passes. Measuring from `created_at` would make every retry look like a stuck queue. All times come from the database's `now()`, so clock drift between processes doesn't matter.

For example, a health check and a Prometheus gauge:

```ts
app.get("/health/queue", async (_req, res) => {
  const stuck = (await queue.stats()).filter((s) => (s.oldestDueSeconds ?? 0) > 300 || s.expiredLocks > 0)
  res.status(stuck.length > 0 ? 503 : 200).json({ stuck })
})

// prom-client
new Gauge({
  name: "jobs_oldest_due_seconds",
  help: "Seconds the oldest due job has waited",
  labelNames: ["name"],
  async collect(){
    for(const s of await queue.stats()) this.set({ name: s.name }, s.oldestDueSeconds ?? 0)
  },
})
```

- **Only names with jobs are returned**, sorted by name. pg-relay has no list of job names, so a name that was never written, or whose jobs were all pruned, is missing rather than all zeros. If you have a list of names, fill in zeros yourself.
- **Cost:** `stats()` is meant to be polled, every 15–60 seconds for a metrics scrape. `succeeded` and `failed` count every finished job still in the table, so its cost grows with unpruned rows. Run [`pruneJobs`](#retrying-and-cleaning-up) on a schedule.
- It takes no locks and never blocks claiming. Like `getJob` and `listJobs`, it creates the table first if needed, and returns `[]` on an empty one.

## Writing safe handlers

A job can run more than once:
- when its worker dies after doing the work but before `complete()`
- when its lock expires during a long pause, such as a stalled event loop, and another worker claims it
- when `complete()` itself fails, for example because the database is unreachable

So make handlers safe to repeat. The usual pattern is to key whatever the job produces by `job.id`, and to write it as an upsert before calling `complete()`:

```ts
await subscriber.listen("generate-summary", async (job) => {
  const summary = await generate(job.payload, { signal: job.signal })

  // A unique job_id column means a re-run overwrites its own result instead of adding a second one.
  await db.query(
    `INSERT INTO summaries (job_id, body) VALUES ($1, $2)
     ON CONFLICT (job_id) DO UPDATE SET body = EXCLUDED.body`,
    [job.id, summary],
  )

  await job.complete()
})
```

- Write the result first, then call `complete()`. In the opposite order, a crash in between leaves a succeeded job with no result.
- To find a job's result later (say, from a status page polling `getJob`), look it up by the job id.
- For side effects outside your database, such as emails or payments, pass `job.id` as the provider's idempotency key where it supports one.

## Connecting

```ts
new PgQueue("jobs")                                        // DATABASE_URL
new PgQueue("jobs", { connectionString: "postgres://…" })  // explicit URL
new PgQueue("jobs", { pool })                              // your own pg.Pool
new PgQueue("jobs", { schema: "relay" })                   // table relay.jobs
```

- `queue.close()` ends the pool only if the queue opened it. A pool you pass in stays open.
- The table name must be a lowercase Postgres identifier of at most 50 characters (`/^[a-z_][a-z0-9_]{0,49}$/`).

### Keeping the table out of your ORM's way

By default the table goes in the connection's current schema, usually `public`, next to your app's own tables. Tools that manage that schema, such as Prisma Migrate, may treat a table they didn't create as drift, and resetting the schema would drop your jobs.

Give pg-relay its own schema instead:

```ts
const queue = new PgQueue("jobs", { schema: "relay" })
```

- `init()` runs `CREATE SCHEMA IF NOT EXISTS relay`, so the database user needs permission to create schemas, or the schema has to exist already.
- Every query refers to `relay.jobs` directly, whatever the connection's `search_path` is.
- The schema name must be a lowercase Postgres identifier, and can't start with `pg_`: Postgres reserves that prefix for its own system schemas.

## The table

The table is created by `queue.init()`. You rarely need to call `init()` yourself, because the first `writeJob`, `listen`, `getJob`, `listJobs` or `stats` calls it.

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
