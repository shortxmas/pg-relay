# pg-relay

A small job queue backed by a Postgres table, for apps that run as two processes:

- the **app runtime** writes jobs with a `Publisher`
- the **worker** listens for jobs with a `Subscriber`, does the work, and reports back whether the job succeeded or failed

There is no broker. A job is a row: publishing inserts it, and the worker claims it with `FOR UPDATE SKIP LOCKED`, keeps it locked while it runs, and marks it finished or schedules a retry.

> **Status:** early development (0.x). The API may change between minor versions.

This README covers install, a quick start and guides. The full reference for every export is in [`docs/`](docs/index.md), one file per module, and ships with the package at `node_modules/pg-relay/docs/`:

| Doc | Covers |
|---|---|
| [docs/index.md](docs/index.md) | Every export, how the pieces fit, a job's lifecycle |
| [docs/queue.md](docs/queue.md) | `PgQueue`: connecting, schemas, `getJob`, `listJobs`, `retryJob`, `pruneJobs`, `stats`, the table |
| [docs/publisher.md](docs/publisher.md) | `Publisher`: `writeJob`, config, deduplication, writing in your own transaction |
| [docs/subscriber.md](docs/subscriber.md) | `Subscriber`: `listen`, claiming and locks, handler outcomes, `onFailed`, `stop` |
| [docs/job.md](docs/job.md) | The job types: `JobConfig`, `JobRecord`, filters, stats |

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

Constructing these does no I/O and doesn't read `DATABASE_URL`, so the file is safe to import anywhere, including during `next build`. The connection opens, and the table is created, the first time a job is written or listened for. See [`new PgQueue`](docs/queue.md#new-pgqueuetnametablename-options) for other ways to connect.

### Writing jobs (app runtime)

```ts
import { publisher } from "./queue"

const { id, deduped } = await publisher.writeJob("generate-report", {
  payload: { reportId: "r_123" },                   // any JSON the worker needs
  config: { maxRetries: 3, lockTtlSeconds: 900 },   // optional; see JobConfig
  dedupeKey: "report:r_123",                        // optional; see Deduplication
})
```

No job names are registered ahead of time. A job is whatever name the publisher writes and the subscriber listens for. Reference: [`writeJob`](docs/publisher.md#writejob), [`JobConfig`](docs/job.md#jobconfig), [deduplication](docs/publisher.md#deduplication).

### Listening for jobs (worker)

```ts
// worker.ts
import { subscriber } from "./queue"

await subscriber.listen("generate-report", async (job) => {
  const report = await buildReport(job.payload, { signal: job.signal })
  await saveReport(job.id, report)
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

`listen` creates the table if needed, then starts polling and resolves. If the table can't be set up, it rejects, so the worker stops before it ever polls. A handler that throws, or returns without calling `complete` or `fail`, counts as a failure. Reference: [`listen`](docs/subscriber.md#listen), [finishing a job](docs/subscriber.md#finishing-a-job).

### Typed job names

Optionally, list the queue's job names as a union. Nothing changes at runtime: names are only checked when your code compiles.

```ts
type JobName = "generate-report" | "send-email"

const queue = new PgQueue<JobName>("jobs")
export const publisher = new Publisher(queue)    // inferred: Publisher<JobName>
export const subscriber = new Subscriber(queue)  // inferred: Subscriber<JobName>
```

- `writeJob` and `listen` only accept those names, so `publisher.writeJob("send-emial")` is a compile error.
- The `name` filters of `listJobs`, `pruneJobs` and `stats` are checked the same way, and every returned `name` is typed as `JobName`.
- Inside `listen("send-email", (job) => …)` and its `onFailed` hook, `job.name` is typed as `"send-email"`.

## Guides

### Writing jobs in your own transaction

Most jobs exist because something else in the database just changed: a review was approved, so email the customer. The job and the change have to land together. Written on separate connections, either order can go wrong:
- **Job first, then the app's transaction commits.** If the app's transaction rolls back, the job is already written, and the worker emails the customer about an approval that never happened.
- **App's transaction commits, then the job.** If the process dies or the pool times out in between, the job is lost. Retrying the request usually doesn't help either, because the triggering write is guarded to happen once (`WHERE approved_at IS NULL`).

Pass the transaction you already have as `tx`, a node-postgres client or a Prisma interactive-transaction client, and the job commits or rolls back with your other writes:

```ts
await prisma.$transaction(async (tx) => {
  await tx.review.update({ where: { id }, data: { status: "approved" } })
  await publisher.writeJob("send-email", { payload: { reviewId: id } }, { tx })
})
```

The worker can't claim the job until you commit. A duplicate `dedupeKey` never raises an error, so it can't abort your transaction. Read the [rules](docs/publisher.md#rules) before relying on it: they cover the node-postgres form, isolation levels, the same-database requirement and why a `pg.Pool` is rejected.

### Writing safe handlers

A job can run more than once: its worker dies after doing the work but before `complete()`, its lock expires during a long pause and another worker claims it, or `complete()` itself fails. So make handlers safe to repeat. Key whatever the job produces by `job.id`, and write it as an upsert before calling `complete()`:

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
- For side effects outside your database, such as emails or payments, pass `job.id` as the provider's idempotency key where it supports one.

### When a job fails for good

Most jobs have a record in the app that shows their progress: a report that's "generating", an export that's "in progress". When the job fails for good, that record has to move to a failed state, or the UI waits forever. The handler can't always do this itself: when the worker dies during the job's last attempt, no handler code runs. Pass `onFailed` to `listen` to hear about every failure:

```ts
await subscriber.listen("generate-report", handler, {
  onFailed: async (job) => {
    if (typeof job.payload.reportId !== "string") return
    await db.query(`UPDATE reports SET status = 'failed' WHERE id = $1`, [job.payload.reportId])
  },
})
```

It's best effort: a worker that dies between the `failed` write and the hook never calls it. If you need certainty, also sweep `queue.listJobs({ name, status: "failed" })` on a schedule. Reference: [`onFailed`](docs/subscriber.md#onfailed).

### Stopping the worker

Every deploy, scale-down or node drain sends SIGTERM, waits a grace period (30 seconds by default on Kubernetes), then SIGKILLs. Jobs that call a model or render a document often run longer than that. `stop()` aborts each running `job.signal`, so pass the signal to your slow calls:

```ts
await subscriber.listen("generate-report", async (job) => {
  const report = await callModel(input, { signal: job.signal })   // throws as soon as stop() runs
  await saveReport(job.id, report)
  await job.complete()
})
// interrupted by stop() → the job is pending again, due now, with the attempt refunded
```

The interrupted job goes straight back to the queue, without waiting out its lock and without using up a retry, so a deploy never fails a healthy job. `stop({ abort: false })` waits for handlers instead of aborting them. Reference: [`stop`](docs/subscriber.md#stop), [stop outcomes](docs/subscriber.md#stop-outcomes).

### Showing a job's progress

Read a job back with `queue.getJob(id)`, or find jobs with `queue.listJobs({ name, status, payload, … })`. A [`JobRecord`](docs/job.md#jobrecordtpayload-tname) has everything a status page needs: `status`, `attempts`, `maxRetries`, `runAfter` (when a retry is due) and `lastError`. Reference: [`getJob`](docs/queue.md#getjob), [`listJobs`](docs/queue.md#listjobs), with a `describeJob` example.

Give users a retry button with `queue.retryJob(id)`, which sends a failed job back to `pending` with its retries reset. Finished jobs stay in the table until you delete them, so run `queue.pruneJobs({ olderThanSeconds })` on a schedule. Reference: [`retryJob`](docs/queue.md#retryjob), [`pruneJobs`](docs/queue.md#prunejobs).

### Monitoring the queue

`queue.stats()` answers "is the queue keeping up?" and "is anything stuck?" in one read-only query, per job name:

```ts
await queue.stats()
// [{ name: "send-email", pending: 3, due: 3, running: 1, succeeded: 4120, failed: 2,
//    oldestDueSeconds: 41.7, expiredLocks: 0 }, …]
```

Alert on `oldestDueSeconds` growing (workers down, stuck or too few), `failed` going up, and `expiredLocks > 0` (workers that died mid-job). Age is measured from when a job became due, not when it was written, so delayed jobs and retry backoffs don't look late. Reference: [`stats`](docs/queue.md#stats), with a health-check example.

### Keeping the table out of your ORM's way

By default the table goes in the connection's current schema, usually `public`, next to your app's own tables. Tools that manage that schema, such as Prisma Migrate, may treat a table they didn't create as drift, and resetting the schema would drop your jobs. Give pg-relay its own schema instead:

```ts
const queue = new PgQueue("jobs", { schema: "relay" })   // table relay.jobs, schema created if missing
```

Reference: [`PgQueueOptions`](docs/queue.md#pgqueueoptions).
