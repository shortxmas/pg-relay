# pg-relay reference

The full reference for every export of `pg-relay`, one file per source module. These files ship with the package, so they're also at `node_modules/pg-relay/docs/`. For install, a quick start and guides, see the [README](../README.md).

| Module | Doc | Exports |
|---|---|---|
| `src/queue.ts` | [queue.md](queue.md) | `PgQueue`, `PgQueueOptions`, `PgRelayInitError` |
| `src/publisher.ts` | [publisher.md](publisher.md) | `Publisher`, `DEFAULT_JOB_CONFIG`, `WriteResult`, `WriteOptions`, `TransactionClient` |
| `src/subscriber.ts` | [subscriber.md](subscriber.md) | `Subscriber`, `SubscriberConfig`, `ListenConfig`, `DEFAULT_LISTEN_CONFIG`, `SubscriberLogger`, `ClaimedJob`, `JobHandler`, `FailOptions`, `FailedJob`, `FailedHook`, `StopOptions`, `LockLostError`, `SubscriberStoppedError`, `retryDelaySeconds` |
| `src/job.ts` | [job.md](job.md) | `JobPayload`, `JsonValue`, `JobConfig`, `JobConditions`, `JobStatus`, `JobRecord`, `JobListFilter`, `PruneJobsOptions`, `JobStatsFilter`, `JobNameStats` |

Everything is exported from the package root:

```ts
import { PgQueue, Publisher, Subscriber, type ClaimedJob } from "pg-relay"
```

## How the pieces fit

pg-relay is a job queue in one Postgres table. There's no broker and no other process.

- A **`PgQueue`** names the table and owns the connection to it. It creates the table on first use, and reads, retries, prunes and counts jobs.
- A **`Publisher`**, used by the app, writes jobs: one row each, `pending`.
- A **`Subscriber`**, used by the worker, polls the table, claims due jobs with `FOR UPDATE SKIP LOCKED`, hands each to your handler, and writes the outcome the handler reports.

Both processes import the same module that constructs these three objects. Constructing them does no I/O.

## A job's lifecycle

```
            writeJob
               │
               ▼
   ┌──────► pending ◄──────────────────────────────┐
   │           │  claimed once run_after <= now()  │ fail(error) with retries left
   │           ▼  (attempts + 1, locked)           │ (run_after = now() + backoff)
   │        running ───────────────────────────────┤
   │           │                                   │ released by stop()
   │           │                                   │ (run_after = now(), attempts − 1)
   │           ├──── complete() ──────► succeeded  │
   │           │                                   │
   │           └──── fail, no retries left ──► failed ──► onFailed hook
   │                 or fail(error, { retry: false })
   │
   └──── queue.retryJob(id)  (failed → pending, attempts = 0)
```

- A `running` job whose worker dies keeps its row until its lock expires. The next claim takes it over and counts another attempt. If that attempt was the job's last, it's marked `failed` without running again.
- `succeeded` and `failed` rows stay until `queue.pruneJobs` deletes them.

## Conventions

- **Times** come from the database's `now()`, never the process clock.
- **Errors** thrown by pg-relay have messages starting with `pg-relay:`. Arguments are checked before any SQL is sent.
- **Typed job names:** `PgQueue<"a" | "b">` restricts every job name argument, filter and returned `name` to that union, at compile time only. `Publisher` and `Subscriber` infer it from the queue.
- **Seconds everywhere:** every duration option is a whole number of seconds, except `pollIntervalSeconds`, which can be fractional.
