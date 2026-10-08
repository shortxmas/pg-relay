# job

`src/job.ts`: the types that describe a job, its config, its stored row and the filters that read it. Types only, no runtime code. [Index](index.md)

Every generic takes `TName extends string = string`, the queue's job names when it lists them (see [typed job names](index.md#conventions)), and some take `TPayload extends JobPayload = JobPayload`.

## `JsonValue`, `JobPayload`

```ts
type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue }
type JobPayload = { [key: string]: JsonValue }
```

A payload is a JSON object, stored as `jsonb`. It goes through `JSON.stringify`, so a `Date` is stored as an ISO string and `undefined` fields are dropped. pg-relay never validates payloads. Workers should check one before trusting it.

## `JobConfig`

How a job is retried and locked. Every field is optional. `writeJob` fills in what's missing from [`DEFAULT_JOB_CONFIG`](publisher.md#default_job_config) and stores the result on the row.

| Field | Default | Rule | Meaning |
|---|---|---|---|
| `maxRetries` | `4` | whole number ≥ 0 | Retries after the first attempt. `4` means up to 5 attempts. |
| `retryBackoffSeconds` | `30` | whole number ≥ 0 | Wait before the first retry. It doubles each retry: `backoff × 2^(attempts−1)`. |
| `lockTtlSeconds` | `300` | whole number ≥ 1 | How long a claim stays locked without renewal. The subscriber renews every `lockTtlSeconds / 3`. |
| `delaySeconds` | `0` | whole number ≥ 0 | Seconds after writing before the job may first run. Not stored: it only sets the first `run_after`. |

## `JobConditions<TPayload>`

The second argument of [`writeJob`](publisher.md#writejob): everything about a job except its name.

```ts
type JobConditions<TPayload extends JobPayload = JobPayload> = {
  payload?: TPayload    // defaults to {}
  config?: JobConfig
  dedupeKey?: string    // non-empty; see publisher.md#deduplication
}
```

## `JobStatus`

```ts
type JobStatus = "pending" | "running" | "succeeded" | "failed"
```

| Status | Meaning |
|---|---|
| `pending` | Waiting to run. Due once `runAfter <= now()`. This includes a job waiting out a retry backoff (`attempts > 0`). |
| `running` | Claimed by a worker that holds its lock until `lockedUntil`. |
| `succeeded` | The handler called `complete()`. Final. |
| `failed` | Failed with no retries left, or with `retry: false`. Final, unless `queue.retryJob` resets it. |

## `JobRecord<TPayload, TName>`

A row of the queue's table in camelCase, as returned by `getJob`, `listJobs` and `retryJob`.

| Field | Type | Meaning |
|---|---|---|
| `id` | `string` | uuid |
| `name` | `TName` | Job name |
| `payload` | `TPayload` | As stored |
| `status` | `JobStatus` | |
| `attempts` | `number` | Runs started, counting one in progress |
| `maxRetries`, `retryBackoffSeconds`, `lockTtlSeconds` | `number` | The job's stored config |
| `runAfter` | `Date` | Not claimed before this time |
| `lockedUntil` | `Date \| null` | When the current claim's lock expires |
| `lockedBy` | `string \| null` | The current claim's token, `host:pid:random` |
| `lastError` | `string \| null` | The most recent failure's message, at most 4096 characters. Kept after a retry or a success. |
| `dedupeKey` | `string \| null` | |
| `createdAt`, `updatedAt` | `Date` | |
| `finishedAt` | `Date \| null` | Set on `succeeded` and `failed` |

## `JobListFilter<TName>`

The filter for [`queue.listJobs`](queue.md#listjobs). Every field set must match.

| Field | Type | Matches |
|---|---|---|
| `name` | `TName` | That job name |
| `status` | `JobStatus \| JobStatus[]` | Any of these statuses |
| `dedupeKey` | `string` | That key |
| `payload` | `JobPayload` | Payloads that contain these fields and values, nested objects included (`jsonb @>`) |
| `payloadPath` | `{ path: string; vars?: Record<string, JsonValue> }` | A Postgres JSON path condition, with `$name` variables passed as parameters |
| `createdAfter`, `createdBefore` | `Date` | Written strictly after / before, compared to the millisecond |
| `limit` | `number` | At most this many. Whole number ≥ 1, default `100`. |
| `offset` | `number` | Skip this many first. Whole number ≥ 0, default `0`. |

## `PruneJobsOptions<TName>`

The options for [`queue.pruneJobs`](queue.md#prunejobs).

```ts
type PruneJobsOptions<TName extends string = string> = {
  olderThanSeconds: number                                         // whole number ≥ 0
  status?: "succeeded" | "failed" | ("succeeded" | "failed")[]     // default both
  name?: TName
}
```

## `JobStatsFilter<TName>`, `JobNameStats<TName>`

The filter for, and one result row of, [`queue.stats`](queue.md#stats).

```ts
type JobStatsFilter<TName extends string = string> = { name?: TName | TName[] }

type JobNameStats<TName extends string = string> = {
  name: TName
  pending: number                   // status = 'pending', due or not
  due: number                       // pending with run_after <= now()
  running: number
  succeeded: number
  failed: number
  oldestDueSeconds: number | null   // now() − the oldest due job's run_after; null when none are due
  expiredLocks: number              // running with locked_until < now()
}
```
