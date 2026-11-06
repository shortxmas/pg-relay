# subscriber

`src/subscriber.ts`: `Subscriber`, which claims jobs and runs your handlers. Used by the worker process. [Index](index.md)

## `new Subscriber(queue, config?)`

```ts
const subscriber = new Subscriber(queue, { concurrency: 2, pollIntervalSeconds: 1, logger })
subscriber.queue   // the queue it claims from
```

Constructing one does no I/O. It throws if the config is invalid (see [`ListenConfig`](#listenconfig)).

### `SubscriberConfig`

`ListenConfig` defaults for every `listen` on this subscriber, plus:

| Option | Default | Meaning |
|---|---|---|
| `logger` | `console` | A `SubscriberLogger`, `{ warn(...args), error(...args) }`. Subscriber-wide only. |

What gets logged:
- `error`: a poll or claim query failed (the slot retries after `pollIntervalSeconds`), a handler threw after it had already finished its job, an abandoned handler threw, or an `onFailed` hook threw.
- `warn`: renewing a lock failed (it's retried until the lock actually expires), an ack was dropped because the lock was lost, a handler was abandoned for exceeding `maxRuntimeSeconds`, or an abandoned handler called `complete`/`fail`/`release`.

### `ListenConfig`

| Option | Default | Rule | Meaning |
|---|---|---|---|
| `concurrency` | `1` | whole number ≥ 1 | Jobs of one name this subscriber runs at once. Each slot polls on its own. |
| `pollIntervalSeconds` | `5` | > 0, can be fractional | Wait after a poll that found nothing (or failed). After a claimed job, the slot polls again immediately. |
| `maxRuntimeSeconds` | none (no limit) | > 0 and ≤ 2147483 (about 24.8 days), can be fractional | How long a handler may run before its attempt fails and pg-relay stops waiting for it. See [Run time limit](#run-time-limit). |

The defaults are exported as `DEFAULT_LISTEN_CONFIG`. Config passed to `new Subscriber` applies to every `listen`. Config passed to one `listen` overrides it for that name. An option passed as `undefined` keeps the default.

## `listen`

```ts
subscriber.listen<TPayload, N extends TName>(
  name: N,
  handler: JobHandler<TPayload, N>,
  config?: ListenConfig & { onFailed?: FailedHook<TPayload, N> },
): Promise<void>

type JobHandler<TPayload, TName> = (job: ClaimedJob<TPayload, TName>) => unknown
```

Creates the queue's table if needed, starts `concurrency` slots polling for jobs called `name`, and resolves once polling has started.

```ts
await subscriber.listen<{ reportId: string }>("generate-report", async (job) => {
  const report = await build(job.payload.reportId, { signal: job.signal })
  await saveReport(job.id, report)   // keyed by job.id, so a re-run overwrites its own result
  await job.complete()
}, { concurrency: 4 })
```

- It rejects with [`PgRelayInitError`](queue.md#pgrelayiniterror) if the table can't be set up. Awaiting it makes a misconfigured worker exit before it polls.
- It rejects if `name` is empty, the subscriber was stopped, the subscriber is already listening for `name`, or the config is invalid.
- Each `listen` has its own slots, so a slow job name can't starve the others. One subscriber can listen for many names.
- `TPayload` only types `job.payload`. It isn't validated, so check payloads you don't trust.
- pg-relay never installs signal handlers or calls `process.exit`. Wire up [`stop`](#stop) yourself.

### Claiming

Each slot claims one job at a time, with a single `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED)`:
- **What's claimable:** a `pending` job with `run_after <= now()`, or a `running` job whose lock expired (its worker died).
- **Order:** oldest `run_after` first, then oldest `created_at`.
- **The claim** sets `running`, adds 1 to `attempts`, and locks the job for `lockTtlSeconds` under a token unique to this claim.
- **Final attempt:** if the claim pushes `attempts` past `maxRetries + 1`, the job's last attempt was already started by a worker that died. The job is marked `failed` with `lastError` "worker stopped during the final attempt", the handler is **not** called, and `onFailed` fires.

### Locks

While the handler runs, the subscriber renews the lock every `lockTtlSeconds / 3`. If a renewal finds that another claim holds the job, its lock expired, for example during a long event-loop stall, and another worker took it over. `job.signal` then aborts with `LockLostError`, and this worker's `complete`/`fail`/`release` return `false` without writing anything.

Pick `lockTtlSeconds` so that a stalled or dead worker is detected in an acceptable time. It doesn't need to cover the job's whole run time, because renewal extends it.

### Run time limit

The lock only expires when the **worker** stops. A handler stuck on an `await` that never settles (a call made without `job.signal`, a stuck socket, a forgotten promise) keeps its lock renewed, its slot taken, and `stop()` waiting, for as long as the process lives. `maxRuntimeSeconds` is the backstop:

```ts
await subscriber.listen("generate-report", handler, { maxRuntimeSeconds: 1800 })
```

The timer starts when the job is claimed. If the handler is still running when it fires:
1. `job.signal` aborts with [`RuntimeExceededError`](#errors), unless it was already aborted, in which case the first reason stays.
2. pg-relay stops waiting for the handler. A promise can't be cancelled, so the handler is **abandoned**: whatever it rejects with later goes to `logger.error`, and a later `complete`/`fail`/`release` resolves `false` and writes nothing.
3. The attempt fails as if with `fail(new RuntimeExceededError(...))`: retried after its backoff if retries are left, otherwise `failed`, and `onFailed` fires. `lastError` reads `pg-relay: job <id> exceeded maxRuntimeSeconds (1800)`. This holds even when `stop()` aborted the job first: the attempt counts and isn't refunded, so a job that always hangs still runs out of retries.
4. The slot is freed and claims the next job.

If the handler had already finished the job, or another worker took over its lock, step 3 writes nothing: the outcome already recorded stands. The handler is still abandoned, so cleanup that hangs after `complete()` can't hold a slot.

The limit is a backstop, not a substitute for passing `job.signal`: the abandoned handler keeps running in the background until its promise settles, if it ever does.

## `ClaimedJob`

```ts
type ClaimedJob<TPayload, TName> = {
  id: string
  name: TName
  payload: TPayload
  attempts: number        // runs started so far, counting this one (1 on the first run)
  maxRetries: number
  signal: AbortSignal     // aborts with LockLostError, SubscriberStoppedError or RuntimeExceededError
  complete(): Promise<boolean>
  fail(error: unknown, options?: FailOptions): Promise<boolean>
  release(options?: ReleaseOptions): Promise<boolean>
}

type FailOptions = { retry?: boolean }            // false fails now, even with retries left
type ReleaseOptions = { delaySeconds?: number }   // whole seconds from 0 to 604800 (7 days); default 0
```

`job.attempts > job.maxRetries` means this is the last attempt.

### Finishing a job

Every job is finished by exactly one ack: a call to `complete`, `fail` or `release`. What the handler does decides the outcome:

| Handler | Result |
|---|---|
| Calls `complete()` | `succeeded` |
| Calls `fail(error)` with retries left | `pending`, due after [`retryDelaySeconds`](#retrydelayseconds)`(retryBackoffSeconds, attempts)` |
| Calls `fail(error)` with none left | `failed` |
| Calls `fail(error, { retry: false })` | `failed`, even with retries left |
| Calls `release({ delaySeconds })` | `pending`, due after `delaySeconds` (default `0`), with `attempts` one lower: see [Putting a job back](#putting-a-job-back) |
| Throws without finishing | Same as `fail(thrown)` |
| Returns without finishing | Same as `fail(new Error("handler returned without calling complete or fail"))` |
| Throws after finishing | Logged; the outcome stands |
| Calls `complete`/`fail`/`release` a second time | That call throws |
| Still running after `maxRuntimeSeconds` | Same as `fail(new RuntimeExceededError(...))`, and the handler is abandoned: see [Run time limit](#run-time-limit) |

- `complete`, `fail` and `release` resolve `true` when the outcome was written, and `false` when it was dropped: this claim had lost the lock (the other worker's claim stands), or the handler was abandoned (the timeout already settled the attempt). Nothing is written in either case.
- `lastError` stores the error's `message` (or `String(error)` for a non-Error), truncated to 4096 characters. `complete()` leaves it unchanged.
- **After a [stop](#stop) aborts the job**, the table differs: see [Stop outcomes](#stop-outcomes).

### Putting a job back

`release` is for a handler that can't run the job *right now* for a reason that isn't the job's fault, such as a busy resource:

```ts
await subscriber.listen("extract-spec", async (job) => {
  if (!scratch.hasRoomFor(job.payload)) {
    await job.release({ delaySeconds: 60 })   // try again in a minute, without using up an attempt
    return
  }
  await extract(job.payload, { signal: job.signal })
  await job.complete()
})
```

What it writes, in one update that only lands while this claim still holds the job:

| Column | Value |
|---|---|
| `status` | `pending` |
| `attempts` | `attempts - 1`: the attempt this claim started is refunded |
| `run_after` | `now() + delaySeconds` |
| `locked_by`, `locked_until`, `finished_at` | `NULL` |
| `last_error` | Unchanged: a release isn't a failure |

- It's an ack like `complete` and `fail`: it stops the lock renewal, a later ack throws, and it resolves `false` (writing nothing) after a lost lock or an abandoned handler.
- `onFailed` never fires for a release.
- A job on its final attempt can be released. Its next run is that same final attempt again, not one past the limit.
- `delaySeconds` must be a whole number from `0` to `604800` (7 days). Anything else rejects with `Error("pg-relay: delaySeconds must be a whole number from 0 to 604800, got …")` before anything is written. A rejected release isn't an ack: the job stays claimed, and the handler can still call `complete`, `fail` or `release`.
- **pg-relay doesn't limit how often a job is released.** A handler that always releases keeps its job `pending` forever, so make sure whatever it waits on frees up, or count releases yourself (for example in your own table keyed by `job.id`) and `fail` the job past a limit. In [`stats()`](queue.md#stats), such jobs show as a high `pending` count with a low `oldestDueSeconds`, since each release moves `run_after` forward.

### `signal`

| `signal.reason` | When | What to do |
|---|---|---|
| `LockLostError` | Another worker took the job over | Stop work. Your ack will return `false`. |
| `SubscriberStoppedError` | `subscriber.stop()` ran | Stop work and throw, or `complete()` if it's done. The job is released without using an attempt. |
| `RuntimeExceededError` | The handler ran for `maxRuntimeSeconds` | Stop work. The attempt has already failed and your ack will return `false`. |

Pass `job.signal` to anything that takes one (`fetch`, SDK calls, `setTimeout` from `node:timers/promises`), or check `job.signal.aborted` between steps. A reason never changes once set: a job that lost its lock keeps `LockLostError` even if `stop()` runs later.

### Handlers must be safe to repeat

A job can run more than once: its worker dies after the work but before `complete()`, its lock expires during a long pause and another worker claims it, or `complete()` itself fails. So:
- Key whatever the job produces by `job.id`, and write it as an upsert **before** calling `complete()`.
- Pass `job.id` as the idempotency key to external providers (email, payments) that support one.

## `onFailed`

```ts
type FailedHook<TPayload, TName> = (job: FailedJob<TPayload, TName>) => unknown

type FailedJob<TPayload, TName> = {
  id: string
  name: TName
  payload: TPayload    // as stored, not validated
  attempts: number
  maxRetries: number
  lastError: string    // what was stored in last_error
  error?: unknown      // the original error; absent when no handler ran
}
```

A per-`listen` hook for moving the app's own record to a failed state when a job fails for good:

```ts
await subscriber.listen("generate-report", handler, {
  onFailed: async (job) => {
    if (typeof job.payload.reportId !== "string") return
    await db.query(`UPDATE reports SET status = 'failed' WHERE id = $1`, [job.payload.reportId])
  },
})
```

**When it fires:** every time pg-relay moves a job of that name to `failed`:
- `fail(error, { retry: false })`
- `fail(error)` with no retries left
- the handler throws, or returns without finishing, with no retries left
- the handler is still running after `maxRuntimeSeconds` on its last attempt (`error` is the `RuntimeExceededError`)
- a claim finds the job's final attempt was already started by a dead worker (no handler runs, `error` is absent, `attempts` is `maxRetries + 2`)

**When it doesn't:** on `succeeded`, a retry back to `pending`, a job [released by `stop()`](#stop-outcomes), or a failure that wasn't written because the lock was lost.

**How it runs:**
- After the handler has returned or thrown, never at the same time as it. The one exception is a handler abandoned for exceeding `maxRuntimeSeconds`: the hook runs as soon as it's abandoned, while the handler may still be running in the background.
- In the job's slot: the slot claims nothing else until the hook settles, and `stop()` waits for it.
- If it throws, the error goes to `logger.error` and the job stays `failed`. It isn't retried, so make its writes safe to repeat.
- It fires again if `queue.retryJob` resets the job and it fails again.
- **Best effort:** it runs after the `failed` write commits, so a worker that dies in between never calls it. If you need certainty, also run a sweep over `queue.listJobs({ name, status: "failed" })`.

## `stop`

```ts
subscriber.stop(options?: StopOptions): Promise<void>

type StopOptions = { abort?: boolean }   // default true
```

Stops claiming, aborts every running job's `signal` with a `SubscriberStoppedError`, and resolves once every handler and `onFailed` hook has settled, or the handler was abandoned for exceeding `maxRuntimeSeconds`.

```ts
for (const signal of ["SIGTERM", "SIGINT"] as const) {
  process.once(signal, async () => {
    await subscriber.stop()
    await subscriber.queue.close()
    process.exit(0)
  })
}
```

- Locks keep being renewed while aborted handlers wind down, so no other worker takes their jobs in the meantime.
- A job claimed while `stop()` is already running has its signal aborted as soon as its handler starts.
- A handler that ignores its signal is still waited on, up to its `maxRuntimeSeconds` if one is set. If the process is SIGKILLed first, the job's lock expires and its attempt counts, exactly like a crash. `maxRuntimeSeconds` bounds how long `stop()` can take, but it only helps within the grace period if it's shorter than that.
- `onFailed` hooks aren't limited by `maxRuntimeSeconds`, so a hook that hangs still holds up `stop()`.
- Calling `stop()` again returns the first call's promise. Its options are ignored.
- After `stop()`, `listen` rejects. Make a new `Subscriber` to listen again.
- `stop({ abort: false })` stops claiming and waits for handlers without aborting them, for jobs short enough to finish within the deploy's grace period.

### Stop outcomes

After a stop aborts a job:

| Handler | Result |
|---|---|
| Calls `complete()` | `succeeded` |
| Throws, or returns without finishing | Released |
| Calls `fail(error)` | Released: a retryable failure after a stop is taken to be the stop |
| Calls `fail(error, { retry: false })` | `failed`, and `onFailed` fires |
| Calls `release({ delaySeconds })` | `pending`, due after `delaySeconds`: the release is written as asked, not replaced by the stop's due-now |
| Still running after `maxRuntimeSeconds` | Not released: the attempt counts, as in [Run time limit](#run-time-limit) |

**Released** means back to `pending`, due now with no backoff, with `attempts` one lower (the interrupted run doesn't count), `lastError` unchanged, and the lock cleared. `onFailed` doesn't fire. A job interrupted on its final attempt is released too, so a deploy never uses up a job's retries.

This can't make a job loop forever. Only `stop()` and `release()` refund an attempt, and a worker that crashes or runs out of memory never gets that far, so its attempts still count. Neither does a handler that hangs past `maxRuntimeSeconds`.

## Errors

```ts
class LockLostError extends Error { name: "LockLostError" }                     // this worker lost the job's lock
class SubscriberStoppedError extends Error { name: "SubscriberStoppedError" }   // stop() interrupted the job
class RuntimeExceededError extends Error {                                     // the handler ran past maxRuntimeSeconds
  name: "RuntimeExceededError"
  maxRuntimeSeconds: number
}
```

They're used as `job.signal.reason`, and `RuntimeExceededError` is also the error a timed-out attempt fails with (`onFailed`'s `error`). Tell them apart with `instanceof`.

## `retryDelaySeconds`

```ts
retryDelaySeconds(retryBackoffSeconds: number, attempts: number): number   // backoff × 2^(attempts − 1)
```

The wait before the retry that follows the `attempts`-th failed attempt. With the default 30s backoff: 30, 60, 120, 240. Exported so apps can show when a job will run again.
