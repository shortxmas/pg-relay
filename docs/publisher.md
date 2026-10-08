# publisher

`src/publisher.ts`: `Publisher`, which writes jobs. Used by the app runtime. [Index](index.md)

## `new Publisher(queue)`

```ts
const publisher = new Publisher(queue)   // Publisher<TName>, inferred from PgQueue<TName>
publisher.queue                          // the queue it writes to
```

Constructing one does no I/O.

## `writeJob`

```ts
publisher.writeJob(name: TName, conditions?: JobConditions, options?: WriteOptions): Promise<WriteResult>
```

Writes one `pending` job called `name`, after creating the queue's table if needed.

```ts
const { id, deduped } = await publisher.writeJob("generate-report", {
  payload: { reportId: "r_123" },                   // any JSON object; defaults to {}
  config: { maxRetries: 3, lockTtlSeconds: 900 },   // fields left out use DEFAULT_JOB_CONFIG
  dedupeKey: "report:r_123",                        // optional
})
```

- **No job names are registered ahead of time.** A job is whatever name the publisher writes and a subscriber listens for. A job nobody listens for stays `pending`.
- **The job is due** at `now() + delaySeconds`, on the database's clock.
- **Config is stored on the row**, so changing `DEFAULT_JOB_CONFIG` or your code later doesn't affect jobs already queued.
- **Validation happens before any SQL.** It throws if `name` is empty, `dedupeKey` is `""`, `maxRetries`, `retryBackoffSeconds` or `delaySeconds` isn't a whole number ≥ 0, or `lockTtlSeconds` isn't a whole number ≥ 1.
- **Init errors:** it rejects with [`PgRelayInitError`](queue.md#pgrelayiniterror) when the table can't be set up.

See [`JobConditions`](job.md#jobconditionstpayload) and [`JobConfig`](job.md#jobconfig) for the fields.

### `WriteResult`

```ts
type WriteResult = {
  id: string
  deduped: boolean   // true when an unfinished job with the same name and dedupeKey already existed
}
```

## Deduplication

While a job with the same name and `dedupeKey` is `pending` or `running`, `writeJob` writes nothing and returns that job's id with `deduped: true`. The new call's payload and config are ignored.

- Once that job `succeeded` or `failed`, the key is free again and the next write creates a new job.
- Keys are scoped to the job name: `"report"` and `"export"` can both use `"r_1"`.
- A unique partial index enforces keys, so concurrent writes of one key produce exactly one job. The others get `deduped: true`.
- A duplicate is never an error (`INSERT … ON CONFLICT DO NOTHING`), so it's safe inside your own transaction.
- Jobs without a `dedupeKey` are never deduped.

Typical keys name the thing the job is about: `"report:r_123"`, `"user:42:welcome-email"`.

## Writing jobs in your own transaction

Pass `{ tx }` to write the job inside a transaction you already have open, so it commits or rolls back with your other writes (the transactional-outbox pattern):

```ts
// node-postgres
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

// Prisma interactive transaction
await prisma.$transaction(async (tx) => {
  await tx.review.update({ where: { id }, data: { status: "approved" } })
  await publisher.writeJob("send-email", { payload: { reviewId: id } }, { tx })
})
```

Without `tx`, the job is written on the queue's own pool and commits immediately. Then a rollback of your transaction can't take the job back, and a crash between your commit and `writeJob` loses the job.

### `WriteOptions`, `TransactionClient`

```ts
type WriteOptions = { tx?: TransactionClient }

type TransactionClient =
  | { query(sql: string, params?: unknown[]): Promise<{ rows: any[] }> }      // pg PoolClient or Client
  | { $queryRawUnsafe(sql: string, ...params: unknown[]): Promise<any[]> }    // Prisma TransactionClient
```

The type is structural, so pg-relay has no dependency on Prisma. An object with `$queryRawUnsafe` is treated as Prisma. Otherwise one with `query` is treated as `pg`.

### Rules

- **Both queries go through `tx`:** the insert and the follow-up lookup of a deduped job.
- **The worker can't claim the job until you commit.** If you roll back, it never existed.
- **Dedupe inside the transaction:** writing the same name and key twice returns the first job with `deduped: true`, and the transaction stays usable.
- **Dedupe across transactions:** if another open transaction has written the same key, `writeJob` waits for it. If that transaction commits, you get its job with `deduped: true`. If it rolls back, your job is written.
- **Isolation level:** under `REPEATABLE READ` or `SERIALIZABLE`, a duplicate key committed by another transaction after yours started raises a serialization failure (`40001`). Retry the transaction, as for any `40001`. The default, `READ COMMITTED`, never does this.
- **Same database:** the connection must be on the queue's database. pg-relay can't check this.
- **The queue still needs its own connection** (`DATABASE_URL`, `connectionString` or `pool`). The table is created there on first use, outside your transaction. An init failure rejects before anything is sent on `tx`.
- **`tx` must be one connection.** A `pg.Pool` throws a `TypeError`, because its queries may run on any connection, outside your transaction. So does an object with neither method. Prisma's array form, `$transaction([a, b])`, can't run pg-relay's SQL and isn't supported.
- **Parameters are cast explicitly** (`::text`, `::int`, …) and the id is returned as text, so the SQL means the same through `pg` and Prisma.

## `DEFAULT_JOB_CONFIG`

```ts
const DEFAULT_JOB_CONFIG: Required<JobConfig> = {
  maxRetries: 4,             // 5 attempts in total
  retryBackoffSeconds: 30,   // retries after 30s, 60s, 120s, 240s
  lockTtlSeconds: 300,       // 5-minute lock, renewed every 100s while the handler runs
  delaySeconds: 0,           // due immediately
}
```

Exported for reference. Override fields per job with `config`.
