import { Pool } from "pg"
import { DEFAULT_JOB_CONFIG, Publisher } from "./publisher"
import { PgQueue, PgRelayInitError } from "./queue"
import { Subscriber } from "./subscriber"
import { DATABASE_URL, useTestDb } from "./testing/db"

const { admin, tableName, schemaName, track } = useTestDb()

/** A queue on DATABASE_URL, closed after each test. */
function queue(table = tableName()): PgQueue{
    return track(new PgQueue(table, { connectionString: DATABASE_URL }))
}

async function columnsOf(table: string): Promise<string[]>{
    const { rows } = await admin.query<{ column_name: string }>(
        "SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY column_name",
        [table],
    )
    return rows.map((row) => row.column_name)
}

describe("constructor", () => {
    it.each(["Jobs", "1jobs", "jobs-x", "jobs x", "", "a".repeat(51)])("rejects the table name %p", (name) => {
        expect(() => new PgQueue(name)).toThrow(/tableName/)
    })

    it.each(["Jobs", "1x", "a-b", "", "a".repeat(64)])("rejects the schema name %p", (schema) => {
        expect(() => new PgQueue("jobs", { schema })).toThrow(/schema/)
    })

    it("names its table with the schema when it has one", () => {
        expect(new PgQueue("jobs").qualifiedTableName).toBe("jobs")
        expect(new PgQueue("jobs", { schema: "pg_relay" }).qualifiedTableName).toBe("pg_relay.jobs")
    })

    it("needs neither a connection nor DATABASE_URL until init", () => {
        const saved = process.env.DATABASE_URL
        delete process.env.DATABASE_URL
        try{
            expect(() => new PgQueue("jobs")).not.toThrow()
        }finally{
            if(saved !== undefined) process.env.DATABASE_URL = saved
        }
    })
})

describe("init", () => {
    it("creates the table with pg-relay's columns", async () => {
        const table = tableName()
        await queue(table).init()

        expect(await columnsOf(table)).toEqual([
            "attempts", "created_at", "dedupe_key", "finished_at", "id", "last_error", "locked_by",
            "locked_until", "lock_ttl_seconds", "max_retries", "name", "payload", "retry_backoff_seconds", "run_after",
            "status", "updated_at",
        ].sort())
    })

    it("creates the claim and dedupe indexes", async () => {
        const table = tableName()
        await queue(table).init()

        const { rows } = await admin.query<{ indexname: string }>(
            "SELECT indexname FROM pg_indexes WHERE tablename = $1 ORDER BY indexname",
            [table],
        )
        expect(rows.map((row) => row.indexname)).toEqual(
            [`${table}_dedupe_idx`, `${table}_pending_idx`, `${table}_pkey`, `${table}_running_idx`],
        )
    })

    it("returns the same promise on every call", () => {
        const q = queue()
        expect(q.init()).toBe(q.init())
        return q.init()
    })

    it("lets many queues initialize the same table at once", async () => {
        const table = tableName()
        await expect(
            Promise.all(Array.from({ length: 10 }, () => queue(table).init())),
        ).resolves.toBeDefined()
    })

    it("keeps the rows of a table that already exists", async () => {
        const table = tableName()
        await queue(table).init()
        await admin.query(
            `INSERT INTO ${table} (name, payload, max_retries, retry_backoff_seconds, lock_ttl_seconds) VALUES ('x', '{}', 0, 1, 1)`,
        )

        await queue(table).init()

        const { rows } = await admin.query(`SELECT count(*)::int AS n FROM ${table}`)
        expect(rows[0].n).toBe(1)
    })

    it("rejects a table that exists but isn't a pg-relay table", async () => {
        const table = tableName()
        await admin.query(`CREATE TABLE ${table} (id int)`)

        const error = await queue(table).init().catch((e: unknown) => e)
        expect(error).toBeInstanceOf(PgRelayInitError)
        expect((error as Error).message).toMatch(/not a pg-relay table/)
    })

    it("rejects with the cause when the database is unreachable", async () => {
        const q = new PgQueue(tableName(), { connectionString: "postgres://localhost:1/nope" })
        track(q)

        const error = await q.init().catch((e: unknown) => e)
        expect(error).toBeInstanceOf(PgRelayInitError)
        expect((error as Error & { cause: { code: string } }).cause.code).toBe("ECONNREFUSED")
    })

    it("rejects when there is no connectionString, pool or DATABASE_URL", async () => {
        const saved = process.env.DATABASE_URL
        delete process.env.DATABASE_URL
        try{
            const q = new PgQueue(tableName())
            track(q)
            await expect(q.init()).rejects.toThrow(PgRelayInitError)
            await expect(q.init()).rejects.toThrow(/DATABASE_URL/)
        }finally{
            if(saved !== undefined) process.env.DATABASE_URL = saved
        }
    })

    it("connects with DATABASE_URL by default", async () => {
        const saved = process.env.DATABASE_URL
        process.env.DATABASE_URL = DATABASE_URL
        try{
            const table = tableName()
            const q = new PgQueue(table)
            track(q)
            await q.init()
            expect(await columnsOf(table)).toContain("name")
        }finally{
            if(saved === undefined) delete process.env.DATABASE_URL
            else process.env.DATABASE_URL = saved
        }
    })

    it("uses a pool it is given", async () => {
        const pool = new Pool({ connectionString: DATABASE_URL })
        const table = tableName()
        const q = new PgQueue(table, { pool })

        await q.init()

        expect(q.pool).toBe(pool)
        expect(await columnsOf(table)).toContain("name")
        await pool.end()
    })
})

describe("close", () => {
    it("ends a pool the queue opened", async () => {
        const q = new PgQueue(tableName(), { connectionString: DATABASE_URL })
        await q.init()

        await q.close()

        await expect(q.pool.query("SELECT 1")).rejects.toThrow(/after calling end/)
    })

    it("leaves a pool it was given open", async () => {
        const pool = new Pool({ connectionString: DATABASE_URL })
        const q = new PgQueue(tableName(), { pool })
        await q.init()

        await q.close()

        await expect(pool.query("SELECT 1")).resolves.toBeDefined()
        await pool.end()
    })

    it("does nothing when the queue never connected", async () => {
        await expect(new PgQueue(tableName()).close()).resolves.toBeUndefined()
    })
})

describe("getJob", () => {
    it("returns a written job as a JobRecord", async () => {
        const q = queue()
        const { id } = await new Publisher(q).writeJob("send-email", {
            payload: { to: "a@b.co" },
            config: { maxRetries: 2 },
            dedupeKey: "k",
        })

        expect(await q.getJob(id)).toEqual({
            id,
            name: "send-email",
            payload: { to: "a@b.co" },
            status: "pending",
            attempts: 0,
            maxRetries: 2,
            retryBackoffSeconds: DEFAULT_JOB_CONFIG.retryBackoffSeconds,
            lockTtlSeconds: DEFAULT_JOB_CONFIG.lockTtlSeconds,
            runAfter: expect.any(Date),
            lockedUntil: null,
            lockedBy: null,
            lastError: null,
            dedupeKey: "k",
            createdAt: expect.any(Date),
            updatedAt: expect.any(Date),
            finishedAt: null,
        })
    })

    it("shows a job waiting for a retry", async () => {
        const table = tableName()
        const q = queue(table)
        const { id } = await new Publisher(q).writeJob("send-email")
        await admin.query(
            `UPDATE ${table} SET attempts = 2, last_error = 'boom', run_after = now() + interval '60 seconds' WHERE id = $1`,
            [id],
        )

        const job = await q.getJob(id)

        expect(job).toMatchObject({ status: "pending", attempts: 2, lastError: "boom" })
        expect(job!.runAfter.getTime()).toBeGreaterThan(Date.now() + 50_000)
    })

    it("returns null for an id that isn't in the table", async () => {
        expect(await queue().getJob("00000000-0000-0000-0000-000000000000")).toBeNull()
    })

    it("returns null for an id that isn't a uuid", async () => {
        expect(await queue().getJob("nope")).toBeNull()
    })
})

describe("listJobs", () => {
    /** A queue holding three jobs, written oldest first: a, b (dedupeKey "x"), then another a. */
    async function seeded(){
        const table = tableName()
        const q = queue(table)
        const publisher = new Publisher(q)
        const first = await publisher.writeJob("a")
        const second = await publisher.writeJob("b", { dedupeKey: "x" })
        const third = await publisher.writeJob("a")
        await admin.query(`UPDATE ${table} SET status = 'failed', finished_at = now() WHERE id = $1`, [first.id])
        return { q, ids: [first.id, second.id, third.id] }
    }

    it("returns every job, newest first", async () => {
        const { q, ids } = await seeded()

        expect((await q.listJobs()).map((job) => job.id)).toEqual([...ids].reverse())
    })

    it("filters by name", async () => {
        const { q, ids } = await seeded()

        expect((await q.listJobs({ name: "a" })).map((job) => job.id)).toEqual([ids[2], ids[0]])
    })

    it("filters by one status or several", async () => {
        const { q, ids } = await seeded()

        expect((await q.listJobs({ status: "failed" })).map((job) => job.id)).toEqual([ids[0]])
        expect((await q.listJobs({ status: ["pending", "running"] })).map((job) => job.id)).toEqual([ids[2], ids[1]])
    })

    it("filters by dedupeKey", async () => {
        const { q, ids } = await seeded()

        expect((await q.listJobs({ dedupeKey: "x" })).map((job) => job.id)).toEqual([ids[1]])
    })

    it("returns at most limit jobs", async () => {
        const { q, ids } = await seeded()

        expect((await q.listJobs({ limit: 2 })).map((job) => job.id)).toEqual([ids[2], ids[1]])
    })

    it.each([0, -1, 1.5])("rejects the limit %p", async (limit) => {
        await expect(queue().listJobs({ limit })).rejects.toThrow(/limit/)
    })

    it("returns JobRecords", async () => {
        const { q } = await seeded()

        expect((await q.listJobs({ status: "failed" }))[0]).toMatchObject({ name: "a", finishedAt: expect.any(Date) })
    })

    it("creates the table if needed", async () => {
        expect(await queue().listJobs()).toEqual([])
    })
})

describe("schema", () => {
    /** A queue whose table lives in a fresh Postgres schema. */
    function schemaQueue(table = tableName(), schema = schemaName()){
        return { q: track(new PgQueue(table, { connectionString: DATABASE_URL, schema })), table, schema }
    }

    it("creates the schema and puts the table and its indexes in it", async () => {
        const { q, table, schema } = schemaQueue()

        await q.init()

        const tables = await admin.query("SELECT table_schema FROM information_schema.tables WHERE table_name = $1", [table])
        expect(tables.rows).toEqual([{ table_schema: schema }])
        const indexes = await admin.query("SELECT count(*)::int AS n FROM pg_indexes WHERE schemaname = $1 AND tablename = $2", [schema, table])
        expect(indexes.rows[0].n).toBe(4)
    })

    it("uses a schema that already exists", async () => {
        const schema = schemaName()
        await admin.query(`CREATE SCHEMA ${schema}`)

        await expect(schemaQueue(tableName(), schema).q.init()).resolves.toBeUndefined()
    })

    it("keeps tables of the same name in different schemas apart", async () => {
        const table = tableName()
        const a = schemaQueue(table).q
        const b = schemaQueue(table).q

        const { id } = await new Publisher(a).writeJob("x")

        expect(await a.getJob(id)).not.toBeNull()
        expect(await b.getJob(id)).toBeNull()
        expect(await b.listJobs()).toEqual([])
    })

    it("checks the columns of the table in its own schema", async () => {
        const { q, table, schema } = schemaQueue()
        await admin.query(`CREATE SCHEMA ${schema}`)
        await admin.query(`CREATE TABLE ${schema}.${table} (id int)`)

        await expect(q.init()).rejects.toThrow(/not a pg-relay table/)
    })

    it("supports reading, pruning and retrying in its schema", async () => {
        const { q, table, schema } = schemaQueue()
        const { id } = await new Publisher(q).writeJob("x")
        await admin.query(`UPDATE ${schema}.${table} SET status = 'failed', finished_at = now() - interval '1 hour'`)

        expect((await q.listJobs({ status: "failed" })).map((job) => job.id)).toEqual([id])
        expect(await q.retryJob(id)).toMatchObject({ id, status: "pending" })
        await admin.query(`UPDATE ${schema}.${table} SET status = 'succeeded', finished_at = now() - interval '1 hour'`)
        expect(await q.pruneJobs({ olderThanSeconds: 60 })).toBe(1)
    })
})

describe("listJobs filters", () => {
    async function withJobs(){
        const table = tableName()
        const q = queue(table)
        const publisher = new Publisher(q)
        const week1 = await publisher.writeJob("summary", { payload: { rangeStart: "2026-09-28T00:00:00.000Z", source: "CRON" } })
        const week2 = await publisher.writeJob("summary", { payload: { rangeStart: "2026-10-05T00:00:00.000Z", source: "USER" } })
        const week3 = await publisher.writeJob("summary", { payload: { rangeStart: "2026-10-12T00:00:00.000Z", source: "USER" } })
        const other = await publisher.writeJob("export", { payload: { reportId: "r_1" } })
        // An hour apart, oldest first: written back to back, two could share a millisecond.
        for(const [hoursAgo, { id }] of [week1, week2, week3, other].reverse().entries()){
            await admin.query(`UPDATE ${table} SET created_at = now() - make_interval(hours => $2) WHERE id = $1`, [id, hoursAgo])
        }
        return { q, table, ids: { week1: week1.id, week2: week2.id, week3: week3.id, other: other.id } }
    }
    const ids = (jobs: { id: string }[]) => jobs.map((job) => job.id)

    it("filters by payload fields", async () => {
        const { q, ids: { week2, week3 } } = await withJobs()

        expect(ids(await q.listJobs({ payload: { source: "USER" } }))).toEqual([week3, week2])
    })

    it("filters by nested payload fields", async () => {
        const q = queue()
        const { id } = await new Publisher(q).writeJob("x", { payload: { report: { id: "r_1", tags: ["a", "b"] } } })
        await new Publisher(q).writeJob("x", { payload: { report: { id: "r_2" } } })

        expect(ids(await q.listJobs({ payload: { report: { id: "r_1" } } }))).toEqual([id])
    })

    it("filters by a JSON path condition with variables", async () => {
        const { q, ids: { week2 } } = await withJobs()

        const jobs = await q.listJobs({
            name: "summary",
            payloadPath: {
                path: "$.rangeStart >= $from && $.rangeStart < $to",
                vars: { from: "2026-10-05T00:00:00.000Z", to: "2026-10-12T00:00:00.000Z" },
            },
        })

        expect(ids(jobs)).toEqual([week2])
    })

    it("leaves out jobs whose payload lacks the path's fields", async () => {
        const { q, ids: { week1, week2, week3 } } = await withJobs()

        const jobs = await q.listJobs({ payloadPath: { path: '$.rangeStart >= "2026-01-01"' } })

        expect(ids(jobs)).toEqual([week3, week2, week1])
    })

    it("rejects an invalid JSON path", async () => {
        const { q } = await withJobs()

        await expect(q.listJobs({ payloadPath: { path: "$.(" } })).rejects.toThrow()
    })

    it("filters by when jobs were created", async () => {
        const { q, table, ids: { week1, week2, week3, other } } = await withJobs()
        const at = async (id: string) => (await admin.query(`SELECT created_at FROM ${table} WHERE id = $1`, [id])).rows[0].created_at as Date

        expect(ids(await q.listJobs({ createdAfter: await at(week2) }))).toEqual([other, week3])
        expect(ids(await q.listJobs({ createdBefore: await at(week2) }))).toEqual([week1])
        expect(ids(await q.listJobs({ createdAfter: await at(week1), createdBefore: await at(other) }))).toEqual([week3, week2])
    })

    it("skips offset jobs", async () => {
        const { q, ids: { week1, week2 } } = await withJobs()

        expect(ids(await q.listJobs({ limit: 2, offset: 2 }))).toEqual([week2, week1])
        expect(await q.listJobs({ offset: 10 })).toEqual([])
    })

    it.each([-1, 1.5])("rejects the offset %p", async (offset) => {
        await expect(queue().listJobs({ offset })).rejects.toThrow(/offset/)
    })

    it("combines every filter", async () => {
        const { q, ids: { week3 } } = await withJobs()

        const jobs = await q.listJobs({
            name: "summary",
            status: "pending",
            payload: { source: "USER" },
            payloadPath: { path: "$.rangeStart >= $from", vars: { from: "2026-10-10" } },
        })

        expect(ids(jobs)).toEqual([week3])
    })
})

describe("pruneJobs", () => {
    /** One job in every status; the finished ones finished `hoursAgo` hours ago. */
    async function withEveryStatus(hoursAgo = 2){
        const table = tableName()
        const q = queue(table)
        const publisher = new Publisher(q)
        const job = async (name: string, status: string) => {
            const { id } = await publisher.writeJob(name)
            const finished = status === "succeeded" || status === "failed"
            await admin.query(
                `UPDATE ${table} SET status = $2, finished_at = CASE WHEN $3 THEN now() - make_interval(hours => $4) END WHERE id = $1`,
                [id, status, finished, hoursAgo],
            )
            return id
        }
        return {
            q,
            ids: {
                pending: await job("a", "pending"),
                running: await job("a", "running"),
                succeeded: await job("a", "succeeded"),
                failed: await job("b", "failed"),
            },
        }
    }
    const remaining = async (q: PgQueue) => (await q.listJobs()).map((job) => job.status).sort()

    it("deletes succeeded and failed jobs that finished before the cutoff and returns how many", async () => {
        const { q } = await withEveryStatus()

        expect(await q.pruneJobs({ olderThanSeconds: 3600 })).toBe(2)
        expect(await remaining(q)).toEqual(["pending", "running"])
    })

    it("keeps finished jobs newer than the cutoff", async () => {
        const { q } = await withEveryStatus()

        expect(await q.pruneJobs({ olderThanSeconds: 3 * 3600 })).toBe(0)
        expect(await remaining(q)).toEqual(["failed", "pending", "running", "succeeded"])
    })

    it("deletes only the given status", async () => {
        const { q } = await withEveryStatus()

        expect(await q.pruneJobs({ olderThanSeconds: 3600, status: "succeeded" })).toBe(1)
        expect(await remaining(q)).toEqual(["failed", "pending", "running"])
    })

    it("deletes only the given job name", async () => {
        const { q } = await withEveryStatus()

        expect(await q.pruneJobs({ olderThanSeconds: 3600, name: "b" })).toBe(1)
        expect(await remaining(q)).toEqual(["pending", "running", "succeeded"])
    })

    it.each([
        [{ olderThanSeconds: -1 }, /olderThanSeconds/],
        [{ olderThanSeconds: 1.5 }, /olderThanSeconds/],
        [{ olderThanSeconds: 60, status: "pending" }, /status/],
        [{ olderThanSeconds: 60, status: ["failed", "running"] }, /status/],
    ])("rejects %p", async (options, message) => {
        await expect(queue().pruneJobs(options as Parameters<PgQueue["pruneJobs"]>[0])).rejects.toThrow(message)
    })
})

describe("retryJob", () => {
    async function failedJob(config = {}){
        const table = tableName()
        const q = queue(table)
        const publisher = new Publisher(q)
        const { id } = await publisher.writeJob("x", { config, dedupeKey: "k" })
        await admin.query(
            `UPDATE ${table} SET status = 'failed', attempts = 5, last_error = 'boom',
                 finished_at = now(), run_after = now() - interval '1 day' WHERE id = $1`,
            [id],
        )
        return { q, table, publisher, id }
    }

    it("puts a failed job back to pending, due now, with its attempts reset", async () => {
        const { q, id } = await failedJob()

        const job = await q.retryJob(id)

        expect(job).toMatchObject({ id, status: "pending", attempts: 0, finishedAt: null, lockedBy: null })
        expect(job!.runAfter.getTime()).toBeGreaterThan(Date.now() - 60_000)
        expect(await q.getJob(id)).toEqual(job)
    })

    it("keeps the last error for reference", async () => {
        const { q, id } = await failedJob()

        expect((await q.retryJob(id))!.lastError).toBe("boom")
    })

    it("gets picked up and run again by a subscriber", async () => {
        const { q, id } = await failedJob()
        await q.retryJob(id)

        const claimed = await new Promise<number>((resolve) => {
            const subscriber = new Subscriber(q, { pollIntervalSeconds: 0.02 })
            void subscriber.listen("x", async (job) => {
                await job.complete()
                resolve(job.attempts)
                void subscriber.stop()
            })
        })

        expect(claimed).toBe(1)
    })

    it.each(["pending", "running", "succeeded"])("returns null and changes nothing for a %s job", async (status) => {
        const { q, table, id } = await failedJob()
        await admin.query(`UPDATE ${table} SET status = $2 WHERE id = $1`, [id, status])

        expect(await q.retryJob(id)).toBeNull()
        expect((await q.getJob(id))!.status).toBe(status)
    })

    it("returns null for an unknown id or one that isn't a uuid", async () => {
        const q = queue()

        expect(await q.retryJob("00000000-0000-0000-0000-000000000000")).toBeNull()
        expect(await q.retryJob("nope")).toBeNull()
    })

    it("rejects when an unfinished job already holds its dedupeKey", async () => {
        const { q, publisher, id } = await failedJob()
        await publisher.writeJob("x", { dedupeKey: "k" })

        await expect(q.retryJob(id)).rejects.toThrow(/dedupeKey/)
        expect((await q.getJob(id))!.status).toBe("failed")
    })
})

// Compile-time checks, enforced by `npm run typecheck`. The functions are never called, so nothing
// touches the database; each test only asserts that the checks were defined.
describe("job name types", () => {
    type Name = "generate-summary" | "send-email"

    it("limits names in filters and records to the queue's job names", () => {
        const checks = async (q: PgQueue<Name>) => {
            await q.listJobs({ name: "send-email" })
            // @ts-expect-error not one of the queue's job names
            await q.listJobs({ name: "send-emial" })
            await q.pruneJobs({ olderThanSeconds: 60, name: "generate-summary" })
            // @ts-expect-error not one of the queue's job names
            await q.pruneJobs({ olderThanSeconds: 60, name: "nope" })

            const listed: Name = (await q.listJobs())[0].name
            const fetched: Name | undefined = (await q.getJob("id"))?.name
            const retried: Name | undefined = (await q.retryJob("id"))?.name
            // @ts-expect-error a record's name is one of the queue's names, not any string
            const narrow: "send-email" = (await q.listJobs())[0].name
            return [listed, fetched, retried, narrow]
        }
        expect(checks).toBeInstanceOf(Function)
    })

    it("accepts any name when the queue doesn't list them", () => {
        const checks = async (q: PgQueue) => {
            await q.listJobs({ name: "anything" })
            const name: string = (await q.listJobs())[0].name
            return name
        }
        expect(checks).toBeInstanceOf(Function)
    })
})
