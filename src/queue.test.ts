import { Pool } from "pg"
import { DEFAULT_JOB_CONFIG, Publisher } from "./publisher"
import { PgQueue, PgRelayInitError } from "./queue"
import { DATABASE_URL, useTestDb } from "./testing/db"

const { admin, tableName, track } = useTestDb()

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
