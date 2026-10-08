import { Pool, type PoolClient } from "pg"
import { PgRelayInitError, PgQueue } from "./queue"
import { DEFAULT_JOB_CONFIG, Publisher, type TransactionClient } from "./publisher"
import { Subscriber } from "./subscriber"
import { DATABASE_URL, useTestDb } from "./testing/db"

const db = useTestDb()

/** A publisher on a fresh table, plus a reader for that table's rows. */
function setup(){
    const table = db.tableName()
    const publisher = new Publisher(db.track(new PgQueue(table, { connectionString: DATABASE_URL })))
    const rows = async () => (await db.admin.query(`SELECT * FROM ${table} ORDER BY created_at`)).rows
    return { table, publisher, rows }
}

/** A connection with a transaction open on it, rolled back (if still open) and released after the test. */
async function begin(){
    const client = await db.admin.connect()
    await client.query("BEGIN")
    let open = true
    db.defer(async () => {
        if(open) await client.query("ROLLBACK").catch(() => {})
        client.release()
    })
    return {
        client,
        commit: async () => { open = false; await client.query("COMMIT") },
        rollback: async () => { open = false; await client.query("ROLLBACK") },
    }
}

/** What a Prisma interactive-transaction client looks like to pg-relay, over a pg connection. */
function prismaLike(client: PoolClient){
    return { $queryRawUnsafe: async (sql: string, ...params: unknown[]) => (await client.query(sql, params)).rows }
}

const txKinds: [string, (client: PoolClient) => TransactionClient][] = [
    ["a pg client", (client) => client],
    ["a Prisma transaction client", prismaLike],
]

describe("writeJob", () => {
    it("writes a pending row and returns its id", async () => {
        const { publisher, rows } = setup()

        const { id } = await publisher.writeJob("send-email", { payload: { to: "a@b.co" } })

        const [row] = await rows()
        expect(row).toMatchObject({
            id,
            name: "send-email",
            payload: { to: "a@b.co" },
            status: "pending",
            attempts: 0,
            locked_by: null,
            locked_until: null,
            finished_at: null,
        })
    })

    it("makes the job due immediately", async () => {
        const { publisher, rows } = setup()

        await publisher.writeJob("send-email")

        const [row] = await rows()
        const { rows: [{ now }] } = await db.admin.query("SELECT now() AS now")
        expect(row.run_after.getTime()).toBeLessThanOrEqual(now.getTime())
    })

    it("creates the table on the first write", async () => {
        const { publisher, rows } = setup()

        await publisher.writeJob("send-email")

        expect(await rows()).toHaveLength(1)
    })

    it("stores an empty payload when none is given", async () => {
        const { publisher, rows } = setup()

        await publisher.writeJob("send-email")

        expect((await rows())[0].payload).toEqual({})
    })

    it("stores nested JSON payloads as written", async () => {
        const { publisher, rows } = setup()
        const payload = { report: { id: "r_1", sections: ["a", "b"] }, retryable: true, score: 1.5, note: null }

        await publisher.writeJob("summarize", { payload })

        expect((await rows())[0].payload).toEqual(payload)
    })

    it("uses the default config when none is given", async () => {
        const { publisher, rows } = setup()

        await publisher.writeJob("send-email")

        expect((await rows())[0]).toMatchObject({
            max_retries: DEFAULT_JOB_CONFIG.maxRetries,
            retry_backoff_seconds: DEFAULT_JOB_CONFIG.retryBackoffSeconds,
            lock_ttl_seconds: DEFAULT_JOB_CONFIG.lockTtlSeconds,
        })
    })

    it("stores the job's own config", async () => {
        const { publisher, rows } = setup()

        await publisher.writeJob("summarize", { config: { maxRetries: 2, retryBackoffSeconds: 60, lockTtlSeconds: 900 } })

        expect((await rows())[0]).toMatchObject({ max_retries: 2, retry_backoff_seconds: 60, lock_ttl_seconds: 900 })
    })

    it("fills config fields the job leaves out with defaults", async () => {
        const { publisher, rows } = setup()

        await publisher.writeJob("summarize", { config: { maxRetries: 0 } })

        expect((await rows())[0]).toMatchObject({
            max_retries: 0,
            retry_backoff_seconds: DEFAULT_JOB_CONFIG.retryBackoffSeconds,
            lock_ttl_seconds: DEFAULT_JOB_CONFIG.lockTtlSeconds,
        })
    })

    it("writes each call as its own job", async () => {
        const { publisher, rows } = setup()

        const first = await publisher.writeJob("send-email")
        const second = await publisher.writeJob("send-email")

        expect(first.id).not.toBe(second.id)
        expect(await rows()).toHaveLength(2)
    })

    it.each([
        ["an empty name", "", {}, /name/],
        ["negative maxRetries", "x", { config: { maxRetries: -1 } }, /maxRetries/],
        ["fractional maxRetries", "x", { config: { maxRetries: 1.5 } }, /maxRetries/],
        ["negative retryBackoffSeconds", "x", { config: { retryBackoffSeconds: -1 } }, /retryBackoffSeconds/],
        ["fractional retryBackoffSeconds", "x", { config: { retryBackoffSeconds: 0.5 } }, /retryBackoffSeconds/],
        ["a zero lockTtlSeconds", "x", { config: { lockTtlSeconds: 0 } }, /lockTtlSeconds/],
        ["fractional lockTtlSeconds", "x", { config: { lockTtlSeconds: 1.5 } }, /lockTtlSeconds/],
        ["negative delaySeconds", "x", { config: { delaySeconds: -1 } }, /delaySeconds/],
        ["fractional delaySeconds", "x", { config: { delaySeconds: 0.5 } }, /delaySeconds/],
        ["an empty dedupeKey", "x", { dedupeKey: "" }, /dedupeKey/],
    ])("rejects %s without writing", async (_label, name, conditions, message) => {
        const { publisher, rows } = setup()
        await publisher.writeJob("ok")

        await expect(publisher.writeJob(name, conditions)).rejects.toThrow(message)

        expect(await rows()).toHaveLength(1)
    })

    it("rejects with the queue's init error when the table can't be set up", async () => {
        const publisher = new Publisher(db.track(new PgQueue(db.tableName(), { connectionString: "postgres://localhost:1/nope" })))

        await expect(publisher.writeJob("send-email")).rejects.toThrow(PgRelayInitError)
    })

    describe("delaySeconds", () => {
        it("makes the job due that many seconds after it was written", async () => {
            const { publisher, rows } = setup()

            await publisher.writeJob("reminder", { config: { delaySeconds: 86_400 } })

            const [row] = await rows()
            expect(row.run_after.getTime() - row.created_at.getTime()).toBe(86_400_000)
        })

        it("defaults to no delay", async () => {
            const { publisher, rows } = setup()

            await publisher.writeJob("reminder")

            const [row] = await rows()
            expect(row.run_after.getTime()).toBe(row.created_at.getTime())
        })
    })

    describe("dedupeKey", () => {
        it("stores the key and reports a new job as not deduped", async () => {
            const { publisher, rows } = setup()

            const result = await publisher.writeJob("report", { dedupeKey: "report:r_1" })

            expect(result.deduped).toBe(false)
            expect((await rows())[0]).toMatchObject({ id: result.id, dedupe_key: "report:r_1" })
        })

        it("returns the unfinished job with the same name and key instead of writing another", async () => {
            const { publisher, rows } = setup()

            const first = await publisher.writeJob("report", { dedupeKey: "report:r_1" })
            const second = await publisher.writeJob("report", { dedupeKey: "report:r_1", payload: { other: true } })

            expect(second).toEqual({ id: first.id, deduped: true })
            expect(await rows()).toHaveLength(1)
        })

        it("dedupes against a running job", async () => {
            const { publisher, rows } = setup()
            const first = await publisher.writeJob("report", { dedupeKey: "report:r_1" })
            await db.admin.query(`UPDATE ${publisher.queue.tableName} SET status = 'running'`)

            const second = await publisher.writeJob("report", { dedupeKey: "report:r_1" })

            expect(second).toEqual({ id: first.id, deduped: true })
            expect(await rows()).toHaveLength(1)
        })

        it.each(["succeeded", "failed"])("writes a new job once the earlier one has %s", async (status) => {
            const { publisher, rows } = setup()
            const first = await publisher.writeJob("report", { dedupeKey: "report:r_1" })
            await db.admin.query(`UPDATE ${publisher.queue.tableName} SET status = $1`, [status])

            const second = await publisher.writeJob("report", { dedupeKey: "report:r_1" })

            expect(second.deduped).toBe(false)
            expect(second.id).not.toBe(first.id)
            expect(await rows()).toHaveLength(2)
        })

        it("scopes keys to the job name", async () => {
            const { publisher, rows } = setup()

            await publisher.writeJob("report", { dedupeKey: "r_1" })
            const other = await publisher.writeJob("export", { dedupeKey: "r_1" })

            expect(other.deduped).toBe(false)
            expect(await rows()).toHaveLength(2)
        })

        it("never dedupes jobs without a key", async () => {
            const { publisher } = setup()

            const first = await publisher.writeJob("report")

            expect(first.deduped).toBe(false)
        })

        it("writes one job when the same key is written concurrently", async () => {
            const { publisher, rows } = setup()

            const results = await Promise.all(
                Array.from({ length: 10 }, () => publisher.writeJob("report", { dedupeKey: "report:r_1" })),
            )

            expect(new Set(results.map((r) => r.id)).size).toBe(1)
            expect(results.filter((r) => !r.deduped)).toHaveLength(1)
            expect(await rows()).toHaveLength(1)
        })
    })
})

describe("writeJob in the caller's transaction", () => {
    describe.each(txKinds)("with %s", (_kind, wrap) => {
        it("writes a job that appears only once the transaction commits", async () => {
            const { publisher } = setup()
            const { client, commit } = await begin()

            const { id, deduped } = await publisher.writeJob("send-email", { payload: { reviewId: "r_1" } }, { tx: wrap(client) })

            expect(deduped).toBe(false)
            expect(await publisher.queue.getJob(id)).toBeNull()
            await commit()
            expect(await publisher.queue.getJob(id)).toMatchObject({ status: "pending", payload: { reviewId: "r_1" } })
        })

        it("writes nothing when the transaction rolls back", async () => {
            const { publisher, rows } = setup()
            const { client, rollback } = await begin()

            await publisher.writeJob("send-email", {}, { tx: wrap(client) })
            await rollback()

            expect(await rows()).toEqual([])
        })

        it("stores the job's config and delay as given", async () => {
            const { publisher } = setup()
            const { client, commit } = await begin()

            const { id } = await publisher.writeJob("reminder", {
                config: { maxRetries: 2, retryBackoffSeconds: 7, lockTtlSeconds: 9, delaySeconds: 60 },
            }, { tx: wrap(client) })
            await commit()

            const job = await publisher.queue.getJob(id)
            expect(job).toMatchObject({ maxRetries: 2, retryBackoffSeconds: 7, lockTtlSeconds: 9 })
            expect(job!.runAfter.getTime() - job!.createdAt.getTime()).toBe(60_000)
        })

        it("dedupes a second write of the same key in the same transaction, leaving it usable", async () => {
            const { publisher, rows } = setup()
            const { client, commit } = await begin()

            const first = await publisher.writeJob("report", { dedupeKey: "k" }, { tx: wrap(client) })
            const second = await publisher.writeJob("report", { dedupeKey: "k" }, { tx: wrap(client) })

            expect(second).toEqual({ id: first.id, deduped: true })
            await expect(client.query("SELECT 1")).resolves.toBeDefined()
            await commit()
            expect(await rows()).toHaveLength(1)
        })
    })

    it("isn't claimed by a subscriber until the transaction commits", async () => {
        const { publisher } = setup()
        const subscriber = new Subscriber(publisher.queue, { pollIntervalSeconds: 0.02, logger: { warn: () => {}, error: () => {} } })
        db.defer(() => subscriber.stop())
        const claimed: string[] = []
        await subscriber.listen("send-email", async (job) => {
            claimed.push(job.id)
            await job.complete()
        })
        const { client, commit } = await begin()

        const { id } = await publisher.writeJob("send-email", {}, { tx: client })
        await new Promise((resolve) => setTimeout(resolve, 300))
        expect(claimed).toEqual([])

        await commit()
        const deadline = Date.now() + 5_000
        while(claimed.length === 0 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 20))
        expect(claimed).toEqual([id])
    })

    it("is never claimed when the transaction rolls back", async () => {
        const { publisher } = setup()
        const subscriber = new Subscriber(publisher.queue, { pollIntervalSeconds: 0.02, logger: { warn: () => {}, error: () => {} } })
        db.defer(() => subscriber.stop())
        const handler = jest.fn()
        await subscriber.listen("send-email", handler)
        const { client, rollback } = await begin()

        await publisher.writeJob("send-email", {}, { tx: client })
        await rollback()
        await new Promise((resolve) => setTimeout(resolve, 300))

        expect(handler).not.toHaveBeenCalled()
    })

    it.each([
        ["commits", true],
        ["rolls back", false],
    ])("makes a concurrent write of the same key wait, then dedupes only if the first transaction %s", async (_label, commits) => {
        const { publisher, rows } = setup()
        const a = await begin()
        const b = await begin()
        const first = await publisher.writeJob("report", { dedupeKey: "k" }, { tx: a.client })

        let settled = false
        const writing = publisher.writeJob("report", { dedupeKey: "k" }, { tx: b.client }).finally(() => (settled = true))
        await new Promise((resolve) => setTimeout(resolve, 200))
        expect(settled).toBe(false)

        await (commits ? a.commit() : a.rollback())
        const second = await writing
        await b.commit()

        if(commits){
            expect(second).toEqual({ id: first.id, deduped: true })
            expect(await rows()).toHaveLength(1)
        }else{
            expect(second.deduped).toBe(false)
            expect((await rows()).map((row) => row.id)).toEqual([second.id])
        }
    })

    it("creates the queue's table on its own pool, then writes through the transaction", async () => {
        const { table, publisher } = setup()
        const { client, commit } = await begin()

        const { id } = await publisher.writeJob("send-email", {}, { tx: client })

        const { rows: tables } = await db.admin.query("SELECT 1 FROM information_schema.tables WHERE table_name = $1", [table])
        expect(tables).toHaveLength(1)
        expect(await publisher.queue.getJob(id)).toBeNull()
        await commit()
        expect(await publisher.queue.getJob(id)).not.toBeNull()
    })

    it.each([
        ["a pg Pool", () => new Pool({ connectionString: DATABASE_URL })],
        ["an object that can't run SQL", () => ({})],
    ])("rejects %s as tx with a TypeError, writing nothing", async (_label, make) => {
        const { publisher, rows } = setup()
        await publisher.writeJob("ok")
        const tx = make()
        db.defer(async () => { if(tx instanceof Pool) await tx.end() })

        await expect(publisher.writeJob("send-email", {}, { tx: tx as TransactionClient })).rejects.toThrow(TypeError)
        await expect(publisher.writeJob("send-email", {}, { tx: tx as TransactionClient })).rejects.toThrow(/tx must be/)

        expect(await rows()).toHaveLength(1)
    })
})

describe("a queue in its own schema", () => {
    it("writes and dedupes jobs in the schema's table", async () => {
        const schema = db.schemaName()
        const table = db.tableName()
        const publisher = new Publisher(db.track(new PgQueue(table, { connectionString: DATABASE_URL, schema })))

        const first = await publisher.writeJob("report", { dedupeKey: "r_1" })
        const second = await publisher.writeJob("report", { dedupeKey: "r_1" })

        expect(second).toEqual({ id: first.id, deduped: true })
        const { rows } = await db.admin.query(`SELECT id FROM ${schema}.${table}`)
        expect(rows).toEqual([{ id: first.id }])
    })
})

// Compile-time checks, enforced by `npm run typecheck`; never called.
describe("job name types", () => {
    it("takes the queue's job names, inferred from the queue", () => {
        const checks = async (queue: PgQueue<"generate-summary" | "send-email">) => {
            const publisher = new Publisher(queue)
            await publisher.writeJob("send-email", { payload: { to: "a@b.co" } })
            // @ts-expect-error not one of the queue's job names
            await publisher.writeJob("send-emial")
        }
        expect(checks).toBeInstanceOf(Function)
    })

    it("takes a pg client or a Prisma transaction client as tx, and nothing else", () => {
        const checks = async (
            queue: PgQueue<"send-email">,
            client: PoolClient,
            prisma: { $queryRawUnsafe<T = unknown>(query: string, ...values: unknown[]): Promise<T> },
        ) => {
            const publisher = new Publisher(queue)
            await publisher.writeJob("send-email", {}, { tx: client })
            await publisher.writeJob("send-email", {}, { tx: prisma })
            // @ts-expect-error not one of the queue's job names, with tx too
            await publisher.writeJob("send-emial", {}, { tx: client })
            // @ts-expect-error can't run SQL
            await publisher.writeJob("send-email", {}, { tx: {} })
            // @ts-expect-error can't run SQL
            await publisher.writeJob("send-email", {}, { tx: { query: "SELECT 1" } })
        }
        expect(checks).toBeInstanceOf(Function)
    })

    it("takes any name when the queue doesn't list them", () => {
        const checks = async (queue: PgQueue) => {
            await new Publisher(queue).writeJob("anything")
        }
        expect(checks).toBeInstanceOf(Function)
    })
})
