import { PgRelayInitError, PgQueue } from "./queue"
import { DEFAULT_JOB_CONFIG, Publisher } from "./publisher"
import { DATABASE_URL, useTestDb } from "./testing/db"

const db = useTestDb()

/** A publisher on a fresh table, plus a reader for that table's rows. */
function setup(){
    const table = db.tableName()
    const publisher = new Publisher(db.track(new PgQueue(table, { connectionString: DATABASE_URL })))
    const rows = async () => (await db.admin.query(`SELECT * FROM ${table} ORDER BY created_at`)).rows
    return { publisher, rows }
}

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
