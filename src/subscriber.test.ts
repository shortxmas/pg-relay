import { Publisher } from "./publisher"
import { PgQueue, PgRelayInitError } from "./queue"
import {
    type ClaimedJob, type FailedJob, LockLostError, retryDelaySeconds, Subscriber, type SubscriberConfig, SubscriberStoppedError,
} from "./subscriber"
import { DATABASE_URL, useTestDb } from "./testing/db"

const db = useTestDb()
const quiet = { error: () => {}, warn: () => {} }

/** A queue on a fresh table, with helpers to make subscribers on it and read its rows. */
function setup(){
    const table = db.tableName()
    const queue = db.track(new PgQueue(table, { connectionString: DATABASE_URL }))
    const publisher = new Publisher(queue)

    /** A fast-polling subscriber, stopped after the test. */
    const subscriber = (config: SubscriberConfig = {}) => {
        const s = new Subscriber(queue, { pollIntervalSeconds: 0.02, logger: quiet, ...config })
        db.defer(() => s.stop())
        return s
    }
    const row = async (id: string) => (await db.admin.query(`SELECT * FROM ${table} WHERE id = $1`, [id])).rows[0]
    const rows = async () => (await db.admin.query(`SELECT * FROM ${table} ORDER BY created_at`)).rows

    return { table, queue, publisher, subscriber, row, rows }
}

/** Resolves with fn's first truthy result, polling every 20ms; fails after `timeoutMs`. */
async function waitFor<T>(fn: () => T | Promise<T>, timeoutMs = 5_000): Promise<NonNullable<T>>{
    const deadline = Date.now() + timeoutMs
    for(;;){
        const value = await fn()
        if(value) return value as NonNullable<T>
        if(Date.now() > deadline) throw new Error("waitFor timed out")
        await sleep(20)
    }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** A promise plus the function that resolves it. */
function gate(){
    let open!: () => void
    const opened = new Promise<void>((resolve) => (open = resolve))
    return { opened, open }
}

describe("retryDelaySeconds", () => {
    it.each([
        [30, 1, 30],
        [30, 2, 60],
        [30, 4, 240],
        [0, 3, 0],
    ])("waits %is x 2^(attempts-1): attempt %i → %is", (backoff, attempts, expected) => {
        expect(retryDelaySeconds(backoff, attempts)).toBe(expected)
    })
})

describe("listen", () => {
    it("hands each claimed job to the handler", async () => {
        const { publisher, subscriber } = setup()
        const { id } = await publisher.writeJob("send-email", { payload: { to: "a@b.co" } })
        const received: ClaimedJob[] = []

        await subscriber().listen("send-email", async (job) => {
            received.push(job)
            await job.complete()
        })

        await waitFor(() => received.length === 1)
        expect(received[0]).toMatchObject({ id, name: "send-email", payload: { to: "a@b.co" }, attempts: 1 })
    })

    it("marks a completed job succeeded and releases its lock", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("send-email")

        await subscriber().listen("send-email", (job) => job.complete().then(() => {}))

        const done = await waitFor(async () => {
            const r = await row(id)
            return r.status === "succeeded" && r
        })
        expect(done).toMatchObject({ attempts: 1, locked_by: null, locked_until: null })
        expect(done.finished_at).toBeInstanceOf(Date)
    })

    it("only claims jobs with its name", async () => {
        const { publisher, subscriber, row } = setup()
        const other = await publisher.writeJob("export")
        const mine = await publisher.writeJob("send-email")

        await subscriber().listen("send-email", (job) => job.complete().then(() => {}))

        await waitFor(async () => (await row(mine.id)).status === "succeeded")
        expect(await row(other.id)).toMatchObject({ status: "pending", attempts: 0 })
    })

    it("doesn't claim a job before its delay is up", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("reminder", { config: { delaySeconds: 60 } })
        const handler = jest.fn()

        await subscriber().listen("reminder", handler)
        await sleep(200)

        expect(handler).not.toHaveBeenCalled()
        expect(await row(id)).toMatchObject({ status: "pending", attempts: 0 })
    })

    it("claims jobs in the order they became due", async () => {
        const { publisher, subscriber } = setup()
        const written = []
        for(let i = 0; i < 3; i++) written.push((await publisher.writeJob("x")).id)
        const seen: string[] = []

        await subscriber().listen("x", async (job) => {
            seen.push(job.id)
            await job.complete()
        })

        await waitFor(() => seen.length === 3)
        expect(seen).toEqual(written)
    })

    it("creates the table before polling", async () => {
        const { table, subscriber } = setup()

        await subscriber().listen("x", jest.fn())

        const { rows } = await db.admin.query("SELECT 1 FROM information_schema.tables WHERE table_name = $1", [table])
        expect(rows).toHaveLength(1)
    })

    it("rejects with the queue's init error when the table can't be set up", async () => {
        const queue = db.track(new PgQueue(db.tableName(), { connectionString: "postgres://localhost:1/nope" }))
        const subscriber = new Subscriber(queue, { logger: quiet })

        await expect(subscriber.listen("x", jest.fn())).rejects.toThrow(PgRelayInitError)
    })

    it("rejects listening for the same name twice", async () => {
        const { subscriber } = setup()
        const s = subscriber()
        await s.listen("x", jest.fn())

        await expect(s.listen("x", jest.fn())).rejects.toThrow(/already listening/)
    })

    it("rejects an empty name", async () => {
        const { subscriber } = setup()

        await expect(subscriber().listen("", jest.fn())).rejects.toThrow(/name/)
    })

    it.each([
        [{ concurrency: 0 }, /concurrency/],
        [{ concurrency: 1.5 }, /concurrency/],
        [{ pollIntervalSeconds: 0 }, /pollIntervalSeconds/],
    ])("rejects the config %p", async (config, message) => {
        const { subscriber } = setup()

        expect(() => subscriber(config)).toThrow(message)
        await expect(subscriber().listen("x", jest.fn(), config)).rejects.toThrow(message)
    })
})

describe("fail", () => {
    it("schedules a retry after the backoff while retries are left", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 2, retryBackoffSeconds: 60 } })

        await subscriber().listen("x", (job) => job.fail(new Error("boom")).then(() => {}))

        const retried = await waitFor(async () => {
            const r = await row(id)
            return r.last_error && r
        })
        expect(retried).toMatchObject({ status: "pending", attempts: 1, last_error: "boom", locked_by: null, locked_until: null })
        expect(retried.run_after.getTime() - retried.updated_at.getTime()).toBe(60_000)
    })

    it("runs the job again once its backoff is up", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 2, retryBackoffSeconds: 0 } })
        const attempts: number[] = []

        await subscriber().listen("x", async (job) => {
            attempts.push(job.attempts)
            if(job.attempts === 1) await job.fail(new Error("flaky"))
            else await job.complete()
        })

        await waitFor(async () => (await row(id)).status === "succeeded")
        expect(attempts).toEqual([1, 2])
    })

    it("marks the job failed when it has no retries left", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 1, retryBackoffSeconds: 0 } })
        const handler = jest.fn((job: ClaimedJob) => job.fail(new Error("still broken")).then(() => {}))

        await subscriber().listen("x", handler)

        const failed = await waitFor(async () => {
            const r = await row(id)
            return r.status === "failed" && r
        })
        expect(handler).toHaveBeenCalledTimes(2)
        expect(failed).toMatchObject({ attempts: 2, last_error: "still broken", locked_by: null })
        expect(failed.finished_at).toBeInstanceOf(Date)
    })

    it("fails without retrying when retry is false", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 3 } })

        await subscriber().listen("x", (job) => job.fail(new Error("bad input"), { retry: false }).then(() => {}))

        await waitFor(async () => (await row(id)).status === "failed")
        expect(await row(id)).toMatchObject({ attempts: 1, last_error: "bad input" })
    })

    it("stores anything else that's thrown as text", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 0 } })

        await subscriber().listen("x", (job) => job.fail("plain string").then(() => {}))

        await waitFor(async () => (await row(id)).status === "failed")
        expect((await row(id)).last_error).toBe("plain string")
    })
})

describe("handler outcomes", () => {
    it("treats a thrown error as a failure that can be retried", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 1, retryBackoffSeconds: 60 } })

        await subscriber().listen("x", async () => {
            throw new Error("kaput")
        })

        await waitFor(async () => (await row(id)).last_error)
        expect(await row(id)).toMatchObject({ status: "pending", last_error: "kaput" })
    })

    it("treats returning without complete or fail as a failure that can be retried", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 1, retryBackoffSeconds: 60 } })

        await subscriber().listen("x", async () => {})

        await waitFor(async () => (await row(id)).last_error)
        expect(await row(id)).toMatchObject({ status: "pending" })
        expect((await row(id)).last_error).toMatch(/without calling complete or fail/)
    })

    it("keeps the ack when the handler throws after it", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x")
        let handled = false

        await subscriber().listen("x", async (job) => {
            await job.complete()
            handled = true
            throw new Error("late")
        })

        await waitFor(() => handled)
        await sleep(100)
        expect(await row(id)).toMatchObject({ status: "succeeded", last_error: null })
    })

    it("throws on a second ack", async () => {
        const { publisher, subscriber } = setup()
        await publisher.writeJob("x")
        let second: unknown

        await subscriber().listen("x", async (job) => {
            await job.complete()
            second = await job.fail(new Error("too late")).catch((error: unknown) => error)
        })

        await waitFor(() => second)
        expect((second as Error).message).toMatch(/already/)
    })

    it("returns false from an ack once another worker holds the lock", async () => {
        const { table, publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x")
        let completed: boolean | undefined

        await subscriber().listen("x", async (job) => {
            await db.admin.query(`UPDATE ${table} SET locked_by = 'another-worker' WHERE id = $1`, [job.id])
            completed = await job.complete()
        })

        await waitFor(() => completed !== undefined)
        expect(completed).toBe(false)
        expect(await row(id)).toMatchObject({ status: "running", locked_by: "another-worker" })
    })
})

describe("locks", () => {
    it("locks a claimed job for its lockTtlSeconds", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { lockTtlSeconds: 120 } })
        const release = gate()

        await subscriber().listen("x", async (job) => {
            await release.opened
            await job.complete()
        })

        const running = await waitFor(async () => {
            const r = await row(id)
            return r.status === "running" && r
        })
        expect(running.locked_until.getTime() - running.updated_at.getTime()).toBe(120_000)
        expect(running.locked_by).toEqual(expect.any(String))
        release.open()
    })

    it("renews the lock while the handler runs so no one else claims the job", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { lockTtlSeconds: 1 } })
        const handler = jest.fn(async (job: ClaimedJob) => {
            await sleep(2_500)
            expect(job.signal.aborted).toBe(false)
            expect(await job.complete()).toBe(true)
        })

        await subscriber().listen("x", handler)
        await subscriber().listen("x", handler)

        await waitFor(async () => (await row(id)).status === "succeeded", 6_000)
        expect(handler).toHaveBeenCalledTimes(1)
    })

    it("aborts job.signal when the lock is lost", async () => {
        const { table, publisher, subscriber } = setup()
        await publisher.writeJob("x", { config: { lockTtlSeconds: 3 } })
        let reason: unknown

        await subscriber().listen("x", async (job) => {
            await db.admin.query(`UPDATE ${table} SET locked_by = 'another-worker' WHERE id = $1`, [job.id])
            await new Promise((resolve) => job.signal.addEventListener("abort", resolve))
            reason = job.signal.reason
        })

        await waitFor(() => reason)
        expect(reason).toBeInstanceOf(LockLostError)
    })

    it("reclaims a job whose lock expired, counting the lost attempt", async () => {
        const { table, queue, subscriber } = setup()
        await queue.init()
        const { rows: [{ id }] } = await db.admin.query(
            `INSERT INTO ${table} (name, payload, status, attempts, max_retries, retry_backoff_seconds, lock_ttl_seconds, locked_by, locked_until)
             VALUES ('x', '{}', 'running', 1, 4, 30, 60, 'dead-worker', now() - interval '1 second')
             RETURNING id`,
        )
        const attempts: number[] = []

        await subscriber().listen("x", async (job) => {
            attempts.push(job.attempts)
            await job.complete()
        })

        await waitFor(() => attempts.length === 1)
        expect(attempts).toEqual([2])
        expect(id).toEqual(expect.any(String))
    })

    it("fails a job whose lock expired during its final attempt, without running it again", async () => {
        const { table, queue, subscriber, row } = setup()
        await queue.init()
        const { rows: [{ id }] } = await db.admin.query(
            `INSERT INTO ${table} (name, payload, status, attempts, max_retries, retry_backoff_seconds, lock_ttl_seconds, locked_by, locked_until)
             VALUES ('x', '{}', 'running', 1, 0, 30, 60, 'dead-worker', now() - interval '1 second')
             RETURNING id`,
        )
        const handler = jest.fn()

        await subscriber().listen("x", handler)

        await waitFor(async () => (await row(id)).status === "failed")
        expect(handler).not.toHaveBeenCalled()
        expect((await row(id)).last_error).toMatch(/final attempt/)
    })
})

describe("concurrency", () => {
    it("runs up to `concurrency` jobs at once", async () => {
        const { publisher, subscriber } = setup()
        for(let i = 0; i < 3; i++) await publisher.writeJob("x")
        const release = gate()
        let started = 0

        await subscriber({ concurrency: 3 }).listen("x", async (job) => {
            started++
            await release.opened
            await job.complete()
        })

        await waitFor(() => started === 3)
        release.open()
    })

    it("runs one job at a time by default", async () => {
        const { publisher, subscriber, rows } = setup()
        for(let i = 0; i < 3; i++) await publisher.writeJob("x")
        let inFlight = 0
        let maxInFlight = 0

        await subscriber().listen("x", async (job) => {
            maxInFlight = Math.max(maxInFlight, ++inFlight)
            await sleep(50)
            inFlight--
            await job.complete()
        })

        await waitFor(async () => (await rows()).every((r) => r.status === "succeeded"))
        expect(maxInFlight).toBe(1)
    })

    it("lets a listen override the subscriber's concurrency", async () => {
        const { publisher, subscriber } = setup()
        for(let i = 0; i < 2; i++) await publisher.writeJob("x")
        const release = gate()
        let started = 0

        await subscriber({ concurrency: 1 }).listen("x", async (job) => {
            started++
            await release.opened
            await job.complete()
        }, { concurrency: 2 })

        await waitFor(() => started === 2)
        release.open()
    })

    it("runs each job exactly once across several subscribers", async () => {
        const { publisher, subscriber, rows } = setup()
        for(let i = 0; i < 30; i++) await publisher.writeJob("x")
        const runs = new Map<string, number>()
        const handler = async (job: ClaimedJob) => {
            runs.set(job.id, (runs.get(job.id) ?? 0) + 1)
            await sleep(5)
            await job.complete()
        }

        for(let i = 0; i < 3; i++) await subscriber({ concurrency: 3 }).listen("x", handler)

        await waitFor(async () => (await rows()).every((r) => r.status === "succeeded"))
        expect(runs.size).toBe(30)
        expect([...runs.values()].every((count) => count === 1)).toBe(true)
    })
})

describe("stop", () => {
    it("with abort: false, waits for running handlers without aborting them, then claims nothing more", async () => {
        const { publisher, subscriber, row } = setup()
        const first = await publisher.writeJob("x")
        const release = gate()
        let started = false
        let aborted: boolean | undefined
        const s = subscriber()
        await s.listen("x", async (job) => {
            started = true
            await release.opened
            aborted = job.signal.aborted
            await job.complete()
        })
        await waitFor(() => started)

        let stopped = false
        const stopping = s.stop({ abort: false }).then(() => (stopped = true))
        await sleep(100)
        expect(stopped).toBe(false)

        release.open()
        await stopping
        expect(aborted).toBe(false)
        expect((await row(first.id)).status).toBe("succeeded")

        const second = await publisher.writeJob("x")
        await sleep(200)
        expect((await row(second.id)).status).toBe("pending")
    })

    it("rejects listening after stop", async () => {
        const { subscriber } = setup()
        const s = subscriber()
        await s.stop()

        await expect(s.listen("x", jest.fn())).rejects.toThrow(/stopped/)
    })

    it("resolves for a subscriber that never listened", async () => {
        const { subscriber } = setup()

        await expect(subscriber().stop()).resolves.toBeUndefined()
    })
})

describe("stop aborting running jobs", () => {
    const aborted = (job: ClaimedJob) => new Promise<void>((resolve) => {
        if(job.signal.aborted) return resolve()
        job.signal.addEventListener("abort", () => resolve(), { once: true })
    })

    /**
     * Starts a job whose handler waits for its signal to abort and then calls `afterAbort`, and stops
     * the subscriber once the job is running. Resolves with the row while it ran, the row after stop()
     * resolved, the signal's reason and the onFailed hook.
     */
    async function interrupt(afterAbort: (job: ClaimedJob) => unknown, config = {}){
        const { publisher, subscriber, row, ...rest } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 3, retryBackoffSeconds: 60, ...config } })
        await db.admin.query(`UPDATE ${rest.table} SET last_error = 'earlier' WHERE id = $1`, [id])
        const onFailed = jest.fn()
        let reason: unknown
        let started = false
        const s = subscriber()
        await s.listen("x", async (job) => {
            started = true
            await aborted(job)
            reason = job.signal.reason
            await afterAbort(job)
        }, { onFailed })
        await waitFor(() => started)
        const running = await row(id)

        await s.stop()

        return { id, running, stopped: await row(id), reason, onFailed, publisher, subscriber, row, ...rest }
    }

    /** Expects the job back to pending, due now, with the interrupted attempt refunded and the rest unchanged. */
    function expectReleased(running: Record<string, unknown>, stopped: Record<string, unknown>){
        expect(stopped).toMatchObject({
            status: "pending",
            attempts: (running.attempts as number) - 1,
            last_error: "earlier",
            locked_by: null,
            locked_until: null,
            finished_at: null,
        })
        expect((stopped.run_after as Date).getTime()).toBeLessThanOrEqual(Date.now())
    }

    it("aborts job.signal with a SubscriberStoppedError and releases a job whose handler throws", async () => {
        const { running, stopped, reason, onFailed } = await interrupt((job) => {
            throw job.signal.reason
        })

        expect(reason).toBeInstanceOf(SubscriberStoppedError)
        expect(running).toMatchObject({ status: "running", attempts: 1 })
        expectReleased(running, stopped)
        expect(onFailed).not.toHaveBeenCalled()
    })

    it("releases a job whose handler returns without finishing it", async () => {
        const { running, stopped } = await interrupt(() => {})

        expectReleased(running, stopped)
    })

    it("releases a job the handler fails with retries allowed", async () => {
        const { running, stopped, onFailed } = await interrupt((job) => job.fail(new Error("stopped")))

        expectReleased(running, stopped)
        expect(onFailed).not.toHaveBeenCalled()
    })

    it("fails a job the handler fails with retry: false, and calls onFailed", async () => {
        const { stopped, onFailed } = await interrupt((job) => job.fail(new Error("permanent"), { retry: false }))

        expect(stopped).toMatchObject({ status: "failed", last_error: "permanent" })
        expect(onFailed).toHaveBeenCalledTimes(1)
    })

    it("marks a job the handler completes after the abort as succeeded", async () => {
        const { stopped } = await interrupt((job) => job.complete())

        expect(stopped.status).toBe("succeeded")
    })

    it("lets a new subscriber claim a released job straight away, with the interrupted run's attempts", async () => {
        const { running, subscriber, row, id } = await interrupt((job) => { throw job.signal.reason })
        const attempts: number[] = []

        await subscriber().listen("x", async (job) => {
            attempts.push(job.attempts)
            await job.complete()
        })

        await waitFor(async () => (await row(id)).status === "succeeded", 2_000)
        expect(attempts).toEqual([running.attempts])
    })

    it("releases a job interrupted on its final attempt, which then runs normally", async () => {
        const { running, stopped, subscriber, row, id, onFailed } = await interrupt((job) => { throw job.signal.reason }, { maxRetries: 0 })
        expectReleased(running, stopped)
        expect(onFailed).not.toHaveBeenCalled()
        const handler = jest.fn((job: ClaimedJob) => job.complete())

        await subscriber().listen("x", handler)

        await waitFor(async () => (await row(id)).status === "succeeded")
        expect(handler).toHaveBeenCalledTimes(1)
        expect((await row(id)).attempts).toBe(1)
    })

    it("keeps renewing the lock while an aborted handler winds down", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { lockTtlSeconds: 1 } })
        const claims: string[] = []
        let started = false
        const s = subscriber()
        await s.listen("x", async (job) => {
            started = true
            await aborted(job)
            await sleep(2_500) // longer than the lock TTL: only renewal keeps the job
            throw job.signal.reason
        })
        await waitFor(() => started)
        await subscriber().listen("x", async (job) => {
            claims.push(job.id)
            await job.complete()
        })

        let stopped = false
        await s.stop().then(() => (stopped = true))
        const claimedDuringWindDown = claims.length
        await waitFor(async () => (await row(id)).status === "succeeded")

        expect(stopped).toBe(true)
        expect(claimedDuringWindDown).toBe(0)
        expect(claims).toEqual([id])
    })

    it("keeps a lost lock's LockLostError as the reason and writes nothing", async () => {
        const { table, publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { lockTtlSeconds: 1 } })
        let reason: unknown
        let lost = false
        const s = subscriber()
        await s.listen("x", async (job) => {
            await db.admin.query(`UPDATE ${table} SET locked_by = 'another-worker' WHERE id = $1`, [job.id])
            await aborted(job)
            lost = true
            await sleep(100)
            reason = job.signal.reason
            throw job.signal.reason
        })
        await waitFor(() => lost)

        await s.stop()

        expect(reason).toBeInstanceOf(LockLostError)
        expect(await row(id)).toMatchObject({ status: "running", attempts: 1, locked_by: "another-worker" })
    })

    it("returns the same promise when called twice, and releases each job once", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x")
        let started = false
        const s = subscriber()
        await s.listen("x", async (job) => {
            started = true
            await aborted(job)
            throw job.signal.reason
        })
        await waitFor(() => started)

        const first = s.stop()
        const second = s.stop()

        expect(second).toBe(first)
        await expect(Promise.all([first, second])).resolves.toBeDefined()
        expect(await row(id)).toMatchObject({ status: "pending", attempts: 0 })
    })
})

describe("onFailed", () => {
    /** A hook that records every job it's called with. */
    function recorder(){
        const calls: FailedJob[] = []
        const onFailed = jest.fn((job: FailedJob) => { calls.push(job) })
        return { calls, onFailed }
    }

    it("fires once with the job and the original error on fail(err, { retry: false })", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { payload: { reportId: "r_1" }, config: { maxRetries: 3 } })
        const { calls, onFailed } = recorder()
        const error = new Error("bad input")

        await subscriber().listen("x", (job) => job.fail(error, { retry: false }).then(() => {}), { onFailed })

        await waitFor(() => calls.length)
        await sleep(100)
        expect(onFailed).toHaveBeenCalledTimes(1)
        expect(calls[0]).toEqual({
            id, name: "x", payload: { reportId: "r_1" }, attempts: 1, maxRetries: 3, lastError: "bad input", error,
        })
        expect((await row(id)).status).toBe("failed")
    })

    it("fires on fail(err) with no retries left, not while retries are left", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 1, retryBackoffSeconds: 60 } })
        const { onFailed } = recorder()

        await subscriber().listen("x", (job) => job.fail(new Error("broken")).then(() => {}), { onFailed })

        await waitFor(async () => (await row(id)).last_error)
        await sleep(100)
        expect(await row(id)).toMatchObject({ status: "pending" })
        expect(onFailed).not.toHaveBeenCalled()
    })

    it("fires on fail(err) on the last attempt", async () => {
        const { publisher, subscriber } = setup()
        await publisher.writeJob("x", { config: { maxRetries: 1, retryBackoffSeconds: 0 } })
        const { calls, onFailed } = recorder()

        await subscriber().listen("x", (job) => job.fail(new Error("broken")).then(() => {}), { onFailed })

        await waitFor(() => calls.length)
        expect(calls[0]).toMatchObject({ attempts: 2, maxRetries: 1, lastError: "broken" })
    })

    it("fires with the thrown error when the handler throws on its last attempt", async () => {
        const { publisher, subscriber } = setup()
        await publisher.writeJob("x", { config: { maxRetries: 0 } })
        const { calls, onFailed } = recorder()
        const error = new Error("kaput")

        await subscriber().listen("x", async () => { throw error }, { onFailed })

        await waitFor(() => calls.length)
        expect(calls[0]).toMatchObject({ lastError: "kaput", error })
    })

    it("fires when the handler returns without finishing on its last attempt", async () => {
        const { publisher, subscriber } = setup()
        await publisher.writeJob("x", { config: { maxRetries: 0 } })
        const { calls, onFailed } = recorder()

        await subscriber().listen("x", async () => {}, { onFailed })

        await waitFor(() => calls.length)
        expect(calls[0].lastError).toMatch(/without calling complete or fail/)
        expect((calls[0].error as Error).message).toMatch(/without calling complete or fail/)
    })

    it("fires without an error, and without running the handler, when the final attempt was already started", async () => {
        const { table, queue, subscriber, row } = setup()
        await queue.init()
        const { rows: [{ id }] } = await db.admin.query(
            `INSERT INTO ${table} (name, payload, status, attempts, max_retries, retry_backoff_seconds, lock_ttl_seconds, locked_by, locked_until)
             VALUES ('x', '{"reportId":"r_1"}', 'running', 1, 0, 30, 60, 'dead-worker', now() - interval '1 second')
             RETURNING id`,
        )
        const handler = jest.fn()
        const { calls, onFailed } = recorder()

        await subscriber().listen("x", handler, { onFailed })

        await waitFor(() => calls.length)
        expect(handler).not.toHaveBeenCalled()
        expect((await row(id)).status).toBe("failed")
        expect(calls[0]).toEqual({
            id, name: "x", payload: { reportId: "r_1" }, attempts: 2, maxRetries: 0, lastError: "worker stopped during the final attempt",
        })
        expect(calls[0]).not.toHaveProperty("error")
    })

    it("doesn't fire when the lock was lost and the failure was dropped", async () => {
        const { table, publisher, subscriber } = setup()
        await publisher.writeJob("x")
        const { onFailed } = recorder()
        let failed: boolean | undefined

        await subscriber().listen("x", async (job) => {
            await db.admin.query(`UPDATE ${table} SET locked_by = 'another-worker' WHERE id = $1`, [job.id])
            failed = await job.fail(new Error("bad input"), { retry: false })
        }, { onFailed })

        await waitFor(() => failed !== undefined)
        await sleep(100)
        expect(failed).toBe(false)
        expect(onFailed).not.toHaveBeenCalled()
    })

    it("doesn't fire on complete()", async () => {
        const { publisher, subscriber, row } = setup()
        const { id } = await publisher.writeJob("x")
        const { onFailed } = recorder()

        await subscriber().listen("x", (job) => job.complete().then(() => {}), { onFailed })

        await waitFor(async () => (await row(id)).status === "succeeded")
        await sleep(100)
        expect(onFailed).not.toHaveBeenCalled()
    })

    it("runs only after the handler has returned", async () => {
        const { publisher, subscriber } = setup()
        await publisher.writeJob("x", { config: { maxRetries: 0 } })
        const events: string[] = []

        await subscriber().listen("x", async (job) => {
            await job.fail(new Error("boom"), { retry: false })
            events.push("failed")
            await sleep(100)
            events.push("handler returned")
        }, { onFailed: () => { events.push("hook") } })

        await waitFor(() => events.includes("hook"))
        expect(events).toEqual(["failed", "handler returned", "hook"])
    })

    it("logs a hook's error, keeps the job failed and keeps claiming", async () => {
        const { publisher, subscriber, row } = setup()
        const first = await publisher.writeJob("x", { config: { maxRetries: 0 } })
        const second = await publisher.writeJob("x", { config: { maxRetries: 0 } })
        const logger = { warn: jest.fn(), error: jest.fn() }
        const onFailed = jest.fn(async () => { throw new Error("hook broke") })

        await subscriber({ logger }).listen("x", (job) => job.fail(new Error("boom"), { retry: false }).then(() => {}), { onFailed })

        await waitFor(() => onFailed.mock.calls.length === 2)
        expect((await row(first.id)).status).toBe("failed")
        expect((await row(second.id)).status).toBe("failed")
        expect(logger.error).toHaveBeenCalledWith(expect.stringMatching(/onFailed/), expect.objectContaining({ message: "hook broke" }))
    })

    it("holds the slot until the hook settles: the next claim waits, and so does stop()", async () => {
        const { publisher, subscriber, row } = setup()
        await publisher.writeJob("x", { config: { maxRetries: 0 } })
        const next = await publisher.writeJob("x", { config: { maxRetries: 0 } })
        const hook = gate()
        let hooked = 0
        const s = subscriber()

        await s.listen("x", (job) => job.fail(new Error("boom"), { retry: false }).then(() => {}), {
            concurrency: 1,
            onFailed: async () => {
                hooked++
                await hook.opened
            },
        })
        await waitFor(() => hooked === 1)
        await sleep(200)
        expect((await row(next.id)).status).toBe("pending")

        let stopped = false
        const stopping = s.stop().then(() => (stopped = true))
        await sleep(100)
        expect(stopped).toBe(false)

        hook.open()
        await stopping
        expect(hooked).toBe(1)
    })

    it("fires again when a retried job fails again", async () => {
        const { queue, publisher, subscriber } = setup()
        const { id } = await publisher.writeJob("x", { config: { maxRetries: 0 } })
        const { calls, onFailed } = recorder()

        await subscriber().listen("x", (job) => job.fail(new Error("boom")).then(() => {}), { onFailed })
        await waitFor(() => calls.length === 1)
        await queue.retryJob(id)

        await waitFor(() => calls.length === 2)
        expect(calls.map((job) => job.id)).toEqual([id, id])
    })
})

describe("a queue in its own schema", () => {
    it("claims, retries, renews and finishes jobs in the schema's table", async () => {
        const schema = db.schemaName()
        const queue = db.track(new PgQueue(db.tableName(), { connectionString: DATABASE_URL, schema }))
        const { id } = await new Publisher(queue).writeJob("x", { config: { retryBackoffSeconds: 0, lockTtlSeconds: 1 } })
        const subscriber = new Subscriber(queue, { pollIntervalSeconds: 0.02, logger: quiet })
        db.defer(() => subscriber.stop())

        await subscriber.listen("x", async (job) => {
            if(job.attempts === 1) return void await job.fail(new Error("once"))
            await sleep(1_200) // longer than the lock TTL: only renewal keeps the job
            expect(await job.complete()).toBe(true)
        })

        await waitFor(async () => (await queue.getJob(id))?.status === "succeeded", 6_000)
        expect(await queue.getJob(id)).toMatchObject({ attempts: 2, lastError: "once" })
    })
})

// Compile-time checks, enforced by `npm run typecheck`; never called.
describe("job name types", () => {
    it("takes the queue's job names and narrows job.name to the one listened for", () => {
        const checks = async (queue: PgQueue<"generate-summary" | "send-email">) => {
            const subscriber = new Subscriber(queue)
            await subscriber.listen("send-email", async (job) => {
                const name: "send-email" = job.name
                await job.complete()
                return name
            })
            // @ts-expect-error not one of the queue's job names
            await subscriber.listen("send-emial", async () => {})

            await subscriber.listen<{ reportId: string }, "generate-summary">("generate-summary", async () => {}, {
                onFailed: (job) => {
                    const name: "generate-summary" = job.name
                    const reportId: string = job.payload.reportId
                    // @ts-expect-error the payload type follows listen's
                    const missing: string = job.payload.nope
                    return [name, reportId, missing]
                },
            })
        }
        expect(checks).toBeInstanceOf(Function)
    })

    it("takes any name when the queue doesn't list them", () => {
        const checks = async (queue: PgQueue) => {
            await new Subscriber(queue).listen("anything", async (job) => {
                const name: string = job.name
                return name
            })
        }
        expect(checks).toBeInstanceOf(Function)
    })
})
