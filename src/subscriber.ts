import { randomUUID } from "node:crypto"
import { hostname } from "node:os"
import type { JobPayload } from "./job"
import { PgQueue } from "./queue"

export type ListenConfig = {
    // jobs of one name this subscriber runs at once
    concurrency?: number
    // seconds to wait after a poll finds nothing (or fails) before polling again
    pollIntervalSeconds?: number
}

export type SubscriberLogger = {
    warn(...args: unknown[]): void
    error(...args: unknown[]): void
}

/** Defaults for every listen; each listen can override them. */
export type SubscriberConfig = ListenConfig & {
    // defaults to console
    logger?: SubscriberLogger
}

export const DEFAULT_LISTEN_CONFIG: Required<ListenConfig> = {
    concurrency: 1,
    pollIntervalSeconds: 5,
}

export type FailOptions = {
    // false fails the job now, even with retries left. Default true.
    retry?: boolean
}

/** A job the subscriber claimed, handed to the handler with the means to finish it. */
export type ClaimedJob<TPayload extends JobPayload = JobPayload, TName extends string = string> = {
    id: string
    name: TName
    payload: TPayload
    // runs started so far, counting this one
    attempts: number
    maxRetries: number
    // aborts with LockLostError if another worker takes over the job
    signal: AbortSignal
    /** Marks the job succeeded. false when the lock was lost and the outcome was dropped. */
    complete(): Promise<boolean>
    /** Retries the job after its backoff, or fails it when no retries are left or `retry` is false. */
    fail(error: unknown, options?: FailOptions): Promise<boolean>
}

export type JobHandler<TPayload extends JobPayload = JobPayload, TName extends string = string> =
    (job: ClaimedJob<TPayload, TName>) => unknown

/** A job pg-relay just moved to failed, handed to listen's onFailed hook. */
export type FailedJob<TPayload extends JobPayload = JobPayload, TName extends string = string> = {
    id: string
    name: TName
    // as stored: it isn't validated, and a malformed payload may be why the job failed
    payload: TPayload
    attempts: number
    maxRetries: number
    // what was stored in last_error: the error's message, truncated
    lastError: string
    // the original error; absent when the worker died during the final attempt and no handler ran
    error?: unknown
}

export type FailedHook<TPayload extends JobPayload = JobPayload, TName extends string = string> =
    (job: FailedJob<TPayload, TName>) => unknown

/** This worker no longer holds the job's lock: it expired and another worker claimed the job. */
export class LockLostError extends Error{
    constructor(jobId: string){
        super(`pg-relay: lost the lock on job ${jobId}`)
        this.name = "LockLostError"
    }
}

/** Seconds before the retry that follows the `attempts`-th failed attempt: backoff x 2^(attempts-1). */
export function retryDelaySeconds(retryBackoffSeconds: number, attempts: number): number{
    return retryBackoffSeconds * 2 ** (attempts - 1)
}

const MAX_ERROR_LENGTH = 4096
const NO_ACK = "handler returned without calling complete or fail"
const FINAL_ATTEMPT = "worker stopped during the final attempt"

type ClaimedRow = {
    id: string
    name: string
    payload: JobPayload
    attempts: number
    max_retries: number
    retry_backoff_seconds: number
    lock_ttl_seconds: number
}

/** Claims and runs jobs from a queue. Used by the worker. */
export class Subscriber<TName extends string = string>{
    readonly queue: PgQueue<TName>
    private readonly config: Required<ListenConfig>
    private readonly logger: SubscriberLogger
    private readonly listening = new Set<string>()
    private readonly loops: Promise<void>[] = []
    private readonly stopping = new AbortController()

    constructor(queue: PgQueue<TName>, config: SubscriberConfig = {}){
        const { logger, ...listenConfig } = config
        this.queue = queue
        this.config = resolveListenConfig(DEFAULT_LISTEN_CONFIG, listenConfig)
        this.logger = logger ?? console
    }

    /**
     * Creates the queue's table if needed, then polls for jobs called `name` and hands each to
     * `handler`. Resolves once polling has started; rejects if the table can't be set up.
     */
    async listen<TPayload extends JobPayload = JobPayload, N extends TName = TName>(
        name: N,
        handler: JobHandler<TPayload, N>,
        config: ListenConfig & {
            // called after each job of this name moves to failed, once its handler has settled
            onFailed?: FailedHook<TPayload, N>
        } = {},
    ): Promise<void>{
        if(!name) throw new Error("pg-relay: listen needs a job name")
        if(this.stopping.signal.aborted) throw new Error("pg-relay: this subscriber was stopped")
        if(this.listening.has(name)) throw new Error(`pg-relay: already listening for "${name}"`)
        const { onFailed, ...listenConfig } = config
        const { concurrency, pollIntervalSeconds } = resolveListenConfig(this.config, listenConfig)

        this.listening.add(name)
        try{
            await this.queue.init()
        }catch(error){
            this.listening.delete(name)
            throw error
        }

        for(let i = 0; i < concurrency; i++){
            this.loops.push(this.pollLoop(name, handler as unknown as JobHandler, onFailed as FailedHook | undefined, pollIntervalSeconds * 1000))
        }
    }

    /** Stops claiming jobs and resolves once every running handler has finished. */
    async stop(): Promise<void>{
        this.stopping.abort()
        await Promise.all(this.loops)
    }

    /** Polls until stopped: straight away after a claimed job, otherwise after `intervalMs`. */
    private async pollLoop(name: string, handler: JobHandler, onFailed: FailedHook | undefined, intervalMs: number): Promise<void>{
        const signal = this.stopping.signal
        while(!signal.aborted){
            let claimed = false
            try{
                claimed = await this.processNext(name, handler, onFailed)
            }catch(error){
                this.logger.error(`pg-relay: processing "${name}" failed:`, error)
            }
            if(!claimed) await sleep(intervalMs, signal)
        }
    }

    /** Claims the next due job called `name` and runs it. false when there was nothing to claim. */
    private async processNext(name: string, handler: JobHandler, onFailed?: FailedHook): Promise<boolean>{
        // A fresh token per claim, so acks can only land while this claim still holds the job.
        const lockedBy = `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`
        const table = this.queue.qualifiedTableName
        const { rows } = await this.queue.pool.query<ClaimedRow>(
            `UPDATE ${table} AS j
             SET status = 'running',
                 attempts = j.attempts + 1,
                 locked_by = $2,
                 locked_until = now() + make_interval(secs => j.lock_ttl_seconds),
                 updated_at = now()
             WHERE j.id = (
                 SELECT id FROM ${table}
                 WHERE name = $1
                   AND ((status = 'pending' AND run_after <= now())
                        OR (status = 'running' AND locked_until < now()))
                 ORDER BY run_after, created_at
                 LIMIT 1
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING j.id, j.name, j.payload, j.attempts, j.max_retries, j.retry_backoff_seconds, j.lock_ttl_seconds`,
            [name, lockedBy],
        )
        const row = rows[0]
        if(!row) return false

        // Its last allowed attempt was already started by a worker that died; don't run it again.
        if(row.attempts > row.max_retries + 1){
            if(await this.release(row.id, lockedBy, { status: "failed", error: FINAL_ATTEMPT })){
                await this.notifyFailed(onFailed, failedJob(row, FINAL_ATTEMPT))
            }
            return true
        }

        await this.run(row, lockedBy, handler, onFailed)
        return true
    }

    private async run(row: ClaimedRow, lockedBy: string, handler: JobHandler, onFailed?: FailedHook): Promise<void>{
        const lock = new AbortController()
        let acked = false
        // set when this run's failed write lands; onFailed gets it once the handler has settled
        let failed: FailedJob | undefined

        const heartbeat = setInterval(() => {
            this.renewLock(row.id, lockedBy).then((held) => {
                if(held || acked) return
                clearInterval(heartbeat)
                lock.abort(new LockLostError(row.id))
            }).catch((error: unknown) => {
                // Keep trying: the lock is only lost once it actually expires.
                this.logger.warn(`pg-relay: renewing the lock on job ${row.id} failed:`, error)
            })
        }, row.lock_ttl_seconds * 1000 / 3)

        const ack = async (finish: () => Promise<boolean>): Promise<boolean> => {
            if(acked) throw new Error(`pg-relay: job ${row.id} was already completed or failed`)
            acked = true
            clearInterval(heartbeat)
            const held = await finish()
            if(!held) this.logger.warn(`pg-relay: job ${row.id} lost its lock; its outcome was dropped`)
            return held
        }

        const fail = (error: unknown, options: FailOptions = {}) => ack(async () => {
            const retry = options.retry !== false && row.attempts <= row.max_retries
            const held = await this.release(row.id, lockedBy, retry
                ? { status: "pending", error: error, delaySeconds: retryDelaySeconds(row.retry_backoff_seconds, row.attempts) }
                : { status: "failed", error: error })
            if(held && !retry) failed = { ...failedJob(row, errorMessage(error)), error }
            return held
        })

        const job: ClaimedJob = {
            id: row.id,
            name: row.name,
            payload: row.payload,
            attempts: row.attempts,
            maxRetries: row.max_retries,
            signal: lock.signal,
            complete: () => ack(() => this.release(row.id, lockedBy, { status: "succeeded" })),
            fail,
        }

        try{
            await handler(job)
            if(!acked) await fail(new Error(NO_ACK))
        }catch(error){
            if(acked) this.logger.error(`pg-relay: handler for job ${row.id} threw after finishing it:`, error)
            else await fail(error)
        }finally{
            clearInterval(heartbeat)
        }
        if(failed) await this.notifyFailed(onFailed, failed)
    }

    /** Runs the onFailed hook, if any; its errors are logged, never thrown. */
    private async notifyFailed(onFailed: FailedHook | undefined, job: FailedJob): Promise<void>{
        if(!onFailed) return
        try{
            await onFailed(job)
        }catch(error){
            this.logger.error(`pg-relay: onFailed for job ${job.id} threw:`, error)
        }
    }

    /** Extends the lock by the job's TTL. false when this claim no longer holds the job. */
    private async renewLock(id: string, lockedBy: string): Promise<boolean>{
        const { rowCount } = await this.queue.pool.query(
            `UPDATE ${this.queue.qualifiedTableName}
             SET locked_until = now() + make_interval(secs => lock_ttl_seconds), updated_at = now()
             WHERE id = $1 AND status = 'running' AND locked_by = $2`,
            [id, lockedBy],
        )
        return rowCount === 1
    }

    /**
     * Moves a running job out of this claim: succeeded, failed, or back to pending after a delay.
     * false when this claim no longer holds the job, in which case nothing changes.
     */
    private async release(
        id: string,
        lockedBy: string,
        outcome: { status: "succeeded" } | { status: "failed"; error: unknown } | { status: "pending"; error: unknown; delaySeconds: number },
    ): Promise<boolean>{
        const finished = outcome.status !== "pending"
        const { rowCount } = await this.queue.pool.query(
            `UPDATE ${this.queue.qualifiedTableName}
             SET status = $3,
                 last_error = CASE WHEN $3 = 'succeeded' THEN last_error ELSE $4 END,
                 run_after = CASE WHEN $3 = 'pending' THEN now() + make_interval(secs => $5) ELSE run_after END,
                 finished_at = CASE WHEN $6 THEN now() ELSE NULL END,
                 locked_by = NULL,
                 locked_until = NULL,
                 updated_at = now()
             WHERE id = $1 AND status = 'running' AND locked_by = $2`,
            [
                id,
                lockedBy,
                outcome.status,
                "error" in outcome ? errorMessage(outcome.error) : null,
                "delaySeconds" in outcome ? outcome.delaySeconds : 0,
                finished,
            ],
        )
        return rowCount === 1
    }
}

function failedJob(row: ClaimedRow, lastError: string): FailedJob{
    return { id: row.id, name: row.name, payload: row.payload, attempts: row.attempts, maxRetries: row.max_retries, lastError }
}

function resolveListenConfig(defaults: Required<ListenConfig>, config: ListenConfig): Required<ListenConfig>{
    const resolved = { ...defaults, ...config }
    if(!Number.isInteger(resolved.concurrency) || resolved.concurrency < 1){
        throw new Error(`pg-relay: concurrency must be a whole number of 1 or more, got ${resolved.concurrency}`)
    }
    if(!(resolved.pollIntervalSeconds > 0)){
        throw new Error(`pg-relay: pollIntervalSeconds must be more than 0, got ${resolved.pollIntervalSeconds}`)
    }
    return resolved
}

function errorMessage(error: unknown): string{
    const message = error instanceof Error ? error.message : String(error)
    return message.slice(0, MAX_ERROR_LENGTH)
}

/** Resolves after `ms`, or as soon as `signal` aborts. */
function sleep(ms: number, signal: AbortSignal): Promise<void>{
    return new Promise((resolve) => {
        if(signal.aborted) return resolve()
        const timer = setTimeout(done, ms)
        signal.addEventListener("abort", done, { once: true })
        function done(){
            clearTimeout(timer)
            signal.removeEventListener("abort", done)
            resolve()
        }
    })
}
