import type { JobConditions, JobConfig } from "./job"
import { PgQueue } from "./queue"

/**
 * Config a job gets for every field it leaves out: due immediately, 5 attempts, retries after
 * 30s, 60s, 120s, 240s, and a 5-minute lock.
 */
export const DEFAULT_JOB_CONFIG: Required<JobConfig> = {
    maxRetries: 4,
    retryBackoffSeconds: 30,
    lockTtlSeconds: 300,
    delaySeconds: 0,
}

export type WriteResult = {
    id: string
    // true when an unfinished job with the same name and dedupeKey already existed and no row was written
    deduped: boolean
}

// How often a dedupe write retries when the conflicting job finishes between its insert and select.
const DEDUPE_ATTEMPTS = 3

/** Writes jobs to a queue. Used by the app runtime. Takes the queue's job names, if it lists them. */
export class Publisher<TName extends string = string>{
    readonly queue: PgQueue<TName>

    constructor(queue: PgQueue<TName>){
        this.queue = queue
    }

    /** Writes a pending job called `name`, creating the queue's table first if needed. */
    async writeJob(name: TName, conditions: JobConditions = {}): Promise<WriteResult>{
        if(!name) throw new Error("pg-relay: a job needs a name")
        if(conditions.dedupeKey === "") throw new Error("pg-relay: dedupeKey can't be empty")
        const config = { ...DEFAULT_JOB_CONFIG, ...conditions.config }
        assertCount("maxRetries", config.maxRetries)
        assertCount("retryBackoffSeconds", config.retryBackoffSeconds)
        assertCount("lockTtlSeconds", config.lockTtlSeconds, 1)
        assertCount("delaySeconds", config.delaySeconds)

        await this.queue.init()
        const table = this.queue.qualifiedTableName
        const values = [
            name, JSON.stringify(conditions.payload ?? {}), config.maxRetries, config.retryBackoffSeconds,
            config.lockTtlSeconds, config.delaySeconds, conditions.dedupeKey ?? null,
        ]

        for(let attempt = 1; attempt <= DEDUPE_ATTEMPTS; attempt++){
            const inserted = await this.queue.pool.query<{ id: string }>(
                `INSERT INTO ${table}
                     (name, payload, max_retries, retry_backoff_seconds, lock_ttl_seconds, run_after, dedupe_key)
                 VALUES ($1, $2::jsonb, $3, $4, $5, now() + make_interval(secs => $6), $7)
                 ON CONFLICT (name, dedupe_key) WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'running')
                 DO NOTHING
                 RETURNING id`,
                values,
            )
            if(inserted.rows.length > 0) return { id: inserted.rows[0].id, deduped: false }

            const existing = await this.queue.pool.query<{ id: string }>(
                `SELECT id FROM ${table}
                 WHERE name = $1 AND dedupe_key = $2 AND status IN ('pending', 'running')`,
                [name, conditions.dedupeKey],
            )
            if(existing.rows.length > 0) return { id: existing.rows[0].id, deduped: true }
            // The conflicting job finished in between; its key is free again, so insert once more.
        }
        throw new Error(`pg-relay: could not write "${name}" with dedupeKey "${conditions.dedupeKey}"`)
    }
}

function assertCount(field: string, value: number, min = 0): void{
    if(!Number.isInteger(value) || value < min){
        throw new Error(`pg-relay: ${field} must be a whole number of ${min} or more, got ${value}`)
    }
}
