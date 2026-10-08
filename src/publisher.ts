import { Pool } from "pg"
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

/**
 * A connection with a transaction already open on it: a pg PoolClient or Client after BEGIN, or the
 * `tx` of Prisma's interactive `$transaction(async (tx) => …)`. Structural, so pg-relay doesn't depend
 * on Prisma.
 */
export type TransactionClient =
    | { query(sql: string, params?: unknown[]): Promise<{ rows: any[] }> }
    | { $queryRawUnsafe(sql: string, ...params: unknown[]): Promise<any[]> }

/** How writeJob writes. */
export type WriteOptions = {
    // write the job on this connection's open transaction, so it commits or rolls back with the
    // caller's other writes; the connection must be on the queue's database
    tx?: TransactionClient
}

type Exec = <T>(sql: string, params: unknown[]) => Promise<T[]>

// How often a dedupe write retries when the conflicting job finishes between its insert and select.
const DEDUPE_ATTEMPTS = 3

/** Writes jobs to a queue. Used by the app runtime. Takes the queue's job names, if it lists them. */
export class Publisher<TName extends string = string>{
    readonly queue: PgQueue<TName>

    constructor(queue: PgQueue<TName>){
        this.queue = queue
    }

    /**
     * Writes a pending job called `name`, creating the queue's table first if needed (on the queue's own
     * pool). With `tx`, the job is written in the caller's transaction.
     */
    async writeJob(name: TName, conditions: JobConditions = {}, options: WriteOptions = {}): Promise<WriteResult>{
        if(!name) throw new Error("pg-relay: a job needs a name")
        if(conditions.dedupeKey === "") throw new Error("pg-relay: dedupeKey can't be empty")
        const config = { ...DEFAULT_JOB_CONFIG, ...conditions.config }
        assertCount("maxRetries", config.maxRetries)
        assertCount("retryBackoffSeconds", config.retryBackoffSeconds)
        assertCount("lockTtlSeconds", config.lockTtlSeconds, 1)
        assertCount("delaySeconds", config.delaySeconds)
        const exec = options.tx ? toExec(options.tx) : this.exec

        await this.queue.init()
        const table = this.queue.qualifiedTableName
        const values = [
            name, JSON.stringify(conditions.payload ?? {}), config.maxRetries, config.retryBackoffSeconds,
            config.lockTtlSeconds, config.delaySeconds, conditions.dedupeKey ?? null,
        ]

        for(let attempt = 1; attempt <= DEDUPE_ATTEMPTS; attempt++){
            // Every parameter is cast, and the id returned as text, so the SQL means the same through pg and
            // Prisma, which type untyped parameters and uuids differently.
            const inserted = await exec<{ id: string }>(
                `INSERT INTO ${table}
                     (name, payload, max_retries, retry_backoff_seconds, lock_ttl_seconds, run_after, dedupe_key)
                 VALUES ($1::text, $2::jsonb, $3::int, $4::int, $5::int, now() + make_interval(secs => $6::double precision), $7::text)
                 ON CONFLICT (name, dedupe_key) WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'running')
                 DO NOTHING
                 RETURNING id::text`,
                values,
            )
            if(inserted.length > 0) return { id: inserted[0].id, deduped: false }

            const existing = await exec<{ id: string }>(
                `SELECT id::text FROM ${table}
                 WHERE name = $1::text AND dedupe_key = $2::text AND status IN ('pending', 'running')`,
                [name, conditions.dedupeKey],
            )
            if(existing.length > 0) return { id: existing[0].id, deduped: true }
            // The conflicting job finished in between; its key is free again, so insert once more.
        }
        throw new Error(`pg-relay: could not write "${name}" with dedupeKey "${conditions.dedupeKey}"`)
    }

    private exec: Exec = async (sql, params) => (await this.queue.pool.query(sql, params)).rows
}

function toExec(tx: TransactionClient): Exec{
    // A pool has query too, but each call may use a different connection, outside the transaction.
    if(tx instanceof Pool){
        throw new TypeError("pg-relay: tx must be a pg client or a Prisma transaction client, not a pg Pool")
    }
    if(typeof (tx as { $queryRawUnsafe?: unknown }).$queryRawUnsafe === "function"){
        const prisma = tx as Extract<TransactionClient, { $queryRawUnsafe: unknown }>
        return async (sql, params) => prisma.$queryRawUnsafe(sql, ...params)
    }
    if(typeof (tx as { query?: unknown }).query === "function"){
        const client = tx as Extract<TransactionClient, { query: unknown }>
        return async (sql, params) => (await client.query(sql, params)).rows
    }
    throw new TypeError("pg-relay: tx must be a pg client or a Prisma transaction client")
}

function assertCount(field: string, value: number, min = 0): void{
    if(!Number.isInteger(value) || value < min){
        throw new Error(`pg-relay: ${field} must be a whole number of ${min} or more, got ${value}`)
    }
}
