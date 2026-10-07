import { Pool } from "pg"
import type { JobListFilter, JobPayload, JobRecord, JobStatus, PruneJobsOptions } from "./job"

export type PgQueueOptions = {
    // defaults to process.env.DATABASE_URL, read when the queue first connects
    connectionString?: string
    // an existing pool to use instead of opening one; close() leaves it open
    pool?: Pool
    // Postgres schema for the table, created by init() if missing. Defaults to the connection's
    // current schema (usually public). A schema of its own keeps the table away from ORMs like Prisma.
    schema?: string
}

/** Thrown when the queue can't connect, or its table can't be created or isn't a pg-relay table. */
export class PgRelayInitError extends Error{
    constructor(message: string, options?: { cause?: unknown }){
        super(message, options)
        this.name = "PgRelayInitError"
    }
}

// Unquoted Postgres identifier, short enough that "<table>_pending_idx" stays under the 63-byte limit.
const TABLE_NAME = /^[a-z_][a-z0-9_]{0,49}$/
// Unquoted Postgres identifier of at most 63 bytes.
const SCHEMA_NAME = /^[a-z_][a-z0-9_]{0,62}$/

// Ids are checked before querying: anything that isn't a uuid would make Postgres reject the query
// instead of finding no job.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

const DEFAULT_LIST_LIMIT = 100
const FINISHED: JobStatus[] = ["succeeded", "failed"]

// Every column init creates; an existing table missing any of them belongs to something else.
const COLUMNS = [
    "id", "name", "payload", "status", "attempts", "max_retries", "retry_backoff_seconds", "lock_ttl_seconds", "run_after",
    "locked_until", "locked_by", "last_error", "dedupe_key", "created_at", "updated_at", "finished_at",
]

/**
 * The table jobs are written to and claimed from, and the connection to it. Constructing one does no
 * I/O and needs no DATABASE_URL, so a module that creates it can be imported anywhere.
 */
export class PgQueue{
    readonly tableName: string
    readonly schema?: string
    // the table as SQL refers to it: "schema.table", or "table" without a schema
    readonly qualifiedTableName: string
    private readonly connectionString?: string
    private connection?: Pool
    private ownsPool = false
    private initialized?: Promise<void>

    constructor(tableName: string, options: PgQueueOptions = {}){
        if(!TABLE_NAME.test(tableName)){
            throw new Error(`pg-relay: tableName "${tableName}" must match ${TABLE_NAME}`)
        }
        if(options.schema !== undefined && !SCHEMA_NAME.test(options.schema)){
            throw new Error(`pg-relay: schema "${options.schema}" must match ${SCHEMA_NAME}`)
        }
        this.tableName = tableName
        this.schema = options.schema
        this.qualifiedTableName = options.schema ? `${options.schema}.${tableName}` : tableName
        this.connectionString = options.connectionString
        this.connection = options.pool
    }

    /** The queue's pool, opened on first use. Throws PgRelayInitError when there's nothing to connect to. */
    get pool(): Pool{
        if(!this.connection){
            const connectionString = this.connectionString ?? process.env.DATABASE_URL
            if(!connectionString){
                throw new PgRelayInitError("pg-relay: pass connectionString or pool, or set DATABASE_URL")
            }
            this.connection = new Pool({ connectionString })
            this.ownsPool = true
        }
        return this.connection
    }

    /**
     * Creates the table and indexes if they don't exist, then checks the table has pg-relay's columns.
     * Runs once per instance; later calls return the same promise, so a failure stays failed.
     */
    init(): Promise<void>{
        this.initialized ??= this.checkAndInitDatabase()
        return this.initialized
    }

    /** The job with this id, or null when there isn't one. Creates the table first if needed. */
    async getJob<TPayload extends JobPayload = JobPayload>(id: string): Promise<JobRecord<TPayload> | null>{
        await this.init()
        if(!UUID.test(id)) return null
        const { rows } = await this.pool.query<JobRow>(`SELECT * FROM ${this.qualifiedTableName} WHERE id = $1`, [id])
        return rows[0] ? toJobRecord<TPayload>(rows[0]) : null
    }

    /** Jobs matching every field of `filter`, newest first. Creates the table first if needed. */
    async listJobs<TPayload extends JobPayload = JobPayload>(filter: JobListFilter = {}): Promise<JobRecord<TPayload>[]>{
        const limit = filter.limit ?? DEFAULT_LIST_LIMIT
        const offset = filter.offset ?? 0
        assertCount("limit", limit, 1)
        assertCount("offset", offset)
        await this.init()

        const conditions: string[] = []
        const values: unknown[] = []
        const where = (sql: string, value: unknown) => {
            values.push(value)
            conditions.push(sql.replace("?", `$${values.length}`))
        }
        if(filter.name !== undefined) where("name = ?", filter.name)
        if(filter.status !== undefined) where("status = ANY(?)", ([] as JobStatus[]).concat(filter.status))
        if(filter.dedupeKey !== undefined) where("dedupe_key = ?", filter.dedupeKey)
        if(filter.payload !== undefined) where("payload @> ?::jsonb", JSON.stringify(filter.payload))
        if(filter.payloadPath !== undefined){
            // Silent: a payload without the path's fields doesn't match, rather than failing the query.
            where("jsonb_path_match(payload, ?::jsonpath, ", filter.payloadPath.path)
            values.push(JSON.stringify(filter.payloadPath.vars ?? {}))
            conditions[conditions.length - 1] += `$${values.length}::jsonb, true)`
        }
        // Compared in milliseconds, the precision of a JS Date: a job's own createdAt, read back and passed
        // in, would otherwise fall before the microsecond value stored for it.
        if(filter.createdAfter !== undefined) where("date_trunc('milliseconds', created_at) > ?", filter.createdAfter)
        if(filter.createdBefore !== undefined) where("date_trunc('milliseconds', created_at) < ?", filter.createdBefore)
        values.push(limit, offset)

        const { rows } = await this.pool.query<JobRow>(
            `SELECT * FROM ${this.qualifiedTableName}
             ${conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : ""}
             ORDER BY created_at DESC, id
             LIMIT $${values.length - 1} OFFSET $${values.length}`,
            values,
        )
        return rows.map((row) => toJobRecord<TPayload>(row))
    }

    /**
     * Deletes succeeded and failed jobs that finished more than `olderThanSeconds` ago, optionally
     * only those with `status` or `name`. Pending and running jobs are never deleted. Returns how many.
     */
    async pruneJobs(options: PruneJobsOptions): Promise<number>{
        assertCount("olderThanSeconds", options.olderThanSeconds)
        const statuses = ([] as JobStatus[]).concat(options.status ?? FINISHED)
        if(statuses.length === 0 || statuses.some((status) => !FINISHED.includes(status))){
            throw new Error(`pg-relay: pruneJobs only deletes succeeded or failed jobs, got status ${JSON.stringify(options.status)}`)
        }
        await this.init()

        const { rowCount } = await this.pool.query(
            `DELETE FROM ${this.qualifiedTableName}
             WHERE status = ANY($1)
               AND finished_at < now() - make_interval(secs => $2)
               AND ($3::text IS NULL OR name = $3)`,
            [statuses, options.olderThanSeconds, options.name ?? null],
        )
        return rowCount ?? 0
    }

    /**
     * Sends a failed job back to pending, due now, with its attempts reset so it gets its full retries
     * again. lastError is kept for reference. Returns the updated job, or null when there's no failed job
     * with this id. Rejects if another unfinished job already holds the job's dedupeKey.
     */
    async retryJob<TPayload extends JobPayload = JobPayload>(id: string): Promise<JobRecord<TPayload> | null>{
        await this.init()
        if(!UUID.test(id)) return null
        try{
            const { rows } = await this.pool.query<JobRow>(
                `UPDATE ${this.qualifiedTableName}
                 SET status = 'pending', attempts = 0, run_after = now(), finished_at = NULL,
                     locked_by = NULL, locked_until = NULL, updated_at = now()
                 WHERE id = $1 AND status = 'failed'
                 RETURNING *`,
                [id],
            )
            return rows[0] ? toJobRecord<TPayload>(rows[0]) : null
        }catch(error){
            if((error as { code?: string }).code === "23505"){
                throw new Error(`pg-relay: can't retry job ${id}: an unfinished job with the same name and dedupeKey exists`, { cause: error })
            }
            throw error
        }
    }

    /** Ends the pool if this queue opened it. */
    async close(): Promise<void>{
        if(this.ownsPool && this.connection) await this.connection.end()
    }

    private checkAndInitDatabase = async (): Promise<void> => {
        let client
        try{
            client = await this.pool.connect()
        }catch(error){
            if(error instanceof PgRelayInitError) throw error
            throw new PgRelayInitError(`pg-relay: could not connect to the database for "${this.qualifiedTableName}"`, { cause: error })
        }

        try{
            await client.query("BEGIN")
            // Serializes init across processes: concurrent CREATE ... IF NOT EXISTS can still collide.
            await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [`pg-relay:${this.qualifiedTableName}`])
            if(this.schema) await client.query(`CREATE SCHEMA IF NOT EXISTS ${this.schema}`)
            await client.query(createTableSql(this.qualifiedTableName))

            // Checked before the indexes, which would otherwise fail first on a foreign table's missing columns.
            const { rows } = await client.query<{ column_name: string }>(
                `SELECT column_name FROM information_schema.columns
                 WHERE table_schema = COALESCE($2, current_schema()) AND table_name = $1`,
                [this.tableName, this.schema ?? null],
            )
            const present = new Set(rows.map((row) => row.column_name))
            const missing = COLUMNS.filter((column) => !present.has(column))
            if(missing.length > 0){
                throw new PgRelayInitError(
                    `pg-relay: table "${this.qualifiedTableName}" already exists but is not a pg-relay table (missing ${missing.join(", ")})`,
                )
            }

            await client.query(createIndexesSql(this.tableName, this.qualifiedTableName))
            await client.query("COMMIT")
        }catch(error){
            await client.query("ROLLBACK").catch(() => {})
            if(error instanceof PgRelayInitError) throw error
            throw new PgRelayInitError(`pg-relay: could not create table "${this.qualifiedTableName}"`, { cause: error })
        }finally{
            client.release()
        }
    }
}

type JobRow = {
    id: string
    name: string
    payload: JobPayload
    status: JobStatus
    attempts: number
    max_retries: number
    retry_backoff_seconds: number
    lock_ttl_seconds: number
    run_after: Date
    locked_until: Date | null
    locked_by: string | null
    last_error: string | null
    dedupe_key: string | null
    created_at: Date
    updated_at: Date
    finished_at: Date | null
}

function toJobRecord<TPayload extends JobPayload>(row: JobRow): JobRecord<TPayload>{
    return {
        id: row.id,
        name: row.name,
        payload: row.payload as TPayload,
        status: row.status,
        attempts: row.attempts,
        maxRetries: row.max_retries,
        retryBackoffSeconds: row.retry_backoff_seconds,
        lockTtlSeconds: row.lock_ttl_seconds,
        runAfter: row.run_after,
        lockedUntil: row.locked_until,
        lockedBy: row.locked_by,
        lastError: row.last_error,
        dedupeKey: row.dedupe_key,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        finishedAt: row.finished_at,
    }
}

function createTableSql(table: string): string{
    return `
        CREATE TABLE IF NOT EXISTS ${table} (
            id                     uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
            name                   text        NOT NULL,
            payload                jsonb       NOT NULL,
            status                 text        NOT NULL DEFAULT 'pending'
                                               CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
            attempts               integer     NOT NULL DEFAULT 0,
            max_retries            integer     NOT NULL,
            retry_backoff_seconds  integer     NOT NULL,
            lock_ttl_seconds       integer     NOT NULL,
            run_after              timestamptz NOT NULL DEFAULT now(),
            locked_until           timestamptz,
            locked_by              text,
            last_error             text,
            dedupe_key             text,
            created_at             timestamptz NOT NULL DEFAULT now(),
            updated_at             timestamptz NOT NULL DEFAULT now(),
            finished_at            timestamptz
        )
    `
}

// Index names can't be schema-qualified: Postgres puts each index in its table's schema.
function createIndexesSql(table: string, qualifiedTable: string): string{
    return `
        CREATE INDEX IF NOT EXISTS ${table}_pending_idx ON ${qualifiedTable} (name, run_after, created_at)
            WHERE status = 'pending';
        CREATE INDEX IF NOT EXISTS ${table}_running_idx ON ${qualifiedTable} (locked_until)
            WHERE status = 'running';
        CREATE UNIQUE INDEX IF NOT EXISTS ${table}_dedupe_idx ON ${qualifiedTable} (name, dedupe_key)
            WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'running');
    `
}

function assertCount(field: string, value: number, min = 0): void{
    if(!Number.isInteger(value) || value < min){
        throw new Error(`pg-relay: ${field} must be a whole number of ${min} or more, got ${value}`)
    }
}
