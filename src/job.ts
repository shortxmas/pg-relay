export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue }

/** Custom JSON the worker needs to do the job. */
export type JobPayload = { [key: string]: JsonValue }

/** How a job is retried. Every field is optional; the publisher fills in defaults. */
export type JobConfig = {
    // amount of total retries this job can take after its first attempt
    maxRetries?: number
    // amount of seconds the first retry waits; doubles on each retry after that
    retryBackoffSeconds?: number
    // seconds a claimed job stays locked to its worker; if it isn't finished or renewed by then,
    // the worker is assumed dead and another may claim the job
    lockTtlSeconds?: number
    // seconds after writing before the job may first run
    delaySeconds?: number
}

/** Everything about a job besides its name, which is passed to writeJob and listen alongside it. */
export type JobConditions<TPayload extends JobPayload = JobPayload> = {
    payload?: TPayload
    config?: JobConfig
    // while a job with this name and key is pending or running, writing another returns that job
    dedupeKey?: string
}

export type JobStatus = "pending" | "running" | "succeeded" | "failed"

export type JobListFilter<TName extends string = string> = {
    name?: TName
    status?: JobStatus | JobStatus[]
    dedupeKey?: string
    // jobs whose payload contains these fields and values (Postgres jsonb @>)
    payload?: JobPayload
    // jobs whose payload matches a Postgres JSON path condition, e.g.
    // { path: "$.rangeStart >= $from", vars: { from: "2026-10-05T00:00:00Z" } }
    payloadPath?: { path: string; vars?: { [name: string]: JsonValue } }
    // jobs written strictly after / strictly before this time
    createdAfter?: Date
    createdBefore?: Date
    // most jobs returned, newest first; defaults to 100
    limit?: number
    // jobs to skip before the first one returned; defaults to 0
    offset?: number
}

/** Which finished jobs pruneJobs deletes. */
export type PruneJobsOptions<TName extends string = string> = {
    // delete jobs that finished more than this many seconds ago
    olderThanSeconds: number
    // defaults to both finished statuses
    status?: "succeeded" | "failed" | ("succeeded" | "failed")[]
    name?: TName
}

/** A row of the queue's table. delaySeconds isn't stored: it only ever set the first runAfter. */
export type JobRecord<TPayload extends JobPayload = JobPayload, TName extends string = string> = Omit<Required<JobConfig>, "delaySeconds"> & {
    id: string
    name: TName
    payload: TPayload
    status: JobStatus
    // runs started so far, counting the one in progress
    attempts: number
    runAfter: Date
    lockedUntil: Date | null
    lockedBy: string | null
    lastError: string | null
    dedupeKey: string | null
    createdAt: Date
    updatedAt: Date
    finishedAt: Date | null
}

/** Which job names queue.stats() counts. */
export type JobStatsFilter<TName extends string = string> = {
    name?: TName | TName[]
}

/** One job name's counts from queue.stats(), all measured with the database's now(). */
export type JobNameStats<TName extends string = string> = {
    name: TName
    // status = 'pending', due or not
    pending: number
    // pending with run_after <= now(): what a worker would claim right now
    due: number
    running: number
    succeeded: number
    failed: number
    // seconds since the run_after of the oldest due job; null when none are due
    oldestDueSeconds: number | null
    // running with locked_until < now(): their worker stopped renewing the lock
    expiredLocks: number
}
