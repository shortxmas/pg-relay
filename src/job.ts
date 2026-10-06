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

export type JobListFilter = {
    name?: string
    status?: JobStatus | JobStatus[]
    dedupeKey?: string
    // most jobs returned, newest first; defaults to 100
    limit?: number
}

/** A row of the queue's table. delaySeconds isn't stored: it only ever set the first runAfter. */
export type JobRecord<TPayload extends JobPayload = JobPayload> = Omit<Required<JobConfig>, "delaySeconds"> & {
    id: string
    name: string
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
