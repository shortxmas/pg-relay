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

/** A row of the queue's table. */
export type JobRecord<TPayload extends JobPayload = JobPayload> = Required<JobConfig> & {
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
