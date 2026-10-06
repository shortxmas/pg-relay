import { Pool } from "pg"

export type PgQueueOptions = {
    // defaults to process.env.DATABASE_URL, read when the queue first connects
    connectionString?: string
    // an existing pool to use instead of opening one; close() leaves it open
    pool?: Pool
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
    private readonly connectionString?: string
    private connection?: Pool
    private ownsPool = false
    private initialized?: Promise<void>

    constructor(tableName: string, options: PgQueueOptions = {}){
        if(!TABLE_NAME.test(tableName)){
            throw new Error(`pg-relay: tableName "${tableName}" must match ${TABLE_NAME}`)
        }
        this.tableName = tableName
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
            throw new PgRelayInitError(`pg-relay: could not connect to the database for "${this.tableName}"`, { cause: error })
        }

        try{
            await client.query("BEGIN")
            // Serializes init across processes: concurrent CREATE ... IF NOT EXISTS can still collide.
            await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [`pg-relay:${this.tableName}`])
            await client.query(createTableSql(this.tableName))

            // Checked before the indexes, which would otherwise fail first on a foreign table's missing columns.
            const { rows } = await client.query<{ column_name: string }>(
                `SELECT column_name FROM information_schema.columns
                 WHERE table_schema = current_schema() AND table_name = $1`,
                [this.tableName],
            )
            const present = new Set(rows.map((row) => row.column_name))
            const missing = COLUMNS.filter((column) => !present.has(column))
            if(missing.length > 0){
                throw new PgRelayInitError(
                    `pg-relay: table "${this.tableName}" already exists but is not a pg-relay table (missing ${missing.join(", ")})`,
                )
            }

            await client.query(createIndexesSql(this.tableName))
            await client.query("COMMIT")
        }catch(error){
            await client.query("ROLLBACK").catch(() => {})
            if(error instanceof PgRelayInitError) throw error
            throw new PgRelayInitError(`pg-relay: could not create table "${this.tableName}"`, { cause: error })
        }finally{
            client.release()
        }
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

function createIndexesSql(table: string): string{
    return `
        CREATE INDEX IF NOT EXISTS ${table}_pending_idx ON ${table} (name, run_after, created_at)
            WHERE status = 'pending';
        CREATE INDEX IF NOT EXISTS ${table}_running_idx ON ${table} (locked_until)
            WHERE status = 'running';
        CREATE UNIQUE INDEX IF NOT EXISTS ${table}_dedupe_idx ON ${table} (name, dedupe_key)
            WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'running');
    `
}
