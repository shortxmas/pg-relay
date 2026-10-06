// Shared by the integration tests: they need a real Postgres and each works in its own table.
import { randomUUID } from "node:crypto"
import { Pool } from "pg"
import { PgQueue } from "../queue"

if(!process.env.TEST_DB_URL){
    // jest.config.js loads .env before the tests start.
    throw new Error("TEST_DB_URL is not set: add it to .env (see .env.example) or the environment")
}
export const DATABASE_URL = process.env.TEST_DB_URL

/**
 * Per-test-file database helpers. Call in a test file's top level: it registers afterEach/afterAll
 * hooks that close the queues it made and drop the tables it named.
 */
export function useTestDb(){
    const admin = new Pool({ connectionString: DATABASE_URL })
    const tables: string[] = []
    const queues: PgQueue[] = []
    const cleanups: (() => Promise<unknown>)[] = []

    // Cleanups first: they may still use a queue's pool (a subscriber finishing its jobs).
    afterEach(async () => {
        await Promise.all(cleanups.splice(0).map((cleanup) => cleanup()))
        await Promise.all(queues.splice(0).map((q) => q.close()))
    })

    afterAll(async () => {
        for(const table of tables) await admin.query(`DROP TABLE IF EXISTS ${table}`)
        await admin.end()
    })

    return {
        admin,

        /** A unique table name, dropped after the file's tests. */
        tableName(): string{
            const name = `t_${randomUUID().replaceAll("-", "").slice(0, 16)}`
            tables.push(name)
            return name
        },

        /** Registers a queue to close after the current test. */
        track(queue: PgQueue): PgQueue{
            queues.push(queue)
            return queue
        },

        /** Runs `cleanup` after the current test, before its queues close. */
        defer(cleanup: () => Promise<unknown>): void{
            cleanups.push(cleanup)
        },
    }
}

