import { SQL } from "bun"

const url = process.env.POLYQUEUE_DB_URL
if (!url) throw new Error("POLYQUEUE_DB_URL environment variable is required")

export const db = new SQL(url)
