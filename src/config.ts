export function loadConfig() {
  const pgUser = process.env.POSTGRES_USER ?? "openzerg"
  const pgPass = process.env.POSTGRES_PASSWORD ?? "openzerg"
  const pgDb = process.env.POSTGRES_DB ?? "openzerg"
  const pgHost = process.env.POSTGRES_HOST ?? "postgres"
  const pgPort = process.env.POSTGRES_PORT ?? "5432"
  const databaseURL = process.env.DATABASE_URL
    ?? `postgresql://${pgUser}:${pgPass}@${pgHost}:${pgPort}/${pgDb}`

  return {
    port: parseInt(process.env.PORT ?? "25000", 10),
    databaseURL,
    jwtSecret: process.env.JWT_SECRET ?? "dev-secret-change-me",
    idleTimeoutSec: parseInt(process.env.IDLE_TIMEOUT_SEC ?? "3600", 10),
  }
}

export type Config = ReturnType<typeof loadConfig>
