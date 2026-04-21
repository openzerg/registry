export function loadConfig() {
  const gelDSN = process.env.GEL_DSN ?? "gel://admin@uz-gel/main?tls_security=insecure"

  return {
    port: parseInt(process.env.PORT ?? "25000", 10),
    gelDSN,
    jwtSecret: process.env.JWT_SECRET ?? "dev-secret-change-me",
    idleTimeoutSec: parseInt(process.env.IDLE_TIMEOUT_SEC ?? "3600", 10),
  }
}

export type Config = ReturnType<typeof loadConfig>
