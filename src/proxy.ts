import type { IncomingMessage, ServerResponse } from "node:http"
import type { DB } from "./db.js"

const PROXY_ROUTES: Record<string, string> = {
  "/api/agent": "agent",
  "/api/skills": "skill-manager",
  "/api/ai-proxy": "ai-proxy",
}

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, Connect-Protocol-Version, Connect-Accept-Encoding",
  "Access-Control-Max-Age": "86400",
}

export function createProxyHandler(db: DB) {
  return async (req: IncomingMessage, res: ServerResponse): Promise<boolean> => {
    const url = req.url ?? "/"
    const setCors = (r: ServerResponse) => {
      for (const [k, v] of Object.entries(CORS_HEADERS)) r.setHeader(k, v)
    }

    for (const [prefix, instanceType] of Object.entries(PROXY_ROUTES)) {
      if (!url.startsWith(prefix)) continue

      if (req.method === "OPTIONS") {
        setCors(res)
        res.writeHead(204)
        res.end()
        return true
      }

      const rows = await db.selectFrom("registry_instances")
        .select(["publicUrl"])
        .where("instanceType", "=", instanceType)
        .where("lifecycle", "=", "active")
        .orderBy("lastSeen", "desc")
        .limit(1)
        .execute()

      if (rows.length === 0) {
        setCors(res)
        res.writeHead(502, { "Content-Type": "application/json" })
        res.end(JSON.stringify({ error: `No active ${instanceType} instance found` }))
        return true
      }

      const targetBase = rows[0].publicUrl.replace(/\/$/, "")
      const targetPath = url.slice(prefix.length) || "/"
      const targetUrl = `${targetBase}${targetPath}`

      try {
        const headers: Record<string, string> = {}
        for (const [k, v] of Object.entries(req.headers)) {
          if (v && !["host", "connection"].includes(k)) {
            headers[k] = Array.isArray(v) ? v[0] : v
          }
        }

        const resp = await fetch(targetUrl, {
          method: req.method,
          headers,
          body: req.method !== "GET" && req.method !== "HEAD" ? req : undefined,
          // @ts-ignore duplex for streaming request body
          duplex: req.method !== "GET" && req.method !== "HEAD" ? "half" : undefined,
        })

        setCors(res)
        for (const [k, v] of resp.headers.entries()) {
          if (!["transfer-encoding", "connection", "content-encoding", "content-length"].includes(k)) {
            res.setHeader(k, v)
          }
        }
        res.writeHead(resp.status)
        if (resp.body) {
          const reader = resp.body.getReader()
          try {
            while (true) {
              const { done, value } = await reader.read()
              if (done) break
              res.write(value)
            }
          } finally {
            reader.releaseLock()
          }
        }
        res.end()
      } catch (err) {
        setCors(res)
        res.writeHead(502, { "Content-Type": "application/json" })
        res.end(JSON.stringify({ error: `Proxy error: ${err}` }))
      }
      return true
    }

    return false
  }
}
