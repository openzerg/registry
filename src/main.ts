import { createServer, type IncomingMessage, type ServerResponse } from "node:http"
import { connectNodeAdapter } from "@connectrpc/connect-node"
import { WorkspaceManagerClient, ToolServerManagerClient } from "@openzerg/common"
import { createGelClient, gelQuery } from "@openzerg/common/gel"
import { listIdleSessions, setSessionStopped } from "@openzerg/common/queries"
import { loadConfig } from "./config.js"
import { createRegistryRouter } from "./router.js"
import { createProxyHandler } from "./proxy.js"

const cfg = loadConfig()

async function main() {
  const gel = createGelClient(cfg.gelDSN)

  const wm = new WorkspaceManagerClient({
    baseURL: process.env.WM_URL || "http://localhost:25020",
  })
  const tsm = new ToolServerManagerClient({
    baseURL: process.env.TSM_URL || "http://localhost:25021",
  })

  const rpcHandler = connectNodeAdapter({
    routes: createRegistryRouter(gel, wm, tsm),
  })

  const proxyHandler = createProxyHandler(gel)

  const corsHeaders = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, Connect-Protocol-Version, X-Registry-Token",
  }

  const handler = async (req: IncomingMessage, res: ServerResponse) => {
    res.setHeader("Access-Control-Allow-Origin", corsHeaders["Access-Control-Allow-Origin"])
    res.setHeader("Access-Control-Allow-Methods", corsHeaders["Access-Control-Allow-Methods"])
    res.setHeader("Access-Control-Allow-Headers", corsHeaders["Access-Control-Allow-Headers"])
    if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return }
    if (await proxyHandler(req, res)) return
    rpcHandler(req, res)
  }

  createServer(handler).listen(cfg.port, () => {
    console.log(`registry listening on :${cfg.port}`)
  })

  setInterval(async () => {
    try {
      const threshold = BigInt(Math.floor(Date.now() / 1000)) - BigInt(cfg.idleTimeoutSec)

      const idleSessions = await gelQuery(() => listIdleSessions(gel, { threshold: Number(threshold) }))
        .match(
          (sessions) => sessions,
          (e) => {
            console.error("[registry] idle scanner listIdleSessions error:", e.message)
            return []
          },
        )

      for (const session of idleSessions) {
        console.log(`[registry] auto-stopping idle session ${session.id}`)
        const stopResult = await wm.stopWorker(session.workerId)
        if (stopResult.isErr()) {
          console.error(`[registry] WM stopWorker failed: ${stopResult.error.message}`)
        }
        const ts = BigInt(Math.floor(Date.now() / 1000))
        gelQuery(() => setSessionStopped(gel, { id: session.id, updatedAt: Number(ts) }))
          .match(
            () => {},
            (e) => console.error(`[registry] setSessionStopped failed: ${e.message}`),
          )
      }
    } catch (err) {
      console.error("[registry] idle scanner error:", err)
    }
  }, 60_000)
}

main().catch(e => {
  console.error(e)
  process.exit(1)
})
