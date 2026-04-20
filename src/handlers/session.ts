import { ConnectError, Code } from "@connectrpc/connect"
import { randomUUID, now, dbQuery, unwrap } from "./common.js"
import type { DB } from "../db.js"
import type { Session } from "@openzerg/common/entities/session-schema.js"
import type { SessionTemplate } from "@openzerg/common/entities/sessiontemplate-schema.js"
import type {
  IWorkspaceManager,
  IToolServerManager,
} from "@openzerg/common"
import type {
  ListSessionsRequest, GetSessionRequest, CreateSessionRequest,
  UpdateSessionMetaRequest, UpdateSessionHotConfigRequest,
  UpdateSessionColdConfigRequest, SwitchSessionTemplateRequest,
  DeleteSessionRequest, StartSessionRequest, StopSessionRequest,
  ResolveSessionRequest,
} from "@openzerg/common/gen/registry/v1_pb.js"

function safeParseJson(raw: string): unknown {
  try { return JSON.parse(raw) } catch { return null }
}

function sessionToInfo(s: Session) {
  const toolEntries = (safeParseJson(s.toolServers || "[]") ?? []) as Array<{ type: string; config?: Record<string, string> }>
  const skillRefs = (safeParseJson(s.skills || "[]") ?? []) as Array<{ slug: string }>
  const extraPkgs = (safeParseJson(s.extraPkgs || "[]") ?? []) as string[]
  return {
    sessionId: s.id, title: s.title, state: s.state,
    templateId: s.templateId,
    createdAt: s.createdAt, updatedAt: s.updatedAt,
    systemPrompt: s.systemPrompt,
    providerConfig: {
      upstream: s.upstream, apiKey: s.apiKey, modelId: s.modelId,
      maxTokens: s.maxTokens, contextLength: s.contextLength,
      autoCompactLength: s.autoCompactLength,
    },
    toolServerConfig: toolEntries.map(e => ({ type: e.type, config: e.config ?? {} })),
    skillConfig: skillRefs.map(sr => ({ slug: sr.slug })),
    extraPackage: extraPkgs,
    workerId: s.workerId, agentId: s.agentId,
    sessionToken: s.sessionToken, workspaceId: s.workspaceId,
    inputTokens: s.inputTokens, outputTokens: s.outputTokens,
    lastActiveAt: s.lastActiveAt,
  }
}

interface SnapshotFields {
  systemPrompt: string
  upstream: string
  apiKey: string
  modelId: string
  maxTokens: number
  contextLength: number
  autoCompactLength: number
  toolServers: string
  skills: string
  extraPkgs: string
}

function templateToSnapshot(t: SessionTemplate): SnapshotFields {
  return {
    systemPrompt: t.systemPrompt,
    upstream: t.upstream, apiKey: t.apiKey, modelId: t.modelId,
    maxTokens: t.maxTokens, contextLength: t.contextLength,
    autoCompactLength: t.autoCompactLength,
    toolServers: t.toolServers, skills: t.skills, extraPkgs: t.extraPkgs,
  }
}

function applyOverrides(snapshot: SnapshotFields, req: CreateSessionRequest): SnapshotFields {
  return {
    systemPrompt: req.systemPrompt || snapshot.systemPrompt,
    upstream: req.providerConfig?.upstream || snapshot.upstream,
    apiKey: req.providerConfig?.apiKey || snapshot.apiKey,
    modelId: req.providerConfig?.modelId || snapshot.modelId,
    maxTokens: req.providerConfig?.maxTokens || snapshot.maxTokens,
    contextLength: req.providerConfig?.contextLength || snapshot.contextLength,
    autoCompactLength: req.providerConfig?.autoCompactLength || snapshot.autoCompactLength,
    toolServers: req.toolServerConfig?.length ? JSON.stringify(req.toolServerConfig.map(e => ({ type: e.type, config: e.config ?? {} }))) : snapshot.toolServers,
    skills: req.skillConfig?.length ? JSON.stringify(req.skillConfig.map(s => ({ slug: s.slug }))) : snapshot.skills,
    extraPkgs: req.extraPackage?.length ? JSON.stringify(req.extraPackage) : snapshot.extraPkgs,
  }
}

export function registerSessionHandlers(db: DB, wm: IWorkspaceManager, tsm: IToolServerManager) {
  return {
    listSessions(req: ListSessionsRequest) {
      return unwrap(dbQuery(async () => {
        let query = db.selectFrom("registry_sessions").selectAll()
        if (req.state) query = query.where("state", "=", req.state)
        const sessions: Session[] = await query.orderBy("createdAt", "desc").execute()
        return { sessions: sessions.map(sessionToInfo) }
      }))
    },

    getSession(req: GetSessionRequest) {
      return unwrap(dbQuery(async () => {
        const s = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        if (!s) throw new ConnectError("Session not found", Code.NotFound)
        return sessionToInfo(s)
      }))
    },

    createSession(req: CreateSessionRequest) {
      return unwrap(dbQuery(async () => {
        let snapshot: SnapshotFields

        if (req.templateId) {
          const template = await db.selectFrom("session_templates").selectAll()
            .where("id", "=", req.templateId).executeTakeFirst()
          if (!template) throw new ConnectError("Template not found", Code.NotFound)
          snapshot = applyOverrides(templateToSnapshot(template), req)
        } else {
          snapshot = {
            systemPrompt: req.systemPrompt ?? "",
            upstream: req.providerConfig?.upstream ?? "",
            apiKey: req.providerConfig?.apiKey ?? "",
            modelId: req.providerConfig?.modelId ?? "",
            maxTokens: req.providerConfig?.maxTokens ?? 0,
            contextLength: req.providerConfig?.contextLength ?? 0,
            autoCompactLength: req.providerConfig?.autoCompactLength ?? 0,
            toolServers: req.toolServerConfig?.length ? JSON.stringify(req.toolServerConfig.map(e => ({ type: e.type, config: e.config ?? {} }))) : "[]",
            skills: req.skillConfig?.length ? JSON.stringify(req.skillConfig.map(s => ({ slug: s.slug }))) : "[]",
            extraPkgs: req.extraPackage?.length ? JSON.stringify(req.extraPackage) : "[]",
          }
        }

        const sessionId = randomUUID()
        const token = `stk-${randomUUID()}`
        const ts = now()

        let workspaceId = req.workspaceId
        if (!workspaceId) {
          const wmResult = await wm.createWorkspace(sessionId)
          if (wmResult.isErr()) throw new ConnectError(`WM createWorkspace failed: ${wmResult.error.message}`, Code.Internal)
          workspaceId = wmResult.value.workspaceId
        }

        await db.insertInto("registry_sessions").values({
          id: sessionId, title: req.title ?? "", templateId: req.templateId ?? "",
          state: "stopped",
          systemPrompt: snapshot.systemPrompt,
          upstream: snapshot.upstream, apiKey: snapshot.apiKey, modelId: snapshot.modelId,
          maxTokens: snapshot.maxTokens, contextLength: snapshot.contextLength,
          autoCompactLength: snapshot.autoCompactLength,
          toolServers: snapshot.toolServers, skills: snapshot.skills, extraPkgs: snapshot.extraPkgs,
          workerId: "", agentId: "",
          sessionToken: token, workspaceId,
          inputTokens: 0n, outputTokens: 0n, lastActiveAt: 0n,
          createdAt: ts, updatedAt: ts,
        }).execute()

        const s = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", sessionId).executeTakeFirst()
        return {
          sessionId,
          sessionToken: token,
          session: sessionToInfo(s!),
        }
      }))
    },

    updateSessionMeta(req: UpdateSessionMetaRequest) {
      return unwrap(dbQuery(async () => {
        const ts = now()
        const updates: Record<string, unknown> = { updatedAt: ts }
        if (req.title) updates.title = req.title
        await db.updateTable("registry_sessions").set(updates)
          .where("id", "=", req.sessionId).execute()
        const s = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        if (!s) throw new ConnectError("Session not found", Code.NotFound)
        return sessionToInfo(s)
      }))
    },

    updateSessionHotConfig(req: UpdateSessionHotConfigRequest) {
      return unwrap(dbQuery(async () => {
        const ts = now()
        const updates: Record<string, unknown> = { updatedAt: ts }
        if (req.systemPrompt) updates.systemPrompt = req.systemPrompt
        if (req.providerConfig) {
          const pc = req.providerConfig
          if (pc.upstream) updates.upstream = pc.upstream
          if (pc.apiKey) updates.apiKey = pc.apiKey
          if (pc.modelId) updates.modelId = pc.modelId
          if (pc.maxTokens) updates.maxTokens = pc.maxTokens
          if (pc.contextLength) updates.contextLength = pc.contextLength
          if (pc.autoCompactLength) updates.autoCompactLength = pc.autoCompactLength
        }
        if (req.skillConfig?.length) {
          updates.skills = JSON.stringify(req.skillConfig.map(s => ({ slug: s.slug })))
        }
        await db.updateTable("registry_sessions").set(updates)
          .where("id", "=", req.sessionId).execute()
        const s = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        if (!s) throw new ConnectError("Session not found", Code.NotFound)
        return sessionToInfo(s)
      }))
    },

    updateSessionColdConfig(req: UpdateSessionColdConfigRequest) {
      return unwrap(dbQuery(async () => {
        const session = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        if (!session) throw new ConnectError("Session not found", Code.NotFound)
        if (session.state !== "stopped") {
          throw new ConnectError("Cold config changes require session to be stopped", Code.FailedPrecondition)
        }

        const ts = now()
        const updates: Record<string, unknown> = { updatedAt: ts }
        if (req.toolServerConfig?.length) {
          updates.toolServers = JSON.stringify(req.toolServerConfig.map(e => ({ type: e.type, config: e.config ?? {} })))
        }
        if (req.extraPackage?.length) {
          updates.extraPkgs = JSON.stringify(req.extraPackage)
        }
        await db.updateTable("registry_sessions").set(updates)
          .where("id", "=", req.sessionId).execute()
        const s = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        return sessionToInfo(s!)
      }))
    },

    switchSessionTemplate(req: SwitchSessionTemplateRequest) {
      return unwrap(dbQuery(async () => {
        const session = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        if (!session) throw new ConnectError("Session not found", Code.NotFound)
        if (session.state !== "stopped") {
          throw new ConnectError("Cannot switch template on a running session", Code.FailedPrecondition)
        }

        const template = await db.selectFrom("session_templates").selectAll()
          .where("id", "=", req.templateId).executeTakeFirst()
        if (!template) throw new ConnectError("Template not found", Code.NotFound)

        const snapshot = templateToSnapshot(template)
        const ts = now()
        await db.updateTable("registry_sessions").set({
          templateId: req.templateId,
          systemPrompt: snapshot.systemPrompt,
          upstream: snapshot.upstream, apiKey: snapshot.apiKey, modelId: snapshot.modelId,
          maxTokens: snapshot.maxTokens, contextLength: snapshot.contextLength,
          autoCompactLength: snapshot.autoCompactLength,
          toolServers: snapshot.toolServers, skills: snapshot.skills, extraPkgs: snapshot.extraPkgs,
          updatedAt: ts,
        }).where("id", "=", req.sessionId).execute()

        const s = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        return sessionToInfo(s!)
      }))
    },

    deleteSession(req: DeleteSessionRequest) {
      return unwrap(dbQuery(async () => {
        const session = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        if (!session) throw new ConnectError("Session not found", Code.NotFound)
        if (session.state !== "stopped") {
          throw new ConnectError("Cannot delete a running session", Code.FailedPrecondition)
        }

        await db.deleteFrom("registry_messages").where("sessionId", "=", req.sessionId).execute()
        await db.deleteFrom("registry_sessions").where("id", "=", req.sessionId).execute()

        if (session.workspaceId) {
          const remaining = await db.selectFrom("registry_sessions")
            .select(["id"])
            .where("workspaceId", "=", session.workspaceId)
            .execute()
          if (remaining.length === 0) {
            const delResult = await wm.deleteWorkspace(session.workspaceId)
            if (delResult.isErr()) {
              console.error(`WM deleteWorkspace failed: ${delResult.error.message}`)
            }
          }
        }

        return {}
      }))
    },

    startSession(req: StartSessionRequest) {
      return unwrap(dbQuery(async () => {
        const session = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        if (!session) throw new ConnectError("Session not found", Code.NotFound)
        if (session.state === "running" || session.state === "creating") {
          throw new ConnectError(`Session already in state: ${session.state}`, Code.FailedPrecondition)
        }

        const ts = now()
        await db.updateTable("registry_sessions").set({
          state: "creating", updatedAt: ts,
        }).where("id", "=", req.sessionId).execute()

        if (!session.workspaceId) {
          await db.updateTable("registry_sessions").set({
            state: "stopped", updatedAt: now(),
          }).where("id", "=", req.sessionId).execute()
          throw new ConnectError("Session has no workspace", Code.Internal)
        }

        const workerImage = process.env.WORKER_IMAGE ?? "localhost/openzerg/worker:latest"

        const startResult = await wm.ensureWorkspaceWorker({
          workspaceId: session.workspaceId,
          image: workerImage,
          env: {
            REGISTRY_URL: process.env.REGISTRY_INTERNAL_URL ?? "http://registry:25000",
            SESSION_TOKEN: session.sessionToken,
            NIX_PKGS: JSON.stringify((safeParseJson(session.extraPkgs || "[]") ?? []) as string[]),
          },
        })

        if (startResult.isErr()) {
          await db.updateTable("registry_sessions").set({
            state: "stopped", updatedAt: now(),
          }).where("id", "=", req.sessionId).execute()
          throw new ConnectError(`WM ensureWorkspaceWorker failed: ${startResult.error.message}`, Code.Internal)
        }

        const workerResp = startResult.value
        const ts2 = now()
        await db.updateTable("registry_sessions").set({
          state: "idle", workerId: workerResp.workerId, lastActiveAt: ts2, updatedAt: ts2,
        }).where("id", "=", req.sessionId).execute()

        return {}
      }))
    },

    stopSession(req: StopSessionRequest) {
      return unwrap(dbQuery(async () => {
        const session = await db.selectFrom("registry_sessions").selectAll()
          .where("id", "=", req.sessionId).executeTakeFirst()
        if (!session) throw new ConnectError("Session not found", Code.NotFound)
        if (session.state === "stopped") return {}

        const ts = now()
        await db.updateTable("registry_sessions").set({
          state: "stopped", workerId: "", updatedAt: ts,
        }).where("id", "=", req.sessionId).execute()

        return {}
      }))
    },

    resolveSession(req: ResolveSessionRequest) {
      return unwrap(dbQuery(async () => {
        const session = await db.selectFrom("registry_sessions").selectAll()
          .where("sessionToken", "=", req.sessionToken).executeTakeFirst()
        if (!session) throw new ConnectError("Session not found", Code.NotFound)

        let workerUrl = ""
        let workerSecret = ""
        let workspaceRoot = ""
        if (session.workerId) {
          const worker = await db.selectFrom("wm_workers").selectAll()
            .where("id", "=", session.workerId).executeTakeFirst()
          if (worker) {
            workerUrl = worker.filesystemUrl || `http://${worker.containerName}`
            workerSecret = worker.secret
            workspaceRoot = worker.workspaceRoot
          }
        }

        const serverEntries = (safeParseJson(session.toolServers || "[]") ?? []) as Array<{ type: string; config?: Record<string, string> }>
        const serverTypes = serverEntries.map(e => e.type).filter(Boolean)
        const configMap = new Map(serverEntries.map(e => [e.type, e.config ?? {}]))

        let toolServerUrls: Array<{ name: string; url: string; config: string }> = []
        if (serverTypes.length > 0) {
          const toolsResult = await tsm.resolveTools(session.id, serverTypes)
          if (toolsResult.isOk()) {
            toolServerUrls = toolsResult.value.toolServerUrls.map(u => ({
              name: u.name, url: u.url, config: typeof u.config === "string" ? u.config : JSON.stringify(u.config),
            }))
          }
        }

        return {
          sessionId: session.id,
          templateId: session.templateId,
          systemPrompt: session.systemPrompt,
          providerConfig: {
            upstream: session.upstream, apiKey: session.apiKey, modelId: session.modelId,
            maxTokens: session.maxTokens, contextLength: session.contextLength,
            autoCompactLength: session.autoCompactLength,
          },
          toolServerConfig: serverEntries.map(e => ({ type: e.type, config: e.config ?? {} })),
          skillConfig: (safeParseJson(session.skills || "[]") ?? []) as Array<{ slug: string }>,
          extraPackage: (safeParseJson(session.extraPkgs || "[]") ?? []) as string[],
          workerId: session.workerId,
          workerUrl, workerSecret, workspaceRoot,
          agentUrl: session.agentId,
          toolServerUrls,
          workspaceId: session.workspaceId,
        }
      }))
    },
  }
}
