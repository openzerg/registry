import { randomUUID } from "node:crypto"
import { gelQuery, unwrap } from "@openzerg/common/gel"
import { ok, okAsync, err, ResultAsync } from "neverthrow"
import { NotFoundError, ValidationError, type AppError } from "@openzerg/common"
import {
  listSessions, getSessionById, getTemplateForSession, insertSession,
  updateSessionMeta, updateSessionHotConfig, updateSessionColdConfig,
  switchSessionTemplate, deleteSessionMessages, deleteSessionById,
  listSessionsByWorkspace, setSessionCreating, setSessionStopped, setSessionIdle,
  resolveSessionByToken, getWorkerForSession,
} from "@openzerg/common/queries"
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
import type { GelClient } from "@openzerg/common/gel"

function safeParseJson(raw: string): unknown {
  try { return JSON.parse(raw) } catch { return null }
}

type SessionRow = NonNullable<Awaited<ReturnType<typeof getSessionById>>>

function sessionToInfo(s: SessionRow) {
  const toolEntries = (safeParseJson(s.toolServers || "[]") ?? []) as Array<{ type: string; config?: Record<string, string> }>
  const skillRefs = (safeParseJson(s.skills || "[]") ?? []) as Array<{ slug: string }>
  const extraPkgs = (safeParseJson(s.extraPkgs || "[]") ?? []) as string[]
  return {
    sessionId: s.id, title: s.title, state: s.state,
    templateId: s.templateId,
    createdAt: BigInt(s.createdAt), updatedAt: BigInt(s.updatedAt),
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
    inputTokens: BigInt(s.inputTokens), outputTokens: BigInt(s.outputTokens),
    lastActiveAt: BigInt(s.lastActiveAt),
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

function templateToSnapshot(t: NonNullable<Awaited<ReturnType<typeof getTemplateForSession>>>): SnapshotFields {
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

export function registerSessionHandlers(gel: GelClient, wm: IWorkspaceManager, tsm: IToolServerManager) {
  return {
    listSessions(req: ListSessionsRequest) {
      return unwrap(
        gelQuery(() => listSessions(gel, { state: req.state || null }))
          .andThen((sessions) => ok({ sessions: sessions.map(sessionToInfo) })),
      )
    },

    getSession(req: GetSessionRequest) {
      return unwrap(
        gelQuery(() => getSessionById(gel, { id: req.sessionId }))
          .andThen((s) => {
            if (!s) return err(new NotFoundError("Session not found"))
            return ok(sessionToInfo(s))
          }),
      )
    },

    createSession(req: CreateSessionRequest) {
      let snapshot: SnapshotFields

      const buildSnapshot = (): ResultAsync<SnapshotFields, AppError> => {
        if (req.templateId) {
          return gelQuery(() => getTemplateForSession(gel, { templateId: req.templateId }))
            .andThen((template) => {
              if (!template) return err(new NotFoundError("Template not found"))
              return ok(applyOverrides(templateToSnapshot(template), req))
            })
        }
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
        return okAsync(snapshot)
      }

      return unwrap(
        buildSnapshot().andThen((snap) => {
          const token = `stk-${randomUUID()}`
          const ts = BigInt(Math.floor(Date.now() / 1000))

          const ensureWorkspace = (workspaceId: string | undefined): ResultAsync<string, AppError> => {
            if (workspaceId) return okAsync(workspaceId)
            return wm.createWorkspace("").map((r) => r.workspaceId)
          }

          return ensureWorkspace(req.workspaceId).andThen((workspaceId) =>
            gelQuery(() => insertSession(gel, {
              title: req.title ?? "",
              templateId: req.templateId ?? "",
              systemPrompt: snap.systemPrompt,
              upstream: snap.upstream,
              apiKey: snap.apiKey,
              modelId: snap.modelId,
              maxTokens: snap.maxTokens,
              contextLength: snap.contextLength,
              autoCompactLength: snap.autoCompactLength,
              toolServers: snap.toolServers,
              skills: snap.skills,
              extraPkgs: snap.extraPkgs,
              sessionToken: token,
              workspaceId,
              createdAt: Number(ts),
              updatedAt: Number(ts),
            })).andThen((result) => ok({
              sessionId: result.id,
              sessionToken: token,
              session: sessionToInfo(result),
            })),
          )
        }),
      )
    },

    updateSessionMeta(req: UpdateSessionMetaRequest) {
      const ts = BigInt(Math.floor(Date.now() / 1000))
      return unwrap(
        gelQuery(() => updateSessionMeta(gel, {
          id: req.sessionId,
          updatedAt: Number(ts),
          title: req.title || null,
        })).andThen((s) => {
          if (!s) return err(new NotFoundError("Session not found"))
          return ok(sessionToInfo(s))
        }),
      )
    },

    updateSessionHotConfig(req: UpdateSessionHotConfigRequest) {
      const ts = BigInt(Math.floor(Date.now() / 1000))
      return unwrap(
        gelQuery(() => updateSessionHotConfig(gel, {
          id: req.sessionId,
          updatedAt: Number(ts),
          systemPrompt: req.systemPrompt || null,
          upstream: req.providerConfig?.upstream || null,
          apiKey: req.providerConfig?.apiKey || null,
          modelId: req.providerConfig?.modelId || null,
          maxTokens: req.providerConfig?.maxTokens || null,
          contextLength: req.providerConfig?.contextLength || null,
          autoCompactLength: req.providerConfig?.autoCompactLength || null,
          skills: req.skillConfig?.length ? JSON.stringify(req.skillConfig.map(s => ({ slug: s.slug }))) : null,
        })).andThen((s) => {
          if (!s) return err(new NotFoundError("Session not found"))
          return ok(sessionToInfo(s))
        }),
      )
    },

    updateSessionColdConfig(req: UpdateSessionColdConfigRequest) {
      return unwrap(
        gelQuery(() => getSessionById(gel, { id: req.sessionId }))
          .andThen((session) => {
            if (!session) return err(new NotFoundError("Session not found"))
            if (session.state !== "stopped") {
              return err(new ValidationError("Cold config changes require session to be stopped"))
            }
            const ts = BigInt(Math.floor(Date.now() / 1000))
            return gelQuery(() => updateSessionColdConfig(gel, {
              id: req.sessionId,
              updatedAt: Number(ts),
              toolServers: req.toolServerConfig?.length
                ? JSON.stringify(req.toolServerConfig.map(e => ({ type: e.type, config: e.config ?? {} })))
                : null,
              extraPkgs: req.extraPackage?.length
                ? JSON.stringify(req.extraPackage)
                : null,
            }))
          }).andThen((s) => ok(sessionToInfo(s!))),
      )
    },

    switchSessionTemplate(req: SwitchSessionTemplateRequest) {
      return unwrap(
        gelQuery(() => getSessionById(gel, { id: req.sessionId }))
          .andThen((session) => {
            if (!session) return err(new NotFoundError("Session not found"))
            if (session.state !== "stopped") {
              return err(new ValidationError("Cannot switch template on a running session"))
            }
            return gelQuery(() => getTemplateForSession(gel, { templateId: req.templateId }))
              .andThen((template) => {
                if (!template) return err(new NotFoundError("Template not found"))
                const snapshot = templateToSnapshot(template)
                const ts = BigInt(Math.floor(Date.now() / 1000))
                return gelQuery(() => switchSessionTemplate(gel, {
                  id: req.sessionId,
                  templateId: req.templateId,
                  systemPrompt: snapshot.systemPrompt,
                  upstream: snapshot.upstream,
                  apiKey: snapshot.apiKey,
                  modelId: snapshot.modelId,
                  maxTokens: snapshot.maxTokens,
                  contextLength: snapshot.contextLength,
                  autoCompactLength: snapshot.autoCompactLength,
                  toolServers: snapshot.toolServers,
                  skills: snapshot.skills,
                  extraPkgs: snapshot.extraPkgs,
                  updatedAt: Number(ts),
                }))
              })
          }).andThen((s) => ok(sessionToInfo(s!))),
      )
    },

    deleteSession(req: DeleteSessionRequest) {
      return unwrap(
        gelQuery(() => getSessionById(gel, { id: req.sessionId }))
          .andThen((session) => {
            if (!session) return err(new NotFoundError("Session not found"))
            if (session.state !== "stopped") {
              return err(new ValidationError("Cannot delete a running session"))
            }
            return gelQuery(() => deleteSessionMessages(gel, { sessionId: req.sessionId }))
              .andThen(() => gelQuery(() => deleteSessionById(gel, { id: req.sessionId })))
              .andThen(() => {
                if (session.workspaceId) {
                  return gelQuery(() => listSessionsByWorkspace(gel, { workspaceId: session.workspaceId }))
                    .andThen((remaining) => {
                      if (remaining.length === 0) {
                        return wm.deleteWorkspace(session.workspaceId)
                          .mapErr((e) => {
                            console.error(`WM deleteWorkspace failed: ${e.message}`)
                            return e
                          }).andThen(() => ok({}))
                      }
                      return ok({})
                    })
                }
                return ok({})
              })
          }),
      )
    },

    startSession(req: StartSessionRequest) {
      return unwrap(
        gelQuery(() => getSessionById(gel, { id: req.sessionId }))
          .andThen((session) => {
            if (!session) return err(new NotFoundError("Session not found"))
            if (session.state === "running" || session.state === "creating") {
              return err(new ValidationError(`Session already in state: ${session.state}`))
            }

            const ts = BigInt(Math.floor(Date.now() / 1000))
            return gelQuery(() => setSessionCreating(gel, { id: req.sessionId, updatedAt: Number(ts) }))
              .andThen(() => {
                if (!session.workspaceId) {
                  const ts2 = BigInt(Math.floor(Date.now() / 1000))
                  return gelQuery(() => setSessionStopped(gel, { id: req.sessionId, updatedAt: Number(ts2) }))
                    .andThen(() => err(new ValidationError("Session has no workspace")))
                }

                const workerImage = process.env.WORKER_IMAGE ?? "localhost/openzerg/worker:latest"
                return wm.ensureWorkspaceWorker({
                  workspaceId: session.workspaceId,
                  image: workerImage,
                  env: {
                    REGISTRY_URL: process.env.REGISTRY_INTERNAL_URL ?? "http://registry:25000",
                    SESSION_TOKEN: session.sessionToken,
                    NIX_PKGS: JSON.stringify((safeParseJson(session.extraPkgs || "[]") ?? []) as string[]),
                  },
                }).andThen((workerResp) => {
                  const ts4 = BigInt(Math.floor(Date.now() / 1000))
                  return gelQuery(() => setSessionIdle(gel, {
                    id: req.sessionId,
                    workerId: workerResp.workerId,
                    lastActiveAt: Number(ts4),
                    updatedAt: Number(ts4),
                  })).andThen(() => ok({}))
                }).orElse((startErr) => {
                  const ts3 = BigInt(Math.floor(Date.now() / 1000))
                  return gelQuery(() => setSessionStopped(gel, { id: req.sessionId, updatedAt: Number(ts3) }))
                    .andThen(() => err(startErr))
                })
              })
          }),
      )
    },

    stopSession(req: StopSessionRequest) {
      return unwrap(
        gelQuery(() => getSessionById(gel, { id: req.sessionId }))
          .andThen((session) => {
            if (!session) return err(new NotFoundError("Session not found"))
            if (session.state === "stopped") return ok({})
            const ts = BigInt(Math.floor(Date.now() / 1000))
            return gelQuery(() => setSessionStopped(gel, { id: req.sessionId, updatedAt: Number(ts) }))
              .andThen(() => ok({}))
          }),
      )
    },

    resolveSession(req: ResolveSessionRequest) {
      return unwrap(
        gelQuery(() => resolveSessionByToken(gel, { sessionToken: req.sessionToken }))
          .andThen((session) => {
            if (!session) return err(new NotFoundError("Session not found"))

            let workerUrl = ""
            let workerSecret = ""
            let workspaceRoot = ""

            const resolveWorker: ResultAsync<void, AppError> = session.workerId
              ? gelQuery(() => getWorkerForSession(gel, { workerId: session.workerId }))
                  .map((worker) => {
                    if (worker) {
                      workerUrl = worker.filesystemUrl || `http://${worker.containerName}`
                      workerSecret = worker.secret
                      workspaceRoot = worker.workspaceRoot
                    }
                  })
              : okAsync(undefined)

            return resolveWorker.andThen(() => {
              const serverEntries = (safeParseJson(session.toolServers || "[]") ?? []) as Array<{ type: string; config?: Record<string, string> }>
              const serverTypes = serverEntries.map(e => e.type).filter(Boolean)

              const resolveToolServers: ResultAsync<Array<{ name: string; url: string; config: string }>, AppError> = serverTypes.length > 0
                ? tsm.resolveTools(session.id, serverTypes)
                    .map((toolsResult) =>
                      toolsResult.toolServerUrls.map(u => ({
                        name: u.name, url: u.url,
                        config: typeof u.config === "string" ? u.config : JSON.stringify(u.config),
                      })),
                    )
                : okAsync([])

              return resolveToolServers.map((toolServerUrls) => ({
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
              }))
            })
          }),
      )
    },
  }
}
