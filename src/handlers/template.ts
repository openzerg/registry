import { ConnectError, Code } from "@connectrpc/connect"
import { randomUUID, now, dbQuery, unwrap } from "./common.js"
import type { DB } from "../db.js"
import type { SessionTemplate } from "@openzerg/common/entities/sessiontemplate-schema.js"
import type {
  GetTemplateRequest, CreateTemplateRequest,
  UpdateTemplateRequest, DeleteTemplateRequest,
} from "@openzerg/common/gen/registry/v1_pb.js"

function safeParseJson(raw: string): unknown {
  try { return JSON.parse(raw) } catch { return null }
}

function templateRowToInfo(r: SessionTemplate) {
  const toolEntries = (safeParseJson(r.toolServers || "[]") ?? []) as Array<{ type: string; config?: Record<string, string> }>
  const skillRefs = (safeParseJson(r.skills || "[]") ?? []) as Array<{ slug: string }>
  const extraPkgs = (safeParseJson(r.extraPkgs || "[]") ?? []) as string[]
  return {
    id: r.id, name: r.name, description: r.description,
    systemPrompt: r.systemPrompt,
    providerConfig: {
      upstream: r.upstream, apiKey: r.apiKey, modelId: r.modelId,
      maxTokens: r.maxTokens, contextLength: r.contextLength,
      autoCompactLength: r.autoCompactLength,
    },
    toolServerConfig: toolEntries.map(e => ({ type: e.type, config: e.config ?? {} })),
    skillConfig: skillRefs.map(s => ({ slug: s.slug })),
    extraPackage: extraPkgs,
    createdAt: r.createdAt, updatedAt: r.updatedAt,
  }
}

export function registerTemplateHandlers(db: DB) {
  return {
    listTemplates() {
      return unwrap(dbQuery(async () => {
        const rows: SessionTemplate[] = await db.selectFrom("session_templates").selectAll().orderBy("name", "asc").execute()
        return { templates: rows.map(templateRowToInfo) }
      }))
    },

    getTemplate(req: GetTemplateRequest) {
      return unwrap(dbQuery(async () => {
        const row = await db.selectFrom("session_templates").selectAll()
          .where("id", "=", req.templateId).executeTakeFirst()
        if (!row) throw new ConnectError("Template not found", Code.NotFound)
        return templateRowToInfo(row)
      }))
    },

    createTemplate(req: CreateTemplateRequest) {
      return unwrap(dbQuery(async () => {
        const id = randomUUID()
        const ts = now()
        const pc = req.providerConfig
        const toolServers = (req.toolServerConfig ?? []).map((e: { type: string; config: { [key: string]: string } }) => ({ type: e.type, config: e.config ?? {} }))
        const skills = (req.skillConfig ?? []).map((s: { slug: string }) => ({ slug: s.slug }))
        const extraPkgs = req.extraPackage ?? []
        await db.insertInto("session_templates").values({
          id, name: req.name, description: req.description ?? "",
          systemPrompt: req.systemPrompt ?? "",
          upstream: pc?.upstream ?? "", apiKey: pc?.apiKey ?? "", modelId: pc?.modelId ?? "",
          maxTokens: pc?.maxTokens ?? 0, contextLength: pc?.contextLength ?? 0,
          autoCompactLength: pc?.autoCompactLength ?? 0,
          toolServers: JSON.stringify(toolServers),
          skills: JSON.stringify(skills),
          extraPkgs: JSON.stringify(extraPkgs),
          createdAt: ts, updatedAt: ts,
        }).execute()
        const row = await db.selectFrom("session_templates").selectAll().where("id", "=", id).executeTakeFirst()
        return templateRowToInfo(row!)
      }))
    },

    updateTemplate(req: UpdateTemplateRequest) {
      return unwrap(dbQuery(async () => {
        const ts = now()
        const pc = req.providerConfig
        const toolServers = (req.toolServerConfig ?? []).map((e: { type: string; config: { [key: string]: string } }) => ({ type: e.type, config: e.config ?? {} }))
        const skills = (req.skillConfig ?? []).map((s: { slug: string }) => ({ slug: s.slug }))
        const extraPkgs = req.extraPackage ?? []
        await db.updateTable("session_templates").set({
          name: req.name, description: req.description,
          systemPrompt: req.systemPrompt,
          upstream: pc?.upstream ?? "", apiKey: pc?.apiKey ?? "", modelId: pc?.modelId ?? "",
          maxTokens: pc?.maxTokens ?? 0, contextLength: pc?.contextLength ?? 0,
          autoCompactLength: pc?.autoCompactLength ?? 0,
          toolServers: JSON.stringify(toolServers),
          skills: JSON.stringify(skills),
          extraPkgs: JSON.stringify(extraPkgs),
          updatedAt: ts,
        }).where("id", "=", req.id).execute()
        const row = await db.selectFrom("session_templates").selectAll().where("id", "=", req.id).executeTakeFirst()
        if (!row) throw new ConnectError("Template not found", Code.NotFound)
        return templateRowToInfo(row)
      }))
    },

    deleteTemplate(req: DeleteTemplateRequest) {
      return unwrap(dbQuery(async () => {
        await db.deleteFrom("session_templates").where("id", "=", req.templateId).execute()
        return {}
      }))
    },
  }
}
