import { gelQuery, unwrap } from "@openzerg/common/gel"
import { ok, err } from "neverthrow"
import { NotFoundError } from "@openzerg/common"
import {
  listAllTemplates, getTemplateById, insertTemplate,
  updateTemplateById, deleteTemplateById,
} from "@openzerg/common/queries"
import type {
  GetTemplateRequest, CreateTemplateRequest,
  UpdateTemplateRequest, DeleteTemplateRequest,
} from "@openzerg/common/gen/registry/v1_pb.js"
import type { GelClient } from "@openzerg/common/gel"

function safeParseJson(raw: string): unknown {
  try { return JSON.parse(raw) } catch { return null }
}

type TemplateRow = NonNullable<Awaited<ReturnType<typeof getTemplateById>>>

function templateRowToInfo(r: TemplateRow) {
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
    createdAt: BigInt(r.createdAt), updatedAt: BigInt(r.updatedAt),
  }
}

export function registerTemplateHandlers(gel: GelClient) {
  return {
    listTemplates() {
      return unwrap(
        gelQuery(() => listAllTemplates(gel))
          .andThen((rows) => ok({ templates: rows.map(templateRowToInfo) })),
      )
    },

    getTemplate(req: GetTemplateRequest) {
      return unwrap(
        gelQuery(() => getTemplateById(gel, { id: req.templateId }))
          .andThen((row) => {
            if (!row) return err(new NotFoundError("Template not found"))
            return ok(templateRowToInfo(row))
          }),
      )
    },

    createTemplate(req: CreateTemplateRequest) {
      const ts = BigInt(Math.floor(Date.now() / 1000))
      const pc = req.providerConfig
      const toolServers = (req.toolServerConfig ?? []).map((e: { type: string; config: { [key: string]: string } }) => ({ type: e.type, config: e.config ?? {} }))
      const skills = (req.skillConfig ?? []).map((s: { slug: string }) => ({ slug: s.slug }))
      const extraPkgs = req.extraPackage ?? []
      return unwrap(
        gelQuery(() => insertTemplate(gel, {
          name: req.name,
          description: req.description ?? "",
          systemPrompt: req.systemPrompt ?? "",
          upstream: pc?.upstream ?? "",
          apiKey: pc?.apiKey ?? "",
          modelId: pc?.modelId ?? "",
          maxTokens: pc?.maxTokens ?? 0,
          contextLength: pc?.contextLength ?? 0,
          autoCompactLength: pc?.autoCompactLength ?? 0,
          toolServers: JSON.stringify(toolServers),
          skills: JSON.stringify(skills),
          extraPkgs: JSON.stringify(extraPkgs),
          createdAt: Number(ts),
          updatedAt: Number(ts),
        })).andThen((row) => ok(templateRowToInfo(row))),
      )
    },

    updateTemplate(req: UpdateTemplateRequest) {
      const ts = BigInt(Math.floor(Date.now() / 1000))
      const pc = req.providerConfig
      const toolServers = (req.toolServerConfig ?? []).map((e: { type: string; config: { [key: string]: string } }) => ({ type: e.type, config: e.config ?? {} }))
      const skills = (req.skillConfig ?? []).map((s: { slug: string }) => ({ slug: s.slug }))
      const extraPkgs = req.extraPackage ?? []
      return unwrap(
        gelQuery(() => updateTemplateById(gel, {
          id: req.id,
          name: req.name,
          description: req.description,
          systemPrompt: req.systemPrompt,
          upstream: pc?.upstream ?? "",
          apiKey: pc?.apiKey ?? "",
          modelId: pc?.modelId ?? "",
          maxTokens: pc?.maxTokens ?? 0,
          contextLength: pc?.contextLength ?? 0,
          autoCompactLength: pc?.autoCompactLength ?? 0,
          toolServers: JSON.stringify(toolServers),
          skills: JSON.stringify(skills),
          extraPkgs: JSON.stringify(extraPkgs),
          updatedAt: Number(ts),
        })).andThen((row) => {
          if (!row) return err(new NotFoundError("Template not found"))
          return ok(templateRowToInfo(row))
        }),
      )
    },

    deleteTemplate(req: DeleteTemplateRequest) {
      return unwrap(
        gelQuery(() => deleteTemplateById(gel, { id: req.templateId }))
          .andThen(() => ok({})),
      )
    },
  }
}
