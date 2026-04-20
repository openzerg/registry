import { ConnectError, Code } from "@connectrpc/connect"
import { randomUUID, now, dbQuery, unwrap } from "./common.js"
import type { DB } from "../db.js"
import type { Message } from "@openzerg/common/entities/message-schema.js"
import type { ListMessagesRequest, CreateMessageRequest, DeleteMessagesFromRequest } from "@openzerg/common/gen/registry/v1_pb.js"

export function registerMessageHandlers(db: DB) {
  return {
    listMessages(req: ListMessagesRequest) {
      return unwrap(dbQuery(async () => {
        let query = db.selectFrom("registry_messages").selectAll()
          .where("sessionId", "=", req.sessionId)
          .where("compacted", "=", false)
          .orderBy("createdAt", "desc")
        if (req.beforeId) {
          const before = await db.selectFrom("registry_messages").select(["createdAt"])
            .where("id", "=", req.beforeId).executeTakeFirst()
          if (before) query = query.where("createdAt", "<", before.createdAt)
        }
        const limit = req.limit || 100
        const rows: Message[] = await query.limit(limit + 1).execute()
        const hasMore = rows.length > limit
        const messages = rows.slice(0, limit).map((m) => ({
          id: m.id, sessionId: m.sessionId, role: m.role,
          parentMessageId: m.parentMessageId, toolCallId: m.toolCallId,
          toolName: m.toolName, content: m.content,
          tokenUsage: m.tokenUsage, metadata: m.metadata,
          compacted: m.compacted, createdAt: m.createdAt,
        }))
        return {
          messages, hasMore,
          nextBeforeId: hasMore && messages.length > 0 ? messages[messages.length - 1].id : "",
        }
      }))
    },

    createMessage(req: CreateMessageRequest) {
      return unwrap(dbQuery(async () => {
        const id = randomUUID()
        const ts = now()
        await db.insertInto("registry_messages").values({
          id, sessionId: req.sessionId, role: req.role,
          parentMessageId: req.parentMessageId ?? "",
          toolCallId: req.toolCallId ?? "",
          toolName: req.toolName ?? "",
          content: req.content ?? "",
          tokenUsage: req.tokenUsage ?? "{}",
          metadata: req.metadata ?? "{}",
          compacted: false, createdAt: ts,
        }).execute()
        return { messageId: id }
      }))
    },

    deleteMessagesFrom(req: DeleteMessagesFromRequest) {
      return unwrap(dbQuery(async () => {
        const msg = await db.selectFrom("registry_messages").select(["createdAt"])
          .where("id", "=", req.messageId)
          .where("sessionId", "=", req.sessionId).executeTakeFirst()
        if (!msg) throw new ConnectError("Message not found", Code.NotFound)
        await db.deleteFrom("registry_messages")
          .where("sessionId", "=", req.sessionId)
          .where("createdAt", ">=", msg.createdAt).execute()
        return {}
      }))
    },
  }
}
