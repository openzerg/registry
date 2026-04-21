import { gelQuery, unwrap } from "@openzerg/common/gel"
import { ok, err } from "neverthrow"
import { NotFoundError } from "@openzerg/common"
import {
  listMessages, getMessageAnchorCreatedAt, insertMessage as insertRegistryMessage,
  getMessageForDelete, deleteMessagesFrom,
} from "@openzerg/common/queries"
import type { ListMessagesRequest, CreateMessageRequest, DeleteMessagesFromRequest } from "@openzerg/common/gen/registry/v1_pb.js"
import type { GelClient } from "@openzerg/common/gel"

export function registerMessageHandlers(gel: GelClient) {
  return {
    listMessages(req: ListMessagesRequest) {
      return unwrap(
        gelQuery(() => {
          let beforeCreatedAt: number | null = null
          return (req.beforeId
            ? getMessageAnchorCreatedAt(gel, { id: req.beforeId }).then((anchor) => {
                beforeCreatedAt = anchor?.createdAt ?? null
                return beforeCreatedAt
              })
            : Promise.resolve(null)
          ).then(() => {
            const limit = req.limit || 100
            return listMessages(gel, {
              sessionId: req.sessionId,
              limit: limit + 1,
              beforeCreatedAt,
            }).then((rows) => {
              const hasMore = rows.length > limit
              const messages = rows.slice(0, limit).map((m) => ({
                id: m.id, sessionId: m.sessionId, role: m.role,
                parentMessageId: m.parentMessageId, toolCallId: m.toolCallId,
                toolName: m.toolName, content: m.content,
                tokenUsage: m.tokenUsage, metadata: m.metadata,
                compacted: m.compacted, createdAt: BigInt(m.createdAt),
              }))
              return {
                messages, hasMore,
                nextBeforeId: hasMore && messages.length > 0 ? messages[messages.length - 1].id : "",
              }
            })
          })
        }),
      )
    },

    createMessage(req: CreateMessageRequest) {
      const ts = BigInt(Math.floor(Date.now() / 1000))
      return unwrap(
        gelQuery(() => insertRegistryMessage(gel, {
          sessionId: req.sessionId,
          role: req.role,
          parentMessageId: req.parentMessageId ?? "",
          toolCallId: req.toolCallId ?? "",
          toolName: req.toolName ?? "",
          content: req.content ?? "",
          tokenUsage: req.tokenUsage ?? "{}",
          metadata: req.metadata ?? "{}",
          createdAt: Number(ts),
        })).andThen((result) => ok({ messageId: result.id })),
      )
    },

    deleteMessagesFrom(req: DeleteMessagesFromRequest) {
      return unwrap(
        gelQuery(() => getMessageForDelete(gel, {
          id: req.messageId,
          sessionId: req.sessionId,
        })).andThen((msg) => {
          if (!msg) return err(new NotFoundError("Message not found"))
          return gelQuery(() => deleteMessagesFrom(gel, {
            sessionId: req.sessionId,
            createdAt: msg.createdAt,
          })).andThen(() => ok({}))
        }),
      )
    },
  }
}
