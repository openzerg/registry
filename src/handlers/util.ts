import { ConnectError, Code } from "@connectrpc/connect"
import { randomUUID } from "node:crypto"

export function now(): bigint {
  return BigInt(Math.floor(Date.now() / 1000))
}

export function errorToCode(e: { code?: string }): Code {
  switch (e.code) {
    case "NOT_FOUND": return Code.NotFound
    case "VALIDATION": return Code.InvalidArgument
    case "PERMISSION_DENIED": return Code.PermissionDenied
    case "UNAUTHENTICATED": return Code.Unauthenticated
    case "CONFLICT": return Code.AlreadyExists
    default: return Code.Internal
  }
}
