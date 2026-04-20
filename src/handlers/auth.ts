import { ConnectError, Code } from "@connectrpc/connect"
import { randomUUID } from "node:crypto"
import type { LoginRequest } from "@openzerg/common/gen/registry/v1_pb.js"

export function registerAuthHandlers() {
  return {
    login(req: LoginRequest) {
      const masterKey = process.env.MASTER_API_KEY ?? "dev-master-key"
      if (req.apiKey !== masterKey) {
        throw new ConnectError("Invalid API key", Code.Unauthenticated)
      }
      return Promise.resolve({ userToken: `ut-${randomUUID()}`, expiresInSec: 86400 })
    },
  }
}
