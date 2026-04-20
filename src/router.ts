import type { ConnectRouter } from "@connectrpc/connect"
import { RegistryService } from "@openzerg/common/gen/registry/v1_pb.js"
import type { IWorkspaceManager, IToolServerManager } from "@openzerg/common"
import type { DB } from "./db.js"
import { registerAuthHandlers } from "./handlers/auth.js"
import { registerRegistryHandlers } from "./handlers/registry.js"
import { registerTemplateHandlers } from "./handlers/template.js"
import { registerSessionHandlers } from "./handlers/session.js"
import { registerMessageHandlers } from "./handlers/message.js"

export function createRegistryRouter(
  db: DB,
  wm: IWorkspaceManager,
  tsm: IToolServerManager,
): (router: ConnectRouter) => void {
  return (router: ConnectRouter) => {
    const auth = registerAuthHandlers()
    const registry = registerRegistryHandlers(db)
    const template = registerTemplateHandlers(db)
    const session = registerSessionHandlers(db, wm, tsm)
    const message = registerMessageHandlers(db)

    router.service(RegistryService, {
      ...auth,
      ...registry,
      ...template,
      ...session,
      ...message,
    })
  }
}
