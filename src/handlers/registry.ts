import { randomUUID, now, dbQuery, unwrap } from "./common.js"
import type { DB } from "../db.js"
import type { Instance } from "@openzerg/common/entities/instance-schema.js"
import type { RegisterRequest, HeartbeatRequest, ListInstancesRequest } from "@openzerg/common/gen/registry/v1_pb.js"

export function registerRegistryHandlers(db: DB) {
  return {
    register(req: RegisterRequest) {
      return unwrap(dbQuery(async () => {
        const id = randomUUID()
        const serviceToken = `st-${randomUUID()}`
        const ts = now()
        await db.insertInto("registry_instances").values({
          id, name: req.name, instanceType: req.instanceType,
          ip: req.ip, port: req.port, publicUrl: req.publicUrl,
          lifecycle: "active", lastSeen: ts,
          metadata: JSON.stringify(req.metadata ?? {}),
          createdAt: ts, updatedAt: ts,
        }).execute()
        return { instanceId: id, serviceToken }
      }))
    },

    heartbeat(req: HeartbeatRequest) {
      return unwrap(dbQuery(async () => {
        const ts = now()
        await db.updateTable("registry_instances")
          .set({ lastSeen: ts, updatedAt: ts, lifecycle: "active" })
          .where("id", "=", req.instanceId).execute()
        return {}
      }))
    },

    listInstances(req: ListInstancesRequest) {
      return unwrap(dbQuery(async () => {
        let query = db.selectFrom("registry_instances").selectAll()
        if (req.instanceType) query = query.where("instanceType", "=", req.instanceType)
        const rows: Instance[] = await query.execute()
        return {
          instances: rows.map((r) => ({
            instanceId: r.id, name: r.name, instanceType: r.instanceType,
            url: r.publicUrl, lifecycle: r.lifecycle, lastSeen: r.lastSeen,
            metadata: typeof r.metadata === "string" ? JSON.parse(r.metadata) : r.metadata ?? {},
          })),
        }
      }))
    },
  }
}
