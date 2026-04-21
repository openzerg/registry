import { randomUUID } from "node:crypto"
import { gelQuery, unwrap } from "@openzerg/common/gel"
import { ok } from "neverthrow"
import {
  listInstances, insertInstance, heartbeatInstance,
} from "@openzerg/common/queries"
import type { RegisterRequest, HeartbeatRequest, ListInstancesRequest } from "@openzerg/common/gen/registry/v1_pb.js"
import type { GelClient } from "@openzerg/common/gel"

export function registerRegistryHandlers(gel: GelClient) {
  return {
    register(req: RegisterRequest) {
      const serviceToken = `st-${randomUUID()}`
      const ts = BigInt(Math.floor(Date.now() / 1000))
      return unwrap(
        gelQuery(() => insertInstance(gel, {
          name: req.name,
          instanceType: req.instanceType,
          ip: req.ip,
          port: req.port,
          publicUrl: req.publicUrl,
          lastSeen: Number(ts),
          metadata: JSON.stringify(req.metadata ?? {}),
          createdAt: Number(ts),
          updatedAt: Number(ts),
        })).andThen((result) => ok({ instanceId: result.id, serviceToken })),
      )
    },

    heartbeat(req: HeartbeatRequest) {
      const ts = BigInt(Math.floor(Date.now() / 1000))
      return unwrap(
        gelQuery(() => heartbeatInstance(gel, {
          id: req.instanceId,
          lastSeen: Number(ts),
          updatedAt: Number(ts),
        })).andThen(() => ok({})),
      )
    },

    listInstances(req: ListInstancesRequest) {
      return unwrap(
        gelQuery(() => listInstances(gel, {
          instanceType: req.instanceType || null,
        })).andThen((rows) => ok({
          instances: rows.map((r) => ({
            instanceId: r.id,
            name: r.name,
            instanceType: r.instanceType,
            url: r.publicUrl,
            lifecycle: r.lifecycle,
            lastSeen: BigInt(r.lastSeen),
            metadata: typeof r.metadata === "string" ? JSON.parse(r.metadata) : r.metadata ?? {},
          })),
        })),
      )
    },
  }
}
