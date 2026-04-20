import { describe, test, expect, beforeAll, afterAll } from "bun:test"
import { createServer, type Server } from "node:http"
import { connectNodeAdapter } from "@connectrpc/connect-node"
import { RegistryClient, type IWorkspaceManager, type IToolServerManager } from "@openzerg/common"
import type { Result } from "neverthrow"
import { ResultAsync } from "neverthrow"
import { PodmanCompose, waitForPort } from "../../openzerg/e2e/compose-helper.js"
import { openDB, autoMigrate } from "../src/db.js"
import { createRegistryRouter } from "../src/router.js"
import type { AppError } from "@openzerg/common"
import { randomUUID } from "node:crypto"

const PG_PORT = 15432
const PG_URL = `postgres://e2e:e2e@127.0.0.1:${PG_PORT}/e2e_test`
const REGISTRY_PORT = 25200
const COMPOSE_FILE = import.meta.dir + "/compose.yaml"

const compose = new PodmanCompose({
  projectName: "registry",
  composeFile: COMPOSE_FILE,
})

let client: RegistryClient
let server: Server
let stopWorkerCalls: string[] = []
let deleteWorkspaceCalls: string[] = []

function unwrap<T>(result: Result<T, AppError>): T {
  if (result.isOk()) return result.value
  throw result.error
}

beforeAll(async () => {
  process.env.MASTER_API_KEY = "test-master-key"
  await compose.up(["postgres"])
  await waitForPort(PG_PORT, 30_000)
  let migrated = false
  for (let i = 0; i < 10; i++) {
    try { await autoMigrate(PG_URL); migrated = true; break } catch { await new Promise(r => setTimeout(r, 1000)) }
  }
  if (!migrated) throw new Error("autoMigrate failed after 10 retries")
  const db = openDB(PG_URL)

  const mockWM: IWorkspaceManager = {
    health: () => ResultAsync.fromPromise(Promise.resolve({ status: "ok" } as any), () => new Error("fail") as any),
    createWorkspace: (_sessionId: string) => {
      const workspaceId = randomUUID()
      const volumeName = `ws-${workspaceId.slice(0, 12)}`
      return ResultAsync.fromPromise(
        Promise.resolve({ workspaceId, volumeName }),
        () => new Error("fail") as any,
      )
    },
    listWorkspaces: () => ResultAsync.fromPromise(Promise.resolve({ workspaces: [] } as any), () => new Error("fail") as any),
    getWorkspace: (_id: string) => ResultAsync.fromPromise(Promise.resolve({} as any), () => new Error("fail") as any),
    deleteWorkspace: (workspaceId: string) => {
      deleteWorkspaceCalls.push(workspaceId)
      return ResultAsync.fromPromise(Promise.resolve({}), () => new Error("fail") as any)
    },
    startWorker: (_req: any) =>
      ResultAsync.fromPromise(
        Promise.resolve({ workerId: randomUUID(), containerName: "worker-test", secret: "test-secret" }),
        () => new Error("fail") as any,
      ),
    stopWorker: (workerId: string) => {
      stopWorkerCalls.push(workerId)
      return ResultAsync.fromPromise(Promise.resolve({}), () => new Error("fail") as any)
    },
    getWorkerStatus: (_workerId: string) => ResultAsync.fromPromise(Promise.resolve({} as any), () => new Error("fail") as any),
    listWorkers: () => ResultAsync.fromPromise(Promise.resolve({ workers: [] } as any), () => new Error("fail") as any),
    ensureWorkspaceWorker: (_req: any) =>
      ResultAsync.fromPromise(
        Promise.resolve({ workerId: randomUUID(), containerName: "worker-test", secret: "test-secret", volumeName: "ws-test" }),
        () => new Error("fail") as any,
      ),
    updateWorkspaceConfig: (_req: any) =>
      ResultAsync.fromPromise(Promise.resolve({}), () => new Error("fail") as any),
  }

  const mockTSM: IToolServerManager = {
    health: () => ResultAsync.fromPromise(Promise.resolve({ status: "ok" } as any), () => new Error("fail") as any),
    startToolServer: (_req: any) => ResultAsync.fromPromise(Promise.resolve({} as any), () => new Error("fail") as any),
    stopToolServer: (_type: string) => ResultAsync.fromPromise(Promise.resolve({} as any), () => new Error("fail") as any),
    listToolServers: () => ResultAsync.fromPromise(Promise.resolve({ toolServers: [] } as any), () => new Error("fail") as any),
    refreshToolCache: (_type: string) => ResultAsync.fromPromise(Promise.resolve({} as any), () => new Error("fail") as any),
    resolveTools: (_sessionId: string, _types: string[]) =>
      ResultAsync.fromPromise(
        Promise.resolve({ tools: [], systemContext: "", toolServerUrls: [] }),
        () => new Error("fail") as any,
      ),
    executeTool: (_req: any) =>
      ResultAsync.fromPromise(
        Promise.resolve({ resultJson: "", success: false, error: "not implemented" }),
        () => new Error("fail") as any,
      ),
  }

  const handler = connectNodeAdapter({
    routes: createRegistryRouter(db, mockWM, mockTSM),
  })

  server = createServer(handler)
  server.listen(REGISTRY_PORT)
  await new Promise(r => setTimeout(r, 100))

  client = new RegistryClient({
    baseURL: `http://localhost:${REGISTRY_PORT}`,
    token: "",
  })
}, 60_000)

afterAll(async () => {
  server?.close()
  await compose.down()
})

describe("registry E2E", () => {
  test("login with valid key", async () => {
    const result = await client.login("test-master-key")
    expect(result.isOk()).toBe(true)
    if (result.isOk()) {
      expect(result.value.userToken).toBeTruthy()
      expect(result.value.expiresInSec).toBe(86400)
    }
  })

  test("login with invalid key", async () => {
    const result = await client.login("wrong-key")
    expect(result.isErr()).toBe(true)
  })

  test("register instance", async () => {
    const result = await client.register({
      name: "test-worker",
      instanceType: "worker",
      ip: "127.0.0.1",
      port: 25001,
      publicUrl: "http://localhost:25001",
    })
    expect(result.isOk()).toBe(true)
    if (result.isOk()) {
      expect(result.value.instanceId).toBeTruthy()
      expect(result.value.serviceToken).toBeTruthy()
    }
  })

  test("heartbeat", async () => {
    const reg = unwrap(await client.register({
      name: "hb-worker",
      instanceType: "worker",
      ip: "127.0.0.1",
      port: 25002,
      publicUrl: "http://localhost:25002",
    }))
    const hb = await client.heartbeat(reg.instanceId)
    expect(hb.isOk()).toBe(true)
  })

  test("listInstances filters by type", async () => {
    await client.register({
      name: "list-worker",
      instanceType: "worker",
      ip: "127.0.0.1",
      port: 25003,
      publicUrl: "http://localhost:25003",
    })
    const result = await client.listInstances("worker")
    expect(result.isOk()).toBe(true)
    if (result.isOk()) {
      expect(result.value.instances.length).toBeGreaterThanOrEqual(1)
      for (const inst of result.value.instances) {
        expect(inst.instanceType).toBe("worker")
      }
    }
  })

  test("template CRUD", async () => {
    const created = unwrap(await client.createTemplate({
      name: "e2e-template",
      description: "test template",
      systemPrompt: "You are a test assistant",
    }))
    expect(created.name).toBe("e2e-template")

    const fetched = unwrap(await client.getTemplate(created.id))
    expect(fetched.name).toBe("e2e-template")

    const updated = unwrap(await client.updateTemplate({
      id: created.id,
      name: "e2e-template-updated",
      description: "updated desc",
      systemPrompt: "Updated prompt",
    }))
    expect(updated.name).toBe("e2e-template-updated")
    expect(updated.systemPrompt).toBe("Updated prompt")

    const del = await client.deleteTemplate(created.id)
    expect(del.isOk()).toBe(true)
  })

  test("session lifecycle: create -> stop -> delete", async () => {
    const tpl = unwrap(await client.createTemplate({
      name: "session-test-tpl",
      systemPrompt: "test",
    }))

    const created = unwrap(await client.createSession({
      title: "e2e session",
      templateId: tpl.id,
    }))
    expect(created.sessionId).toBeTruthy()
    expect(created.sessionToken).toBeTruthy()
    expect(created.session?.state).toBe("stopped")

    const fetched = unwrap(await client.getSession(created.sessionId))
    expect(fetched.workspaceId).toBeTruthy()

    const del = await client.deleteSession(created.sessionId)
    expect(del.isOk()).toBe(true)

    await client.deleteTemplate(tpl.id)
  })

  test("session with inline config (no template)", async () => {
    const created = unwrap(await client.createSession({
      title: "inline session",
      systemPrompt: "You are inline",
    }))
    expect(created.sessionId).toBeTruthy()
    expect(created.session?.systemPrompt).toBe("You are inline")

    const del = await client.deleteSession(created.sessionId)
    expect(del.isOk()).toBe(true)
  })

  test("message CRUD", async () => {
    const tpl = unwrap(await client.createTemplate({
      name: "msg-test-tpl",
      systemPrompt: "test",
    }))

    const session = unwrap(await client.createSession({
      title: "msg test",
      templateId: tpl.id,
    }))

    const msg = unwrap(await client.createMessage({
      sessionId: session.sessionId,
      role: "user",
      content: "Hello E2E",
    }))
    expect(msg.messageId).toBeTruthy()

    const msgs = unwrap(await client.listMessages({
      sessionId: session.sessionId,
      limit: 10,
    }))
    expect(msgs.messages.length).toBeGreaterThanOrEqual(1)
    expect(msgs.messages[0].content).toBe("Hello E2E")

    await client.deleteSession(session.sessionId)
    await client.deleteTemplate(tpl.id)
  })

  test("createSession with workspaceId reuses existing workspace", async () => {
    const first = unwrap(await client.createSession({ title: "first session" }))
    expect(first.session?.workspaceId).toBeTruthy()

    const second = unwrap(await client.createSession({
      title: "second session — reuse workspace",
      workspaceId: first.session!.workspaceId,
    }))
    expect(second.session?.workspaceId).toBe(first.session!.workspaceId)

    await client.deleteSession(second.sessionId)
    await client.deleteSession(first.sessionId)
  })

  test("deleteSession reference counting — workspace deleted only when last session removed", async () => {
    const first = unwrap(await client.createSession({ title: "ref-count-A" }))
    const wsId = first.session!.workspaceId

    const second = unwrap(await client.createSession({
      title: "ref-count-B",
      workspaceId: wsId,
    }))
    expect(second.session!.workspaceId).toBe(wsId)

    await client.deleteSession(second.sessionId)

    const fetchedA = unwrap(await client.getSession(first.sessionId))
    expect(fetchedA.workspaceId).toBe(wsId)

    await client.deleteSession(first.sessionId)
  })

  test("stopSession does not call stopWorker", async () => {
    const beforeStopCalls = stopWorkerCalls.length
    const session = unwrap(await client.createSession({ title: "stop-test" }))

    const startResult = await client.startSession(session.sessionId)
    expect(startResult.isOk()).toBe(true)

    const stopResult = await client.stopSession(session.sessionId)
    expect(stopResult.isOk()).toBe(true)

    const fetched = unwrap(await client.getSession(session.sessionId))
    expect(fetched.state).toBe("stopped")

    expect(stopWorkerCalls.length).toBe(beforeStopCalls)

    await client.deleteSession(session.sessionId)
  })

  test("deleteSession reference counting — deleteWorkspace called only for last session", async () => {
    const beforeDeleteCalls = deleteWorkspaceCalls.length
    const first = unwrap(await client.createSession({ title: "ref-count-A" }))
    const wsId = first.session!.workspaceId

    const second = unwrap(await client.createSession({
      title: "ref-count-B",
      workspaceId: wsId,
    }))
    expect(second.session!.workspaceId).toBe(wsId)

    await client.deleteSession(second.sessionId)
    expect(deleteWorkspaceCalls.length).toBe(beforeDeleteCalls)

    await client.deleteSession(first.sessionId)
    expect(deleteWorkspaceCalls.length).toBe(beforeDeleteCalls + 1)
  })
})
