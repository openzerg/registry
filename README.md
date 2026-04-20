# registry

Registry 服务。提供服务注册发现、认证、角色管理、会话管理、消息持久化和 Worker 管理。

## 数据模型

| 表 | 用途 |
|------|------|
| `registry_instances` | 服务实例注册（ZCP 服务发现） |
| `registry_roles` | 角色配置（system prompt、ZCP 服务器列表、maxSteps） |
| `registry_sessions` | 会话（绑定角色 + Worker + agent） |
| `registry_messages` | 消息持久化（支持 compact 压缩） |
| `registry_workers` | Worker 实例管理 |

## ConnectRPC 接口

| 分组 | 方法 |
|------|------|
| Auth | `Login` |
| Instance | `Register`, `Heartbeat`, `ListInstances` |
| Role CRUD | `ListRoles`, `GetRole`, `CreateRole`, `UpdateRole`, `DeleteRole` |
| Session | `ListSessions`, `GetSession`, `CreateSession`, `UpdateSession`, `DeleteSession`, `BindWorker`, `UnbindWorker`, `ResolveSession` |
| Message | `ListMessages`, `CreateMessage`, `DeleteMessagesFrom` |
| Worker | `SpawnWorker`, `DeleteWorker`, `ListWorkers` |

## 技术栈

| 属性 | 值 |
|------|-----|
| 运行时 | Bun |
| RPC | ConnectRPC v2 |
| 数据库 | PostgreSQL（Kysely + postgres.js） |
| 错误处理 | neverthrow（handler 层 unwrap → ConnectError） |
| API 定义 | common-spec（TypeSpec → proto） |
| Schema 验证 | Zod v4 |

## 环境变量

```bash
DATABASE_URL=postgresql://openzerg:${DB_PASSWORD}@localhost:5433/openzerg
REGISTRY_PORT=15319
MASTER_API_KEY=dev-master-key
```

## 开发

```bash
bun install && bun run typecheck && bun run dev
```
