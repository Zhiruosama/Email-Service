# 阶段 04-A：PostgreSQL 基础与 Migration

- 状态：已完成
- 阶段目标：用可版本化 SQL 建立租户、Message 状态快照和 Outbox 的数据库地基，并在真实 PostgreSQL 18 上验证约束、索引与回滚

## 1. 解决的问题

上一阶段的 Message 只存在于 Go 内存。服务重启后状态会丢失，也无法可靠实现幂等、
并发更新、延迟调度和 Transactional Outbox。

04-A 先解决“数据库结构是否可靠”这一层问题：

- Schema 如何增量演进；
- 多租户幂等键如何保证唯一；
- 损坏的状态快照如何在数据库边界被拒绝；
- Scheduler 和 Relay 将来依赖哪些索引；
- Migration 是否能执行、回滚并重新执行；
- 如何保证测试使用的确实是 PostgreSQL，而不是行为不同的替代数据库。

本阶段还没有实现 Repository，因此现在不能宣称 `Message + Outbox` 同事务已经完成。

## 2. 为什么现在做

状态机已经给出了合法状态、Attempt、deadline、generation 和 sequence 的真实不变量，
Schema 可以据此设计。后续 Repository、Submission API、Scheduler 和 Relay 都依赖这些
表，继续向后开发前必须先稳定持久化边界。

实施顺序是：

```text
领域状态机
    ↓
PostgreSQL Schema 与 Migration（当前阶段）
    ↓
Repository + 乐观锁
    ↓
Message + Outbox 同事务
    ↓
Scheduler / Relay / Worker
```

## 3. 技术选择与替代方案

### 3.1 pgx/v5

项目只面向 PostgreSQL，因此选择 `pgx/v5`。它能够直接使用 PostgreSQL 类型、错误码、
事务和连接池。当前 integration test 通过 pgx 的 `database/sql` adapter 让 Goose 使用
同一个驱动；业务 Repository 将使用原生 pgx 接口。

没有选择通用 ORM，因为当前最重要的查询将包含乐观锁、部分索引、CTE 和
`FOR UPDATE SKIP LOCKED`。显式 SQL 更容易审查事务和锁语义。

### 3.2 Goose SQL Migration

采用顺序编号的 SQL Migration：

```text
db/migrations/sql/00001_create_delivery_core.sql
```

选择 SQL 而不是 Go Migration，是因为表、约束和索引本来就是 SQL，代码审查者可以
直接看到数据库变化。Goose 负责记录版本、按顺序执行以及 `up/down`。

Migration 不会由每个 API 副本在启动时自动执行。生产环境应把 Migration 作为独立
部署步骤，并让运行时账号只拥有必要的 DML 权限，避免多个副本并发执行 DDL。

### 3.3 Testcontainers + PostgreSQL 18.4

普通测试不需要 Docker：

```text
go test ./...
```

数据库集成测试使用独立 build tag：

```text
make test-integration
```

它启动一次性的 `postgres:18.4-alpine`。没有使用 SQLite 模拟，因为 PostgreSQL 的
UUID、JSONB、SQLSTATE、部分索引和后续行锁语义不能由 SQLite 可靠代表。

首次拉取镜像可能较慢，因此容器启动预算为 5 分钟；容器就绪后的数据库 Ping 仍只有
10 秒。两种超时分开，避免为了容忍镜像下载而让数据库连接失败也等待很久。

### 3.4 暂不引入 sqlc

本阶段只有 Migration，还没有 Repository 查询。现在引入 sqlc 只会增加工具链，不能
提供实际收益。Repository 查询稳定后再评估；即使使用 sqlc，事务和并发语义仍需要
人工设计。

## 4. 三张核心表

### 4.1 tenants

`tenants` 建立通用服务的租户边界，包含稳定 UUID、业务 key、状态、默认 locale 和
时间戳。

租户状态限定为：

```text
ACTIVE / PAUSED / DISABLED
```

### 4.2 mail_messages

当前表保存两类数据：

1. 租户级身份：`tenant_id + idempotency_key + payload_fingerprint`；
2. 状态机快照：status、时间、Attempt、generation、sequence、version 和脱敏失败信息。

核心约束包括：

- `(tenant_id, idempotency_key)` 唯一；
- fingerprint 必须正好 32 字节，对应 HMAC-SHA256；
- priority 只能为 0..9；
- status、category 和 duplicate risk policy 必须属于已知值；
- `attempt_count <= max_attempts`；
- deadline 晚于 acceptance；
- scheduled/retry time 早于 deadline；
- `RETRY_SCHEDULED` 必须有 `next_attempt_at`；
- 执行态必须有正数 generation，Attempt 态必须至少有一次 Attempt；
- Failure 的 category/code/retryable 必须同时存在或同时为空。

数据库约束不负责描述所有合法状态转换。例如它不会判断
`DELIVERED → QUEUED` 是否允许；这属于 Go 状态机。数据库只负责拒绝单行内部已经
损坏的快照。

### 4.3 outbox_events

Outbox 保存待发布的命令/事件指针，不保存邮件正文：

```text
aggregate_type
aggregate_id
event_type
aggregate_sequence
dispatch_generation
payload JSON object
status / available_at
lease_owner / lease_until
attempt_count
```

唯一键为：

```text
(aggregate_type, aggregate_id, event_type,
 aggregate_sequence, dispatch_generation)
```

这样同一个领域事件即使因应用层重试再次映射，也不能插入两条相同 Outbox。event type
必须进入唯一键，因为 `MESSAGE_STATUS_CHANGED` 和 `MESSAGE_DISPATCH_REQUESTED` 可以
共享同一个状态 sequence。

## 5. 索引设计

Scheduler 预留两个部分索引：

```text
mail_messages_scheduled_due_idx
mail_messages_retry_due_idx
```

它们只索引 `SCHEDULED` 或 `RETRY_SCHEDULED` 行，避免大量终态历史消息占据调度索引。
索引以时间和 ID 排序，ID 提供稳定的同时间排序。

Relay 预留：

```text
outbox_events_pending_available_idx
outbox_events_pending_lease_idx
```

前者寻找已经到达 `available_at` 的 Pending 事件，后者寻找需要处理的 lease。当前只建立
数据结构与索引，领取 SQL 和 `FOR UPDATE SKIP LOCKED` 将在 Scheduler/Relay 阶段实现。

## 6. 本地开发方式

```bash
cp .env.example .env
make db-up
make migrate-up
make migrate-status
```

停止容器：

```bash
make db-down
```

`db-down` 不带 `--volumes`，因此不会删除本地数据库卷。项目没有提供默认的数据库
清空命令，降低误删开发数据的风险。

## 7. 主要文件

- `compose.yaml`
- `.env.example`
- `db/migrations/embed.go`
- `db/migrations/sql/00001_create_delivery_core.sql`
- `internal/testkit/postgrescontainer/postgres.go`
- `internal/integration/postgres_migration_test.go`
- `Makefile`
- `go.mod`

## 8. 验证

实际执行并通过：

```text
docker compose config --quiet
make migrate-validate
make test-integration
make check-all
go vet ./...
go test -race ./...
go test -race -tags=integration ./internal/integration/...
```

集成测试实际验证：

- 数据库主版本为 PostgreSQL 18；
- Goose 记录 migration version 1；
- 同租户幂等键冲突，不同租户相同 key 可以共存；
- 非法状态、Attempt、retry time 和 fingerprint 被具体 CHECK constraint 拒绝；
- 重复 Outbox identity 被唯一约束拒绝；
- Outbox payload 必须为 JSON object；
- Scheduler/Relay 所需索引存在；
- `down` 后三张业务表消失；
- 再次 `up` 后三张表恢复。

测试不只判断“SQL 执行失败”，还检查 PostgreSQL SQLSTATE 和 constraint name，确保失败
确实来自预期规则，而不是语法错误、连接错误或其他约束。

## 9. 面试表达

### 30 秒版本

> 我先把领域状态机映射为 PostgreSQL 的版本化 Schema，使用 Goose 管理 SQL
> Migration，用 CHECK 和唯一约束保护单行不变量与租户级幂等，同时为 Scheduler 和
> Outbox Relay 建立部分索引。数据库集成测试通过 Testcontainers 运行真实 PostgreSQL
> 18，验证 migration up/down、SQLSTATE、约束和索引；Repository 和事务 Outbox 留到
> 下一小步实现。

### 2 分钟展开重点

1. Go 状态机负责“能否转换”，数据库 CHECK 负责“快照是否损坏”；
2. `(tenant_id, idempotency_key)` 为什么必须包含 tenant；
3. Outbox 唯一键为什么需要 event type、sequence 和 generation；
4. 部分索引为什么不把终态历史消息全部放进调度索引；
5. 为什么用真实 PostgreSQL 测试而不是 SQLite；
6. 为什么应用副本启动时不自动执行 Migration。

### 可能追问

**有了 Go 校验，为什么数据库还要 CHECK？**

未来会有多个写入入口、迁移脚本和运维修复。数据库约束是最后一道防线，也能尽早
暴露 Repository 映射错误。但复杂状态转换继续放在领域层，避免把业务流程拆散到大量
Trigger 中。

**为什么 status 不使用 PostgreSQL ENUM？**

服务只有 PostgreSQL，但状态仍可能演进。`text + CHECK` 保留数据库校验，同时新增或
回滚状态比修改 ENUM 更直接。代价是占用略多空间，但不是当前瓶颈。

**为什么不用内存数据库做集成测试？**

本阶段要证明的正是 PostgreSQL 行为，包括 JSONB、部分索引、constraint name 和
SQLSTATE。替代数据库只能证明 SQL 的一个近似版本。

**为什么当前 Outbox 没有外键到 mail_messages？**

Outbox 是通用表，未来还会承载 Attempt、Delivery Event 和通知等不同 aggregate。它用
`aggregate_type + aggregate_id` 建立逻辑引用，Repository 的同事务写入和唯一约束保护
一致性。业务查询不依赖跨类型外键。

## 10. 尚未解决

- Repository 的 Create/Load/Save；
- version 乐观锁和并发冲突测试；
- Message Snapshot 与 SQL 行之间的映射；
- 领域事件到 Outbox payload 的映射；
- `Message + Outbox` 同事务及回滚测试；
- 加密收件地址、模板变量和密钥版本；
- Scheduler/Relay 的 `FOR UPDATE SKIP LOCKED` 领取；
- RabbitMQ 发布和 Worker 消费。

下一小步是 04-B：实现 PostgreSQL Repository、Snapshot 映射和 version 乐观锁。
