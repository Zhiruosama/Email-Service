# 阶段 05：Transactional Outbox 原子持久化

- 状态：已完成
- 阶段目标：在同一个 PostgreSQL 事务中保存 Message 与其领域事件，并用真实故障测试证明不会出现只成功一半

## 1. 解决的问题

一封立即发送的邮件在受理时至少涉及两类数据：

```text
mail_messages：系统内的权威状态
outbox_events：后续需要跨进程传播的事件或派发命令
```

如果先写 Message，再单独发布 RabbitMQ，会出现经典双写问题：

```text
数据库成功，MQ 失败       → 任务已经受理，却永远没有 Worker 来发送
MQ 成功，数据库失败       → Worker 收到一个数据库里不存在的任务
数据库成功，进程随后崩溃 → 调用方不知道是否应该重试
```

PostgreSQL 和 RabbitMQ 之间没有一个值得引入的统一事务。本阶段把“待发布的事实”先写入
PostgreSQL Outbox，使可靠受理阶段只依赖一个本地数据库事务。

## 2. 为什么现在做

这一阶段依赖前面的三块能力：

1. 状态机产生不可变领域事件；
2. Migration 已建立 Message 和 Outbox 表、唯一约束；
3. MessageRepository 同时兼容 `pgxpool.Pool` 和 `pgx.Tx`，并且不会提前清空领域事件。

Scheduler、Relay 和 Worker 都依赖可靠的 Outbox 输入。如果没有先冻结事务边界，后面
直接接 RabbitMQ，故障时很难判断丢失发生在状态推进、事件生成还是消息发布阶段。

## 3. 整体设计

应用层的原子保存流程是：

```text
Message 状态机
    │ 产生 PendingEvents
    ▼
ReliableMessageStore
    │
    ├── 事务外：校验 MessageRecord，映射并校验全部 OutboxEvent
    │
    └── TransactionManager.WithinTransaction
            ├── tx-bound MessageRepository.Create / Save
            ├── tx-bound OutboxRepository.Append(all events)
            └── COMMIT
                    │
                    └── 成功后才 PullEvents
```

只要任意一步失败，PostgreSQL 回滚全部写入。RabbitMQ 不参与这个事务，下一阶段由
Outbox Relay 异步发布。

## 4. 为什么需要 Transactor 与 UnitOfWork

应用层不能直接依赖 `pgx.Tx`，否则业务编排会和 PostgreSQL 驱动绑死。本阶段在
application ports 中定义：

```go
type Transactor interface {
    WithinTransaction(context.Context, TransactionFunc) error
}

type UnitOfWork interface {
    Messages() MessageRepository
    Outbox() OutboxRepository
}
```

它们的职责不同：

- `Transactor` 管理事务生命周期：Begin、Commit、Rollback；
- `UnitOfWork` 提供绑定到同一个事务的 Repository；
- `ReliableMessageStore` 只决定“哪些业务写入必须原子完成”；
- PostgreSQL Adapter 才知道底层使用的是 `pgx.Tx`。

这里的 Unit of Work 不是缓存所有实体的重型 ORM 模式，只是一个很薄的事务能力集合。
这样未来增加 AttemptRepository 时，可以把 Message、Attempt 和 Outbox 放进同一事务，
不需要让应用层导入数据库驱动。

## 5. 领域事件如何映射为 Outbox

当前保存三种事件：

```text
MESSAGE_ACCEPTED
MESSAGE_STATUS_CHANGED
MESSAGE_DISPATCH_REQUESTED
```

它们全部落 Outbox，但后续用途不同：

- `MESSAGE_DISPATCH_REQUESTED` 由 Relay 路由为 Worker 派发命令；
- `MESSAGE_ACCEPTED` 和 `MESSAGE_STATUS_CHANGED` 可进入审计、订阅通知或其他事件出口；
- 当前阶段只负责可靠持久化，还没有发布 RabbitMQ。

Outbox identity 是：

```text
(aggregate_type,
 aggregate_id,
 event_type,
 aggregate_sequence,
 dispatch_generation)
```

`event_type` 必须在 identity 中，因为同一次状态推进可以同时产生
`MESSAGE_STATUS_CHANGED` 和 `MESSAGE_DISPATCH_REQUESTED`，二者共享 sequence 和
generation，但语义不同。

## 6. 为什么 Outbox 不保存邮件内容

Outbox 会被 Relay 扫描、进入日志和监控，也可能停留较长时间。把收件邮箱、验证码、
模板变量或正文复制进去，会扩大敏感数据泄漏面，也会造成多份数据的清理困难。

当前 payload 使用显式白名单 envelope：

```text
schema_version
tenant_id
message_id
event_type
from / to（这里是状态，不是邮箱地址）
occurred_at
sequence
dispatch_generation
attempt_number
provider_message_id（可选）
reason_code（可选）
failure category/code/retryable（可选、已归一化）
```

另外还限制 payload：

- 必须是 JSON object；
- 最大 64 KiB；
- aggregate 和 event identity 必须合法；
- Go `uint64` 写入 PostgreSQL BIGINT 前必须检查范围。

Worker 以后只从 MQ 获取安全指针，再按 Message ID 从权威存储读取发送所需数据。

## 7. Outbox 幂等与冲突检测

OutboxRepository 使用：

```sql
INSERT INTO outbox_events (...)
VALUES (...)
ON CONFLICT ON CONSTRAINT outbox_events_identity_unique DO NOTHING
RETURNING id;
```

使用 `DO NOTHING` 而不是先 SELECT 或捕获唯一异常，原因与 Message 幂等创建相同：

- 先 SELECT 再 INSERT 存在并发竞态；
- 唯一约束异常会让当前 PostgreSQL 事务进入 aborted 状态；
- `DO NOTHING` 可以在冲突后继续检查已有记录。

冲突后不是无条件当成功，而是执行 JSONB 语义比较：

```sql
SELECT payload = $payload::jsonb
FROM outbox_events
WHERE identity = ...;
```

因此结果分为：

```text
同 identity + 同 JSON 语义 → 幂等成功，不新增记录
同 identity + 不同 payload → ErrOutboxConflict
同 event ID + 不同 identity → ErrOutboxIDConflict
```

JSONB 比较不受对象 key 顺序和无意义空白影响，例如 `{"a":1,"b":2}` 与
`{"b":2,"a":1}` 被视为相同。这可以区分安全重试和真正的事件确定性错误。

## 8. 为什么提交后才 PullEvents

`PendingEvents` 是本次状态变化还没有可靠持久化的事实。错误顺序是：

```text
保存 Message
→ PullEvents
→ Outbox 失败
→ 事务回滚
```

此时数据库没有成功，但内存事件已经消失。正确顺序是：

```text
读取 PendingEvents 并映射
→ 保存 Message
→ 写全部 Outbox
→ COMMIT
→ PullEvents
```

所以：

- Commit 成功：清空事件；
- Message 乐观锁冲突：保留事件；
- Outbox 写失败：保留事件；
- Transaction callback 失败或 panic：回滚并保留事件。

当前 Message 是请求级对象，失败后通常重新加载数据库 Snapshot 并重新执行状态机。
保留事件首先是为了保证对象不会谎称“已持久化”，不是鼓励直接重复保存旧 Snapshot。

## 9. Create 与 Save 的事务语义

### 9.1 Create

```text
BEGIN
  INSERT Message（租户 + 幂等键原子判定）
  CREATED   → INSERT 所有 PendingEvents
  DUPLICATE → 不创建新的 Outbox
COMMIT
```

同一个幂等请求重试时返回已有 Message，不会因为本次内存聚合再次产生事件而复制
Outbox。相同幂等键但 fingerprint 不同仍返回 `ErrIdempotencyConflict`。

### 9.2 Save

```text
BEGIN
  UPDATE Message WHERE id = ? AND version = ?
  INSERT 所有 PendingEvents
COMMIT
```

如果 version 已过期，UPDATE 返回 `ErrConcurrentUpdate`，事务不会写入 Outbox。如果
Message 更新成功但 Outbox 失败，事务会把 Message version 和状态一并回滚。

## 10. TransactionManager 的失败处理

PostgreSQL 实现显式使用 `READ COMMITTED`，它已经足以保证单事务内原子写入；并发状态
覆盖由 Message version 乐观锁处理，不需要为每个命令提高到 SERIALIZABLE。

事务退出路径为：

| 路径 | 行为 |
| --- | --- |
| callback 返回 nil | Commit |
| callback 返回 error | Rollback，返回原错误 |
| callback panic | Rollback，重新抛出原 panic |
| Begin/Commit/Rollback 驱动异常 | 映射为稳定 `ErrTransaction` |

Rollback 使用独立、最长 5 秒的后台 context。原因是业务 context 可能已经超时或取消，
但连接仍需要尽力结束事务并归还连接池。panic 不会被吞掉或改写成普通错误。

## 11. 错误隔离

应用层可识别的稳定错误包括：

```text
ErrInvalidOutboxEvent
ErrOutboxConflict
ErrOutboxIDConflict
ErrOutboxRepository
ErrTransaction
ErrNoPendingMessageEvents
ErrMessageEventMapping
```

PostgreSQL 驱动错误保留为内部 cause，便于日志和 tracing 诊断，但不进入公开 unwrap 链，
避免上层 RPC 契约依赖 SQLSTATE、constraint name 或 pgx 类型。context canceled 和
deadline exceeded 仍然可以被调用方识别。

## 12. 主要文件

- `internal/application/ports/outbox_repository.go`
- `internal/application/delivery/reliable_message_store.go`
- `internal/application/delivery/reliable_message_store_test.go`
- `internal/storage/postgres/outbox_repository.go`
- `internal/storage/postgres/outbox_queries.go`
- `internal/storage/postgres/transaction_manager.go`
- `internal/integration/transactional_outbox_test.go`

## 13. 故障矩阵

| 故障或重复场景 | 已验证结果 |
| --- | --- |
| 立即邮件首次 Create | Message 与 3 个事件一起提交 |
| 定时邮件首次 Create | Message 与 Accepted/StatusChanged 一起提交，没有提前派发 |
| 相同 Submit 重试 | 返回 DUPLICATE，不增加 Outbox |
| Create 的 Outbox INSERT 失败 | Message 和 Outbox 全部回滚，pending events 保留 |
| Save 的 version 冲突 | Message 不被覆盖，不写 Outbox |
| Save 后 Outbox INSERT 失败 | Message 状态/version 回滚，pending events 保留 |
| 相同 identity、相同 JSONB | 幂等成功，数据库只有一行 |
| 相同 identity、不同 payload | 整个事务以 ErrOutboxConflict 回滚 |
| callback 返回错误 | callback 内写入全部回滚 |
| callback panic | 写入回滚，原 panic 继续向上抛出 |

集成测试用 PostgreSQL trigger 主动让 Outbox INSERT 报错。这比 mock 一个 Repository
返回 error 更有价值，因为它验证的是真实数据库事务能否撤销此前已经执行的 Message
INSERT/UPDATE。

## 14. 验证

本阶段已实际执行并通过：

```text
gofmt -w internal/application/delivery internal/application/ports internal/storage/postgres internal/integration/transactional_outbox_test.go
go mod tidy
go test ./...
go vet ./...
make test-integration
TEST_POSTGRES_IMAGE=postgres:18.4-alpine go test -count=1 -tags=integration ./internal/integration/...
make migrate-validate
docker compose config --quiet
make check-all
go vet -tags=integration ./internal/integration/...
go test -race ./...
TEST_POSTGRES_IMAGE=postgres:18.4-alpine go test -count=1 -race -tags=integration ./internal/integration/...
go test -count=1 -cover ./internal/application/delivery ./internal/application/ports ./internal/storage/postgres
TEST_POSTGRES_IMAGE=postgres:18.4-alpine go test -count=1 -tags=integration -coverpkg=./internal/application/delivery,./internal/application/ports,./internal/storage/postgres ./internal/integration/...
```

最后一条命令禁用 Go 测试缓存，在真实 PostgreSQL 18.4 容器中完成，耗时约 25.6 秒。
真实 PostgreSQL race 测试耗时约 28.1 秒且未发现数据竞争。由 integration package 驱动
的 application delivery、application ports 和 PostgreSQL Adapter 跨包语句覆盖率为
`73.3%`；普通单元测试中三个包分别为 `42.6%`、`95.3%` 和 `20.9%`。storage 的普通
单元覆盖率较低是因为主要 SQL 正确性与事务语义由真实 PostgreSQL 测试覆盖，不能用
mock 的成功返回代替。

## 15. 面试表达

### 30 秒版本

> 邮件受理同时要保存权威 Message 和待发布事件，直接写数据库再发 MQ 会有双写不一致。
> 我用 Transactional Outbox，让应用层通过 Transactor 和 UnitOfWork 在同一个 pgx 事务中
> 写 Message 与全部领域事件，Commit 后才清空 pending events。Outbox identity 有唯一
> 约束，重复时再比较 JSONB，区分安全重试和内容冲突。真实 PostgreSQL 故障测试证明，
> Outbox 写失败、乐观锁冲突或 panic 时不会留下半套数据。

### 2 分钟版本

1. 先用数据库成功/MQ 失败解释双写窗口；
2. 说明 API 成功只依赖 PostgreSQL，Relay 后续异步发 MQ；
3. 说明 Transactor 管事务、UnitOfWork 暴露同 Tx Repository；
4. 说明全部事件先映射校验，Message 和 Outbox 再同事务写入；
5. 说明 `ON CONFLICT DO NOTHING` 避免事务 aborted；
6. 说明 JSONB equality 如何区分幂等与确定性冲突；
7. 说明为什么 Commit 后才 PullEvents；
8. 用 trigger 故障注入证明 Create 和 Save 都能完整回滚；
9. 最后明确 Outbox 解决的是可靠记录，不提供端到端 Exactly Once。

### 可能追问

**有了 Outbox，为什么还会重复？**

Relay 发布成功后、把 Outbox 标记为 PUBLISHED 前可能崩溃，所以恢复后会再次发布。系统
选择 At Least Once，后续 Worker 仍需使用 event identity、Message 状态和 generation
幂等消费。

**为什么不使用 RabbitMQ 事务或分布式事务？**

它们不能消除 SMTP 这类外部副作用的不确定性，还会提高可用性和运维复杂度。本地
PostgreSQL 事务加可重试 Relay 更容易验证，且符合当前可靠性目标。

**Outbox 是邮件内容吗？**

不是。它是待发布事件/命令的安全 envelope，只含 Message ID、状态序号、generation
等必要字段。正文、收件地址和验证码仍由受保护的权威存储管理。

**为什么不用 SERIALIZABLE？**

当前事务只要求 Message 和 Outbox 原子提交；同 Message 并发写由 version 乐观锁检测。
提高隔离级别会增加序列化失败和重试成本，却不替代业务状态机。

**为什么事件映射放在事务外？**

映射和 JSON 校验不需要数据库，放在事务外可以缩短连接与事务占用时间。真正影响
原子性的 Message/Outbox SQL 仍全部在事务内。

## 16. 尚未解决

- Outbox Relay 的批量领取、lease、publisher confirm 和发布状态更新；
- Scheduler 使用 `FOR UPDATE SKIP LOCKED` 抢占到期 Message；
- RabbitMQ topology 和重复消费处理；
- gRPC SubmitEmail 到 ReliableMessageStore 的应用服务编排；
- 收件地址、模板变量的加密存储与模板版本解析；
- Provider 发送、Attempt、重试、熔断和状态通知。

下一阶段将先实现 Scheduler 与 Outbox Relay 的数据库领取边界，再接 RabbitMQ Adapter。
