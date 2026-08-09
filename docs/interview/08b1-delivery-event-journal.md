# 08-B1：Delivery Event Journal 与可靠通知事实

## 1. 这一阶段解决什么问题

邮件当前状态保存在 `mail_messages`，但它只能回答“现在是什么”，不能完整回答：

- 先后经历过哪些状态；
- 某个状态发生在什么时候；
- AI-Nexus 漏掉了哪一条通知；
- 同一 `message_id` 的事件是否乱序；
- 运维对账应该重放哪个稳定 `event_id`。

08-B1 新增不可变 `delivery_events` Journal，并保证一次领域状态事务同时提交：

```text
MailMessage 当前快照
+ Delivery Event Journal
+ Transactional Outbox 生命周期事件
```

本阶段只建立通知事实与可靠出口，08-B2 再消费 lifecycle queue 并调用 AI-Nexus。

## 2. Journal、Outbox 和 Message Snapshot 的区别

三者解决不同问题：

| 数据 | 回答的问题 | 是否可变 |
| --- | --- | --- |
| `mail_messages` | 当前状态是什么 | 状态机推进时更新 |
| `delivery_events` | 历史上发生过什么 | 只追加，不覆盖 |
| `outbox_events` | 哪个事实还需要可靠发布 | 发布状态、lease、attempt 可变 |

Journal 不能替代 Outbox：仅写历史不会自动可靠送到 RabbitMQ。Outbox 也不能替代 Journal：
它的职责是传输，包含 lease、重试和发布状态，不应该成为业务历史查询模型。

## 3. 为什么没有新建 Notification Outbox 表

现有通用 `outbox_events` 已按事件类型路由：

```text
MESSAGE_DISPATCH_REQUESTED
  → mail.dispatch.v1.q

MESSAGE_ACCEPTED / MESSAGE_STATUS_CHANGED
  → mail.lifecycle.v1.q
```

因此 lifecycle 事件本来就是 Notification Outbox。再建一张 `notification_outbox` 会导致同一
状态产生两套 lease、重试和发布实现，增加一致性与运维成本。08-B1 复用已验证的 Relay、
Publisher Confirm 和 RabbitMQ Quorum Queue，只补充独立 Journal。

## 4. Delivery Event 数据模型

Migration 4 新增：

```text
delivery_events
  id                    uuid primary key
  tenant_id             uuid
  message_id            uuid
  idempotency_key       varchar
  status                varchar
  sequence              bigint
  attempt_number        int
  provider_message_id   varchar nullable
  failure_*             nullable sanitized fields
  occurred_at           timestamptz
  observed_at           timestamptz
```

唯一约束：

```text
UNIQUE (message_id, sequence)
```

状态机每次状态变化都会递增 sequence，因此一个 Message 的 Journal 可以严格按 sequence 排序。
`MESSAGE_DISPATCH_REQUESTED` 不改变状态且复用当前 sequence，只进入 dispatch Outbox，不进入
Delivery Event Journal。

Journal 保存 `idempotency_key`，因为 AI-Nexus 的回调需要用它找到自己的验证码请求；保存的
failure 只有稳定 category/code/retryable，不保存 Provider 原始响应或敏感正文。

## 5. 稳定 event_id

此前 Outbox 每次映射使用随机 UUID。事务失败后重新执行相同状态映射，虽然数据库唯一约束
能防止重复事实，但可能生成另一个 ID，不适合作为跨系统回调幂等键。

现在使用 namespaced UUID v5 风格的 SHA-1 名称 UUID：

```text
event_id = UUID(namespace,
  message_id + event_kind + sequence + dispatch_generation)
```

同一领域事实无论映射多少次都得到同一个 ID；不同事件 kind 即使共享 sequence，例如
`QUEUED` 状态事件与 `DISPATCH_REQUESTED` 命令，也得到不同 ID。

这个 `event_id` 同时用于：

- `delivery_events.id`；
- `outbox_events.id`；
- AMQP `message_id`；
- 08-B2 的 AI-Nexus 回调 `event_id`。

UUID 中的 SHA-1 这里只用于确定性名称映射，不用于密码学签名、密码或内容完整性。事件 ID
不是秘密，安全认证仍由未来 mTLS 负责。

## 6. 为什么 Journal 和 Outbox 必须同事务

如果分开提交，会出现两类永久不一致：

```text
Journal 成功，Outbox 失败
  → 系统知道状态发生过，但 AI-Nexus 永远收不到

Outbox 成功，Journal 失败
  → AI-Nexus 收到事件，本地却没有可查询和对账的历史事实
```

现在受理、Scheduler、Worker claim 和 Worker finalize 的所有状态路径都使用同一 UnitOfWork：

1. 状态机产生 pending domain events；
2. 映射稳定 event ID；
3. 保存 Message snapshot；
4. 追加 Journal lifecycle events；
5. 追加全部 Outbox events；
6. commit 后才清空 pending events。

任何一步失败，PostgreSQL 回滚全部变化。

## 7. Journal 自身如何幂等

Repository 使用：

```sql
INSERT ...
ON CONFLICT ON CONSTRAINT delivery_events_message_sequence_unique
DO NOTHING
```

冲突后不会直接认为成功，而是读取已有事件并比较 event ID、tenant、status、attempt、failure
和时间：

- 完全相同：幂等成功；
- 相同 `message_id + sequence` 但内容不同：`ErrDeliveryEventConflict`。

后者表示领域不变量或数据已经被破坏，Scheduler/Worker 将其视为 poison/fatal，而不是无限
重试掩盖问题。

## 8. 主要代码

- `db/migrations/sql/00004_create_delivery_events.sql`：Journal、约束与查询索引；
- `internal/application/ports/delivery_event_repository.go`：安全事件模型与 Repository Port；
- `internal/storage/postgres/delivery_event_*`：幂等追加与冲突比较；
- `internal/application/delivery/reliable_message_store.go`：稳定 ID 和双写映射；
- `internal/application/delivery/email_submission.go`：受理事务追加 Journal；
- `internal/application/delivery/due_message_scheduler.go`：批量状态事务追加 Journal；
- `internal/application/delivery/dispatch_worker.go`：claim/finalize 事务追加 Journal；
- `internal/storage/postgres/transaction_manager.go`：UnitOfWork 暴露 DeliveryEvents；
- `internal/integration/runtime_composition_test.go`：完整状态序列验证。

## 9. 如何验证

```bash
go test ./...
go test -race ./...
go vet ./...
make migrate-validate
```

真实基础设施测试验证：

- 首次受理生成 `ACCEPTED`、`QUEUED` 两条 Journal；
- 三条初始 Outbox 中只有两条 lifecycle event 与 Journal 共用 ID；
- 重复提交不增加 Message、Journal 或 Outbox；
- 同一 Journal 重复 Append 是幂等成功；
- 相同 message/sequence 更换状态返回冲突；
- 完整链路最终恰好形成：

```text
1 ACCEPTED
2 QUEUED
3 SENDING
4 PROVIDER_ACCEPTED
```

- 最终只有一个 Delivery Attempt，且没有 Pending Outbox。

## 10. 面试表达

### 30 秒版本

> 我在当前状态快照之外增加了不可变 Delivery Event Journal，用于审计、查询和通知对账。
> Message、Journal 和生命周期 Outbox 在每次状态事务中原子提交。现有 Outbox 已经把 lifecycle
> 事件路由到独立队列，所以没有重复建设第二张通知 Outbox。Journal 与 Outbox 共用确定性的
> event ID，下游可以按 event ID 幂等，按 message ID + sequence 防乱序。

### 2 分钟版本

> `mail_messages` 只保存当前状态，无法证明完整历史；Outbox 又是传输状态，不适合做业务审计，
> 所以我新增 append-only 的 delivery_events。一个状态变化会在同一 PostgreSQL UnitOfWork 中
> 更新 Message、追加 Journal、追加 Outbox，任一步失败全部回滚。
>
> 我没有再建 notification_outbox，因为现有 outbox 已按 dispatch 和 lifecycle 两条 RabbitMQ
> 路由隔离，Relay、lease 和 Publisher Confirm 都可以复用。为支持跨系统至少一次回调，我把
> event ID 改为基于 message、kind、sequence、generation 的确定性 namespace UUID，同一事实
> 重算 ID 不变；Journal 和 Outbox 使用同一个 ID。
>
> Journal Repository 通过 message ID + sequence 唯一约束幂等追加，冲突后还会比较完整安全
> 字段，只有语义相同才成功，内容不同视为不变量错误。真实链路验证了从 ACCEPTED 到
> PROVIDER_ACCEPTED 恰好四条连续事件。

## 11. 可能追问

**为什么既保存 Journal 又保留 Message 当前状态，不通过 Event Sourcing 重放？**

当前状态是高频调度和 Worker 判断路径，直接读取快照简单且高效。Journal 用于审计、通知和
对账，不把整个系统升级成 Event Sourcing，可以避免重放版本、Snapshot 重建和事件迁移复杂度。

**Outbox 发布以后可以删除吗？**

不能把删除 Outbox 当成删除业务历史。Journal 是长期事实；Outbox 可以根据运维保留策略归档
或清理已发布记录，但必须覆盖排障和对账窗口，并确保 Journal 与下游状态不受影响。

**sequence 和 event_id 分别解决什么？**

`event_id` 解决重复投递：同一事件重复到达只处理一次。`sequence` 解决乱序：即使新旧事件 ID
都不同，订阅方也不能让较小 sequence 覆盖较新业务状态。

## 12. 尚未解决

- 08-B2 lifecycle RabbitMQ Consumer 与 gRPC Callback Client；
- 租户订阅地址和 mTLS 凭据控制面；
- 回调退避、delivery limit、独立 DLQ 与人工重放；
- `ListEmailEvents` 的分页 Query Adapter；
- Provider webhook 产生的 DELIVERED/BOUNCED/COMPLAINED Journal；
- Journal/Outbox 数据保留和归档策略；
- AI-Nexus 端 event ID 幂等与 sequence 防倒退实现。
