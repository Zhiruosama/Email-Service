# 阶段 06-B：Outbox Relay 与 Lease Fencing

- 状态：已完成
- 阶段目标：安全地领取 PENDING Outbox，在数据库事务外调用 Publisher，并可靠记录成功、重试、死信和 Lease 丢失

## 1. 解决的问题

Transactional Outbox 解决了 Message 和“待发布事实”的数据库双写，但事件仍然只在
`outbox_events` 表里。需要一个后台 Relay 把它们交给传输系统：

```text
Message + Outbox COMMIT
          │
          ▼
    PENDING Outbox
          │
          ▼
        Relay
          │
          ▼
 Publisher / RabbitMQ
```

如果 Relay 只执行 `SELECT → Publish → UPDATE`，会遇到：

- 多个实例同时发布同一事件；
- 持有数据库事务等待网络，形成长事务；
- 进程领取后崩溃，事件永久卡住；
- 旧发布请求晚返回，覆盖新实例的处理结果；
- Publish 成功但数据库未标记，恢复后必须决定是否重发；
- Broker 原始错误进入数据库或公开错误链，造成信息泄漏。

本阶段完成数据库 Lease、fencing、Publisher 端口、失败分类、退避和可控 Fake Publisher。
当时留给 06-C 的真实 RabbitMQ Adapter 现已完成，详见
[RabbitMQ Publisher](06c-rabbitmq-publisher.md)。

## 2. 三段式处理流程

Relay 不能把网络 I/O 放进数据库事务，因此一次发布分为三段：

```text
阶段 A：领取事务
BEGIN
  FOR UPDATE SKIP LOCKED
  设置 lease_owner / lease_until
COMMIT

阶段 B：事务外网络调用
Publisher.Publish(event)
等待 transport confirm 或 timeout

阶段 C：结果事务
BEGIN
  WHERE event_id + lease_token + expected_attempt
  标记 PUBLISHED / 重调度 / DEAD_LETTERED
COMMIT
```

真实 PostgreSQL 测试让 Fake Publisher 从另一个连接查询 `lease_owner`，证明 Publisher
被调用时领取事务已经提交，Lease 对其他连接可见。

## 3. 为什么不能持有事务等待 Publisher

错误设计是：

```text
BEGIN
SELECT FOR UPDATE
调用 RabbitMQ
等待 Confirm
UPDATE PUBLISHED
COMMIT
```

RabbitMQ 卡顿 5 秒，就意味着：

- PostgreSQL 行锁持有 5 秒；
- 数据库连接占用 5 秒；
- VACUUM、取消、运维查询和其他 Relay 可能受到长事务影响；
- Broker 故障会反过来拖垮 PostgreSQL。

当前领取和结果写入都是短事务。网络调用只持有一个有过期时间的逻辑 Lease，不持有
PostgreSQL 事务或行锁。

## 4. 原子领取 SQL

领取使用一个 CTE UPDATE：

```sql
WITH candidates AS (
    SELECT id
    FROM outbox_events
    WHERE status = 'PENDING'
      AND available_at <= transaction_timestamp()
      AND (
          lease_until IS NULL
          OR lease_until <= transaction_timestamp()
      )
    ORDER BY available_at, created_at, id
    LIMIT $batch_size
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox_events AS event
SET
    lease_owner = $claim_token,
    lease_until = transaction_timestamp() + $lease_duration
FROM candidates
WHERE event.id = candidates.id
RETURNING ...;
```

它比“先 SELECT ID，再逐条 UPDATE”更可靠：候选选择和 Lease 写入在一个事务/语句中，
其他实例不会在两步之间抢到相同事件。

查询只领取：

- `status = PENDING`；
- 已经到 `available_at`；
- 没有 Lease，或 Lease 已经过期。

`SKIP LOCKED` 不保证一次返回数量一定填满 LIMIT；存在短暂锁时可以返回较短批次。Relay
是循环角色，下一轮继续领取，而不是为了填满批次等待其他事务。

## 5. Lease 与数据库行锁的区别

```text
数据库行锁：只在事务内存在，连接断开或事务回滚后自动释放
Lease：事务提交后仍存在，到 lease_until 后允许其他实例恢复
```

Scheduler 的工作全部在短事务内，所以行锁足够。Relay 跨事务等待网络，所以必须用
Lease 表示“当前发布权”。

Lease 过期不表示旧 Publisher 一定停止了。网络请求可能已经到 Broker，或者旧请求可能
晚返回，因此还需要 fencing。

## 6. 为什么 lease_owner 实际存 Claim Token

如果 `lease_owner` 只存固定 hostname：

```text
实例 relay-1 领取事件
→ Lease 过期
→ 同一实例下一轮重新领取
→ 第一次的迟到结果和第二次使用相同 owner
```

旧结果仍可能误更新新一轮 Lease。

当前每批生成：

```text
instance_id / random_claim_uuid
```

例如：

```text
relay-a/2f2d4b58-...
```

即使由同一实例重新领取，claim UUID 也不同。完成 SQL 必须同时匹配：

```sql
WHERE id = $event_id
  AND status = 'PENDING'
  AND lease_owner = $claim_token
  AND attempt_count = $expected_attempt - 1
```

如果新实例已经改写 token，旧实例影响 0 行并得到 `ErrOutboxLeaseLost`。这就是 fencing：
不是阻止旧请求返回，而是阻止旧请求修改新所有者的状态。

## 7. 三种结果状态

### 7.1 发布成功

Publisher 返回 nil 的契约是“传输系统已经确认接管”，随后：

```text
status = PUBLISHED
published_at = transaction_timestamp()
attempt_count = expected_attempt
last_error_code = NULL
Lease 清空
```

RabbitMQ Adapter 以后只有收到 Publisher Confirm 才能返回 nil。

### 7.2 临时失败

网络、Broker 不可用、Confirm timeout 等可重试失败：

```text
status 保持 PENDING
attempt_count = expected_attempt
available_at = database_now + backoff
last_error_code = 稳定脱敏码
Lease 清空
```

未到新的 `available_at` 前不会再次领取。

### 7.3 永久失败或重试耗尽

不可路由、确定的配置错误，或达到 `MaxAttempts`：

```text
status = DEAD_LETTERED
attempt_count = expected_attempt
last_error_code = 稳定脱敏码
Lease 清空
```

当前只完成状态落库，生产告警、查询和人工重放控制面尚未实现。

## 8. attempt_count 的精确定义

`attempt_count` 在成功、重调度或死信结果落库时更新，而不是领取时更新。

原因是：

```text
领取成功
→ 进程在调用 Publisher 前崩溃
```

这时没有证据证明发生过发布，不能仅因 Relay 进程崩溃就消耗业务事件的重试额度。

但它也意味着 `attempt_count` 不是精确物理发送次数。最危险窗口是：

```text
Publisher 已 Confirm
→ Relay 崩溃
→ MarkPublished 没执行
```

数据库 attempt_count 不增加，Lease 过期后会再次发布。系统只能承诺 At Least Once，
无法同时精确知道第一次物理发布是否已经发生。Metrics 应分别记录 Publisher 调用和
数据库已记录结果，不把两者混为一个数字。

## 9. Publisher 端口与错误隔离

应用层端口为：

```go
type OutboxPublisher interface {
    Publish(context.Context, OutboxPublication) error
}
```

它不依赖 RabbitMQ 客户端。`OutboxPublication` 只含安全 Outbox event 和 attempt number。

Publisher 使用 `OutboxPublishError` 返回：

```text
Code：稳定、1..128 byte 的机器码
Retryable：是否可重试
Cause：仅供内部观测
```

Cause 不进入 `errors.Unwrap` 链，数据库和上层只看到例如：

```text
BROKER_UNAVAILABLE
UNROUTABLE
PUBLISH_TIMEOUT
PUBLISH_INTERNAL
```

错误码只允许字母、数字、点、下划线、冒号和短横线。Broker 原始响应、地址、payload 或
凭据不能作为 error code 存入 Outbox。

未分类普通 error 默认映射为可重试 `PUBLISH_INTERNAL`，而不是把 `err.Error()` 写入库。

## 10. 超时与取消

每次 Publisher 调用有独立 `PublishTimeout`，并且必须短于 LeaseDuration。

- Publisher context 超时：记录 `PUBLISH_TIMEOUT` 并按策略重试；
- Publisher 明确返回永久错误：进入死信；
- Relay 父 context 被取消：不再用已取消 context 开新结果事务，Lease 留到过期恢复；
- Publisher 返回 nil：相信 Publisher 的 confirm 契约，即使 deadline 在相邻时刻到达。

最后一条很重要：真正的 Adapter 必须保证 nil 只代表 Confirm 成功，不能“写入 socket 后”
就返回 nil。

## 11. Full Jitter 退避

默认策略实现：

```text
maximum = min(cap, base * 2^(attempt-1))
delay = random(0, maximum)
```

Full Jitter 避免 Broker 恢复时所有 Relay 事件在同一秒重试形成惊群。应用依赖
`OutboxRetryPolicy` 接口，集成测试使用固定 0 delay，不等待真实时间；生产使用
`FullJitterBackoff`。

策略结果还要经过边界校验，当前只允许 `0..24h`。异常策略不会写入非法时间，而是返回
`ErrInvalidOutboxRetryDelay` 并让 Lease 自然过期恢复。

## 12. 有界并发

配置同时限制：

```text
BatchSize
PublishConcurrency
LeaseDuration
PublishTimeout
MaxAttempts
```

Relay 领取一个批次后，用固定数量 worker 并发调用 Publisher。并发不能大于批次，避免
一次启动无界 goroutine。

LeaseDuration 需要覆盖正常排队、Publish timeout 和结果事务延迟。即使配置过短，fencing
仍能保证旧任务不能覆盖新 owner，但会产生更多重复发布和 `LeaseLost`，因此这些指标必须
进入后续可观测性。

不同事件的 Publisher 结果各自使用短事务记录，所以 Relay 不宣称整个发布批次原子。
`OutboxRelayResult` 分别返回：

```text
Claimed / Published / Retried / DeadLettered / LeaseLost
```

数据库故障时会保留已经完成的部分统计，并返回聚合 error；未完成事件等待 Lease 恢复。

## 13. 顺序语义

领取按：

```text
available_at, created_at, id
```

但多实例、并发 Publisher 和重试会导致跨事件乱序。系统不承诺 MQ 到达严格有序：

- 状态事件使用 aggregate sequence 防止倒退；
- dispatch command 使用 dispatch generation 拒绝旧代次；
- 消费者按 event identity 幂等。

不能因为数据库查询有 ORDER BY 就向业务承诺端到端顺序。

## 14. Fake Publisher

`internal/publisher/fake` 提供线程安全的可控 Publisher：

- 记录 publication 的隔离副本；
- handler 可以返回成功、临时失败、永久失败或等待 context timeout；
- handler 可以访问真实 PostgreSQL 检查 Lease 可见性；
- 不执行网络 I/O。

它让 Relay 的状态与崩溃语义先被验证。06-C 的 RabbitMQ Adapter 只需实现同一端口，
不能反向改变应用事务设计。

## 15. 故障矩阵

| 故障点 | 结果 |
| --- | --- |
| Claim 事务提交前崩溃 | Lease 回滚，其他实例立即可领取 |
| Claim 提交后、Publish 前崩溃 | Lease 过期后恢复，attempt 不增加 |
| Publish 临时失败 | attempt 增加，设置下一 available_at |
| Publish 永久失败 | DEAD_LETTERED |
| Publish 超时 | 结果不确定，按可重试处理 |
| Confirm 成功、MarkPublished 前崩溃 | Lease 过期后重复发布 |
| 旧 Publisher 在重新领取后返回 | token 不匹配，LeaseLost，不覆盖新 owner |
| 结果数据库事务失败 | 当前 Lease 保留，过期后恢复，可能重复 |
| Relay 父 context 取消 | 未完成 Lease 等待过期恢复 |

## 16. 主要文件

- `internal/application/ports/outbox_delivery.go`
- `internal/application/ports/outbox_delivery_test.go`
- `internal/application/delivery/outbox_relay.go`
- `internal/application/delivery/outbox_relay_test.go`
- `internal/application/delivery/outbox_backoff.go`
- `internal/storage/postgres/outbox_delivery_repository.go`
- `internal/storage/postgres/outbox_delivery_queries.go`
- `internal/storage/postgres/transaction_manager.go`
- `internal/publisher/fake/publisher.go`
- `internal/publisher/fake/publisher_test.go`
- `internal/integration/outbox_relay_test.go`

## 17. 验证

本阶段已经执行并通过：

```text
gofmt -w internal/application/delivery internal/application/ports internal/storage/postgres internal/publisher/fake internal/integration/outbox_relay_test.go
go mod tidy
go test ./...
go vet ./...
go test -tags=integration ./internal/integration/... -run '^$'
TEST_POSTGRES_IMAGE=postgres:18.4-alpine go test -count=3 -tags=integration ./internal/integration/... -run '^TestOutboxRelaySystem$/claims_only_available_events_and_fences_stale_owners'
TEST_POSTGRES_IMAGE=postgres:18.4-alpine go test -count=2 -tags=integration ./internal/integration/...
make migrate-validate
docker compose config --quiet
make check-all
go vet -tags=integration ./internal/integration/...
go test -race ./...
TEST_POSTGRES_IMAGE=postgres:18.4-alpine go test -count=1 -race -tags=integration ./internal/integration/...
go test -count=1 -cover ./internal/application/delivery ./internal/application/ports ./internal/storage/postgres ./internal/publisher/fake
TEST_POSTGRES_IMAGE=postgres:18.4-alpine go test -count=1 -tags=integration -coverpkg=./internal/application/delivery,./internal/application/ports,./internal/storage/postgres,./internal/publisher/fake ./internal/integration/...
```

真实 PostgreSQL 18.4 测试覆盖：

- available_at、活跃 Lease 和过期 Lease；
- claim database timestamp、token 和 attempt number；
- stale token/attempt 被 `ErrOutboxLeaseLost` 拒绝；
- `SKIP LOCKED` 与 batch size 组合；
- Publisher 调用时 Lease 已提交且对其他连接可见；
- success、retryable、permanent、timeout 和 retry exhausted；
- Confirm 成功但 MarkPublished 未执行后的重复发布；
- PUBLISHED/PENDING/DEAD_LETTERED 字段一致性。

测试过程中还验证了 `SKIP LOCKED` 不应被错误理解为“每批必定填满 LIMIT”：存在瞬时锁时
可以返回较短批次，下一轮继续领取才能构成完整恢复。

最终无缓存真实 PostgreSQL 功能测试约 30.1 秒，race 测试约 31.8 秒，均通过。由
integration package 驱动的 application delivery、application ports、PostgreSQL Adapter
和 Fake Publisher 跨包语句覆盖率为 `71.1%`。普通单元测试中四个包分别为 `31.3%`、
`84.3%`、`16.3%` 和 `94.1%`；领取、Lease、条件 UPDATE 和崩溃恢复的主要覆盖来自真实
PostgreSQL，而不是模拟数据库返回值。

## 18. 面试表达

### 30 秒版本

> 我把 Outbox Relay 设计为领取事务、事务外 Publish、结果事务三段，避免持有数据库锁
> 等待网络。领取使用 CTE + `FOR UPDATE SKIP LOCKED` 原子设置 Lease，每批生成唯一 claim
> token；结果更新必须匹配 event ID、token 和 expected attempt，旧请求晚返回只能得到
> LeaseLost。临时失败用 Full Jitter 重调度，永久失败或耗尽进入死信。Confirm 后、标记前
> 崩溃仍会重复发布，所以系统明确采用 At Least Once，消费者必须幂等。

### 2 分钟版本

1. 从 Outbox 仍在数据库、需要异步发布开始；
2. 解释为什么不能用长事务包住 RabbitMQ；
3. 画出 Claim Tx / Publish / Result Tx 三段；
4. 解释 Lease 负责恢复、fencing 负责拒绝旧 owner；
5. 展示 token + expected attempt 条件 UPDATE；
6. 解释成功、重试、死信三种落库结果；
7. 解释 attempt_count 为什么不是精确物理发送次数；
8. 用 Confirm 后崩溃说明 At Least Once；
9. 说明 Full Jitter、并发和稳定错误码；
10. 用真实数据库 Lease 可见性与旧 token 测试作证据。

### 可能追问

**为什么 Lease 过期后旧实例不能继续标记成功？**

它可以继续返回，但结果 UPDATE 必须匹配唯一 claim token。如果事件已被重新领取，token
已经变化，旧 UPDATE 影响 0 行并返回 LeaseLost。

**为什么 MarkPublished 不强制 lease_until > now？**

Lease 刚过期但还没有其他实例重新领取时，旧 owner 仍是数据库当前 owner，接受已经拿到
的 Confirm 可以避免无意义重复。如果新实例先领取，token 已变化，fencing 会拒绝旧结果。

**为什么不能保证 Exactly Once？**

RabbitMQ 与 PostgreSQL 没有共同事务。Confirm 成功后、数据库标记前进程可能崩溃；恢复
时无法证明 Broker 是否已经接管，只能重发并由消费者幂等。

**attempt_count 为什么不在 Claim 时加一？**

Claim 后、Publish 前可能崩溃，此时没有发生网络尝试的证据。当前只在结果落库时增加，
避免 Relay 自身崩溃耗尽业务事件重试额度。代价是它不等于不可观测窗口内的物理发送数。

**LeaseDuration 配太短会怎样？**

其他实例可能在旧 Publish 完成前重新领取，造成重复；旧结果会因 token 变化被拒绝，不会
破坏数据库状态。LeaseLost 和重复率会升高，因此要结合 batch、并发和 P99 Confirm 延迟
配置并告警。

## 19. 尚未解决

- RabbitMQ exchange/queue 声明、persistent message、mandatory、Publisher Confirm、按需
  重连和 Channel 生命周期已在 [06-C RabbitMQ Publisher](06c-rabbitmq-publisher.md) 完成；
- Relay role 的循环、空批次退避、优雅停机和 readiness；
- Outbox lag、LeaseLost、Publish latency 和 DLQ 告警；
- DEAD_LETTERED 查询、人工重放和权限审计；
- Worker 对 event identity、sequence 和 dispatch generation 的幂等消费。

本阶段的直接后继 06-C 保持了 Publisher 端口和事务语义，只增加真实 transport confirm、
mandatory return、持久消息和路由键映射。当前下一阶段是 RabbitMQ Worker 与 Fake
Provider。
