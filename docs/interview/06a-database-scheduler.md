# 阶段 06-A：数据库 Scheduler

- 状态：已完成
- 阶段目标：多实例安全地领取到期定时/重试邮件，在短事务中原子推进状态并创建派发 Outbox

## 1. 解决的问题

定时邮件不能在创建时立即进入 RabbitMQ，否则会出现：

- 数小时或数月的消息长期占用 Broker；
- 取消、租户暂停和修改调度策略难以及时生效；
- 服务重启后要依赖 Broker 的延迟语义恢复；
- RabbitMQ TTL/DLX 的队首行为不适合作为产品级长时间调度真相。

前一阶段已经让定时邮件可靠停留在 PostgreSQL：

```text
SCHEDULED       + scheduled_at
RETRY_SCHEDULED + next_attempt_at
```

本阶段增加 Scheduler，把已经到期的数据库计时器推进为：

```text
未超过 deadline → QUEUED + MESSAGE_DISPATCH_REQUESTED
已经超过 deadline → EXPIRED，不创建 dispatch command
```

## 2. 为什么 Scheduler 不直接发送邮件

Scheduler 的职责是推进数据库状态，不执行外部副作用：

```text
PostgreSQL Timer
      │
      ▼
Scheduler：SCHEDULED/RETRY_SCHEDULED → QUEUED + Outbox
      │
      ▼
Outbox Relay：Outbox → RabbitMQ
      │
      ▼
Worker：RabbitMQ → Provider
```

如果 Scheduler 在持有数据库行锁时调用 RabbitMQ 或 SMTP，网络延迟会把一个几毫秒的短
事务变成长事务，放大锁等待、连接池占用和故障影响。当前 Scheduler 只做 SELECT、状态机、
UPDATE 和 INSERT Outbox。

## 3. 整体事务

一次 `RunOnce` 执行一个有界批次：

```text
BEGIN READ COMMITTED
  SELECT due messages
  ORDER BY due_time, priority DESC, id
  LIMIT batch_size
  FOR UPDATE SKIP LOCKED

  对每条 Message：
    database_now >= dispatch_deadline → Message.Expire(database_now)
    否则                             → Message.Queue(database_now)
    UPDATE mail_messages WHERE id AND version

  INSERT all pending events INTO outbox_events
COMMIT

COMMIT 成功后才 PullEvents
```

任意 Message Save 或 Outbox INSERT 失败，整个批次回滚。Scheduler 返回的 result 只统计
已经 Commit 的 `Claimed/Queued/Expired`，失败时不会报告虚假的处理数量。

## 4. 数据库为什么是时间权威

多个 Scheduler 副本可能运行在不同节点。如果使用各自的 `time.Now()`：

```text
节点 A 快 3 秒 → 提前领取
节点 B 慢 4 秒 → 延后领取
```

更危险的是，SQL 用数据库时间查询、Go 状态机却用节点时间判断 deadline，两层可能对
同一 Message 得出不同结论。

本阶段让领取 SQL 使用 PostgreSQL：

```sql
transaction_timestamp()
```

查询把这个时间随每行一起返回。`DueMessageBatch.EvaluatedAt` 再传给 `Queue/Expire`，所以：

- due predicate 使用它；
- deadline 判断使用它；
- `updated_at` 和领域事件 `occurred_at` 使用它；
- 同一事务中的所有行看到完全相同的时间；
- Scheduler 节点本地时钟不参与业务决定。

选择 `transaction_timestamp()` 而不是 `clock_timestamp()`，是因为前者在一个事务内稳定，
不会让同一批前后两条消息因为处理耗时看到不同的边界时间。

## 5. `FOR UPDATE SKIP LOCKED` 如何工作

假设有三条到期任务：

```text
M1  09:00
M2  09:01
M3  09:02
```

两个 Scheduler 同时运行：

```text
Scheduler A                         Scheduler B
BEGIN                               BEGIN
锁住 M1、M2                         看到 M1、M2 已锁
                                    SKIP LOCKED
                                    锁住 M3
更新 M1、M2                         更新 M3
COMMIT                              COMMIT
```

`FOR UPDATE` 的作用是让其他事务不能同时修改已经领取的 Message；`SKIP LOCKED` 的作用
不是等待这些行，而是继续寻找其他可处理任务。因此多个实例可以并行分摊批次。

如果 A 在 Commit 前崩溃，PostgreSQL 会回滚并释放 M1/M2 的行锁；它们仍是原状态，下次
扫描会再次出现。如果 A 已 Commit，Message 已经变为 `QUEUED/EXPIRED`，不再符合 due
query。这个恢复过程不需要额外补偿任务。

## 6. 为什么 Scheduler 不设置 Lease

Lease 表示“数据库事务已经结束，但某个实例仍在执行一段可能较慢的外部工作”。例如
Outbox Relay 需要：

```text
数据库领取并设置 lease
→ 提交事务、释放行锁
→ 调用 RabbitMQ
→ 等待 Publisher Confirm
→ 按 lease owner 标记 PUBLISHED
```

Scheduler 不跨事务做外部工作：领取、状态推进、Outbox 写入都在同一个短事务内完成。
事务未提交时由 PostgreSQL 行锁保护，事务提交后工作已经完成。因此给 Message 增加
lease 只会增加状态、索引和过期恢复复杂度，没有提供额外正确性。

结论是：

```text
Scheduler   → 短事务行锁，不需要 Message Lease
Outbox Relay → 跨网络发布，需要 Outbox Lease
```

## 7. 为什么限制批次大小

Batch size 必须在 `1..1000`，生产默认值以后通过配置选择更保守的数量，例如 100。

批次过大会：

- 同时持有太多行锁；
- 让一个事务生成大量 UPDATE/INSERT；
- 增加 WAL、连接占用和回滚成本；
- 一条异常数据导致更大的批次一起回滚。

批次过小则增加事务和查询次数。上限只负责保护边界，最终值应根据 Scheduler lag、
数据库延迟和 Outbox 写入量压测调整，而不是把 1000 当成推荐值。

`RunOnce` 只执行一个批次，不在应用服务内部创建无限循环。未来 runtime role 负责 ticker、
jitter、空批次退避、优雅停机和 readiness，这让核心逻辑容易做确定性测试。

## 8. 查询顺序与索引

领取顺序为：

```text
due_time ASC
priority DESC
message_id ASC
```

- 先处理等待最久的任务，避免新高优先级任务无限饿死旧任务；
- 到期时间相同时优先级高的先处理；
- ID 提供稳定 tie-breaker。

Migration 已有两个部分索引：

```text
mail_messages_scheduled_due_idx
  ON (scheduled_at, id) WHERE status = 'SCHEDULED'

mail_messages_retry_due_idx
  ON (next_attempt_at, id) WHERE status = 'RETRY_SCHEDULED'
```

历史终态不会进入调度索引。当前查询同时扫描 Scheduled 和 Retry 两类到期任务，
PostgreSQL 可以对两个部分索引做 BitmapOr/排序；数据规模增长后用 `EXPLAIN ANALYZE` 和
真实分布决定是否拆成两个领取流，不能只根据空表执行计划提前优化。

## 9. 状态机仍然是唯一修改入口

Scheduler 没有执行：

```sql
UPDATE mail_messages SET status = 'QUEUED';
```

它先 Restore 聚合，再调用：

```go
Message.Queue(databaseNow)
Message.Expire(databaseNow)
```

这样复用了已有规则：

- deadline 到达后禁止 Queue；
- Queue 自动增加 `dispatch_generation`；
- Queue 自动生成 StatusChanged 和 DispatchRequested；
- Expire 只允许合法来源状态且不生成派发命令；
- sequence、updated time 和 pending events 由聚合统一维护。

SQL 行锁解决“谁在同一时间处理这行”，状态机解决“这次业务转换是否合法”，两者不能
互相替代。

## 10. 与乐观锁的关系

Scheduler 已经持有 `FOR UPDATE` 行锁，但 Save 仍然带 `WHERE id AND version`：

- 行锁防止领取后被另一个事务并发更新；
- version 是所有 Message 写入路径共同遵守的持久化契约；
- 如果未来代码错误地用旧 Snapshot 进入事务，乐观锁仍会拒绝覆盖；
- 不为 Scheduler 单独维护一套无 version UPDATE，减少语义分叉。

取消请求与 Scheduler 的竞态由数据库决定顺序：

```text
取消先提交    → status 不再符合 due query
Scheduler 先锁 → 先 Queue；取消保存旧 version 时冲突，重新加载后仍可从 QUEUED 取消
```

Worker 真正发送前还要重新检查状态、generation、deadline 和租户状态。

## 11. 租户暂停

due query 只领取 `tenants.status = 'ACTIVE'` 的任务：

- PAUSED/DISABLED 租户的 Message 保持原调度状态；
- 租户恢复 ACTIVE 后重新参与扫描；
- 如果恢复时已经超过 deadline，状态机会让它进入 EXPIRED；
- Scheduler 查询与租户暂停恰好并发时仍存在先后顺序，Worker 必须再次检查租户状态。

当前还没有实现租户控制面和 Worker 二次检查，但数据库过滤及恢复语义已经通过测试。

## 12. 端口与分层

应用层新增：

```text
DueMessageQuery
DueMessageBatch
DueMessageRepository
DueMessageScheduler
```

`DueMessageRepository` 进入已有 UnitOfWork。PostgreSQL Adapter 的构造函数只接受
`pgx.Tx`，而不是兼容 Pool 的通用 DBTX，因为脱离显式事务执行 `FOR UPDATE` 会在语句
结束时立刻释放锁，给调用方一种“已经领取”的错误感觉。

通用 Message 扫描器被收敛为最小 `Scan(...any)` 接口，因此 `pgx.Row` 和 `pgx.Rows` 都
复用同一套 Snapshot 映射与损坏数据校验，没有第二套 Scheduler 映射逻辑。

## 13. 故障与边界

| 场景 | 结果 |
| --- | --- |
| 没有到期任务 | 空批次成功，不修改数据 |
| Scheduled 到期且未过 deadline | QUEUED + Status/Dispatch Outbox |
| Retry 到期且未过 deadline | generation 递增后 QUEUED + Status/Dispatch Outbox |
| 到期但已过 deadline | EXPIRED + Status Outbox，无 Dispatch |
| 未来任务 | 不领取 |
| 租户 PAUSED | 不领取，恢复 ACTIVE 后重新判断 |
| Outbox INSERT 失败 | 整批 Message/Outbox 回滚 |
| Scheduler Commit 前崩溃 | 行锁释放，任务保持原状态供下次扫描 |
| 另一实例已锁最早任务 | SKIP LOCKED 后继续领取其余任务 |
| 锁持有者 Rollback | 被跳过任务下次重新领取 |

## 14. 主要文件

- `internal/application/ports/due_message_repository.go`
- `internal/application/ports/due_message_repository_test.go`
- `internal/application/delivery/due_message_scheduler.go`
- `internal/application/delivery/due_message_scheduler_test.go`
- `internal/storage/postgres/due_message_repository.go`
- `internal/storage/postgres/due_message_queries.go`
- `internal/storage/postgres/message_mapping.go`
- `internal/storage/postgres/transaction_manager.go`
- `internal/integration/due_message_scheduler_test.go`

## 15. 验证

本阶段已经执行并通过：

```text
gofmt -w internal/application/delivery internal/application/ports internal/storage/postgres internal/integration/due_message_scheduler_test.go
go mod tidy
go test ./...
go vet ./...
go test -tags=integration ./internal/integration/... -run '^$'
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

真实 PostgreSQL 18.4 测试覆盖：

- Scheduled、Retry、Expired 和 Future 四种时间分支；
- Message version、状态和 Outbox 数量；
- Retry 再出队时 generation/dispatch command 增加；
- Outbox trigger 故障后的整批回滚与下一轮恢复；
- PAUSED 租户跳过及 ACTIVE 后恢复；
- 一个事务持锁时，batch size 1 的另一 Scheduler 在 2 秒 context 内跳过该行并只领取
  下一条；
- 持锁事务 Rollback 后被跳过任务重新领取。

最终真实 PostgreSQL 功能测试约 26.4 秒，race 测试约 28.1 秒，均通过。由 integration
package 驱动的 application delivery、application ports 和 PostgreSQL Adapter 跨包语句
覆盖率为 `74.2%`。普通单元测试中三个包分别为 `29.3%`、`95.7%` 和 `19.7%`；Scheduler
的核心正确性依赖 PostgreSQL 行锁、事务时间和真实回滚，因此主要覆盖来自 integration
test，而不是伪造 SQL 行为的 mock。

## 16. 面试表达

### 30 秒版本

> 我把 PostgreSQL 作为定时和重试的调度真相。Scheduler 在短事务中使用
> `FOR UPDATE SKIP LOCKED` 批量领取到期 Message，多实例遇到已锁行会跳过继续处理。
> 查询使用 `transaction_timestamp()`，状态机的 Queue/Expire 与 SQL 到期判断共享同一
> 数据库时间。Message 状态和派发 Outbox 同事务提交，失败就整体回滚。因为 Scheduler
> 不跨网络做工作，所以只需要事务行锁，不需要 Lease；Lease 留给后续 Outbox Relay。

### 2 分钟版本

1. 解释为什么长延迟任务存 PostgreSQL 而不是 RabbitMQ TTL；
2. 画出 Scheduler、Relay、Worker 的职责边界；
3. 用两个实例和三条任务解释 `FOR UPDATE SKIP LOCKED`；
4. 解释数据库时间避免节点时钟偏差；
5. 说明 deadline 到达时 Expire 而不是延迟发送验证码；
6. 说明 Queue 自动增加 generation 并产生 dispatch event；
7. 说明 Message + Outbox 同事务和 Commit 后 PullEvents；
8. 对比短事务行锁与跨网络 Lease；
9. 用真实持锁、Rollback 和 trigger 故障注入作为测试证据。

### 可能追问

**`SKIP LOCKED` 会不会让任务永久饿死？**

短事务很快释放锁，下一轮会重新扫描；测试也证明持锁者 Rollback 后任务可恢复。如果某
事务长期持锁，这是数据库长事务故障，需要 transaction timeout、连接监控和告警，不能
用重复领取掩盖。

**为什么锁了行还要 version？**

行锁保护当前 Scheduler 事务，version 保护所有写路径的统一并发契约，并能发现旧
Snapshot 或错误编排。保留乐观锁的成本很低，避免 Scheduler 出现特制 UPDATE。

**为什么不用一个 UPDATE SKIP LOCKED 直接批量改状态？**

直接 SQL 会绕过领域状态机的 deadline、generation、sequence 和领域事件规则，而且
Retry 与 Scheduled 的合法转换细节会复制到数据库脚本。当前批次有上限，逐聚合执行
规则更容易审查和测试。

**为什么是整个批次一起回滚，不是一条一个事务？**

一次批量领取和写入可以减少事务往返，并保持实现简单。批次有严格上限；如果生产数据
证明单条 poison record 会影响可用性，可以加入 savepoint/隔离队列，但不能静默跳过。

**数据库时间会不会也不准？**

它仍需要基础设施时间同步，但至少所有 Scheduler 使用同一个权威来源，SQL 与领域层不
会互相矛盾。调度 lag 指标还需要区分数据库时间与实际处理时间。

## 17. 尚未解决

- Scheduler role 的 ticker、空批次退避、jitter、优雅停机和配置加载；
- Scheduler lag、批次耗时、queued/expired 数量等 Metrics/Tracing；
- Outbox Relay 的 lease 领取、Publisher 接口和状态更新；
- RabbitMQ topology、mandatory publish 和 Publisher Confirm；
- Worker 对状态、generation、deadline 和租户状态的二次检查；
- 租户控制面及暂停操作的鉴权审计。

下一步是 06-B Outbox Relay 数据库层：先实现 PENDING Outbox 的短事务 lease 领取、确认
成功和失败重调度，再通过 Fake Publisher 验证崩溃窗口，最后接 RabbitMQ Adapter。
