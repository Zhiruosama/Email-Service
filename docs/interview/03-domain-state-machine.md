# 阶段 03：领域模型与邮件状态机

- 状态：已完成
- 阶段目标：让所有邮件状态修改通过同一个基础设施无关的 Message 聚合执行，并用测试保护不变量

## 1. 解决的问题

邮件状态会被 API、Scheduler、Worker、Provider 回调和 Reconciler 修改。如果这些模块
直接写 `status`，同一条规则会散落在多处，并可能出现：

- 已取消邮件重新进入队列；
- 超过 deadline 后仍开始 Provider Attempt；
- 旧 MQ generation 再次发送邮件；
- Attempt 已耗尽但仍计划重试；
- 重复或乱序 Provider 事实让状态倒退；
- 方法失败后 Message 已经被部分修改。

本阶段建立一个统一规则入口：外部模块只能调用 Message 的具名方法，不能直接修改
内部字段。

## 2. 为什么在数据库之前实现

状态转换和业务不变量不属于 PostgreSQL、gRPC 或 RabbitMQ。先实现纯 Go 领域模型有
三个好处：

- 数据库 Schema 可以根据真实不变量设计；
- 状态规则可以在毫秒级单元测试中完整覆盖；
- 后续 API、Scheduler 和 Worker 复用同一套规则，而不是各写一套判断。

数据库阶段只负责持久化 Snapshot 和处理并发版本冲突，不重新解释业务状态。

## 3. Message 聚合

`Message` 是一封逻辑邮件的状态修改入口，持有：

```text
id
status
scheduledAt / dispatchDeadline / nextAttemptAt
attemptCount / maxAttempts
dispatchGeneration
providerAcceptedAt / providerMessageID
latestSequence / version
lastFailure
pendingEvents
```

字段均不导出。调用方只能读取安全 Getter 或 Snapshot，并通过以下方法修改：

```text
Queue
StartSending
ScheduleRetry
MarkSubmissionUnknown
MarkPermanentlyFailed
MarkDeadLettered
MarkUnknownFinal
Cancel
Expire
ApplyDeliveryFact
```

没有提供通用的 `TransitionTo(status)`，因为那会允许调用方绕过 deadline、Attempt 和
generation 等规则。

## 4. 构造和恢复分离

`New` 创建新 Message：

- 校验 ID、deadline、计划时间和最大 Attempt；
- 先产生 `MESSAGE_ACCEPTED`；
- 未来任务进入 `SCHEDULED`；
- 立即任务进入 `QUEUED`、generation 变为 1，并产生 DispatchRequested。

`Restore` 从数据库 Snapshot 恢复对象，但不产生领域事件。否则每次查询数据库都会
被误认为发生了新的状态变化并重复创建 Outbox。

恢复时还会拒绝损坏快照，例如：

- 未知状态；
- Attempt 超过最大值；
- 活跃投递状态没有 generation；
- `RETRY_SCHEDULED` 没有 `nextAttemptAt`；
- deadline 早于 acceptedAt。

## 5. 核心不变量

### 5.1 Deadline

`Queue` 和 `StartSending` 都要求：

```text
now < dispatchDeadline
```

等于 deadline 时也不允许开始新投递，避免边界语义在不同组件中不一致。

### 5.2 Attempt 预算

`StartSending` 分配一个可观测 Attempt 并增加计数。即使 Worker 随后崩溃，该 Attempt
也能由后续 Reconciler 发现，不能假装它没有发生。

`ScheduleRetry` 要求：

```text
failure.retryable = true
attemptCount < maxAttempts
now < nextAttemptAt < dispatchDeadline
```

预算耗尽后由调用方明确进入 `DEAD_LETTERED`。

### 5.3 Dispatch Generation

Message 每次从 `SCHEDULED/RETRY_SCHEDULED` 进入 `QUEUED` 都递增 generation，并产生
DispatchRequested。Worker 的 MQ 命令必须和当前 generation 相同；旧命令返回
`ErrStaleDispatchGeneration`，对象保持不变。

### 5.4 幂等取消和过期

- 首次合法取消返回 `changed=true`；
- 重复取消返回 `changed=false, nil`，不增加 sequence；
- 已进入 `SENDING` 后返回 `ErrTooLateToCancel`；
- `Expire` 只在 deadline 时刻或之后生效，重复过期是 No-op。

## 6. 命令与 Provider 事实

`Queue/Cancel/ScheduleRetry` 是命令，可以因当前状态不合法而失败。

`PROVIDER_ACCEPTED/DELIVERED/BOUNCED/COMPLAINED` 是已经发生的外部事实，通过
`ApplyDeliveryFact` 处理，返回：

```text
APPLIED
DUPLICATE
IGNORED_REGRESSION
```

可信的更晚事实允许跳过缺失中间回调，例如：

```text
SUBMISSION_UNKNOWN → DELIVERED
```

因为 Delivered 蕴含 Provider 曾接受。旧事实不能让状态倒退：

```text
DELIVERED → PROVIDER_ACCEPTED
```

会返回 `IGNORED_REGRESSION`，不会修改状态和 sequence。重复 Provider 事件的持久化
去重将在 PostgreSQL 阶段通过 `(provider_id, provider_event_id)` 唯一约束实现。

## 7. 领域事件与 Outbox 的边界

状态机产生内存领域事件：

```text
MESSAGE_ACCEPTED
MESSAGE_STATUS_CHANGED
MESSAGE_DISPATCH_REQUESTED
```

它们不是 RabbitMQ 消息。下一阶段 Application Service 会在保存 Message Snapshot 的
同一数据库事务中，将需要跨进程传播的领域事件映射为 Outbox。

`PullEvents` 只负责取走聚合产生的事实；领域层不认识 Outbox 表、MQ Exchange 或
序列化格式。

## 8. 并发边界

Message 是请求级对象，明确不允许跨 goroutine 共享。它没有全局 Mutex，不同 Message
可以并行运行状态机。

本阶段预留 `version`，下一阶段 Repository 使用：

```sql
UPDATE mail_messages
SET status = $1, version = version + 1
WHERE id = $2 AND version = $3;
```

实现乐观并发。同一 Message 发生冲突时重新读取和重新判断；Scheduler/Relay 的批量
领取则使用 `FOR UPDATE SKIP LOCKED`。状态机解决业务合法性，数据库锁解决并发数据
竞争，两者职责不同。

## 9. 主要文件

- `internal/domain/message/status.go`
- `internal/domain/message/message.go`
- `internal/domain/message/transition.go`
- `internal/domain/message/failure.go`
- `internal/domain/message/event.go`
- `internal/domain/message/error.go`
- `internal/domain/message/status_test.go`
- `internal/domain/message/message_test.go`
- `internal/domain/message/transition_test.go`

## 10. 验证结果

实际通过：

```text
make check-all
go test ./...
go test -race ./...
go vet ./...
```

领域包语句覆盖率：

```text
86.3%
```

测试覆盖立即/定时创建、恢复、合法和非法转换、deadline 边界、Attempt 上限、重试
时间、取消和过期幂等、旧 generation、重复事实、乱序事实及错误时不修改对象。

## 11. 面试表达

### 30 秒版本

> 我把邮件生命周期实现为基础设施无关的 Message 聚合，API、Scheduler、Worker 和
> Provider 回调不能直接修改状态，只能调用具名领域方法。状态机统一保护 deadline、
> Attempt 上限、取消规则和 dispatch generation；Provider 事实支持幂等和有证据的
> 向前跳转，但禁止状态倒退。领域层没有全局锁，不同 Message 可并行，同一 Message
> 的数据库并发后续由 version 乐观锁控制。

### 2 分钟展开重点

1. 解释为什么没有开放 `TransitionTo`；
2. 举旧 generation 防止重复发送的例子；
3. 解释 `SUBMISSION_UNKNOWN → DELIVERED` 为什么合法；
4. 区分状态机业务规则与数据库乐观锁；
5. 用 Race、边界测试和覆盖率作为实现证据。

### 可能追问

**状态机是不是全局瓶颈？**

不是。每次从 Snapshot 构造一封邮件自己的聚合，状态方法是 O(1) 内存判断。不同
Message 并行；只有同一 Message 的有效写入需要有序。

**为什么不直接用数据库 CHECK 约束实现状态机？**

数据库 CHECK 适合字段级不变量，但 deadline、Attempt、generation、当前状态和外部
事实组合成的转换规则更适合领域代码。数据库仍负责唯一约束和并发版本检查。

**为什么错误时强调对象不能变化？**

如果方法先改状态再发现 deadline 或预算错误，Repository 可能持久化半完成对象。
实现先完成全部校验，再一次性修改并产生事件，测试会比较操作前后 Snapshot。

## 12. 尚未解决

当前状态只存在内存中，尚未实现 PostgreSQL Schema、Repository、乐观锁和 Message +
Outbox 同事务。下一阶段将把 Snapshot 映射为真实表结构并通过数据库集成测试验证。
