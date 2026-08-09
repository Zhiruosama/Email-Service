# 阶段 07-A：Worker 核心、Delivery Attempt 与 Fake Provider

- 状态：已完成
- 阶段目标：在不依赖 RabbitMQ Consumer 和真实 SMTP 的情况下，可靠完成一次派发命令的
  领取、Provider 调用、结果持久化和重复命令防护

## 1. 这一阶段解决什么问题

06-C 已经能把 `MESSAGE_DISPATCH_REQUESTED` 从 PostgreSQL Outbox 可靠发布到 RabbitMQ，
但 Broker 接管消息不等于邮件已经发送。还缺少真正处理派发命令的 Worker：

```text
RabbitMQ Event
→ 校验 Message / generation / sequence
→ 领取一次逻辑投递
→ 调用 Provider
→ 保存结果
```

最危险的不是正常成功，而是下面这个窗口：

```text
Message 已变成 SENDING
→ Worker 崩溃
→ RabbitMQ 重投同一条消息
```

如果数据库只保存 Message 状态，系统不知道第几次 Provider 调用已经开始，也无法区分：

- Worker 在调用 Provider 前崩溃；
- Provider 已接受，但结果落库前崩溃；
- 原 Worker 仍在调用 Provider，另一个 Worker 收到了重复事件。

因此本阶段先建立 `delivery_attempts`，再实现与 MQ 无关的 Worker 应用内核。

## 2. 为什么把 07 拆成 07-A 和 07-B

原计划叫“RabbitMQ Worker + Fake Provider”，实际包含两类问题：

1. 业务可靠性：状态机、Attempt、Provider 结果、数据库事务；
2. 传输可靠性：Consumer 连接、prefetch、Manual ACK/Nack、重连、DLQ。

如果一起实现，一旦测试失败，很难判断是 RabbitMQ ACK 时机错误，还是数据库状态推进
错误。现在先让 07-A 的 `DispatchWorker.Process` 只接收一个传输无关的
`DispatchCommand`；07-B 再负责把 AMQP Delivery 转成 Command，并根据返回值 ACK、重试或
死信。

这是六边形架构的实际价值：RabbitMQ 是输入 Adapter，不进入 Worker 核心。

Worker 同时公开错误分类而不是 ACK API：`TRANSIENT` 表示临时基础设施错误，可以重投；
`POISON` 表示畸形命令、未来 generation 或本地不变量破坏，重复投递不会自行恢复。07-B
据此决定 Nack requeue 或 Dead Letter，不需要匹配错误字符串。

## 3. 核心设计：两段短事务

最终流程是：

```text
事务 A
  读取 Message
  校验 tenant / sequence / dispatch_generation
  QUEUED → SENDING
  INSERT delivery_attempts(status=STARTED)
  INSERT status Outbox
COMMIT

事务外
  Provider.Submit(timeout)

事务 B
  重新读取 Message
  校验 generation / attempt_no / SENDING fence
  完成 delivery_attempt
  推进 Message 状态
  INSERT status Outbox
COMMIT
```

Provider 网络调用绝不能放在数据库事务里。SMTP 或 HTTP 可能持续数秒甚至超时；如果
事务一直不提交，会长期占用连接、保留行版本或锁、扩大死锁范围，并让 PostgreSQL 的
吞吐受最慢 Provider 控制。

### 3.1 为什么不能“先调用 Provider，再创建 Attempt”

考虑：

```text
Provider 已接受
→ Worker 在 INSERT Attempt 前崩溃
```

数据库完全没有这次调用的痕迹，重投后很容易再次发送。先提交 `STARTED` Attempt，至少
能让 Reconciler 看到“这次投递已经被分配，结果可能未知”。这不能创造外部 exactly-once，
但能避免系统在失忆后盲目重发。

### 3.2 为什么不能在事务 A 提交后直接 ACK

事务 A 只证明 Worker 获得了逻辑执行权，并不证明 Provider 结果已落库。如果此时 ACK，
随后进程崩溃，RabbitMQ 不再重投，而数据库只剩 `SENDING + STARTED`。07-B 的 ACK 边界
必须在事务 B Commit 之后；如果事务 B 未完成，至少还有 MQ 重投和 Reconciler 两条恢复
线索。

### 3.3 为什么事务 B 要重新读 Message

Provider 调用期间数据库状态可能被 Reconciler 或后续 Provider 事件推进。Worker 不能拿
事务 A 中的旧对象直接覆盖当前记录。事务 B 重新读取，并校验：

```text
status == SENDING
dispatch_generation == claimed generation
attempt_count == claimed attempt number
```

再配合 Message `version` 乐观锁和 Attempt 的 `status = STARTED` Compare-and-Set，旧结果
不能覆盖新所有者的状态。

Claim 和 Finalize 的业务时间都读取当前 PostgreSQL 事务的 `transaction_timestamp()`，使
多 Worker 节点对 deadline 使用同一个时间源。Provider timeout 则仍使用 Go Context 的
本机单调计时，因为它衡量的是一次本地网络调用持续多久，两种时钟职责不同。

## 4. `delivery_attempts` 设计

第一版核心字段：

```text
id
message_id
attempt_no
dispatch_generation
provider_key
status
started_at / finished_at
provider_message_id
error_category / error_code / error_retryable
```

状态只表达“这一次 Provider 调用”的结果：

| Attempt 状态 | 含义 |
| --- | --- |
| `STARTED` | 已分配执行权，Provider 结果尚未可靠记录 |
| `PROVIDER_ACCEPTED` | Provider 明确接受，并返回稳定 Message ID |
| `FAILED` | 明确失败，知道未成功接受，可按错误类别决定业务重试 |
| `SUBMISSION_UNKNOWN` | 可能接受也可能未接受，不能伪装成明确失败 |

Message 与 Attempt 的状态粒度不同。例如一次 Attempt 是 `FAILED`，Message 可以是
`RETRY_SCHEDULED`、`PERMANENTLY_FAILED` 或 `DEAD_LETTERED`。Attempt 记录事实，Message
记录整个逻辑邮件下一步怎么办。

### 4.1 两个唯一约束

```text
UNIQUE(message_id, attempt_no)
UNIQUE(message_id, dispatch_generation)
```

第一个保证“第 N 次尝试”只有一条记录，第二个保证一代 Dispatch Event 只能创建一次
Attempt。应用乐观锁通常已经能让并发 Worker 只有一个成功，但数据库唯一约束是最后
防线，防止未来代码路径绕过应用判断。

### 4.2 CHECK 约束

数据库继续验证：

- `STARTED` 不能有 finished time、Provider Message ID 或错误；
- `PROVIDER_ACCEPTED` 必须有 finished time 和 Provider Message ID；
- `FAILED` 必须有非 `SUBMISSION_UNKNOWN` 的完整错误三元组；
- `SUBMISSION_UNKNOWN` 必须使用同名错误类别；
- 完成时间不能早于开始时间。

应用校验用于快速报错，数据库约束用于抵御绕过应用层的错误写入，两者不是重复浪费。

## 5. 幂等判断：Event ID、Sequence 和 Generation 各管什么

`DispatchCommand` 包含：

```text
event_id
tenant_id
message_id
aggregate_sequence
dispatch_generation
```

- Event ID 标识一条 Outbox/MQ 事件，方便日志和追踪；RabbitMQ 不会自动按它去重；
- Sequence 表示聚合事实顺序，旧 sequence 不能覆盖新状态；
- Dispatch Generation 表示第几轮真正入队。一次重试到期重新 `Queue` 会产生新 generation；
- Attempt Number 表示已经开始过几次 Provider 调用，由状态机在 `StartSending` 时增加。

Worker 的核心判断是：

```text
command generation/sequence < database → STALE，不调用 Provider
command generation/sequence > database → 不可能的未来事件，报 invariant
完全匹配且 Message=QUEUED       → 可以 Claim
完全匹配但状态不允许             → DUPLICATE，不调用 Provider
```

并发时两个 Worker 可能同时读到 `QUEUED`。第一个 Message 乐观更新成功；第二个的
`WHERE id=? AND version=?` 影响 0 行，整个 Claim 事务回滚。它随后收到重投时会看到更高
sequence 或 `SENDING`，作为旧任务结束，不进行第二次 Provider 调用。

## 6. Provider 为什么返回结果联合，而不是只返回 `error`

端口定义三个结果：

```text
ACCEPTED
FAILED
SUBMISSION_UNKNOWN
```

普通 Go 网络函数常返回 `error`，但发邮件场景中一个 `timeout` 信息量不够：

- TCP 连接前超时：通常可以确认未发送；
- Provider 明确返回 429：明确失败且可重试；
- SMTP DATA 完整提交后，最终响应前断线：对方可能已经接受。

如果都写成 `if err != nil { retry }`，验证码可能收到两封。Provider Adapter 的职责就是
把具体协议阶段转换成统一事实，Worker 只根据统一事实推进状态机。

如果 Provider 超时后连一个合法结果都没返回，Worker 使用最保守的
`SUBMISSION_UNKNOWN` 兜底，而不是猜测“肯定没发”。

## 7. 结果如何推进状态

| Provider 结果 | Attempt | Message |
| --- | --- | --- |
| 明确接受 | `PROVIDER_ACCEPTED` | `PROVIDER_ACCEPTED` |
| 明确、可重试失败，额度和 deadline 足够 | `FAILED` | `RETRY_SCHEDULED` |
| 明确、不可重试失败 | `FAILED` | `PERMANENTLY_FAILED` |
| 可重试失败但额度耗尽或下一次越过 deadline | `FAILED` | `DEAD_LETTERED` |
| 提交结果不确定 | `SUBMISSION_UNKNOWN` | `SUBMISSION_UNKNOWN` |

Provider 业务重试写入 `next_attempt_at`，由数据库 Scheduler 在到期后产生新 generation；
Worker 不使用 RabbitMQ 立即 requeue 来做业务退避，避免消息在队首高速循环。

## 8. Fake Provider 的作用

Fake Provider 不是“随便返回成功的假代码”。它是一个可控 Adapter，可以：

- 记录收到的 Provider Request；
- 注入明确成功；
- 注入可重试或永久失败；
- 注入 submission unknown；
- 阻塞调用，制造并发重复命令；
- 在调用期间查询 PostgreSQL，证明事务 A 已提交。

当前 Request 只包含 Message、Tenant、Attempt、Category 和 duplicate risk policy 等安全
元数据，因为正文、收件人密文、模板版本和路由控制面尚未落地。这个阶段不为了“看起来
能发信”而把验证码或邮箱明文临时塞进 MQ/Attempt。

## 9. 崩溃窗口与恢复语义

| 故障点 | 数据库结果 | 后续处理 |
| --- | --- | --- |
| 事务 A Commit 前 | 保持 `QUEUED`，无 Attempt | MQ 重投后安全重新 Claim |
| 事务 A Commit 后、Provider 前 | `SENDING + STARTED` | Reconciler 判断陈旧 Attempt |
| Provider 明确失败、事务 B 前 | `SENDING + STARTED` | 不能假定未调用，等待对账/恢复 |
| Provider 已接受、事务 B 前 | `SENDING + STARTED` | 不盲目重发 `AVOID_DUPLICATE` |
| 事务 B Commit 后、MQ ACK 前 | 最终状态和 Attempt 已落库 | MQ 重投被判定 STALE，不再调用 Provider |

这仍然是 At Least Once，不是外部 Exactly Once。Provider 如果支持幂等 Key 和查询，后续
Reconciler 可以进一步缩小不确定窗口；普通 SMTP 无法彻底消除这个物理限制。

## 10. Shutdown 时为什么还尝试 Finalize

Provider 已经返回后，MQ Consumer 可能正在优雅停机，上层 Context 已取消。如果 Worker
直接使用这个已取消 Context 写数据库，Finalize 必然失败。因此代码为结果持久化建立一个
忽略上层取消、但自身有短超时的收尾 Context。

这不是无限忽略停机：`FinalizeTimeout` 有明确上限。目的只是给“已经发生的外部副作用”
一次有限落库机会，减少无意义的 `SENDING` 对账任务。

## 11. 主要实现文件

- `db/migrations/sql/00002_create_delivery_attempts.sql`
- `internal/application/ports/delivery_attempt_repository.go`
- `internal/application/ports/email_provider.go`
- `internal/application/delivery/dispatch_worker.go`
- `internal/application/delivery/delivery_backoff.go`
- `internal/storage/postgres/delivery_attempt_repository.go`
- `internal/storage/postgres/transaction_clock.go`
- `internal/provider/fake/provider.go`
- `internal/integration/dispatch_worker_test.go`

## 12. 验证

本阶段实际执行：

```bash
make migrate-validate
go test ./...
go test -race ./...
make check
TEST_POSTGRES_IMAGE=postgres:18.4-alpine \
TEST_RABBITMQ_IMAGE=rabbitmq:4.3.4-management-alpine \
go test -tags=integration ./internal/integration/...
TEST_POSTGRES_IMAGE=postgres:18.4-alpine \
go test -tags=integration ./internal/integration \
  -run '^TestDispatchWorker$' -count=1 -v -timeout=2m
```

结果全部通过。完整集成套件曾运行约 `313.278s`；加入 PostgreSQL 事务时钟后又单独运行
最终 Worker 集成用例，耗时约 `4.288s`。关键真实 PostgreSQL 用例证明：

- Provider 执行时已经能从另一连接看到 `SENDING + STARTED`；
- 接受、可重试失败和不确定结果会原子保存 Attempt、Message 与 Outbox；
- 同一事件并发执行只产生一个 Provider 调用；
- Claim Outbox 失败时 Message 和 Attempt 都回滚；
- Provider 接受后 Finalize Outbox 失败时，保留 `SENDING + STARTED`，没有伪造成功；
- Migration up/down、唯一约束、结果一致性 CHECK 和索引均通过真实 PostgreSQL 18 验证。

## 13. 面试表达

### 30 秒版本

> 我实现邮件 Worker 时没有把 Provider 网络请求放进数据库事务，而是设计成两段短事务。
> 第一段用状态机、乐观锁和唯一约束原子创建 STARTED Attempt；事务外调用 Provider；第二
> 段原子完成 Attempt、更新 Message 和写 Outbox。重复 MQ 事件通过 sequence 和 dispatch
> generation 判旧，不会再次调用 Provider。对于 SMTP 提交后断线，Provider 返回显式的
> SUBMISSION_UNKNOWN，而不是把所有 error 都自动重试。

### 2 分钟版本

> Outbox Relay 是 At Least Once，所以 Worker 一定会收到重复事件。我的 Claim 事务先读取
> Message，校验 tenant、aggregate sequence 和 dispatch generation，然后执行
> QUEUED→SENDING，同时插入 STARTED delivery_attempt 和状态 Outbox。Provider 调用发生在
> Commit 之后，因此不会长时间占数据库连接和锁。
>
> Provider 返回后开启第二个短事务，重新读取 Message，并用 generation、attempt number、
> SENDING 状态以及 version 做 fencing，再把 Attempt、Message 和 Outbox 一起提交。数据库
> 还对 message+attempt_no、message+generation 做双唯一约束。事务 B 提交后 RabbitMQ 才能
> ACK；如果 ACK 前崩溃，重投事件会因旧 sequence 被直接确认，不再发第二封。
>
> 外部发送无法承诺 exactly-once，特别是 SMTP DATA 后断线。因此 Provider 端口返回
> ACCEPTED、FAILED、SUBMISSION_UNKNOWN 三种规范化结果。未知结果保留给 Reconciler，不
> 对验证码盲目重试。我用真实 PostgreSQL 测试了并发重复消费、两个事务的回滚和 Provider
> 成功但 Finalize 失败的崩溃窗口。

### 可能追问

**为什么有 Message 乐观锁还需要 Attempt 唯一约束？**

乐观锁保护当前代码路径的聚合更新，唯一约束保护数据不变量。未来可能增加 Reconciler、
人工重放或新 Repository；即使某条代码路径判断有 bug，数据库也不允许同一次序或同一
generation 出现两条 Attempt。

**为什么不对 Message 使用 `FOR UPDATE`？**

单条 MQ 命令只更新一个 Message，乐观锁冲突成本低，而且事务很短。它允许不同邮件完全
并行。`FOR UPDATE SKIP LOCKED` 更适合 Scheduler 这种“从许多到期行中选一批”的竞争领取；
这里 Message ID 已知，不需要扫描抢批次。

**如果 Provider 成功但事务 B 一直失败怎么办？**

数据库保留 `SENDING + STARTED`。07-B 不会因为重投就再次调用 Provider，后续 Reconciler
扫描陈旧 STARTED Attempt：支持查询的 Provider 按 Message ID 对账；普通 SMTP 根据
duplicate risk policy 进入 UNKNOWN_FINAL、人工处理或在明确接受风险后重发。

**为什么业务重试不直接 Nack requeue？**

立即 requeue 可能在队首形成高速红elivery loop，占用网络、CPU 和 Worker。业务失败先
写 `RETRY_SCHEDULED + next_attempt_at`，由 PostgreSQL Scheduler 到期产生新 generation；
MQ requeue 只处理短暂基础设施失败。

**Fake Provider 能证明真实 SMTP 正确吗？**

不能。它证明 Worker 编排、状态和事务边界正确。SMTP Adapter 仍要单独测试协议阶段、
4xx/5xx 分类、DATA 后断线、TLS 和认证。分层以后，SMTP 只负责把具体协议结果规范化，
不会重新实现 Worker 事务。

## 14. 尚未解决

- RabbitMQ Consumer 连接、Manual ACK/Nack、prefetch、优雅停机和重连；
- poison message 的 Dead Letter Exchange、delivery limit 和安全重放；
- 扫描陈旧 `STARTED` Attempt 的 Reconciler；
- Provider Router、凭据版本、路由版本和 Provider 幂等 Key；
- 收件人密文、模板版本、MIME 渲染和真实 SMTP；
- 熔断、限速、舱壁、指标和 Trace。

下一阶段 07-B 会把 RabbitMQ Delivery 转换为当前 `DispatchCommand`，只在
`DispatchWorker.Process` 给出可安全确认的结果后 Manual ACK，并通过 prefetch 和本地并发
上限控制在途邮件数量。
