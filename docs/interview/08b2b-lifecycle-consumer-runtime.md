# 08-B2B：Lifecycle Consumer、通知重试/DLQ 与运行时装配

- 状态：已完成
- 阶段目标：把已发布到 lifecycle queue 的状态事实可靠回调给业务订阅方，并为重复、临时失败、永久失败和停机建立明确的消息处置语义。

## 1. 解决的问题

08-B2A 已经能根据 `event_id` 读取 Journal 并调用 gRPC，但还没有谁从 RabbitMQ 触发它，也没有
回答以下工程问题：

- 回调成功后何时 ACK；
- AI-Nexus 暂时不可用时怎样延迟重试；
- 鉴权失败、坏消息怎样进入独立 DLQ；
- 回调故障会不会占满发信 Worker；
- 进程启动、readiness 和优雅停机是否包含通知链路。

本阶段形成完整链路：

```text
Message + Journal + lifecycle Outbox
  → Outbox Relay + RabbitMQ Publisher Confirm
  → mail.lifecycle.v1.q
  → Lifecycle Consumer
  → Notification Worker 回查 Journal
  → gRPC Callback
  → ACK / delayed retry / notification DLQ
```

## 2. 为什么使用独立 Consumer

派发与通知共享了稳定的 RabbitMQ 连接管理代码，但使用两套 Consumer 实例：

| 维度 | Dispatch Consumer | Lifecycle Consumer |
| --- | --- | --- |
| 队列 | `mail.dispatch.v1.q` | `mail.lifecycle.v1.q` |
| 下游 | Email Provider | 业务系统 Callback |
| DLQ | `mail.dispatch.dead.v1.q` | `mail.lifecycle.dead.v1.q` |
| 连接名 | `mail-worker-*` | `mail-notifier-*` |
| 并发/prefetch | 独立配置 | 独立配置 |

如果共用一个队列或一组 lanes，AI-Nexus 回调超时会占用发信并发，最终出现“邮件其实可以发送，
但通知故障让发信也停了”。独立连接、Channel、prefetch、重试策略和 DLQ 构成了第一层舱壁。

代码没有复制连接重建、lane 和 graceful shutdown 实现，而是在通用 Consumer runner 中注入
dispatch 或 lifecycle delivery handler。复用的是基础设施生命周期，隔离的是业务处理和容量。

## 3. Lifecycle Parser 为什么仍要严格校验 MQ

Worker 不信任 MQ payload 的业务字段，但 Consumer 仍校验传输契约：

- Exchange、routing key、event type 必须相互匹配；
- Content-Type、persistent delivery、App ID 必须正确；
- aggregate ID 必须等于 Correlation ID；
- sequence、dispatch generation、publish attempt 必须是正确的 AMQP long；
- JSON schema version、message ID、event type、sequence 必须与 Properties/Header 一致；
- `MessageId` 必须是合法 `event_id` UUID。

这里是在发现“传输副本是否损坏”，不是使用 payload 生成回调。通过校验后只构造：

```go
notification.Command{EventID: delivery.MessageId}
```

其余字段仍由 Notification Worker 回查 PostgreSQL Journal。

## 4. ACK、Reject 和 Nack 语义

| 结果 | AMQP 动作 | 原因 |
| --- | --- | --- |
| Callback `ACCEPTED` | `Ack(false)` | 首次处理完成 |
| Callback `DUPLICATE` | `Ack(false)` | 同一 event ID 已处理 |
| Callback `IGNORED_STALE` | `Ack(false)` | 旧 sequence 不应继续重试 |
| 临时存储/gRPC 故障 | `Reject(true)` | 进入 Quorum Queue failed delayed retry，并消耗 delivery limit |
| 坏 MQ 消息 | `Nack(false, false)` | 立即进入 notification DLQ |
| 永久 Callback 错误 | `Nack(false, false)` | 重试不能修复鉴权或协议问题 |

RabbitMQ 4.3 的 failed delayed retry 依赖 `basic.reject(requeue=true)`；普通
`basic.nack(requeue=true)` 不会按同样方式消耗 failed-return delivery budget，可能形成无限热循环。
因此项目有意区分 transient 的 Reject 和 poison 的 Nack。

ACK 发生在 gRPC 成功响应之后。如果订阅方已处理，但 ACK 前连接断开，RabbitMQ 会重新投递；
订阅方按稳定 event ID 返回 `DUPLICATE`，Consumer 再 ACK。这是 At Least Once 下实现业务幂等，
不是声称网络提供 Exactly Once。

## 5. 延迟重试与独立 Notification DLQ

`make mq-policy-apply` 现在为两条 live quorum queue 分别应用 Policy：

```text
delivery-limit = 20
delayed-retry-type = failed
delayed-retry-min = 1000 ms
delayed-retry-max = 30000 ms
dead-letter-strategy = at-least-once
overflow = reject-publish
```

通知死信使用独立路由：

```text
mail.dead.v1
  → mail.lifecycle.dead.v1
  → mail.lifecycle.dead.v1.q
```

通知进入 DLQ 不会把 `mail_messages.status` 改成 `DEAD_LETTERED`。邮件状态表达“邮件投递到了哪一步”，
而通知 DLQ 表达“业务系统没有收到某个状态事件”。例如邮件已经 `PROVIDER_ACCEPTED`，回调鉴权
失败不能倒退或篡改这个事实。后续应由通知对账/重放流程单独修复。

## 6. 运行时与 Readiness

Composition Root 新增：

- PostgreSQL `DeliveryEventReader`；
- Notification Worker；
- gRPC Callback Client；
- Lifecycle RabbitMQ Consumer；
- Callback Client 关闭和 Lifecycle Consumer 优雅停机。

readiness 同时要求：

- PostgreSQL 可用；
- Dispatch Consumer 已连接并启动全部 lanes；
- Lifecycle Consumer 已连接并启动全部 lanes。

它不直接要求 AI-Nexus Callback 当前可达。回调暂时不可用时应该由 durable queue 缓冲并重试；
若因此让整个实例 Not Ready，负载均衡会停止新的邮件受理和派发，反而扩大故障域。Callback 长期
失败通过积压、重试、DLQ、Metrics 和告警体现。

新增开发配置：

```text
MAIL_CALLBACK_GRPC_ADDRESS
MAIL_CALLBACK_GRPC_ALLOW_INSECURE
MAIL_CALLBACK_TIMEOUT
MAIL_LIFECYCLE_CONSUMER_LANES
MAIL_LIFECYCLE_CONSUMER_PREFETCH
MAIL_LIFECYCLE_CONSUMER_SHUTDOWN_TIMEOUT
```

地址由部署环境预注册，SubmitEmail 不能提供动态 callback URL。当前开发阶段必须显式允许明文，
未来替换 mTLS 时 gRPC Adapter 与 Worker 无需改变。

## 7. 主要文件

- `internal/messaging/rabbitmq/contract.go`：notification DLQ 契约；
- `internal/consumer/rabbitmq/config.go`：lifecycle 独立拓扑与多 routing key；
- `internal/consumer/rabbitmq/parser.go`：lifecycle envelope 传输校验；
- `internal/consumer/rabbitmq/consumer.go`：共享 runner 和通知 ACK/Retry/DLQ；
- `internal/bootstrap/config.go`：回调、通知 Worker 和 Consumer 配置；
- `internal/bootstrap/app.go`：完整依赖装配与生命周期；
- `internal/bootstrap/readiness.go`：双 Consumer readiness；
- `Makefile`：dispatch/lifecycle 两套可靠性 Policy；
- `internal/integration/rabbitmq_lifecycle_consumer_test.go`：真实 Broker 重试和 DLQ；
- `internal/integration/runtime_composition_test.go`：完整 Callback 纵向链路。

## 8. 验证

单元测试覆盖：

- accepted/status-changed 两种 routing；
- Property、Header、Envelope、event ID 损坏；
- 成功 ACK、临时错误 Reject、永久错误 Nack；
- 停机期间 transient 消息保持未确认；
- lifecycle queue 同时声明两条 binding；
- readiness 必须等待两条 Consumer。

真实 RabbitMQ 测试证明：

- 正常通知被处理并 ACK；
- `GRPC_UNAVAILABLE` 约一秒后重新投递；
- `GRPC_PERMISSION_DENIED` 进入 lifecycle DLQ；
- malformed 消息不进入 Worker，直接进入 lifecycle DLQ。

完整纵向测试从 gRPC `SubmitEmail` 出发，经过 PostgreSQL、Scheduler、Outbox Relay、RabbitMQ、
Fake Provider、Delivery Event Journal、Lifecycle Consumer，最终由真实本地 gRPC Callback Server
收到 sequence 1～4：

```text
ACCEPTED → QUEUED → SENDING → PROVIDER_ACCEPTED
```

Callback Server 同时模拟 event ID 幂等和 sequence 防倒退，因此测试不依赖并发通知的物理到达顺序。

验证命令：

```bash
go test ./...
go test -race ./...
go vet ./...
make migrate-validate
go test -tags=integration ./internal/integration/...
```

## 9. 面试表达

### 30 秒版本

> 我把邮件派发和状态通知拆成两套 RabbitMQ Consumer 舱壁，共享连接重建和 manual ACK 基础代码，
> 但各自有独立连接、并发、重试和 DLQ。通知成功、重复、旧事件都 ACK；临时错误用 Reject 进入
> quorum queue 延迟重试，坏消息和永久错误 Nack 到 notification DLQ。Worker 只使用 MQ event ID
> 回查 Journal，所以 MQ 不是业务事实来源。

### 2 分钟版本

> Lifecycle Consumer 会严格校验 AMQP Property、Header 和 JSON envelope 的一致性，但校验通过后
> 只传 event ID。Notification Worker 再读取与 Message、Outbox 同事务提交的 Journal，调用预注册
> gRPC Callback。这样 payload 被污染或协议升级时不能伪造业务回调，也不会把验证码扩散到 MQ。
>
> ACK 在 Callback 返回 accepted、duplicate 或 ignored-stale 后执行；ACK 丢失导致的重复由稳定
> event ID 幂等解决。临时错误使用 RabbitMQ 4.3 的 Reject failed-return 语义，进入 1～30 秒延迟
> 并消耗 delivery limit；鉴权、协议和坏消息立即进入独立 lifecycle DLQ。通知 DLQ 不修改邮件
> 状态，因为“邮件已发送”和“业务系统漏收状态”是两个不同事实。
>
> 运行时 readiness 要求 PostgreSQL 和两条 Consumer 正常，但不要求 Callback 当前可达。下游故障
> 应由队列缓冲，不能反向阻塞邮件受理。真实集成测试验证了临时重试、永久 DLQ，以及从 Submit
> 到四条状态 Callback 的完整纵向链路。

### 可能追问

**为什么通知失败不把邮件标记成 `DEAD_LETTERED`？**

邮件状态描述 Provider 投递事实，通知状态描述跨系统同步结果。邮件可能已经被 Provider 接受，
回调失败不能改写历史。通知 DLQ 需要独立对账与重放状态。

**多 lane 会导致通知乱序，为什么不强制单线程？**

RabbitMQ 至少一次和失败重试本来就可能乱序。单线程只能降低概率，还会牺牲吞吐并产生队头阻塞。
正确性由 event ID 幂等和 message ID + sequence 防倒退保证，Consumer 并发可以独立扩展。

**为什么 transient 用 Reject，poison 用 Nack？**

项目使用 RabbitMQ 4.3 quorum queue 的 failed delayed retry。Reject(requeue=true) 会进入 failed
return 计数、延迟和 delivery limit；Nack(requeue=false) 则明确表示无需重试，立即死信。

**Callback 不健康为什么 Readiness 仍然成功？**

队列就是为隔离下游短暂故障存在的。只要本服务仍能可靠受理、发送并把通知保存在 durable queue，
就不该自我摘流；需要用 callback lag、失败率和 DLQ 告警，而不是让所有能力一起不可用。

## 10. 尚未解决

- AI-Nexus 真实仓库中的 event ID 幂等表和 sequence 更新事务；
- notification DLQ 的管理 API、对账扫描和安全重放；
- 回调积压、延迟、结果和 DLQ Metrics/Tracing；
- 多租户订阅地址、独立执行池、速率限制和熔断器；
- Callback mTLS、证书轮换和服务身份授权；
- 多实例与真实 AI-Nexus 的故障注入联调。
