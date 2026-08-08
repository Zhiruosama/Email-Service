# 阶段 06-C：RabbitMQ Publisher Adapter

- 状态：已完成
- 阶段目标：让 Outbox Relay 通过真实 RabbitMQ `mandatory` Publish 与 Publisher Confirm
  可靠交接事件，并验证持久化、重复发布和 Broker 重启恢复

## 1. 解决的问题

06-B 已经完成：

```text
PostgreSQL PENDING Outbox
→ Relay 设置 Lease
→ OutboxPublisher.Publish
→ 记录 PUBLISHED / retry / DEAD_LETTERED
```

但当时的 Publisher 是内存 Fake，只能证明 Relay 的事务和状态语义。真实 RabbitMQ
Adapter 必须回答：

- 写入 TCP socket 是否等于 Broker 已接管？
- Exchange 存在但没有 Queue 匹配时，消息会不会静默消失？
- Broker Nack、Confirm timeout 和断连怎样分类？
- 多个 Relay worker 并发 Publish 能不能共享一个 AMQP Channel？
- 断连后怎样恢复而不把消息藏在进程内存？
- RabbitMQ 重启后，Exchange、Queue 和消息是否仍存在？
- 重复 Event ID 发布时系统会发生什么？

本阶段把 `OutboxPublisher` 端口落到真实 RabbitMQ，同时不改变 06-B 的数据库事务设计。

## 2. 为什么现在做

实现 RabbitMQ 太早会让应用层围绕某个客户端 API 生长。现在已经有：

- 安全、传输无关的 `OutboxEvent`；
- 明确的 `OutboxPublisher` 契约；
- Lease + fencing；
- Retry / Dead Letter 决策；
- Confirm 后、MarkPublished 前崩溃会重复的 At Least Once 语义。

因此 RabbitMQ 只需要成为一个 Adapter：

```text
Application Port                 Infrastructure Adapter

OutboxPublisher.Publish   <───   rabbitmq.Publisher.Publish
transport-neutral event          AMQP connection/channel/confirm/return
```

下一阶段 Worker 依赖已经稳定的 Queue、routing key、Message ID 和 Header 契约，所以必须在
Worker 前完成 Publisher。

## 3. 技术选择

### 3.1 RabbitMQ 与 Go 客户端版本

当前固定：

```text
RabbitMQ                  4.3.4
github.com/rabbitmq/amqp091-go  v1.13.0
```

选择 `amqp091-go` 是因为它由 RabbitMQ 官方组织维护，并直接暴露 AMQP 0.9.1 的：

- `Confirm`；
- `PublishWithDeferredConfirmWithContext`；
- `NotifyReturn`；
- durable Exchange / Queue / Binding 声明；
- Connection 与 Channel 生命周期。

没有再套一层功能庞大的第三方 MQ Framework。项目需要明确掌握 Confirm、Return 和故障
窗口，过度封装反而容易把“socket write 成功”误当成“Broker 接管成功”。

### 3.2 为什么没有使用客户端自动恢复

`amqp091-go v1.13.0` 已经提供自动连接和拓扑恢复，但该能力仍被官方 API 标为
experimental。本项目选择：

```text
连接失效
→ 当前 Publish 返回 retryable error
→ 不在内存里缓存或自动重发事件
→ Relay 把结果写回 PostgreSQL Outbox
→ 下一次 Publish 按需重连并重声明拓扑
```

理由：

1. PostgreSQL Outbox 已经是恢复真相，不需要第二套客户端内存重试队列；
2. 断连窗口的 Publish 结果可能未知，必须让 Relay 按 At Least Once 处理；
3. Adapter 自己控制生命周期，后续替换客户端或升级实验性 API 的影响较小；
4. 连接恢复只恢复“后续可发布”，不能证明中断前的消息到底有没有到达 Broker。

这不是否定自动恢复。未来可以在固定版本和故障测试充分后启用，但不能改变 Outbox 的
可靠性职责。

## 4. 第一版拓扑

当前实现：

```text
mail.events.v1 (durable topic exchange)
  ├── mail.dispatch.v1.q (durable quorum)
  │     └── mail.message.dispatch.requested.v1
  │
  └── mail.lifecycle.v1.q (durable quorum)
        ├── mail.message.accepted.v1
        └── mail.message.status.changed.v1
```

事件与 routing key 是显式配置映射：

| Event Type | Routing Key | 初始消费者 |
| --- | --- | --- |
| `MESSAGE_DISPATCH_REQUESTED` | `mail.message.dispatch.requested.v1` | Delivery Worker |
| `MESSAGE_ACCEPTED` | `mail.message.accepted.v1` | 后续通知/审计 |
| `MESSAGE_STATUS_CHANGED` | `mail.message.status.changed.v1` | 后续通知/审计 |

不支持的 Event Type 返回永久 `RABBITMQ_UNSUPPORTED_EVENT`，不会把拼写错误发到一个临时
topic。

### 为什么不立刻按 CRITICAL / BULK 分四条 Queue

目标架构确实要按类别做舱壁，但当前 `OutboxEvent` 的传输字段没有 category。如果
RabbitMQ Adapter 解析 JSON payload 才能路由，就会产生：

```text
基础设施层 → 依赖 messageEventEnvelope 某一版 JSON 结构
```

这会破坏 Adapter 边界。后续应把 category 设计为显式、安全、稳定的路由元数据，再扩展
物理 Queue；本阶段先按事件职责隔离 dispatch 与 lifecycle。

## 5. Durable、Persistent、Confirm 各自保证什么

三者不能混为一谈：

### Durable Exchange / Queue

表示拓扑能在 Broker 重启后恢复。非 durable Queue 即使消息是 persistent，也不能构成
持久投递链。

### Persistent Message

本项目设置：

```text
DeliveryMode = amqp.Persistent
```

它告诉 Broker 这条消息需要持久化处理。它不告诉 Publisher “消息已经安全落盘”。

### Publisher Confirm

每个 Publish 等待自己的 Deferred Confirmation：

```text
Publish
→ Broker Ack  → Adapter 返回 nil
→ Broker Nack → retryable RABBITMQ_NACK
→ 超时/断连   → 结果未知，返回 error
```

只有 Confirm ACK 后才返回 `nil`，因此 06-B Relay 的 `MarkPublished` 前提成立。

即便 Ack，RabbitMQ 与 PostgreSQL 仍没有共同事务：Ack 后、MarkPublished 前崩溃会导致
Outbox 再次发布，所以消费者必须按 Event ID 幂等。

## 6. `mandatory` 解决静默无路由

如果发布时 `mandatory=false`：

```text
Exchange 存在
但 routing key 没有匹配 Queue
→ RabbitMQ 可以直接丢弃消息
→ Publisher 仍可能看到 Confirm ACK
```

Confirm ACK 在这里表示 Broker 已经完成了这次 Publish 的处理，不等于一定进入 Queue。

当前固定：

```text
mandatory = true
immediate = false
```

无路由时 RabbitMQ 先发送 `basic.return`，再确认该 Publish。Adapter 为每个独占 Channel
注册有缓冲的 Return listener，收到匹配 Message ID 的 Return 后返回永久错误：

```text
RABBITMQ_UNROUTABLE / Retryable=false
```

如果 Return Message ID 与当前 Event ID 不一致，说明 Channel correlation 已经不可信，
返回 retryable `RABBITMQ_PROTOCOL` 并废弃连接，而不是猜测这次发布结果。

## 7. 完整发布流程

一次 `Publish`：

```text
1. 校验 OutboxEvent 与 attempt number
2. 按 Event Type 查稳定 routing key
3. 从有界池借一个独占 lane
4. 如果没有健康 Connection：按需连接并声明拓扑
5. 如果 lane 没有健康 Channel：创建 Channel、开启 Confirm、注册 Return
6. 构建 persistent AMQP message
7. mandatory Publish
8. 等待该消息的 Deferred Confirmation
9. Confirm ACK 后检查对应 Return
10. 返回 nil / typed retryable error / typed permanent error
11. 归还 lane
```

Publisher 构造函数只做本地配置校验，不连接 Broker：

```text
New(config) 成功 ≠ RabbitMQ 当前在线
```

这是有意设计。RabbitMQ 故障时，Submission API 仍应能依靠 PostgreSQL 有限受理并让
Outbox 积压；不能因 `role=all` 初始化 Publisher 失败，让 API 一起无法启动。生产上仍需
通过 Relay readiness、Outbox lag 和容量阈值决定何时保护性拒绝新请求。

## 8. Connection 与 Channel 并发模型

AMQP Connection 是长生命周期 TCP 连接，Channel 是复用 Connection 的轻量逻辑会话。

当前模型：

```text
一个 Publisher
  └── 一条长连接
        ├── Channel lane 1 ── 同时最多一个 Publish
        ├── Channel lane 2 ── 同时最多一个 Publish
        └── ... 有界 ChannelPoolSize
```

不让多个 goroutine 同时在同一 Channel Publish，原因是：

- 共享并发可能造成 frame interleaving；
- Confirm delivery tag 属于 Channel；
- Return 也从 Channel 异步到达；
- 即使客户端内部某些方法加锁，业务仍难以安全关联“这一个 Return/Confirm 属于哪一条
  Outbox Event”。

也没有为每条消息建立 Connection 或 Channel，否则会产生 connection/channel churn、
握手开销和 Broker 资源浪费。

`ChannelPoolSize` 固定在 `1..128`。Relay 自己还有 `PublishConcurrency`，实际并发上限是
两者较小值；配置时应让 Channel 池至少覆盖该进程的 Relay 并发。

## 9. 为什么超时要废弃 Connection

Go 客户端的 Publish Context 可以在调用前阻止 Publish，但调用开始后的底层 I/O 不一定
能被 Context 直接打断。例如 Broker resource alarm 可能让 socket write 阻塞。

实现把开始 Publish 放进独立 goroutine，并由外层 Context 做有界等待：

```text
Publish 正常返回 → 等 Confirm
Context 先结束   → 忘记当前 Connection + 有界 CloseDeadline
```

Confirm 等待超时也执行同样隔离。原因是：

- 消息可能已经到 Broker；
- 旧 Channel 上可能稍后到达 Confirm 或 Return；
- 如果立刻复用同一 Channel，迟到信号可能污染下一条消息的判断。

废弃整条 Connection 会让它上面的其他并发 Channel 一起失败。这是明确取舍：少量在途
事件会由 Outbox 重试，代价优于继续使用 correlation 已不可信的连接并可能静默丢消息。

## 10. 消息元数据

AMQP Properties：

| 字段 | 值 | 用途 |
| --- | --- | --- |
| `MessageId` | Outbox Event ID | Consumer 幂等键 |
| `CorrelationId` | Aggregate / Message ID | 关联逻辑邮件 |
| `Type` | Event Type | 事件类型 |
| `AppId` | `mail-service` | 发布者身份 |
| `ContentType` | `application/json` | payload 编码 |
| `DeliveryMode` | Persistent | Broker 重启持久化 |
| `Timestamp` | Adapter Publish 时间 | 传输观测，不是业务 occurred time |

AMQP Headers：

```text
x-mail-aggregate-type
x-mail-aggregate-id
x-mail-aggregate-sequence
x-mail-dispatch-generation
x-mail-publish-attempt
```

Message body 是 Outbox 安全 JSON 的副本。Adapter 不加入邮箱地址、验证码、正文、模板
变量或 Broker 凭据。

`publish-attempt` 表示数据库当前已记录结果语义下的 attempt number，不宣称是不可观测
崩溃窗口里的精确物理 Publish 次数。

## 11. 错误分类

| Code | Retryable | 含义 |
| --- | ---: | --- |
| `RABBITMQ_INVALID_PUBLICATION` | false | Event 或计数非法 |
| `RABBITMQ_UNSUPPORTED_EVENT` | false | 没有配置事件路由 |
| `RABBITMQ_UNAVAILABLE` | true | 连接或 Channel 不可用 |
| `RABBITMQ_TOPOLOGY` | true | 拓扑声明失败，需要运维修复/恢复 |
| `RABBITMQ_PUBLISH` | true | Publish I/O 失败，结果可能未知 |
| `RABBITMQ_CONFIRM_MISSING` | true | Confirm 模式未产生 correlation future |
| `RABBITMQ_NACK` | true | Broker Nack |
| `RABBITMQ_UNROUTABLE` | false | mandatory Return，路由配置错误 |
| `RABBITMQ_PROTOCOL` | true | Return correlation 不可信 |
| `RABBITMQ_CLOSED` | true | Publisher 已关闭 |

错误通过 `OutboxPublishError` 只向 Relay 暴露稳定 Code 和 Retryable。原始 socket、AMQP
错误保存在 `Cause()` 供内部日志/指标使用，不进入数据库稳定错误码。

Topology 失败被视为 retryable，而不是立即把所有积压事件死信。因为它通常是全局配置或
Broker 状态问题，需要告警和修复；在此期间 Outbox 应保留。已经成功建立拓扑后某条消息
收到明确 No Route，则作为单条永久错误处理。

## 12. Queue 参数为什么没有全写在代码里

应用声明固定：

```text
exchange type = topic
exchange durable = true
queue durable = true
x-queue-type = quorum
bindings = 稳定协议契约
```

max length、max bytes、delivery limit、dead-letter exchange 等可运营参数优先使用 RabbitMQ
Policy / Operator Policy。原因是这些值需要按环境容量调整；如果全部成为客户端
`x-arguments`，修改往往需要发版，且声明参数不等价会关闭 Channel。

06-C 只完成 Publisher 侧。Consumer manual ACK、delivery limit、DLQ 与 poison message
处理在 Worker 阶段一起落地，避免声明了没人负责消费/重放的“装饰性 DLQ”。

## 13. 真实故障语义

| 故障点 | Adapter / Relay 结果 |
| --- | --- |
| RabbitMQ 启动前不可用 | New 仍成功；首次 Publish 返回 retryable unavailable |
| Exchange/Queue 首次不存在 | 按需幂等声明 |
| 属性不一致 | 拓扑声明失败，Outbox 重试并应告警 |
| Exchange 无匹配 Binding | mandatory Return，单条永久失败 |
| Broker Nack | retryable，不能标 PUBLISHED |
| Confirm timeout | 结果未知，废弃连接，Outbox 重试 |
| TCP 在 Publish 中断 | 结果未知，Outbox 重试 |
| Broker 恢复 | 下一次 Publish 重连并重声明拓扑 |
| Ack 后、MarkPublished 前崩溃 | 同 Event ID 再发布，Queue 中可出现重复 |
| Publisher Close | 等待 lane 的调用被唤醒，后续 Publish 返回 closed |

## 14. 测试过程中发现的问题

### 14.1 Race Detector 找到 lane 生命周期竞态

最初超时路径会清空：

```text
lane.connection / lane.channel
```

同时 Publish goroutine 还在从 lane 读取 Channel，Race Detector 报告读写竞争。

修复方式不是“加一个大锁包住网络”，而是在启动 goroutine 前捕获本次 Publish 的不可变
Connection/Channel 引用。lane 可以被安全清理和归还，旧 goroutine 只持有旧引用，随后由
Connection Close 解除阻塞。

### 14.2 Testcontainers Stop/Start 改变随机端口

最初使用 Container Stop/Start 测重启，宿主机随机映射端口发生变化，Publisher 仍指向旧
URL。这测试的是“服务发现地址变化”，不是生产前提下的 Broker 重启。

最终故障注入改为：

```text
rabbitmqctl stop_app
rabbitmqctl start_app
```

Broker 应用真正停止并关闭连接，数据目录与访问地址保持不变，可以公平验证持久消息与
按需重连。

## 15. 主要文件

- `internal/publisher/rabbitmq/config.go`
- `internal/publisher/rabbitmq/transport.go`
- `internal/publisher/rabbitmq/publisher.go`
- `internal/publisher/rabbitmq/config_test.go`
- `internal/publisher/rabbitmq/publisher_test.go`
- `internal/testkit/rabbitmqcontainer/rabbitmq.go`
- `internal/integration/rabbitmq_publisher_test.go`
- `compose.yaml`
- `.env.example`
- `Makefile`
- `go.mod`
- `go.sum`

## 16. 验证

已经执行并通过：

```text
go test ./...
go vet ./...
go test -race ./...
go test -tags=integration ./internal/integration -run '^$'
go vet -tags=integration ./internal/integration/...
go test -count=10 ./internal/publisher/rabbitmq
docker compose config --quiet
make migrate-validate
make check-all
TEST_RABBITMQ_IMAGE=rabbitmq:4-management \
  go test -tags=integration ./internal/integration \
  -run '^TestRabbitMQPublisher$' -v -count=1
TEST_POSTGRES_IMAGE=postgres:18.4-alpine \
  TEST_RABBITMQ_IMAGE=rabbitmq:4-management \
  go test -count=1 -tags=integration ./internal/integration/...
TEST_POSTGRES_IMAGE=postgres:18.4-alpine \
  TEST_RABBITMQ_IMAGE=rabbitmq:4-management \
  go test -count=1 -race -tags=integration ./internal/integration \
  -run '^TestRabbitMQPublisher$'
go test -count=1 -cover ./internal/publisher/rabbitmq
```

本机缓存的官方 `rabbitmq:4-management` 在执行时对应 RabbitMQ 4.3.4。精确标签
`rabbitmq:4.3.4-management-alpine` 是 Compose、Testcontainers 默认值；首次拉取精确
alpine 标签在当前网络环境的 5 分钟容器准备窗口内超时，因此没有把那次执行记为通过。

单元测试覆盖：

- 配置、URL、路由与拓扑边界；
- lazy connection；
- persistent message 与全部 AMQP metadata；
- confirmed success；
- dial、topology、publish、missing confirm、Nack；
- matching/mismatched mandatory Return；
- Confirm timeout 与阻塞 Publish 的有界退出；
- 超时后连接废弃和下一次重新 Dial；
- 两个并发 Publish 使用两个独占 Channel；
- Close 幂等与关闭后拒绝发布；
- payload 副本不与调用方共享底层数组。

真实 RabbitMQ + PostgreSQL 测试覆盖：

- durable topic exchange 与 Quorum Queue 属性一致性；
- Confirm 后消息可以从 Queue 读取；
- Message ID、Correlation ID、Type、persistent mode 和 Header；
- 删除 Binding 后 mandatory Return 映射为永久 unroutable；
- 相同 Event ID 发布两次，Consumer 能看到两个重复消息；
- PostgreSQL Outbox → Relay → RabbitMQ → MarkPublished 跨组件链路；
- Broker `stop_app/start_app` 后旧 persistent message 仍在；
- 旧连接断开后 Publisher 按需重连并成功发布新消息。

单节点 Testcontainers 只能验证协议、故障窗口和重启持久化，不能证明三节点 Quorum Queue
在少数节点故障时仍可用。集群多数派、滚动升级和网络分区留到生产化故障演练。

最终全量 PostgreSQL + RabbitMQ integration suite 用时约 `47.0s`；RabbitMQ 跨组件 race
用例约 `21.1s`；RabbitMQ Adapter 单元语句覆盖率为 `79.6%`。冲突标记、TODO/FIXME/HACK
和行尾空白审计均无结果。

## 17. 面试表达

### 30 秒版本

> 我给 Outbox Relay 实现了 RabbitMQ Adapter，使用 durable topic exchange、Quorum Queue、
> persistent message、mandatory 和单消息 Publisher Confirm。一次并发 Publish 独占一个
> 池化 Channel，只有 Confirm ACK 且没有 mandatory Return 才返回成功。超时后废弃连接，
> 下一次由 PostgreSQL Outbox 驱动重连和重试，不在客户端内存缓存消息。真实容器测试验证
> 了无路由、重复 Event ID、Broker 重启持久化，以及 PostgreSQL Relay 到 RabbitMQ 的完整
> 链路。

### 2 分钟版本

1. 先说明 socket write、persistent 和 Confirm 的区别；
2. 解释 Confirm ACK 不等于一定路由，为什么还要 mandatory Return；
3. 画一条 Connection + 有界独占 Channel 池；
4. 解释 Deferred Confirmation 如何对应单条 Event；
5. 展示 Event ID / sequence / generation metadata；
6. 解释为什么构造函数 lazy connect，Broker 故障不阻止 PostgreSQL 受理；
7. 解释为什么不用实验性自动恢复和客户端内存重发；
8. 用 Confirm timeout 说明结果未知和连接隔离；
9. 用 Ack 后数据库崩溃说明重复消息不可消除；
10. 用 RabbitMQ 重启和跨组件集成测试作为证据。

### 可能追问

**Persistent message 为什么还需要 Publisher Confirm？**

Persistent 是给 Broker 的存储意图，Publish 本身是异步的。客户端写 socket 成功不代表
Broker 已经接收并处理；Confirm 才给 Publisher 一个 Ack/Nack 结果。

**Confirm ACK 为什么还会无路由？**

Confirm 表示 Broker 完成了这次 Publish 的处理。如果 mandatory=false，无匹配 Queue 时
“处理完成”可以是丢弃。mandatory=true 才要求 Broker Return，无路由必须同时看 Return。

**为什么不能多个 goroutine 共用一个 Channel？**

Confirm delivery tag 和 Return 都属于 Channel，共享并发会让 correlation 复杂，并可能
发生 AMQP frame interleaving。当前用有界池，每个 Publish 暂时独占一个 Channel。

**为什么超时关掉整条 Connection，影响其他消息？**

超时后旧 Channel 的结果可能迟到，继续复用会污染下一条消息。关闭连接让所有未知在途
事件统一回到 Outbox 重试，可能增加重复，但不冒静默丢失或错配确认的风险。

**为什么 RabbitMQ 挂了，New Publisher 仍成功？**

New 只校验本地配置。RabbitMQ 不是 Submission API 返回 ACCEPTED 的同步依赖，PostgreSQL
Outbox 才是受理真相。Broker 故障由 Relay 积压、readiness、lag 和容量保护处理。

**相同 Event ID 发布两次，RabbitMQ 会去重吗？**

不会。Message ID 是消费者幂等键，不是 RabbitMQ 去重开关。真实测试明确看到两条相同
Event ID 的消息，Worker 必须查数据库状态/generation 后幂等处理。

## 18. 尚未解决

- RabbitMQ Consumer、manual ACK、prefetch 和优雅停机；
- Worker 对 Event ID、sequence、dispatch generation 的幂等；
- Fake Provider 与 Message Attempt 事务；
- Critical / Bulk 物理 Queue 舱壁和稳定 category 路由元数据；
- delivery limit、dead-letter policy、DLQ 查询与安全重放；
- Publisher/Connection/Confirm latency、Return、Nack、Outbox lag 指标；
- TLS、独立 vhost、最小权限用户和凭据轮换；
- 三节点 Quorum Queue 的多数派故障与滚动升级演练。

下一阶段是 07：RabbitMQ Worker + Fake Provider。重点从“Broker 是否接管”转为“Worker
何时 ACK、怎样防止旧 generation 或重复 Event 触发第二次逻辑发送”。
