# 08-B2A：Notification Worker 核心与 gRPC Callback Client

- 状态：已完成
- 阶段目标：用 lifecycle `event_id` 回查权威 Journal，并把安全、可重试分类明确的状态事件投递给预注册 gRPC 订阅方。

## 1. 解决的问题

08-B1 已经把生命周期事实可靠发布到 `mail.lifecycle.v1.q`，但队列里的消息还没有最终送到
AI-Nexus。直接在 RabbitMQ Consumer 中读取 JSON、拼 Proto、调用 gRPC 会把四种职责混在一起：

- 消息格式解析；
- 权威事实读取；
- 回调协议转换；
- RabbitMQ ACK、重试和 DLQ。

08-B2A 先完成与 RabbitMQ 无关的应用核心和 gRPC 出站适配器：

```text
event_id
  → Delivery Event Journal GetByID
  → 校验权威、脱敏的 PersistedDeliveryEvent
  → gRPC DeliveryEventReceiverService.ReportDeliveryEvent
  → ACCEPTED / DUPLICATE / IGNORED_STALE
```

08-B2B 只需要把 lifecycle MQ 消息解析成 `notification.Command{EventID: ...}`，再根据本阶段
提供的稳定错误分类执行 ACK、延迟重试或 DLQ。

## 2. 为什么必须回查 Journal，不能直接信任 MQ Payload

RabbitMQ 消息是传输副本，PostgreSQL Journal 是业务事实来源。生命周期 Outbox 为了避免敏感
数据扩散，本来就没有复制 `idempotency_key` 等全部回调字段。即使未来扩展了 payload，也仍然
不应该让一条损坏、伪造或旧版本 MQ 消息直接生成对外回调。

现在 MQ 只负责携带稳定 `event_id`，Worker 用它读取 Journal：

- `idempotency_key`、status、sequence 和时间来自事务内落库的事实；
- MQ 重复投递会读到同一记录并发送相同 `event_id`；
- MQ schema 改版不会改变已经保存的业务事实；
- 收件地址、验证码、模板变量和密文不会进入回调。

这相当于“消息是唤醒信号，数据库是权威数据”。代价是每条通知多一次主键查询；当前正确性比
这次低成本索引查询更重要，后续压测证明必要时可加批量读取或只读副本。

## 3. 为什么读接口与写接口分开

原有 `DeliveryEventRepository` 只负责事务内 `Append`。本阶段新增独立的：

```go
type DeliveryEventReader interface {
    GetByID(context.Context, string) (PersistedDeliveryEvent, error)
}
```

没有为了方便把读方法塞进写接口。这样做符合接口隔离：

- 状态机事务仍只依赖写能力；
- Notification Worker 只依赖读能力；
- 未来 Reader 可以连接只读副本，Writer 继续连接主库；
- 测试 Fake 更小，不需要实现无关方法。

`PersistedDeliveryEvent` 在业务事件外增加 `observed_at`。`occurred_at` 表示事实发生时间，
`observed_at` 表示 Mail Service 把事实写入 Journal 的时间；两者不能因为机器时钟或外部事件
延迟而强制要求前者小于后者。

## 4. Worker 和 Adapter 的边界

Application Worker 负责：

- 验证 `event_id`；
- 读取并再次校验 Journal；
- 限制单次回调超时；
- 校验订阅方 disposition；
- 把错误归类为 `TRANSIENT` 或 `POISON`。

gRPC Adapter 负责：

- 把领域 status、failure、时间映射到 Proto；
- 调用生成的 gRPC Client；
- 校验响应中的 `event_id` 和 disposition；
- 把 gRPC status 转成稳定、脱敏的错误码和 retryable 标记。

RabbitMQ delivery tag、ACK 和 DLQ 不进入 Worker；gRPC code 也不进入 Worker。应用层因此可以
用 Fake Subscriber 单测，gRPC Adapter 也可以独立替换成签名 Webhook 或其他消息总线。

## 5. 成功、重试和永久失败语义

订阅方的三个响应都表示“不需要再次发送本事件”：

| Disposition | 含义 | 08-B2B 动作 |
| --- | --- | --- |
| `ACCEPTED` | 首次成功处理 | ACK |
| `DUPLICATE` | `event_id` 已处理 | ACK |
| `IGNORED_STALE` | sequence 已落后，不能倒退状态 | ACK |

典型可重试错误：

- `UNAVAILABLE`、`RESOURCE_EXHAUSTED`、`DEADLINE_EXCEEDED`；
- `ABORTED`、`INTERNAL`、网络导致的 `UNKNOWN`；
- 远端 `NOT_FOUND`：AI-Nexus 的本地请求记录可能存在短暂提交竞态，允许有界重试。

典型永久错误：

- `UNAUTHENTICATED`、`PERMISSION_DENIED`；
- `INVALID_ARGUMENT`、`FAILED_PRECONDITION`、`UNIMPLEMENTED`；
- 响应 event ID 不匹配、未返回 disposition 等协议错误；
- MQ 引用的本地 Journal 记录不存在或记录损坏。

“可重试”不代表无限重试。08-B2B 将使用 RabbitMQ quorum queue 的 delivery limit、延迟重试和
独立 notification DLQ 给它加上界。

## 6. 安全设计

Callback 请求只包含 Journal 中允许公开给授权业务方的字段：

- event/message/idempotency identity；
- status、sequence、attempt number；
- occurred/observed time；
- 可选 Provider message ID；
- 脱敏 failure category/code/retryable。

它不读取 Submission 密文，因此代码结构上就无法意外回传验证码。错误对象保存底层 cause 供
内部日志和 Trace 使用，但不加入 `errors.Unwrap` 链，也不把远端 detail 拼进稳定错误文本。

`Dial` 强制调用者显式提供 `credentials.TransportCredentials`：开发测试可以明确传
`insecure.NewCredentials()`，生产装配则传 TLS/mTLS。Adapter 不提供静默回退到明文的默认值。

## 7. 主要文件

- `internal/application/ports/delivery_event_repository.go`：持久化事件和只读 Port；
- `internal/application/ports/delivery_event_subscriber.go`：Subscriber Port、成功 disposition 和类型化失败；
- `internal/storage/postgres/delivery_event_queries.go`：按主键读取 Journal；
- `internal/storage/postgres/delivery_event_repository.go`：扫描、字段一致性和腐坏记录识别；
- `internal/application/notification/worker.go`：回查、超时和错误分类；
- `internal/subscriber/grpcsubscriber/client.go`：Proto 映射、gRPC 调用和 status 分类；
- `internal/subscriber/grpcsubscriber/client_test.go`：真实本地 gRPC Server 联调测试；
- `internal/integration/email_submission_test.go`：真实 PostgreSQL Journal 查询验证。

## 8. 验证

本阶段验证覆盖：

- 三种成功 disposition 都被视为完成；
- 非法 event ID 在访问依赖前被拒绝；
- Journal 缺失、ID 不匹配、非法 disposition 被归为 poison；
- callback 有严格超时，原始未知错误默认 transient；
- gRPC 临时与永久状态码正确分类；
- 响应 event ID 不一致不能误 ACK；
- 所有领域 status 和 failure category 都有 Proto 映射；
- 本地真实 TCP gRPC Server 收到完整安全事件；
- PostgreSQL 可以按稳定 event ID 读回 `observed_at` 和完整 Journal 事实。

使用的检查命令：

```bash
go test ./...
go test -race ./...
go vet ./...
make migrate-validate
go test -tags=integration ./internal/integration/...
```

## 9. 面试表达

### 30 秒版本

> 我把通知链路拆成应用 Worker、gRPC Adapter 和下一阶段的 RabbitMQ Consumer。MQ 事件只作为
> event ID 唤醒信号，Worker 回查 PostgreSQL Journal 生成权威回调，避免相信不完整或被污染的
> MQ payload。回调支持 accepted、duplicate、ignored-stale 三种成功结果，并把 gRPC 错误稳定
> 分类成 transient/poison，给后续有界重试和 DLQ 使用。

### 2 分钟版本

> 生命周期 Outbox 已经能可靠到 RabbitMQ，但直接在 Consumer 中拼 gRPC 请求会混合协议解析、
> 业务事实读取和 ACK 策略。我新增独立 DeliveryEventReader，Worker 只用 MQ 的 event ID 回查
> append-only Journal，所以 idempotency key、sequence 和时间都来自与 Message、Outbox 同事务的
> 权威事实；Submission 密文根本不在依赖里，不会泄漏验证码。
>
> 出站 gRPC Adapter 负责完整的领域到 Proto 映射，并验证响应 event ID。accepted、duplicate、
> ignored-stale 都代表无需重试；Unavailable、限流、超时等是 transient，鉴权、非法参数和协议
> 错误是 poison。错误只暴露稳定 code/retryable，原始 cause 留给内部观测。每次调用有超时，
> TransportCredentials 必须显式注入，因此生产可以接 mTLS，开发明文也必须明确开启。
>
> 当前阶段还没操作 RabbitMQ ACK。下一步的 lifecycle Consumer 只负责解析 event ID，调用 Worker，
> 再把 transient 映射到 quorum queue 延迟重试、poison 映射到独立 DLQ，职责边界会非常清楚。

### 可能追问

**为什么不在更新邮件状态的事务里同步调用 AI-Nexus？**

数据库事务无法和远端 gRPC 形成原子事务。同步调用会长时间持锁，并且“远端成功、本地回滚”或
“远端成功、响应丢失”仍然存在。Transactional Outbox + 至少一次回调用幂等解决这个窗口。

**为什么远端 `NOT_FOUND` 是 transient，本地 Journal `NOT_FOUND` 却是 poison？**

本地 MQ 事件 ID 来自与 Journal 同事务的 Outbox，记录缺失违反本地不变量，重复消费不会修复；
远端业务记录可能正处在另一个事务提交或部署切换的短暂窗口，允许有上限地重试更稳妥。

**回调重复会不会导致 AI-Nexus 重复激活验证码？**

Mail Service 保持稳定 event ID，AI-Nexus 按 event ID 幂等；它还应保存每个 message 的最大
sequence，旧事件返回 `IGNORED_STALE`，不能让状态倒退。Exactly Once 不由网络提供，而由稳定
身份、幂等消费和顺序保护共同实现业务效果。

**每条通知都查 PostgreSQL 会不会慢？**

这是 UUID 主键单行读取，没有锁住 Message 热行。当前先保证事实权威；若压测表明数据库成为
瓶颈，可以增加批量读取、只读副本或缓存，但缓存仍不能改变 Journal 的 source-of-truth 地位。

## 10. 尚未解决

- lifecycle RabbitMQ Consumer、payload parser 和 manual ACK；
- notification queue 的延迟重试、delivery limit、独立 DLX/DLQ；
- Composition Root、环境变量、readiness 和优雅停机装配；
- AI-Nexus 端真实服务联调、event ID 幂等表和 sequence 防倒退；
- 多租户订阅地址、mTLS 证书轮换、隔离执行池与熔断；
- DLQ 人工对账与安全重放工具。
