# 07-B：RabbitMQ Consumer、Manual ACK、延迟重试与 DLQ

## 1. 这一阶段解决什么问题

07-A 已经实现了传输无关的 `DispatchWorker.Process`，但 RabbitMQ Delivery 还没有人消费。
本阶段补齐 RabbitMQ Consumer Adapter，把一条 MQ 消息可靠地翻译为：

```text
RabbitMQ Delivery
  → 严格校验 AMQP Properties / Headers / JSON Envelope
  → DispatchCommand
  → DispatchWorker.Process
  → ACK、延迟重试或 Dead Letter
```

完成后，MQ 传输层和业务编排层的边界是：

- Worker 决定命令是否成功、是否是不可恢复的 poison error；
- Consumer 决定使用哪一种 AMQP acknowledgement；
- RabbitMQ Policy 决定重试延迟、delivery limit 和 DLX；
- Worker 不依赖 `amqp091-go`，RabbitMQ Adapter 也不修改邮件状态。

## 2. 为什么现在做

如果先写 Consumer、后写 Worker，ACK 往往会散落在数据库事务和 Provider 调用之间，很难
回答“究竟在哪一步之后才允许删除消息”。07-A 已经冻结两事务 Worker 的安全边界，所以
07-B 只需遵守一个核心规则：

> 只有 `DispatchWorker.Process` 返回 nil，才说明数据库已经给出了可重复消费的稳定结果，
> Consumer 才能 ACK。

这里的 nil 不只代表 Provider 接受，也包括重复消息、旧 generation、已经终态等幂等结果。

## 3. 整体架构

```text
                      one AMQP connection
                              │
              ┌───────────────┼───────────────┐
              │               │               │
          Channel 1       Channel 2       Channel N
          prefetch=P      prefetch=P      prefetch=P
              │               │               │
          sequential      sequential      sequential
          Process         Process         Process

有效成功 ──────────────── basic.ack
瞬时基础设施失败 ──────── basic.reject(requeue=true)
格式错误/业务毒消息 ───── basic.nack(requeue=false) ──→ DLX ──→ DLQ
进程或连接中断 ────────── 保持 unacked，连接关闭后 Broker 重投
```

默认 `LaneCount=4`、`PrefetchPerLane=1`，因此单实例最多有 4 条正在处理且未确认的
Delivery。总在途上限可近似理解为：

```text
实例数 × LaneCount × PrefetchPerLane
```

### 为什么一条 lane 一个 Channel

AMQP delivery tag 只在产生它的 Channel 内有效。若多个 goroutine 共享 Channel 并交叉 ACK，
很容易用错误 Channel 确认 delivery tag，Broker 会关闭 Channel。独立 Channel 同时形成了
简单的并发舱壁：每条 lane 顺序处理，跨 lane 并行。

Channel 数不能无限增加，所以配置限制为 1..128；prefetch 限制为 1..100。生产参数应根据
Provider P95 延迟、数据库连接池和租户限速压测，不是越大越好。

## 4. 严格消息边界

`ParseDispatchDelivery` 不相信 MQ 内的任何字段，它同时验证：

- exchange、routing key；
- `ContentType=application/json`、persistent delivery mode、event type、App ID；
- body 大小在 1..64 KiB；
- aggregate type、aggregate ID、sequence、dispatch generation、publish attempt Headers；
- Header 的 AMQP 数字类型必须是精确 `int64`，不能静默做有损转换；
- JSON 只有一个值，schema version 为 1，时间非零；
- Message ID、Correlation ID、Header 和 Envelope 中的 ID/sequence/generation 相互一致；
- 最终 `DispatchCommand` 的 UUID 和 PostgreSQL BIGINT 边界有效。

校验错误只暴露稳定错误码，不把 body、邮箱、验证码或 Broker URL 凭据拼进错误文本。
格式不合法的消息不会调用 Worker。

## 5. ACK 决策表

| Worker/解析结果 | AMQP 动作 | 含义 |
| --- | --- | --- |
| 处理成功、重复或陈旧 | `Ack(false)` | 结果已经稳定，可以从队列删除 |
| AMQP/JSON 格式错误 | `Nack(false, false)` | 重试不会变合法，立即进入 DLQ |
| Worker poison error | `Nack(false, false)` | 本地不变量或命令永久无效，立即进入 DLQ |
| 数据库等瞬时错误 | `Reject(true)` | 记一次失败，交给 Quorum Queue 延迟重试 |
| shutdown 时还没发出确认 | 不确认 | 关闭连接后由 Broker 自动重投 |
| ACK/NACK/Reject 本身失败 | 退出当前 lane | 不能假定 Broker 已收到确认，重建 Channel |

没有使用 `multiple=true` 批量确认，因为不同消息可能有不同结果；错误批量 ACK 会永久删除
尚未完成的消息。

## 6. RabbitMQ 4.3 中 Reject 和 Nack 的关键区别

这是本阶段最容易在面试中体现工程深度的地方。

RabbitMQ 4.3 的 Quorum Queue 同时记录 `acquired-count` 和 `delivery-count`：

- AMQP 0.9.1 `basic.nack` 是普通 return，不增加 `delivery-count`；
- `basic.reject` 被视为失败，会增加 `delivery-count`；
- delivery limit 基于 `delivery-count`，不是所有重投次数。

因此瞬时错误不能简单使用 `Nack(requeue=true)`，否则它可能永远不消耗 delivery limit。
当前方案使用 `Reject(true)`，并设置 `delayed-retry-type=failed`：

```text
delay = min(1000ms × delivery_count, 30000ms)
```

也就是约 1s、2s、3s……最多 30s。达到 20 次 delivery limit 后，Broker 将消息死信。

确定的毒消息不能也用 `Reject(false)`：在 `failed` 延迟策略下，它会先进入失败延迟路径。
当前使用 `Nack(false, false)`，由于它不属于 failed retry，`requeue=false` 会让消息立即死信。
这套组合不是凭文档猜出来的，已用 RabbitMQ 4.3.4 集成测试验证。

Consumer 在 Reject 前还有 100ms..2s 的短随机保护，作用是在 Policy 暂时缺失或变更期间降低
本地 hot loop 风险；真正的可持续退避由 Broker Policy 完成。

## 7. 为什么 DLX 配置放 Policy，不写死在 QueueDeclare

应用只声明不可轻易改变的契约：durable topic exchange、durable quorum queue 和 binding。
以下参数由 `mail-dispatch-reliability` Policy 管理：

```json
{
  "dead-letter-exchange": "mail.dead.v1",
  "dead-letter-routing-key": "mail.dispatch.dead.v1",
  "dead-letter-strategy": "at-least-once",
  "overflow": "reject-publish",
  "delivery-limit": 20,
  "delayed-retry-type": "failed",
  "delayed-retry-min": 1000,
  "delayed-retry-max": 30000
}
```

原因有两个：

1. RabbitMQ 官方建议用 Policy 管理 DLX；Policy 可以在线变更，硬编码 `x-arguments` 往往要
   删除并重建队列；
2. 延迟、delivery limit、容量上限属于环境运行参数，不应要求业务重新编译。

Quorum Queue 的 at-least-once dead lettering 还要求 `overflow=reject-publish`。Broker 的
内部 dead-letter consumer 会等目标队列 confirm 后才从源队列删除，所以 DLX 暂时不可用
时消息会留在源队列；代价是更多资源，且最终目标仍可能看到重复消息。

本地执行：

```bash
make mq-up
make mq-policy-status
```

`mq-up` 和 `infra-up` 都会幂等执行 `mq-policy-apply`。routing key 是协议的一部分，策略和
binding 必须完全相同；本阶段集成测试曾实际发现一个 key 漂移，并证明错误配置会让
at-least-once dead-letter worker 持续保留消息而不是静默确认成功。

## 8. 重连与 Channel 自愈

Consumer 是两级监督结构：

- Connection supervisor：连接失败使用 exponential full jitter，成功后声明拓扑并启动 lanes；
- Lane supervisor：单个 Channel/consumer 流关闭时只重建该 lane，不立刻拖垮其他 lane；
- Connection 确认关闭或任一 lane 无法在当前连接恢复时，结束 session 并重拨连接；
- 拨号使用 context-aware `net.Dialer`、连接名、heartbeat 和有界握手时间。

客户端不保存内存消息副本。未 ACK 的 Delivery 由 RabbitMQ 在 Channel/Connection 关闭后
重新投递，业务幂等由 07-A 的 Message sequence、generation 和 Attempt 唯一约束保证。

## 9. 优雅停机

收到进程 context cancellation 后：

1. 取消所有 `ConsumeWithContext`，停止领取新消息；
2. 等待正在执行的 Worker 在 `ShutdownTimeout` 内完成；
3. 所有 lane 退出后关闭 AMQP connection；
4. 若超时，强制关闭 connection，让未确认消息回队列，并返回
   `ErrConsumerShutdown`。

07-A 的 Provider 调用继承 shutdown context，会停止继续等待；但 Provider 已被调用后，
Worker 的 finalize 使用有界 `context.WithoutCancel`，仍尽力把结果落库。这样既不会无限拖住
发布，也不会因为关机立即丢掉一次已经发生的外部调用观察。

真实测试还发现：`ConsumeWithContext` 取消时客户端自己会发送 `basic.cancel`，若 lane 同时
调用 `Channel.Close`，两个握手可能竞争并拖到 shutdown timeout。最终顺序改为 lane 先退出，
再由 session 统一关闭 Connection。

## 10. 主要代码

- `internal/messaging/rabbitmq/contract.go`：Publisher/Consumer 共享拓扑、路由和 Header 契约；
- `internal/consumer/rabbitmq/config.go`：并发、prefetch、超时和退避配置；
- `internal/consumer/rabbitmq/parser.go`：不可信 Delivery 的严格解析；
- `internal/consumer/rabbitmq/transport.go`：可替换的 AMQP Connection/Channel 边界；
- `internal/consumer/rabbitmq/consumer.go`：监督器、lanes、Manual ACK 与优雅停机；
- `internal/consumer/rabbitmq/*_test.go`：解析、ACK 矩阵、lane 和并发退出单元测试；
- `internal/integration/rabbitmq_consumer_test.go`：RabbitMQ 4.3.4 真实集成测试；
- `Makefile`：可靠性 Policy 的应用与查看命令。

## 11. 如何验证

普通单元测试不依赖 Docker：

```bash
go test -race -count=20 ./internal/consumer/rabbitmq
go test ./...
```

只运行本阶段真实 RabbitMQ 测试：

```bash
TEST_RABBITMQ_IMAGE=rabbitmq:4.3.4-management-alpine \
go test -tags=integration ./internal/integration \
  -run '^TestRabbitMQConsumer$' -count=1 -v -timeout=3m
```

它验证：

- 合法消息执行一次并 ACK；
- 第一次瞬时失败约一秒后重投，第二次成功；
- 格式错误立即进入 DLQ；
- RabbitMQ `stop_app/start_app` 后 Consumer 自动重连；
- cancel 后 Consumer 在边界内退出。

## 12. 面试表达

### 30 秒版本

> 我给邮件 Worker 实现了 RabbitMQ 4.3 Consumer Adapter，使用一条连接、多条独立 Channel
> lane，delivery tag 不跨 Channel，prefetch 控制在途背压。消息先严格校验，再调用幂等
> DispatchWorker；成功 ACK，毒消息直接 DLQ，瞬时错误通过 Quorum Queue 的 delayed retry
> 和 delivery limit 有界重试。DLX 用 Policy 配置 at-least-once 转发，并实现了 Channel 自愈、
> Connection 重连和优雅停机。真实 Broker 测试覆盖 ACK、延迟重投、DLQ 和重启恢复。

### 2 分钟版本

> Consumer 层不修改业务状态，只把 AMQP Delivery 转成 DispatchCommand，再根据 Worker 的
> 错误分类做 settlement。默认四条 lane，每条 lane 独占 Channel、prefetch=1、顺序处理，
> 所以并发上限清楚，delivery tag 也不会跨 Channel 误确认。Parser 会交叉验证 Properties、
> Headers 和 JSON Envelope，而且错误不泄露正文。
>
> RabbitMQ 4.3 有个细节：basic.nack 不增加 delivery-count，basic.reject 才算失败，所以
> 瞬时错误用 Reject(true)，配合 failed delayed retry 做线性退避并在 20 次后 DLQ；确定毒
> 消息用 Nack(false,false)，绕过失败重试立即死信。DLX 和延迟参数通过 Policy 在线管理，
> quorum source 使用 at-least-once dead lettering 和 reject-publish overflow。
>
> 停机时先取消消费，再让 Worker 完成有界 finalize，最后统一关闭连接；连接或 Channel
> 断开时未 ACK 消息由 Broker 重投，07-A 的 generation、sequence 和 Attempt 唯一约束负责
> 幂等。集成测试真实重启 RabbitMQ，并抓到过取消握手竞态和 routing key 漂移两个问题。

## 13. 可能追问

**prefetch 为什么默认是 1，会不会吞吐太低？**

它是安全基线，不是最终压测参数。并发主要由 4 条 lane 提供。邮件 Provider 通常是高延迟
外部 I/O，先限制每条 lane 只有一个未确认任务，避免一次预取大量消息后进程崩溃产生大批
重投。之后可按 Provider 延迟和数据库池逐步提高。

**为什么 Broker 重投仍需要 Worker 幂等？**

因为状态事务提交后、ACK 到达 Broker 前进程可能崩溃；Broker 只能知道“没收到 ACK”，无法
知道数据库已成功。至少一次系统把重复当正常情况，不能靠网络时序实现 exactly-once。

**DLQ 为什么也可能重复？**

at-least-once dead-letter worker 依赖目标 confirm。目标已收下但 confirm 丢失时，源端会再
发布一次。因此 DLQ 查看、归档和重放工具也要按 Message ID 幂等。

**数据库整体宕机时，每条消息都延迟重试合适吗？**

不合适。当前有界 delayed retry 防止 hot loop，但全局依赖故障应由下一阶段的健康状态、
熔断/暂停消费和告警处理，避免把整条队列都变成 delayed messages。

## 14. 后续进展与尚未解决

- `cmd/mail-service` 的依赖装配、进程生命周期与动态 readiness 已在
  [07-C 运行时装配](07c-composition-root-runtime.md) 完成；分角色启动仍未实现；
- 指标、Trace、结构化日志和连接状态健康检查；
- 全局数据库/Provider 故障时暂停消费的熔断逻辑；
- DLQ 查询、告警、审计和安全重放工具；
- 陈旧 `STARTED` Attempt Reconciler；
- lifecycle Consumer 与 AI-Nexus 状态通知；
- 真实 SMTP Provider、租户限速和 Provider 舱壁。

下一阶段 08-A 会实现 Submission/Query 应用用例与 gRPC Adapter，让业务方能够通过正式
接口创建并查询邮件任务。
