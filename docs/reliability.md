# 可靠性、调度与故障语义

## 1. 可靠性目标

系统承诺：

- 已返回 `ACCEPTED` 的任务已持久化；
- 进程或 RabbitMQ 短时故障后任务可恢复；
- 内部阶段采用至少一次处理；
- 相同租户和幂等键不会创建两个逻辑任务；
- 状态事件可重复但不会倒退；
- 超过截止时间的邮件不会开始新的发送尝试；
- 每个失败都进入稳定、可观测的类别。

系统不承诺：

- 外部 Provider 只执行一次；
- SMTP 超时后能判断对方是否接受；
- SMTP 接受等于收件人最终收到；
- 多个外部系统之间存在原子事务。

## 2. 幂等模型

幂等唯一键：

```text
(tenant_id, idempotency_key)
```

服务对规范化请求计算带服务端密钥的 payload fingerprint：

```text
HMAC-SHA256(
  idempotency_secret,
  canonical_recipient ||
  sender_identity ||
  template_key ||
  resolved_template_version ||
  locale ||
  canonical_variables ||
  scheduled_at ||
  dispatch_deadline
)
```

重复调用：

- key 与 fingerprint 都相同：返回原任务；
- key 相同但 fingerprint 不同：拒绝并告警；
- 调用方 RPC 超时：使用相同 key 查询或重试；
- 不得因超时生成新 key。

## 3. Transactional Outbox

### 3.1 提交事务

```text
BEGIN
  INSERT/UPDATE mail_message
  INSERT all pending domain events into outbox_events
COMMIT
```

API 返回成功前只依赖 PostgreSQL，不同步依赖 RabbitMQ。

当前领域事件包括 `MESSAGE_ACCEPTED`、`MESSAGE_STATUS_CHANGED` 和
`MESSAGE_DISPATCH_REQUESTED`。立即邮件会在受理事务中写入派发事件；定时邮件只持久化
受理与状态事件，到期后由 Scheduler 推进为 `QUEUED` 并创建派发事件。

Outbox payload 采用有版本的字段白名单，只保存租户、Message ID、状态变化、sequence、
dispatch generation 和脱敏失败分类等路由或审计信息。不得复制收件邮箱、验证码、正文、
模板变量或 Provider 凭据。事件 identity 重复时必须比较 JSONB：语义相同视为幂等重试，
内容不同视为一致性冲突。

领域 pending events 只在事务 Commit 后清理。Message 保存、Outbox 写入、Commit 任一步
失败，都必须回滚数据库并保留请求级事件。

### 3.2 发布

Relay 分三段执行：

```text
短事务领取 PENDING Outbox 并设置唯一 claim token + lease_until
→ 事务外调用 Publisher 并等待有界 Confirm
→ 短事务按 event ID + claim token + expected attempt 记录结果
```

领取使用 `transaction_timestamp()`、有界 batch 和 `FOR UPDATE SKIP LOCKED`。没有 Lease
或 Lease 已过期的到期事件才能领取。唯一 claim token 同时承担 fencing：事件被重新领取
后，旧 Publisher 的迟到结果影响 0 行并记为 LeaseLost。

Relay 发布时启用：

- durable exchange；
- persistent message；
- quorum queue；
- publisher confirms；
- `mandatory=true`；
- 有界 confirm timeout。

数据库标记 `published` 前发生崩溃会导致重复发布，Worker 必须幂等。先标记再发布会
产生丢失窗口，因此禁止这么做。

临时失败清空 Lease 并用 Full Jitter 更新 `available_at`；永久失败或达到最大结果次数后
进入 `DEAD_LETTERED`。`attempt_count` 在成功、重调度或死信结果落库时增加，不在 Claim
时增加，因此它不是不可观测窗口内精确的物理 Publish 次数。

### 3.3 消费

Worker：

1. 收到消息；
2. 校验消息携带的 `dispatch_generation` 与数据库当前代次；
3. 第一段短事务执行 `QUEUED → SENDING`，同时创建 `STARTED` Attempt 和状态 Outbox；
4. 提交第一段事务，再在事务外执行一次有界 Provider 调用；
5. 第二段短事务完成 Attempt、推进聚合状态并写状态 Outbox；
6. 提交第二段事务；
7. ACK RabbitMQ。

状态不匹配或代次过旧的消息直接作为陈旧消息确认，不触发投递。步骤 5 后、步骤 7
前崩溃会产生重复消费，但不会创建新的逻辑 Attempt。步骤 3 后、步骤 4 前崩溃会保留
`SENDING + STARTED`，后续 Reconciler 据此判断“Provider 可能尚未调用，也可能结果未能
持久化”；在不能证明未提交时，不允许重复发送 `AVOID_DUPLICATE` 邮件。

`delivery_attempts` 同时唯一约束 `(message_id, attempt_no)` 和
`(message_id, dispatch_generation)`。应用层的 Message 乐观锁解决并发状态推进，Attempt
唯一约束是数据库最后防线。Provider 返回值显式区分 `ACCEPTED`、已知 `FAILED` 和
`SUBMISSION_UNKNOWN`；网络异常不能不加判断地折叠为一个普通 `error`。

## 4. 延迟发送

长期定时任务不直接堆在 RabbitMQ：

- `scheduled_at`、`next_attempt_at` 和 `dispatch_deadline` 保存在 PostgreSQL，领取判断
  使用同一事务的 `transaction_timestamp()`；
- Scheduler 按时间索引扫描到期任务；
- 多实例使用 `FOR UPDATE SKIP LOCKED` 领取；
- 领取和写 Outbox 在同一事务中；
- Scheduler 每次只在一个短事务中领取有界小批次；
- 任务取消或租户暂停立即反映在数据库状态；
- Commit 前崩溃由 PostgreSQL 回滚并释放行锁，下一轮扫描自动恢复；
- Scheduler 不执行事务外工作，因此 Message 不设置 lease；Outbox Relay 跨网络发布时才
  使用可过期 lease。

这种方式支持长时间预约、取消、重启恢复和查询。RabbitMQ TTL/DLX 可以用于很短的
Broker 级退避，但不作为产品级调度真相。

## 5. 重试调度

每个失败先归类：

| 类别 | 示例 | 默认动作 |
| --- | --- | --- |
| `VALIDATION` | 地址、模板变量错误 | 永久失败 |
| `AUTHENTICATION` | SMTP/API 凭据错误 | Provider 熔断、告警，不重试任务 |
| `RATE_LIMITED` | Provider 429/限速 | 尊重 Retry-After 后重试 |
| `RECIPIENT_REJECTED` | 明确的无效地址 | 永久失败 |
| `CONTENT_REJECTED` | 内容/策略拒绝 | 永久失败并告警 |
| `NETWORK` | DNS、连接失败 | 有界指数退避 |
| `PROVIDER_UNAVAILABLE` | 5xx、临时服务异常 | 退避或安全切换 |
| `TIMEOUT_BEFORE_SEND` | 未开始提交即超时 | 可安全重试 |
| `SUBMISSION_UNKNOWN` | 提交后连接中断 | 进入不确定状态并对账 |
| `INTERNAL` | 本地未分类错误 | 有界重试并告警 |

退避算法使用 full jitter：

```text
delay = random(0, min(cap, base * 2^attempt))
```

最终调度时间还要受以下约束：

```text
next_attempt_at < dispatch_deadline
attempt_count < max_attempts
tenant/provider 未暂停
```

重试任务写回数据库，由 Scheduler 再次放入 Outbox。

## 6. 熔断、舱壁和降级

### 6.1 熔断粒度

熔断键至少包含：

```text
provider + endpoint/region + credential_id
```

不能使用一个全局 SMTP 熔断器，否则一个租户凭据失效会拖垮所有租户。

状态：

```text
CLOSED → OPEN → HALF_OPEN → CLOSED
```

- 网络和 Provider 5xx 计入健康统计；
- 业务地址拒绝不计入 Provider 熔断；
- 认证失败立即打开对应凭据熔断器；
- 半开探测使用独立小并发舱壁；
- 多实例首先允许本地快速熔断，关键状态通过共享存储或控制面广播。

### 6.2 舱壁

至少隔离：

- `CRITICAL` 与其他邮件队列；
- 不同 Provider 的并发池；
- 不同租户的并发和速率；
- Submission API、Worker、Subscriber Worker 的连接池；
- 回调目标之间的执行池。

### 6.3 降级顺序

1. 降低 `BULK` 和 `NOTIFICATION` 吞吐；
2. 暂停新的低优先级计划任务出队；
3. 切换到健康的兼容 Provider；
4. 保留已受理任务并延后发送；
5. 容量或数据库无法保证持久化时拒绝新任务；
6. 永远不返回“成功”来掩盖任务没有落库。

验证码等短截止时间任务在无法及时发送时快速进入失败/过期，使业务方能提示用户
重试，不能在故障恢复数小时后再发送失效验证码。

## 7. MQ 拓扑

06-C 已落地的第一版通用事件拓扑是：

```text
mail.events.v1 (durable topic exchange)
  ├── mail.dispatch.v1.q
  │     └── mail.message.dispatch.requested.v1
  └── mail.lifecycle.v1.q
        ├── mail.message.accepted.v1
        └── mail.message.status.changed.v1
```

两条队列都是 durable Quorum Queue。派发 Worker 只消费 dispatch queue，不会把状态事件
误当成发信命令；独立 Subscriber Worker 消费 lifecycle queue，并通过 event ID 回查权威
Delivery Event Journal。Event ID 使用 AMQP Message ID，
Aggregate ID 使用 Correlation ID，sequence、dispatch generation 和 Outbox publish attempt
使用类型化 Header。消息体仍然是安全 Outbox JSON，不包含邮件正文。

当前按事件类型路由，不从 JSON payload 中解析 category。按 Critical、Transactional、
Notification、Bulk 做物理舱壁仍是目标拓扑，但需要先把 category 设计成稳定、经过校验的
路由元数据；不能让 RabbitMQ 基础设施层依赖某一版业务 JSON envelope。

Publisher 使用一条长连接和有界 Channel 池，一次并发 Publish 独占一个 Channel。Adapter
不使用客户端内存重发队列：连接失效后丢弃旧 Channel，下一次 Outbox Publish 按需重连并
幂等重声明拓扑。Confirm 超时会废弃连接，防止旧 Channel 上迟到的 Return/Confirm 被误认
为下一条消息的结果；这一动作可能让同连接上的其他在途 Publish 失败，但它们都会由
Outbox 按 At Least Once 恢复。

Consumer 使用一条长连接和多条独立 Channel lane。每条 lane 只有一个 Consumer，设置
per-consumer prefetch 并顺序调用 Worker；不同 lane 并行。Delivery tag 只在所属 Channel
内确认。合法消息在 Worker 返回稳定结果后 `Ack(false)`；确定毒消息
`Nack(false, false)` 立即死信；瞬时基础设施错误用 `Reject(true)`，使 RabbitMQ 4.3
Quorum Queue 增加 `delivery-count` 并执行线性 delayed retry。连接关闭时仍未确认的消息由
Broker 重投。

`mail.dispatch.v1.q` 与 `mail.lifecycle.v1.q` 分别通过运行 Policy 配置 `delivery-limit=20`、
`delayed-retry-type=failed`、1s..30s 延迟，以及指向 `mail.dead.v1` 的 at-least-once
dead lettering。该模式要求 `overflow=reject-publish`。DLQ 为 durable quorum queue，绑定
独立的 `mail.dispatch.dead.v1` 和 `mail.lifecycle.dead.v1`。Policy 由
`make mq-policy-apply` 幂等应用，不能与代码 binding 的 routing key 漂移。通知进入 lifecycle
DLQ 不修改邮件投递状态：邮件事实和回调同步结果由不同状态维度表达。

目标按类别隔离时拓扑扩展为：

推荐使用 RabbitMQ Quorum Queues：

```text
mail.dispatch exchange (topic)
  ├── mail.critical.q
  ├── mail.transactional.q
  ├── mail.notification.q
  └── mail.bulk.q

mail.dead exchange
  ├── mail.critical.dead.q
  ├── mail.transactional.dead.q
  └── ...
```

要求：

- 队列持久化；
- Publisher Confirms；
- Consumer Manual Ack；
- `mandatory` 发布；
- 配置 max length/bytes；
- 配置 delivery limit；
- Dead Letter 使用可恢复策略；
- Prefetch 按 Provider 延迟和 Worker 并发调优；
- 消息体只放任务 ID、租户 ID、事件 ID 和必要路由信息，不复制邮件正文；
- 消息体携带 `dispatch_generation`，防止延迟到达的旧消息触发新的发送。

`x-queue-type=quorum` 必须在声明队列时提供。max length、delivery limit 和 dead-letter
策略优先通过 RabbitMQ Policy/Operator Policy 管理，因为它们需要在不改代码、不重建队列
的情况下调整；应用声明只固定交换机、队列类型和 binding 这些协议契约。

RabbitMQ 4.x Quorum Queue 默认存在 delivery limit，必须显式设计 DLQ，防止 poison
message 被静默丢弃。

## 8. Provider 不确定结果

SMTP 最危险的窗口是服务已提交完整 DATA，但收到最终响应前连接断开。此时：

- Provider 可能已经接受；
- 再发可能产生重复邮件；
- 不重试可能丢失邮件。

状态机必须显式包含 `SUBMISSION_UNKNOWN`。处理策略：

- Provider 支持幂等查询：主动查询；
- Provider 支持幂等发送：同 Provider、同 key 重试；
- 普通 SMTP：按照消息类别的 duplicate risk policy 决定；
- 无论选择什么，都记录风险决策，不能伪装成明确成功或明确失败。

## 9. DLQ 和人工重放

进入 dispatch DLQ 的邮件任务必须：

- 已在数据库标记 `DEAD_LETTERED`；
- 保存最后稳定错误类别和脱敏摘要；
- 触发指标与告警；
- 可按租户、Provider、时间和错误码查询；
- 重放时创建新的 Attempt，但保留原 message ID 和审计链；
- 已过截止时间的验证码禁止重放；
- payload 已按保留策略清理时禁止重放正文。

进入 lifecycle DLQ 的状态通知不能把邮件改成 `DEAD_LETTERED`。它必须保留 event ID，按
Journal 对账，并在确认订阅方身份、事件未过保留期后安全重放；重复仍由订阅方 event ID 幂等处理。

## 10. 故障场景检查

| 故障点 | 结果 |
| --- | --- |
| API 落库前崩溃 | 调用方同幂等键重试 |
| API 落库后响应前崩溃 | 返回原任务，不重复创建 |
| Outbox 发布前崩溃 | Relay 恢复后继续 |
| Broker Confirm 丢失 | 可能重复发布，Worker 幂等 |
| Worker 执行前崩溃 | RabbitMQ 重新投递 |
| Provider 成功后 Worker 崩溃 | 可能成为不确定结果，按 Provider 能力对账 |
| 状态落库后 ACK 前崩溃 | 重复消费，不重复推进 |
| 业务回调不可用 | Notification Outbox 重试，不阻塞发信 |
| RabbitMQ 不可用 | API 可继续有限受理，Outbox 积压并触发容量保护 |
| PostgreSQL 不可用 | 无法可靠受理，API 返回不可用 |
