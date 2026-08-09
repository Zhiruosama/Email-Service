# 系统架构

## 1. 总体架构

```text
                         ┌──────────── 控制面 ────────────┐
                         │ Tenant / Template / Sender     │
                         │ Provider / Route / Quota       │
                         └──────────────┬─────────────────┘
                                        │ 发布配置快照
                                        ▼
业务系统 ── gRPC ──► Submission API ── PostgreSQL
                         │                 │
                         │                 ├── mail_messages
                         │                 ├── delivery_attempts
                         │                 ├── outbox_events
                         │                 └── delivery_events
                         │                         │
                         │                         ▼
                         │                    Outbox Relay
                         │                         │ publisher confirm
                         │                         ▼
                         │                 RabbitMQ Quorum Queues
                         │                         │
                         │              ┌──────────┴──────────┐
                         │              ▼                     ▼
                         │        Critical Workers      General Workers
                         │              │                     │
                         │              └──────────┬──────────┘
                         │                         ▼
                         │                  Provider Router
                         │              ┌──────────┼──────────┐
                         │              ▼          ▼          ▼
                         │           QQ SMTP    SMTP B    API Provider
                         │
                         ├── Status Query
                         └── Cancel

Provider Webhook ──► Event Ingress ──► Delivery State Machine
                                             │
                                             ▼
                                      Notification Outbox
                                             │
                                             ▼
                                      Subscriber Workers
                                             │
                               gRPC / signed webhook / event bus
                                             │
                                             ▼
                                          业务系统

Scheduler ──扫描 scheduled_at / next_attempt_at──► Outbox
Reconciler ──扫描超时中间态并查询 Provider───────► 状态机
```

## 2. 部署单元

代码先采用模块化单体，一个仓库、一个二进制，根据角色启动：

```text
mail-service --role=api
mail-service --role=scheduler
mail-service --role=relay
mail-service --role=worker
mail-service --role=event-ingress
mail-service --role=notifier
mail-service --role=reconciler
mail-service --role=all
```

07-C 当前落地的是固定 `all` 形态的后台投递链路，尚未实现命令行 role 开关和 Submission
API。上面的命令是目标部署形态，不应误认为已经全部可用。

本地和小规模环境使用 `all`。生产环境按职责独立扩容和隔离故障。只有当组织规模、
发布节奏或容量证明需要时，才拆成多个仓库或独立服务。

## 3. 模块职责

### 3.1 Submission API

- 认证调用方并解析租户；
- 请求校验、模板和发件身份授权；
- 幂等校验；
- 限流和配额预检查；
- 同一事务创建邮件任务和 Outbox；
- 提供查询、取消和批量提交接口；
- 不执行模板渲染和 SMTP 调用。

### 3.2 Scheduler

- 扫描 `scheduled_at <= now()` 的计划任务；
- 扫描 `next_attempt_at <= now()` 的重试任务；
- 使用 `FOR UPDATE SKIP LOCKED` 并行领取；
- 原子推进状态并写入 Outbox；
- 支持取消、截止时间和租户暂停；
- 使用同一事务的 `transaction_timestamp()` 领取和执行状态机，避免节点时钟差参与业务
  判断；
- 所有工作都在有界短事务内完成，不为 Message 设置跨事务 lease。

### 3.3 Outbox Relay

- 分批领取未发布 Outbox；
- 使用 `FOR UPDATE SKIP LOCKED` 原子设置有期限 Lease；
- 每批使用唯一 claim token，所有结果更新都通过 token 和 expected attempt fencing；
- Publisher 网络调用发生在领取事务提交后；
- 以 RabbitMQ publisher confirms 确认 Broker 已承担消息；
- 使用 `mandatory` 发布并处理不可路由消息；
- Confirm 丢失时允许重复发布；
- 临时失败通过 Full Jitter 更新 `available_at`，永久失败进入死信；
- Relay 不删除业务任务，仅更新 Outbox 投递进度。

RabbitMQ Adapter 使用一条长连接和有界 Publisher Channel 池。一次并发 Publish 独占一个
Channel，避免 AMQP frame 和 Confirm correlation 被共享并发破坏；连接、Channel 或
Confirm 失败时不在客户端内存缓存消息，而是返回稳定错误给 Relay，由 PostgreSQL Outbox
决定重试。

Adapter 构造时只校验配置，不要求 RabbitMQ 在线。第一次 Publish 按需连接并声明拓扑；
连接失效后下一次 Publish 重新连接和声明。这样 `role=all` 启动时 RabbitMQ 故障不会反向
阻止 Submission API 依靠 PostgreSQL 可靠受理，但 Relay readiness、Outbox lag 和容量保护
仍需进入后续可观测性阶段。

### 3.4 Delivery Worker

- 手动 ACK RabbitMQ 消息；
- 根据数据库状态和 `dispatch_generation` 幂等领取任务；
- 检查取消、截止时间和租户状态；
- 在第一段短事务中原子执行 `QUEUED → SENDING`、创建 `STARTED` Attempt 和写 Outbox；
- 使用 PostgreSQL `transaction_timestamp()` 判断 deadline 和记录业务发生时间，避免不同
  Worker 节点的墙上时钟偏差改变状态机决定；
- 第一段事务提交后再调用 Provider，网络 I/O 期间不持有数据库事务或行锁；
- 固定模板版本并渲染 MIME 邮件；
- 通过 Provider Router 选择供应商；
- 执行超时、舱壁、限速和熔断策略；
- 在第二段短事务中原子完成 Attempt、推进消息状态并写 Outbox；
- 第二段数据库事务提交后才 ACK；重复或旧 generation 不再次调用 Provider。

07-A/07-B 已完成上述 Worker 应用内核、Fake Provider 和 RabbitMQ Consumer；07-C 已将
Scheduler、Relay、Publisher、Worker、Consumer 与标准 gRPC Health 装配为可运行后台进程。
当前
Provider Request 只含 Message、Tenant、Attempt 和策略元数据；模板、收件人密文、MIME
渲染与 Provider Router 会在相应数据模型完成后接入，不能用明文占位绕过安全设计。

### 3.5 Provider Router

路由输入：

- 租户和发件身份；
- 邮件类别；
- 收件地址域；
- 数据驻留区域；
- Provider 权重、容量和健康状态；
- 历史尝试和禁止重复选择规则。

路由输出必须包含确定的 `route_id`、Provider 和凭据版本，以便审计和重放。规则发布
后形成不可变版本，任务记录受理时和实际投递时使用的版本。

### 3.6 Event Ingress

- 接受第三方供应商 Webhook；
- 验证签名、时间戳和重放窗口；
- 保存原始事件摘要和供应商事件 ID；
- 快速返回，再异步归一化；
- 将 Provider 特有状态转换为统一状态；
- 幂等推进状态，不允许旧事件覆盖新终态。

### 3.7 Subscriber Worker

- 从 Notification Outbox 读取统一投递事件；
- 投递到预注册的 gRPC、签名 Webhook 或消息总线目标；
- 每个订阅独立重试和熔断；
- 单个业务系统故障不能阻塞其他租户；
- 支持状态查询完成对账。

### 3.8 Reconciler

- 查找长时间停留在 `SENDING`、`SUBMISSION_UNKNOWN` 等状态的任务；
- 对支持查询的 Provider 主动确认；
- 对不支持查询的 SMTP 任务应用配置化的不确定结果策略；
- 修复 Outbox、回调和状态之间可安全推导的不一致；
- 不猜测无法证明的最终送达状态。

## 4. 接口分层

### 4.1 数据面

- `SubmitEmail`
- `BatchSubmitEmail`
- `GetEmail`
- `CancelEmail`
- `ListEmailEvents`

同步返回值只表达任务是否被可靠接受，不表达最终发送成功。

### 4.2 控制面

- 租户和凭据；
- 模板草稿、校验、发布和回滚；
- 发件身份和域名验证；
- Provider 凭据与路由规则；
- 配额、限流和数据保留；
- 订阅目标；
- 暂停租户、Provider 或路由。

控制面 V1 可以先由配置文件和迁移脚本实现，后续再增加管理 API/UI。数据模型从一
开始保留控制面概念，避免业务代码硬编码 QQ SMTP 和 AI-Nexus。

## 5. 模板体系

模板采用：

```text
template_key + immutable_version + locale + validated_variables
```

模板版本一旦发布不可原地修改。切换活动版本只影响新任务；历史任务始终引用已固定
版本，保证重试结果一致。

每个版本包含：

- Subject 模板；
- HTML 模板；
- Plain Text 模板；
- 变量 JSON Schema；
- 允许的邮件类别；
- 安全策略和最大渲染大小；
- 内容摘要与发布时间。

默认禁止任意 HTML。确有需要时，应设计单独的高权限 Raw Email API，并与普通模板
发送分开授权、限流和审计。

## 6. 多供应商和故障转移

Provider Adapter 统一暴露：

```text
Send
Query（可选）
Cancel（可选）
HealthProbe
Capabilities
```

能力包括：

- SMTP/API；
- 供应商幂等键；
- 最终送达 Webhook；
- Provider Message ID；
- 最大消息大小；
- 附件支持；
- 标签和元数据支持。

故障转移不是对所有错误盲目切换：

- 连接前失败：通常可以安全切换；
- Provider 明确拒绝：按错误类别决定；
- SMTP `DATA` 后连接中断：结果不确定，切换可能造成重复邮件；
- Provider 已接受：不得再切换。

每次任务必须有 `duplicate_risk_policy`。验证码默认选择“结果不确定时不跨供应商
重发，并尽快通知调用方不确定状态”；普通通知可按租户策略选择更偏向送达或更偏向
避免重复。

## 7. AI-Nexus 接入

AI-Nexus 作为首个租户：

- 使用 mTLS 服务身份映射到固定租户；
- 使用 `verification_code.v1` 模板；
- 类别为 `CRITICAL`；
- 截止时间约 2 分钟；
- 通过预注册 gRPC Subscriber 接收状态；
- `PROVIDER_ACCEPTED` 或可信的 `DELIVERED` 激活验证码；
- 邮件服务只持有加密后的验证码模板变量。一旦确定不再发生后续发送尝试就立即
  清理，不能等待可能永远不会到来的 `DELIVERED/BOUNCED` 事件。

现有 `ainexus.mail.v1` 可以作为边缘适配协议，但新的核心 Proto 应使用中立 package。
两者不能各自实现不同的投递状态语义。
