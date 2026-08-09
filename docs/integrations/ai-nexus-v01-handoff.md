# AI-Nexus × Mail Service V0.1 对齐与对接手册

本文是 AI-Nexus 接入 Mail Service 的执行基线，供两个仓库的开发窗口共同使用。
它描述当前已经实现的能力，而不是远期平台规划。

## 1. 本次接入目标

V0.1 只打通一条最小、可靠的验证码邮件链路：

```text
用户请求验证码
  -> AI-Nexus 生成验证码并保存业务状态
  -> gRPC SubmitEmail
  -> Mail Service 可靠受理并异步投递
  -> gRPC ReportDeliveryEvent 回调
  -> AI-Nexus 激活、终止或对账验证码
```

本次接入完成后，应满足：

- AI-Nexus 不再直接连接 QQ SMTP；
- AI-Nexus 不把验证码生成、验证和消费职责交给 Mail Service；
- RPC 超时或进程重启不会无意创建两封逻辑邮件；
- Mail Service 暂时不可用时，AI-Nexus 能明确返回“发送请求未成功受理”，而不是误报成功；
- 重复、延迟和乱序回调不会让验证码状态倒退或重复激活；
- 两边可以使用 `message_id` 和 `idempotency_key` 对账，但日志中不得出现验证码和完整邮箱。

## 2. 当前实现边界

### 2.1 V0.1 可以使用的接口

| 方向 | RPC | 当前用途 |
| --- | --- | --- |
| AI-Nexus → Mail Service | `DeliveryService.SubmitEmail` | 可靠受理一封验证码邮件 |
| AI-Nexus → Mail Service | `DeliveryService.GetEmail` | 按 `message_id` 或 `idempotency_key` 查询权威投递状态 |
| Mail Service → AI-Nexus | `DeliveryEventReceiverService.ReportDeliveryEvent` | 至少一次投递邮件生命周期事件 |

Proto 中虽然已经声明 `BatchSubmitEmail`、`CancelEmail` 和 `ListEmailEvents`，但当前服务端尚未实现，
调用会得到 gRPC `UNIMPLEMENTED`。AI-Nexus V0.1 不得依赖它们。

### 2.2 唯一协议来源

唯一可信的协议源文件是 Mail Service 仓库中的：

- `api/proto/mailservice/delivery/v1/common.proto`
- `api/proto/mailservice/delivery/v1/delivery.proto`
- `api/proto/mailservice/delivery/v1/event.proto`

Go 生成包为：

```go
github.com/Zhiruosama/Email-Service/gen/go/mailservice/delivery/v1
```

AI-Nexus 旧文档中的 `ainexus.mail.v1`、`VarifyService/GetVarifyCode` 或其他旧草案不能继续作为
新接口依据。旧接口由邮件服务生成并返回验证码，和当前职责边界冲突。

本地同时开发两个 Go Module 时，可通过工作区 `go.work` 引用 Email Service 的生成包；CI 和正式发布
不能依赖开发机的相对路径。短期可以依赖一个明确的 Email Service Git tag，长期再把 Proto 与生成代码
拆成单独、版本化的 contracts module。无论采用哪种方式，都不要在两个仓库各维护一份可独立修改的 Proto。

## 3. 系统职责

### 3.1 AI-Nexus 拥有

- 六位验证码生成；
- 验证码摘要保存，不保存可直接验证的明文；
- `purpose`、有效期、尝试次数、激活、一次性消费和业务状态机；
- 用户、邮箱、IP 和业务用途维度的限流与防滥用；
- 本次发送的稳定 `request_id`；
- Mail Service 客户端、回调服务端与本地对账任务；
- 面向 AI-Nexus API 调用方的错误语义。

### 3.2 Mail Service 拥有

- 邮件任务幂等受理与持久化；
- `verification_code.v1` 模板校验、版本固定和 MIME 渲染；
- PostgreSQL 调度、Transactional Outbox、RabbitMQ 异步投递；
- SMTP、重试、熔断、并发舱壁、速率保护与死信；
- 邮件生命周期状态机、事件 Journal 和投递状态回调；
- 邮件 Payload 加密保存与非敏感状态查询。

### 3.3 禁止共享的内部设施

两个服务不共享业务数据库、Redis、RabbitMQ 队列或内部表。AI-Nexus 只能通过公开 gRPC 契约使用
Mail Service，不能读取 Mail Service 的 PostgreSQL 来判断邮件状态。

## 4. `SubmitEmail` 请求映射

| AI-Nexus 值 | Mail Service 字段 | 约束 |
| --- | --- | --- |
| 一次逻辑发送的 `request_id` | `idempotency_key` | 必填；未知结果重试时必须复用 |
| 收件邮箱 | `recipient.email` | 必填 |
| 固定配置 | `sender_identity_key` | `ainexus.default` |
| 固定配置 | `content.template.key` | `verification_code.v1` |
| 模板版本 | `content.template.version` | 省略，由受理时固定当前活动版本 |
| 用户语言 | `content.locale` | V0.1 固定 `zh-CN` |
| 验证码数据 | `content.variables` | 见下方 Schema |
| 固定配置 | `category` | `EMAIL_CATEGORY_CRITICAL` |
| 固定配置 | `priority` | 建议 `9` |
| 立即发送 | `scheduled_at` | 省略 |
| 生成时间加 2 分钟 | `dispatch_deadline` | 必填；必须是合法 Timestamp |
| 固定配置 | `duplicate_risk_policy` | `DUPLICATE_RISK_POLICY_AVOID_DUPLICATE` |
| 非敏感关联信息 | `metadata` | 可选；禁止放邮箱、验证码、Token 等敏感值 |

`content.variables` 的约束如下：

```json
{
  "code": "123456",
  "purpose": "LOGIN",
  "valid_for_seconds": 300
}
```

- `code`：恰好六位数字字符串；
- `purpose`：`REGISTER`、`RESET_PASSWORD` 或 `LOGIN`；
- `valid_for_seconds`：`60..1800` 的整数；
- 不允许额外字段。

建议 `idempotency_key` 直接使用 AI-Nexus 为本次业务发送生成的 UUID/ULID。它表示“一次逻辑邮件”，
而不是“一次网络调用”。下面两种情况含义不同：

- 同一个 key、同一份规范化请求再次提交：返回 `SUBMIT_DISPOSITION_DUPLICATE` 和原 `message_id`；
- 同一个 key、不同收件人或不同内容再次提交：返回冲突错误，不能将其当作成功。

## 5. 受理响应与调用失败

AI-Nexus 只有收到以下 disposition，才能记录“邮件任务已经受理”：

- `SUBMIT_DISPOSITION_ACCEPTED`：本次创建了新任务；
- `SUBMIT_DISPOSITION_DUPLICATE`：此前已原子地创建过同一任务。

二者都不代表 SMTP 已接受邮件，更不代表邮件进入收件箱。AI-Nexus 应保存响应中的：

- `message.message_id`；
- `message.idempotency_key`；
- 当前安全状态视图。

建议客户端给单次 RPC 设置一个有限超时。遇到 `DeadlineExceeded`、`Unavailable` 或连接中断时，结果可能
未知：不要生成新的 `idempotency_key`，应使用原 key 重试 `SubmitEmail`，或先用 `GetEmail` 按原 key
查询。Mail Service 的幂等受理负责把这两种恢复路径收敛到同一个 `message_id`。

确定性参数错误、模板变量错误和 key 内容冲突不应盲目重试；应转换成 AI-Nexus 内部错误并报警，
因为它们通常表示集成代码或配置错误。

## 6. 回调契约

AI-Nexus 需要监听一个 gRPC 地址并实现：

```text
mailservice.delivery.v1.DeliveryEventReceiverService.ReportDeliveryEvent
```

Mail Service 对回调使用至少一次投递，因此 AI-Nexus 的处理顺序必须是：

1. 校验事件基本字段并找到本地发送记录；
2. 在同一个本地数据库事务内，根据 `event_id` 去重，并根据 `message_id + sequence` 拒绝旧事件；
3. 更新邮件观测状态和验证码业务状态；
4. 提交事务；
5. 再返回 `ACCEPTED`、`DUPLICATE` 或 `IGNORED_STALE`。

不能在事务提交前返回成功，否则 AI-Nexus 崩溃时 Mail Service 会认为事件已经消费。回调处理不得发送
新的验证码邮件，也不得同步调用 Mail Service 形成循环依赖。

推荐状态映射：

| Mail Service 状态 | AI-Nexus 行为 |
| --- | --- |
| `ACCEPTED/SCHEDULED/QUEUED/SENDING` | 保持 `PENDING_DISPATCH`，只更新观测状态 |
| `RETRY_SCHEDULED` | 保持等待，记录可观测状态 |
| `PROVIDER_ACCEPTED` | 首次到达时幂等激活验证码并开始业务有效期 |
| `DELIVERED` | 若尚未激活则激活；不得延长已激活验证码的有效期 |
| `BOUNCED/COMPLAINED` | 不激活；记录投递失败观测，按业务策略允许重新申请 |
| `PERMANENTLY_FAILED/DEAD_LETTERED` | 进入 `DELIVERY_FAILED`，允许按业务规则重新申请 |
| `CANCELED/EXPIRED` | 终止且不可验证 |
| `SUBMISSION_UNKNOWN` | 保持不可验证，等待对账或后续终态 |
| `UNKNOWN_FINAL` | 终止该验证码并允许重新申请 |

当前 QQ SMTP 链路能够可靠观测到 `PROVIDER_ACCEPTED`，但没有供应商 Webhook 来自动证明最终进入收件箱，
因此 V0.1 不应把 `DELIVERED` 当作必然出现的事件。

## 7. `GetEmail` 对账

`GetEmail` 可以使用 `message_id` 或 `idempotency_key`，每次只能设置一个 selector。推荐用于：

- `SubmitEmail` 返回结果未知时确认是否已经受理；
- AI-Nexus 长时间停机、回调持续失败后恢复状态；
- 运维排障和人工核对。

它返回的是安全状态视图，不包含原始邮箱、模板变量或渲染正文。V0.1 可先实现“按需查询 + 对未知状态
定时补偿”，不需要立即建设全量扫描平台。

## 8. 本地联调配置

默认宿主机拓扑：

```text
AI-Nexus client       -> 127.0.0.1:8080 -> Mail Service
Mail Service callback -> 127.0.0.1:8081 -> AI-Nexus callback server
```

Mail Service 本地关键配置：

```dotenv
MAIL_GRPC_LISTEN_ADDRESS=:8080
MAIL_GRPC_ALLOW_INSECURE=true
MAIL_DEV_TENANT_ID=10000000-0000-4000-8000-000000000001
MAIL_CALLBACK_GRPC_ADDRESS=127.0.0.1:8081
MAIL_CALLBACK_GRPC_ALLOW_INSECURE=true
```

开发租户由 Mail Service 的 gRPC interceptor 注入，`SubmitEmailRequest` 不携带 `tenant_id`。生产环境将以
mTLS 身份替代这套开发配置；当前明文 gRPC 只允许本地联调。

如果两个应用都进入同一个 Compose 网络，地址要改成服务 DNS，例如 `mail-service:8080` 和
`ai-nexus:8081`。容器中的 `127.0.0.1` 只指向容器自身，不能用来访问另一个服务。若 Mail Service
在容器、AI-Nexus 在宿主机，可使用显式的 host gateway 配置，不要把开发机地址硬编码进代码。

Mail Service 启动前还需要：

1. PostgreSQL 和 RabbitMQ 健康；
2. 数据库 Migration 已应用；
3. 固定开发租户种子已写入；
4. RabbitMQ retry/DLQ Policy 已应用；
5. `.env` 中的 Payload 密钥和 SMTP 授权码通过环境注入，未写入镜像或 Git。

## 9. AI-Nexus 实施清单

建议另一个开发窗口按以下顺序推进：

1. 删除或标记废弃旧 `ainexus.mail.v1` 对接草案，确定本文件列出的 Proto 为唯一契约；
2. 接入版本固定的生成包，建立 Mail Service gRPC Client 和连接生命周期管理；
3. 在验证码 Application Service 中生成稳定 `request_id/idempotency_key`，组装 `SubmitEmailRequest`；
4. 保存 `message_id`、幂等 key、验证码摘要和 `PENDING_DISPATCH` 状态；
5. 实现结果未知时的同 key 重试/查询，不在 Transport 层生成新 key；
6. 实现 `DeliveryEventReceiverService` 和事件幂等表/唯一约束；
7. 把验证码激活、失败和过期写成显式状态转换；
8. 增加 `GetEmail` 对账入口或小型补偿任务；
9. 添加单元测试、契约测试和双服务纵向测试；
10. 最后移除 AI-Nexus 原有 QQ SMTP 配置与发送代码。

## 10. 最小验收用例

至少验证以下场景：

1. 正常提交返回 `ACCEPTED`，最终收到 `PROVIDER_ACCEPTED`，验证码只激活一次；
2. 同 key 同请求再次提交返回 `DUPLICATE`，`message_id` 不变；
3. 同 key 改验证码或收件人后提交得到冲突，不能覆盖原任务；
4. `SubmitEmail` 客户端超时后用同 key 重试，不产生第二封逻辑邮件；
5. 同一 `event_id` 回调两次，第二次返回 `DUPLICATE`，业务状态不重复变化；
6. 高 sequence 先到、低 sequence 后到时，后者返回 `IGNORED_STALE`；
7. AI-Nexus callback 暂时不可用时，Mail Service 重试且不丢事件；
8. SMTP 永久失败时验证码不激活，并能向用户表达可重新申请；
9. 日志、错误、Metrics 和查询响应中不出现验证码、SMTP 授权码或完整邮箱。

## 11. 本阶段明确延期

为先完成 AI-Nexus 接入，以下内容不阻塞 V0.1：

- `BatchSubmitEmail`、`CancelEmail`、`ListEmailEvents`；
- 多供应商路由、供应商 Webhook 和自动 `DELIVERED/BOUNCED` 回写；
- 全局分布式限流与完整控制面；
- OTel SDK、Prometheus/OTLP Exporter 和监控大盘；
- 生产 mTLS、KMS 和租户自助管理；
- 通用模板后台和批量营销邮件能力。

生产上线前仍需补齐 mTLS/服务身份、正式密钥管理、终态敏感 Payload 清理和生产监控。这些是发布门槛，
但不应与本地双服务对接混在同一个开发步骤中。

## 12. Docker 化边界

当前仓库的 `compose.yaml` 只容器化了 PostgreSQL 和 RabbitMQ，尚未提供 Mail Service 应用镜像。
完整容器化建议包含：

- 多阶段 `Dockerfile`：构建静态 Go 二进制，运行阶段使用非 root 最小镜像并保留 CA 证书；
- `.dockerignore`：排除 `.git`、本地 `.env`、测试缓存和编辑器文件；
- 一次性 migration job：应用副本只校验 Schema，不让多个副本并发执行 Migration；
- 开发 seed 和 RabbitMQ Policy 初始化步骤；
- 基于标准 gRPC Health Service 的容器健康检查；
- Compose 中使用 `postgres:5432`、`rabbitmq:5672` 和服务 DNS，而不是 `localhost`；
- SMTP 授权码、Payload 密钥通过运行时 Secret/环境变量注入，绝不写入 image layer。

这项工作不会改变邮件领域逻辑，主要是交付和启动编排。可以在 Nexus V0.1 接口接通后单独完成，避免
一边修改业务契约、一边排查容器网络造成定位困难。
