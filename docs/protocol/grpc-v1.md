# gRPC V1 调用语义与约束

## 1. 包和服务

Protobuf package：

```text
mailservice.delivery.v1
```

Mail Service 实现：

```text
DeliveryService
```

预注册的业务订阅方实现：

```text
DeliveryEventReceiverService
```

租户来自认证身份，不从请求体读取。生产环境使用 mTLS 或同等强度的服务身份认证。

## 2. SubmitEmail

`SubmitEmail` 只表示可靠受理。服务必须在邮件任务和 Transactional Outbox 的本地
事务提交以后，才能返回 `SUBMIT_DISPOSITION_ACCEPTED`。

同步响应不表示：

- 已进入 RabbitMQ；
- Worker 已经处理；
- Provider 已经接受；
- 用户已经收到邮件。

### 2.1 字段约束

| 字段 | V1 约束 |
| --- | --- |
| `idempotency_key` | 1～128 字符；`[A-Za-z0-9._:-]`；租户内唯一 |
| `recipient.email` | 规范化后最长 254 字节；必须能解析为单个邮箱地址 |
| `recipient.display_name` | 可选；最长 128 个 Unicode 字符 |
| `sender_identity_key` | 1～64 字符；必须属于认证租户 |
| `template.key` | 1～128 字符；必须是已发布且已授权模板 |
| `template.version` | 可选；存在时必须大于 0 |
| `locale` | 合法 BCP 47 标签；最长 35 字符 |
| `variables` | 必须通过模板版本的 JSON Schema |
| `priority` | 0～9；仅在相同类别内部比较 |
| `scheduled_at` | 可选；过去时间按立即发送处理 |
| `dispatch_deadline` | 必填；必须晚于受理时间和有效的 `scheduled_at` |
| `metadata` | 最多 16 项；键最长 64、值最长 256；总计不超过 4 KiB |

模板变量编码后默认不得超过 16 KiB、嵌套不得超过 8 层。模板可以声明更严格限制。
验证码、Token 等敏感变量允许存在，但不得进入日志、错误详情、Trace 和 Metrics。

默认最大计划时间为受理后 365 天。租户和邮件类别可以配置更短窗口。

### 2.2 幂等

唯一键：

```text
(authenticated_tenant, idempotency_key)
```

处理结果：

- 首次提交：`ACCEPTED`；
- 相同 key、相同规范化 Payload：`DUPLICATE` 并返回原 `message_id`；
- 相同 key、不同 Payload：gRPC `ALREADY_EXISTS`；
- RPC 超时：调用方查询或使用同一 key 重试；
- 调用方不得因超时生成新 key。

`EmailMessage.template.version` 始终返回受理时固定的模板版本，即使请求省略版本。

## 3. BatchSubmitEmail

V1 单批最多 100 项：

- 每项必须有批内唯一 `item_id`；
- 每项必须有独立 `idempotency_key`；
- 每项产生独立逻辑消息和状态生命周期；
- 不保证整个批次原子成功；
- 返回结果顺序与请求顺序一致；
- 单项校验或配额错误放入对应 `BatchSubmitEmailResult.error`；
- 认证失败、请求无法解码、空批次或服务整体不可用使用 RPC 级错误。

调用方在 RPC 结果未知时可以原样重试整个批次。已经成功的项返回 `DUPLICATE`，其他
项继续独立处理。

## 4. GetEmail

调用方通过 `message_id` 或租户内 `idempotency_key` 查询。必须且只能提供一个。

响应不返回：

- 原始收件邮箱；
- 模板变量；
- 渲染后的正文；
- Provider 原始响应；
- 内部加密信息。

`recipient.masked_email` 只用于授权后的人工识别。

## 5. CancelEmail

取消是幂等操作：

- `SCHEDULED/QUEUED/RETRY_SCHEDULED` 可以取消；
- `ACCEPTED` 尚未进入后续阶段时可以取消；
- `SENDING` 及之后通常返回 `TOO_LATE`；
- 已取消返回 `ALREADY_CANCELED`；
- 取消成功不代表能够撤回 Provider 已经接受的邮件。

`reason_code` 只能包含稳定的非敏感业务码，不能放用户输入或邮件内容。

## 6. ListEmailEvents

- 按 `sequence` 升序返回；
- `page_size` 范围 1～100；
- `page_token` 是不透明值，客户端不得解析；
- 同一快照分页期间出现新事件时允许在后续页看到；
- 接口用于诊断和对账，不用于高频轮询。

## 7. 状态语义

```text
ACCEPTED
  ├── SCHEDULED
  └── QUEUED

SCHEDULED ──→ QUEUED / CANCELED / EXPIRED
QUEUED ─────→ SENDING / CANCELED / EXPIRED
SENDING ────→ RETRY_SCHEDULED
          ├─→ SUBMISSION_UNKNOWN
          ├─→ PROVIDER_ACCEPTED
          └─→ PERMANENTLY_FAILED

RETRY_SCHEDULED ──→ QUEUED / EXPIRED / DEAD_LETTERED
SUBMISSION_UNKNOWN → PROVIDER_ACCEPTED / UNKNOWN_FINAL / 重新调度
PROVIDER_ACCEPTED ─→ DELIVERED / BOUNCED / COMPLAINED
```

`PROVIDER_ACCEPTED` 表示 Provider 或 SMTP Server 已接受，不等于进入最终收件箱。
`DELIVERED` 只能来自提供可信送达事件的 Provider。

状态事件按 `message_id + sequence` 排序。较旧事件可以进入审计记录，但不能覆盖聚合
当前状态。

## 8. DeliveryEventReceiverService

订阅地址由控制面预注册，提交请求不能携带 callback URL。

通知语义：

- Mail Service 使用 `event_id` 至少一次投递；
- 同一事件重试必须保持相同 `event_id`；
- 订阅方按 `event_id` 幂等；
- `sequence` 小于等于已处理值的事件不能倒退业务状态；
- `ACCEPTED`、`DUPLICATE` 和 `IGNORED_STALE` 都表示该事件无需继续重试；
- RPC 超时或可重试错误由 Notification Worker 有界退避；
- 回调重试耗尽进入独立死信和对账流程。

## 9. 时间语义

所有时间使用 UTC `google.protobuf.Timestamp`：

- `scheduled_at`：调用方希望开始发送的最早时间；
- `dispatch_deadline`：允许开始新 Provider Attempt 的最晚时间；
- `accepted_at`：Mail Service 可靠受理时间；
- `occurred_at`：领域状态实际发生时间；
- `observed_at`：Mail Service 接收到外部事实的时间；
- `updated_at`：安全状态视图最后更新时间。

服务端使用数据库时间执行调度和截止判断。

## 10. 兼容规则

- V1 发布后不修改已有字段语义；
- 不复用字段号和枚举值；
- 删除字段使用 `reserved` 保留名称和编号；
- 新增枚举时客户端必须把未知值视为不可识别状态，不能当成功；
- 新增字段默认缺失时必须有安全行为；
- 破坏性变更发布为 `mailservice.delivery.v2`；
- 生成代码来自同一份 Proto 规范源，禁止两个仓库复制后分别修改。
