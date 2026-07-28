# gRPC V1 错误模型

## 1. 两类错误

同步 RPC 错误表示请求没有被正常受理或查询无法完成。异步 Provider 失败通过
`DeliveryStatus + FailureInfo` 表达，不伪装成迟到的 RPC 错误。

错误详情必须脱敏，不能包含邮箱、模板变量、正文、凭据或完整 Provider 响应。

## 2. 标准 gRPC Code

| Code | 场景 | 调用方行为 |
| --- | --- | --- |
| `INVALID_ARGUMENT` | 字段、时间、邮箱、模板变量或批次结构无效 | 修正请求，不重试原 Payload |
| `UNAUTHENTICATED` | 缺少或无法验证服务身份 | 不重试，刷新身份或告警 |
| `PERMISSION_DENIED` | 无权使用模板、发件身份、类别或接口 | 不重试 |
| `NOT_FOUND` | 当前租户下不存在目标任务 | 停止查询或检查 key |
| `ALREADY_EXISTS` | 同幂等键对应不同 Payload | 不重试并告警 |
| `FAILED_PRECONDITION` | 当前状态不允许操作 | 读取最新状态 |
| `RESOURCE_EXHAUSTED` | 租户配额、速率或容量保护 | 按服务提示有界退避 |
| `ABORTED` | 并发状态竞争，操作未完成 | 读取状态后有界重试 |
| `UNAVAILABLE` | 服务暂时无法可靠受理 | 相同幂等键重试 |
| `DEADLINE_EXCEEDED` | RPC 结果未知 | 先查询，再使用相同幂等键重试 |
| `INTERNAL` | 未分类服务内部错误 | 相同幂等键有界重试并告警 |

服务端不得对 `SubmitEmail` 配置会绕过调用方幂等策略的隐式无限重试。

## 3. Batch 错误

批次整体错误使用 gRPC Code。单项错误使用 `BatchItemErrorCode`：

| Batch code | 对应语义 |
| --- | --- |
| `INVALID_ARGUMENT` | 当前项内容不合法 |
| `PERMISSION_DENIED` | 当前项使用了未授权资源 |
| `ALREADY_EXISTS` | 当前项幂等键冲突 |
| `RESOURCE_EXHAUSTED` | 当前项触发配额 |
| `UNAVAILABLE` | 当前项未能可靠持久化，可同 key 重试 |
| `INTERNAL` | 未分类错误，可同 key 有界重试 |

响应中的 `message` 面向开发者且必须稳定、脱敏；程序逻辑只能依赖枚举 `code`。

## 4. FailureInfo

`FailureInfo` 描述异步投递失败：

| Category | 示例 |
| --- | --- |
| `VALIDATION` | 模板在执行时发现不兼容 |
| `AUTHENTICATION` | Provider 凭据失效 |
| `RATE_LIMITED` | SMTP 4xx 或 Provider 429 |
| `RECIPIENT_REJECTED` | 明确的无效收件地址 |
| `CONTENT_REJECTED` | 内容或策略拒绝 |
| `PROVIDER_UNAVAILABLE` | Provider 5xx |
| `NETWORK` | DNS、连接或 TLS 故障 |
| `TIMEOUT_BEFORE_SEND` | 尚未提交正文即超时 |
| `SUBMISSION_UNKNOWN` | 提交后未获得确定响应 |
| `INTERNAL` | 本地未分类故障 |

`retryable` 描述当前失败是否具备技术重试可能，不保证任务仍有重试预算或未超过
`dispatch_deadline`。

`FailureInfo.code` 是 Mail Service 定义的稳定机器码，例如：

```text
smtp.auth_failed
smtp.recipient_rejected
provider.rate_limited
network.connect_timeout
submission.result_unknown
template.render_failed
dispatch.deadline_exceeded
```

Provider 原始状态码可以映射成稳定码，但不能原样暴露包含敏感内容的响应文本。
