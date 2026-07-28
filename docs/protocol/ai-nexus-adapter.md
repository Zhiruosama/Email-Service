# AI-Nexus 适配

## 1. 边界

AI-Nexus 拥有：

- 验证码生成；
- 验证码 HMAC 摘要；
- purpose、尝试次数、激活、过期和一次性消费；
- 邮箱/IP/业务用途限流；
- 验证码业务状态。

Mail Service 拥有：

- 邮件可靠受理；
- `verification_code.v1` 模板；
- 异步投递、重试、熔断和死信；
- Provider 状态归一化；
- 投递状态通知。

两个系统不共享 Redis、数据库和内部 RabbitMQ。

## 2. 请求映射

| AI-Nexus 字段/语义 | 通用协议 |
| --- | --- |
| `request_id` | `idempotency_key` |
| 用户邮箱 | `recipient.email` |
| 平台发件身份 | `sender_identity_key = "ainexus.default"` |
| 验证码模板 | `content.template.key = "verification_code.v1"` |
| 模板版本 | 省略，由受理时固定活动版本 |
| 用户语言 | `content.locale`，默认 `zh-CN` |
| 验证码、purpose、有效期 | `content.variables` |
| 验证码类别 | `EMAIL_CATEGORY_CRITICAL` |
| 优先级 | 建议 9 |
| 请求时间 | 不进入 Payload；由服务端记录 `accepted_at` |
| 最晚投递时间 | `dispatch_deadline`，建议生成后 2 分钟 |
| 重复风险 | `DUPLICATE_RISK_POLICY_AVOID_DUPLICATE` |

变量 Schema：

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["code", "purpose", "valid_for_seconds"],
  "properties": {
    "code": {
      "type": "string",
      "pattern": "^[0-9]{6}$"
    },
    "purpose": {
      "type": "string",
      "enum": ["REGISTER", "RESET_PASSWORD", "LOGIN"]
    },
    "valid_for_seconds": {
      "type": "integer",
      "minimum": 60,
      "maximum": 1800
    }
  }
}
```

验证码变量是敏感信息。Mail Service 加密保存，并在确定不会再产生发送尝试后清理。

## 3. 响应映射

AI-Nexus 只有收到：

```text
SUBMIT_DISPOSITION_ACCEPTED
或
SUBMIT_DISPOSITION_DUPLICATE
```

才向客户端返回邮件任务已受理。它们都不代表验证码已经激活。

AI-Nexus 保存：

- Mail Service `message_id`；
- 自己生成的 `request_id/idempotency_key`；
- 验证码摘要；
- `PENDING_DISPATCH` 状态。

## 4. 事件映射

AI-Nexus 实现预注册的 `DeliveryEventReceiverService`：

| Mail Service 状态 | AI-Nexus 行为 |
| --- | --- |
| `ACCEPTED/SCHEDULED/QUEUED/SENDING` | 保持 `PENDING_DISPATCH` |
| `RETRY_SCHEDULED` | 保持等待并更新观测状态 |
| `PROVIDER_ACCEPTED` | 首次到达时幂等激活验证码 |
| `DELIVERED` | 若尚未激活则幂等激活；不得延长已激活有效期 |
| `BOUNCED` | 不激活；记录投递观测状态 |
| `PERMANENTLY_FAILED` | 进入 `DELIVERY_FAILED` |
| `DEAD_LETTERED` | 进入 `DELIVERY_FAILED` |
| `EXPIRED` | 进入 `EXPIRED` |
| `SUBMISSION_UNKNOWN` | 保持不可验证，等待对账 |
| `UNKNOWN_FINAL` | 终止该验证码并允许重新申请 |

AI-Nexus 按 `event_id` 幂等，并按 `message_id + sequence` 防止乱序状态倒退。

## 5. 与旧协议的关系

旧协议：

```text
ainexus.mail.v1.VarifyService.GetVarifyCode
```

必须被替换，不能继续作为新核心协议的一部分。旧协议由外部服务生成并返回验证码，
与新的职责边界冲突。

迁移期间可以在边缘提供临时兼容 Adapter，但 Adapter 必须：

- 调用同一个通用 Application Service；
- 不访问 AI-Nexus Redis；
- 不生成验证码；
- 不建立第二套状态机；
- 有明确下线日期。

AI-Nexus 和 Mail Service 最终应从同一份版本化通用 Proto 生成代码。
