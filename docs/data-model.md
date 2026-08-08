# 领域模型与数据模型

## 1. 聚合边界

`MailMessage` 是核心聚合：

- 一个逻辑消息只对应一个租户和一个收件人；
- 一个逻辑消息可以有多个投递 Attempt；
- 幂等键在租户范围内唯一；
- 聚合状态只能按状态机推进；
- 所有外部事件先转换为领域事件再更新聚合。

## 2. 消息状态机

```text
ACCEPTED
  ├── scheduled_at > now ──→ SCHEDULED
  └── due now ─────────────→ QUEUED

SCHEDULED
  ├── 到期 ────────────────→ QUEUED
  ├── 取消 ────────────────→ CANCELED
  └── 超过 deadline ───────→ EXPIRED

QUEUED
  ├── Worker 领取 ─────────→ SENDING
  ├── 取消 ────────────────→ CANCELED
  └── 超过 deadline ───────→ EXPIRED

SENDING
  ├── 临时失败 ────────────→ RETRY_SCHEDULED
  ├── 结果未知 ────────────→ SUBMISSION_UNKNOWN
  ├── Provider 接受 ───────→ PROVIDER_ACCEPTED
  └── 永久失败 ────────────→ PERMANENTLY_FAILED

RETRY_SCHEDULED
  ├── 到期 ────────────────→ QUEUED
  ├── 重试耗尽 ────────────→ DEAD_LETTERED
  └── 超过 deadline ───────→ EXPIRED

SUBMISSION_UNKNOWN
  ├── 对账确认接受 ────────→ PROVIDER_ACCEPTED
  ├── 对账确认失败 ────────→ RETRY_SCHEDULED/PERMANENTLY_FAILED
  └── 无法确定 ────────────→ UNKNOWN_FINAL

PROVIDER_ACCEPTED
  ├── Webhook ─────────────→ DELIVERED
  ├── 退信 ────────────────→ BOUNCED
  └── 投诉 ────────────────→ COMPLAINED

DELIVERED
  └── 后续投诉 ────────────→ COMPLAINED
```

终态和可追加观测事件需要区分。例如 `PROVIDER_ACCEPTED` 后收到 `BOUNCED` 是更晚
的事实，不应被禁止；但旧的 `QUEUED` 事件绝不能覆盖它。

Provider 回调可能丢失中间事件。状态机允许有可信证据的向前跳转，例如
`SUBMISSION_UNKNOWN → DELIVERED`，但不会反向补写未知的 `provider_accepted_at`。
迟到的 `PROVIDER_ACCEPTED` 只进入必要审计，不得让 `DELIVERED` 状态倒退。

## 3. 核心表

### 3.1 `tenants`

```text
id uuid primary key
key varchar unique
name varchar
status varchar
default_locale varchar
retention_policy_id uuid
created_at timestamptz
updated_at timestamptz
```

### 3.2 `sender_identities`

```text
id uuid primary key
tenant_id uuid
key varchar
from_email_ciphertext bytea
from_email_fingerprint bytea
display_name varchar
reply_to_ciphertext bytea nullable
verification_status varchar
created_at timestamptz
updated_at timestamptz
unique (tenant_id, key)
```

### 3.3 `template_versions`

```text
id uuid primary key
tenant_id uuid nullable
template_key varchar
version int
locale varchar
subject_template text
html_template text
text_template text
variables_schema jsonb
content_digest bytea
status varchar
created_at timestamptz
published_at timestamptz nullable
unique (tenant_id, template_key, version, locale)
```

`tenant_id = null` 表示平台模板。租户模板覆盖规则必须显式配置，不能依赖查询顺序。

### 3.4 `mail_messages`

```text
id uuid primary key
tenant_id uuid
idempotency_key varchar
payload_fingerprint bytea
category varchar
priority smallint
duplicate_risk_policy varchar
recipient_ciphertext bytea
recipient_fingerprint bytea
sender_identity_id uuid
template_version_id uuid
locale varchar
variables_ciphertext bytea
encryption_key_version int
status varchar
scheduled_at timestamptz
dispatch_deadline timestamptz
next_attempt_at timestamptz nullable
dispatch_generation bigint
attempt_count int
max_attempts int
route_version_id uuid nullable
current_provider_id uuid nullable
provider_message_id varchar nullable
provider_accepted_at timestamptz nullable
latest_sequence bigint
last_error_category varchar nullable
last_error_code varchar nullable
last_error_summary varchar nullable
last_error_retryable boolean nullable
version bigint
created_at timestamptz
updated_at timestamptz
accepted_at timestamptz
terminal_at timestamptz nullable
unique (tenant_id, idempotency_key)
```

`version` 用于乐观并发控制。扫描索引至少包括：

```text
(status, scheduled_at)
(status, next_attempt_at)
(tenant_id, created_at desc)
(recipient_fingerprint, created_at desc)
(current_provider_id, status)
```

单 Message 更新使用 `WHERE id = ? AND version = ?`，成功后由数据库执行
`version = version + 1`。影响 0 行表示调用方持有旧 Snapshot，必须重新读取并重新执行
状态机，不能直接覆盖当前状态。Scheduler 的批量领取才使用行锁，两者职责不同。

04-A Migration 先实现租户、幂等身份和状态机快照字段。收件地址、模板变量、路由和
Provider 配置需要先完成加密与控制面模型，将通过后续 Migration 增加，当前不会以
明文占位。

### 3.5 `delivery_attempts`

```text
id uuid primary key
message_id uuid
attempt_no int
provider_id uuid
credential_version_id uuid
route_version_id uuid
status varchar
started_at timestamptz
finished_at timestamptz nullable
provider_message_id varchar nullable
error_category varchar nullable
error_code varchar nullable
error_summary varchar nullable
result_metadata jsonb
unique (message_id, attempt_no)
```

`result_metadata` 必须经过字段白名单过滤，不保存 Provider 原始响应、完整地址或正文。

### 3.6 `delivery_events`

追加式审计事件：

```text
id uuid primary key
message_id uuid
provider_event_id varchar nullable
provider_id uuid nullable
event_type varchar
sequence bigint
occurred_at timestamptz
received_at timestamptz
sanitized_payload jsonb
unique (message_id, sequence)
unique (provider_id, provider_event_id)
```

内部 `sequence` 由 Mail Service 分配。Provider 自己的序列或时间不能直接作为全局
顺序依据。

### 3.7 `outbox_events`

```text
id uuid primary key
aggregate_type varchar
aggregate_id uuid
event_type varchar
aggregate_sequence bigint
dispatch_generation bigint
payload jsonb
status varchar
available_at timestamptz
lease_owner varchar nullable
lease_until timestamptz nullable
attempt_count int
created_at timestamptz
published_at timestamptz nullable
```

Outbox 使用
`(aggregate_type, aggregate_id, event_type, aggregate_sequence, dispatch_generation)`
唯一约束。`event_type` 不可省略，因为一次状态推进产生的状态事件和 dispatch command
可以共享 aggregate sequence。

payload 是有版本的安全 JSON envelope，只允许 Message ID、租户 ID、状态、sequence、
dispatch generation、attempt number 和脱敏失败分类等必要字段。收件地址、验证码、正文、
模板变量和 Provider 凭据不得复制到 Outbox。应用层限制 payload 必须为 JSON object 且不
超过 64 KiB，数据库继续使用 `jsonb_typeof(payload) = 'object'` 兜底。

重复 identity 通过 `INSERT ... ON CONFLICT DO NOTHING` 处理，再使用 PostgreSQL JSONB
相等比较已有 payload：语义相同是幂等成功，内容不同返回一致性冲突。这样既避免唯一
异常让事务进入 aborted 状态，也不会用“幂等”掩盖同一领域事实产生两个不同 payload。

### 3.8 `notification_deliveries`

一条领域事件面向每个订阅目标生成一条交付记录：

```text
id uuid primary key
event_id uuid
subscription_id uuid
status varchar
attempt_count int
next_attempt_at timestamptz
lease_until timestamptz nullable
last_error_code varchar nullable
delivered_at timestamptz nullable
unique (event_id, subscription_id)
```

## 4. 配置模型

还需要：

- `providers`
- `provider_credential_versions`
- `route_versions`
- `subscriptions`
- `quota_policies`
- `retention_policies`
- `suppression_entries`

Provider 凭据只保存密钥系统引用，或使用独立 KEK 加密。数据库中不得出现明文 SMTP
授权码。

## 5. Suppression List

通用邮件服务需要平台级和租户级抑制列表：

- 明确永久退信；
- 用户投诉；
- 租户主动阻止；
- 法规或安全要求。

匹配使用规范化邮箱的 HMAC 指纹。提交阶段可以快速拒绝，Worker 在真正发送前必须
再次检查，以覆盖任务排队期间新增的抑制记录。

验证码或安全邮件是否可以覆盖某些租户级退订，需要由明确策略决定；永久退信和平台
安全封禁不得绕过。

## 6. 数据保留

建议默认策略：

| 数据 | 默认保留 |
| --- | --- |
| 加密正文/变量 | 不再需要后续发送时立即清理，最迟 1 小时 |
| 加密收件地址 | 30 天，可按租户缩短 |
| 邮箱 HMAC 指纹 | 90 天或合规策略 |
| Message/Attempt 状态 | 90 天 |
| 幂等键 | 至少覆盖最大调度与重试窗口，默认 30 天 |
| 投递审计事件 | 90 天 |
| 原始 Provider Webhook | 不保存；仅保存脱敏归一化字段 |

如果允许数月后的定时邮件，幂等和加密 payload 的保留期必须至少覆盖
`scheduled_at + delivery window`。

`PROVIDER_ACCEPTED` 后虽然仍可能出现 `DELIVERED/BOUNCED/COMPLAINED`，邮件正文和
验证码变量已经不再参与发送，应当提前清理，不必等待观测生命周期结束。
