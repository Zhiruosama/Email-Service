# 08-A1：可靠受理内核、幂等指纹与敏感 Payload 加密

## 1. 这一阶段解决什么问题

07-C 已经能把数据库中的任务投递到 Fake Provider，但测试任务仍由集成测试直接写库。
现有 `mail_messages` 也只有生命周期字段，没有收件人、模板版本和变量，因此直接加 gRPC
Handler 会得到一个“能排队、却没有内容可发送”的空壳 API。

08-A1 先完成可靠受理的应用内核：

```text
SubmitEmailCommand
  → 输入规范化与边界校验
  → TemplateResolver 固定不可变模板版本并校验变量
  → 规范化业务 Payload
  ├── HMAC-SHA256 → payload_fingerprint
  └── AES-256-GCM → encrypted_payload
  → PostgreSQL transaction
       ├── database time
       ├── Message 状态机
       ├── Submission 快照
       └── Transactional Outbox
  → ACCEPTED / DUPLICATE / idempotency conflict
```

本阶段只实现应用用例和持久化，不注册 gRPC Service。这样下一阶段的 Handler 只负责
Proto 映射、身份注入和错误码翻译，不承担业务规则。

## 2. 为什么 Message 与 Submission 分开

`message.Message` 是生命周期聚合，只管理状态、时间、Attempt、generation、sequence 和
状态迁移；收件人、模板、变量是受理后不可变的投递输入。

如果把全部邮件内容塞进状态机，会造成：

- 每次 Worker 状态变化都携带和复制敏感大对象；
- 状态机测试被模板、JSON 和加密细节污染；
- 查询安全状态时容易意外返回原始邮箱或验证码；
- 未来清理敏感内容会影响聚合恢复。

因此持久化 `MessageRecord` 组合两部分：

```text
MessageRecord
  ├── Message             可变生命周期聚合
  └── SubmissionDetails   不可变、安全持久化快照
```

`SubmissionDetails` 中只有发件身份键、模板键/版本、locale、脱敏邮箱和非敏感 metadata
可以明文查询；原始邮箱、display name 和模板变量一起放在认证加密 Payload 中。

## 3. Migration 00003

第三号迁移在 `mail_messages` 增加：

```text
sender_identity_key
template_key
template_version
locale
recipient_masked
payload_key_id
encrypted_payload
submission_metadata
```

数据库 CHECK 保证这些字段“全部为空或全部存在”。允许全部为空是为了兼容此前由底层测试
构造的生命周期记录；新的 `EmailSubmissionService` 永远创建完整 Submission。后续完成数据
迁移后，可以进一步把这一约束收紧为全部 `NOT NULL`。

`payload_key_id` 不是密钥，而是密钥版本标识。密钥轮换时，旧数据仍可根据 key ID 选择正确
密钥解密；真正密钥不能写入业务表。

## 4. 幂等为何需要规范化 Payload + HMAC

唯一约束仍然是：

```text
(tenant_id, idempotency_key)
```

但只看 key 无法区分两种情况：

1. 客户端超时后原样重试，应返回原 `message_id`；
2. 客户端错误复用了 key，却换了邮箱、验证码或截止时间，应拒绝覆盖。

服务先把邮箱域名、BCP 47 locale、JSON 对象、metadata key 顺序和 UTC 时间规范化，再对完整
业务 Payload 计算：

```text
HMAC-SHA256(fingerprint_key, domain_separator || canonical_payload)
```

相同 key + 相同 HMAC 返回 `DUPLICATE`；相同 key + 不同 HMAC 返回
`ErrIdempotencyConflict`，后续映射为 gRPC `ALREADY_EXISTS`。

这里不用普通 SHA-256。邮箱和六位验证码的搜索空间很小，拿到普通哈希的攻击者可以离线
枚举；HMAC 没有服务端密钥就无法验证猜测。domain separator 还防止同一 HMAC key 被其他
用途复用时产生跨协议碰撞语义。

## 5. 为什么使用 AES-256-GCM

AES-GCM 同时提供：

- 机密性：数据库泄漏后看不到原始邮箱和验证码；
- 完整性：密文或关联数据被篡改时解密失败；
- 随机化：同一 Payload 两次加密得到不同密文。

存储格式为：

```text
12-byte random nonce || ciphertext || 16-byte authentication tag
```

AAD 使用 `tenant_id/message_id`。密文即使被复制到另一个租户或任务，也不能通过认证解密。
每次加密必须生成新 nonce；同一 AES-GCM key 下复用 nonce 会严重破坏安全性。

加密和幂等指纹使用两把独立 32-byte key，因为它们的安全用途不同。组件从构造参数接收
key；08-A2 已把 strict base64 密钥配置接入 Bootstrap，生产阶段再由 KMS/Vault 提供和轮换。

## 6. 为什么 TemplateResolver 属于应用端口

调用方可以省略模板版本，但可靠受理以后，任务必须固定一个不可变版本，否则同一任务重试
可能使用后来发布的新模板。

应用层只依赖：

```go
Resolve(tenant, templateKey, requestedVersion, locale, variables)
    → resolved immutable version + validated canonical variables
```

它不关心模板来自 PostgreSQL、配置文件还是控制面缓存。08-A1 通过测试 Resolver 验证用例；
后续会实现已发布模板目录。Resolver 还负责租户授权和变量 Schema 校验，使 gRPC Adapter
不能绕开模板规则。

## 7. 输入规范化与校验

应用边界当前执行：

- tenant 必须是 UUID；
- idempotency key 必须满足协议字符集且不超过 128；
- 收件人必须是单个 bare address，禁止 CR/LF 和地址列表，域名经 IDNA 后转小写；
- display name 限制 Unicode 字符数并禁止换行；
- sender/template key 使用安全字符白名单；
- locale 解析并规范化为 BCP 47；
- variables 必须是 16 KiB 内、最多 8 层的 JSON object；
- metadata 最多 16 项、总计最多 4 KiB，并禁止换行；
- priority、category、duplicate risk policy 必须是已知值；
- database acceptance time 判断 deadline 和最长 365 天计划窗口。

“过去的 scheduled_at 按立即发送”不是 Handler 特判，而由状态机构造时根据数据库时间进入
`QUEUED`。

## 8. 同事务保证了什么

提交事务内依次执行：

1. 读取 PostgreSQL transaction time；
2. 创建初始 Message 状态；
3. 插入包含加密 Submission 的 `mail_messages`；
4. 插入 `MESSAGE_ACCEPTED`、状态变化和立即投递 Outbox；
5. commit 后才返回 `ACCEPTED`。

因此不会出现：

- Message 成功但没有 Payload；
- Payload 成功但没有状态；
- API 返回成功但 Outbox 未提交。

重复请求在唯一约束冲突后读取已有记录并 constant-time 比较 HMAC，不会创建第二条 Outbox。

## 9. 主要代码

- `db/migrations/sql/00003_add_submission_payload.sql`：安全 Submission 列与一致性约束；
- `internal/application/delivery/email_submission.go`：规范化、模板解析、事务受理和幂等语义；
- `internal/application/ports/submission.go`：TemplateResolver 与 PayloadProtector 端口；
- `internal/application/ports/message_repository.go`：不可变 SubmissionDetails；
- `internal/security/payload/protector.go`：HMAC-SHA256 与 AES-256-GCM；
- `internal/storage/postgres/message_*`：Submission 的写入、读取和腐败数据检查；
- `internal/integration/email_submission_test.go`：真实 PostgreSQL 受理测试。

## 10. 如何验证

普通测试：

```bash
go test ./...
go test -race ./...
go vet ./...
```

真实 PostgreSQL 测试：

```bash
TEST_POSTGRES_IMAGE=postgres:18.4-alpine \
go test -tags=integration ./internal/integration \
  -run '^TestEmailSubmissionIsAtomicIdempotentAndEncrypted$' -count=1 -v
```

测试证明：

1. 首次请求返回 `ACCEPTED`；
2. 同 Payload 同 key 返回 `DUPLICATE` 和原 `message_id`；
3. 同 key 更换验证码返回幂等冲突；
4. 数据库只有 1 条 Message 和 3 条初始 Outbox；
5. `encrypted_payload` 和 Outbox 都不含验证码明文；
6. 模板版本已固定，查询只暴露脱敏邮箱；
7. Migration 3 不破坏此前 Repository、Scheduler、Worker 和 Runtime 回归测试。

## 11. 面试表达

### 30 秒版本

> 我先没有急着接 gRPC，而是补齐可靠受理内核。请求会先规范化、解析并固定模板版本，完整
> Payload 用 HMAC 做幂等指纹，再用 AES-GCM 加密保存。Message、加密 Submission 和 Outbox
> 在 PostgreSQL 同一事务提交，所以 API 返回受理后任务和投递事件一定同时存在。同 key 同
> Payload 返回原任务，同 key 不同 Payload 拒绝覆盖，并用真实 PostgreSQL 测试验证了密文、
> 原子性和 Outbox 不泄密。

### 2 分钟版本

> 状态机只管理邮件生命周期，不应该携带邮箱和验证码，所以我把不可变 Submission 快照与
> Message 聚合分开，但放在同一条持久化记录和事务中。模板通过 Application Port 解析，省略
> 版本时也会在受理时固定已发布版本，并校验变量。
>
> 幂等比较的是规范化完整 Payload 的 HMAC-SHA256，而不是只看 idempotency key，也不是普通
> SHA-256。这样 RPC 超时后原样重试会返回原 message ID，而复用 key 修改收件人或验证码会
> 返回冲突；HMAC 还能防止低熵邮箱和验证码被离线枚举。敏感 Payload 用 AES-256-GCM 和随机
> nonce 加密，tenant/message ID 作为 AAD，数据库只明文保存脱敏邮箱和模板标识。
>
> 最后在一个 PostgreSQL 事务里读取数据库时间、创建状态机、写 Message/Submission 和
> Transactional Outbox，commit 后才返回 ACCEPTED。Testcontainers 验证首次、重复、冲突、
> 行数、Outbox 数量以及验证码没有进入密文外的列或事件。

## 12. 可能追问

**为什么不把密文也放到 RabbitMQ？**

RabbitMQ 命令只携带 message ID、tenant、sequence 和 generation。Worker 按 ID 从权威数据库
读取 Payload，避免敏感内容复制到 Broker、DLQ 和运维界面，也降低密钥轮换和数据清理范围。

**HMAC key 轮换后，老请求的幂等比较怎么办？**

当前实现是一把活动指纹 key，生产化前需要给 fingerprint 增加 key version 或在保留窗口内
同时支持新旧 key。加密数据已经保存 `payload_key_id`，可以按版本解密。不能直接换 key 后
让同一请求产生不同指纹。

**数据库管理员仍能看到 metadata，安全吗？**

协议规定 metadata 只能放非敏感关联信息，应用做大小和换行限制，但语义敏感性还需要调用方
规范与审计。验证码、Token、完整邮箱必须放模板 variables/recipient，进入加密 Payload。

## 13. 尚未解决

- 08-A2 已完成 gRPC Submit/Get Adapter、开发租户身份注入和标准错误映射；生产 mTLS 待实现；
- Bootstrap 的密钥配置、真实模板目录和 sender identity 授权；
- Worker 解密 Payload、模板渲染和真实 Provider 请求；
- Batch、Cancel、ListEvents；
- KMS/Vault、密钥轮换、敏感 Payload 到期清理；
- 指纹 key version 与轮换窗口；
- 数据迁移完成后把 Submission 列收紧为 `NOT NULL`。
