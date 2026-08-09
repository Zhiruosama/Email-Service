# 09-A1：Payload 解密、模板渲染与安全 MIME

- 状态：已完成
- 阶段目标：把已领取邮件的加密 Submission 安全转换成一次性 Provider 投递材料，并把构建失败可靠收敛到现有 Attempt 与状态机。

## 1. 解决的问题

08-A1 已经把收件地址、display name 和模板变量放进 AES-GCM 密文，但此前 Dispatch Worker
交给 Fake Provider 的只有 Message、Tenant、Attempt 和策略元数据。也就是说，可靠调度链路
已经存在，却还不能生成真正可交给 SMTP 的邮件。

如果直接在 SMTP Adapter 里同时解密、查模板和拼 MIME，会产生三个问题：

- SMTP Adapter 同时承担内容、安全和传输职责，难以替换成 API Provider；
- 模板或密文失败发生在 `QUEUED → SENDING` 之后，若直接返回错误，消息会卡在 `SENDING`；
- 收件地址、验证码和正文容易被复制到 MQ、Attempt、错误日志或测试 Fake 中。

本阶段引入 Delivery Material 边界：Provider 只接收已经准备好的 SMTP Envelope 与 MIME；
内容构建仍由应用层编排，传输实现无需理解模板变量或密文格式。

## 2. 完整流程与事务边界

```text
RabbitMQ dispatch command
  → 短事务 1：QUEUED → SENDING + STARTED Attempt + lifecycle Outbox
  → 事务外：AES-GCM Open → 身份交叉校验 → 固定版本模板渲染 → MIME 编码
  → 事务外：Provider.Submit(DeliveryMaterial)
  → 清空 MIME byte slice
  → 短事务 2：完成 Attempt + 推进 Message 状态 + lifecycle Outbox
  → 数据库提交后 ACK RabbitMQ
```

解密、模板渲染、MIME 构建和 Provider 网络调用都不放进数据库事务。未来模板可能来自缓存或
控制面，SMTP 也可能阻塞数秒；如果在持锁事务内执行，会扩大锁等待、消耗连接池，并降低多
Worker 并发能力。

构建失败不能简单返回 Go error。第一段事务已经提交，Attempt 已经是 `STARTED`，所以 Worker
把失败归一化成 `ProviderResult{Outcome: FAILED}`，再复用原有第二段事务完成 Attempt 和状态机：

| 构建失败 | Retryable | 状态结果 | 原因 |
| --- | --- | --- | --- |
| 当前实例缺少历史 key version | 是 | `RETRY_SCHEDULED` | 配置/密钥服务恢复后可能成功 |
| 模板控制面短暂故障 | 是 | `RETRY_SCHEDULED` | 外部依赖可能恢复 |
| AES-GCM 认证失败 | 否 | `PERMANENTLY_FAILED` | 重试同一密文不会恢复 |
| Payload schema 或身份不一致 | 否 | `PERMANENTLY_FAILED` | 数据已损坏或违反不变量 |
| 模板不存在、变量不合法 | 否 | `PERMANENTLY_FAILED` | 同一固定版本重试不会改变 |

## 3. 为什么是 Delivery Material Port

核心 Port 是：

```go
type DeliveryMaterial struct {
    EnvelopeFrom string
    EnvelopeTo   string
    MIMEMessage  []byte
}

type DeliveryMaterialBuilder interface {
    Build(context.Context, MessageRecord, StartedDeliveryAttempt) (DeliveryMaterial, error)
}
```

它把“内容准备”和“外部传输”分开：

- `DeliveryMaterialService` 负责解密、校验、渲染和 MIME；
- `EmailProvider` 负责 SMTP 或供应商 API 的提交和响应分类；
- Fake Provider 与未来 SMTP Provider 使用同一请求，不需要 Worker 写 Provider 分支；
- 单元测试可以分别验证密码学、模板、MIME 和状态机失败语义。

没有选择把明文写回数据库再让 SMTP Worker 读取，因为这会失去静态加密的价值；也没有把完整
MIME 放进 Outbox/RabbitMQ，因为会制造更多敏感副本，并让消息中间件承担正文存储与清理责任。

## 4. 认证解密与身份交叉校验

`PayloadProtector.Open` 使用受理时相同的 AES-256-GCM 和 AAD：

```text
AAD = tenant_id + "/" + message_id
```

GCM 不只保密，还认证密文和 AAD。密文被改、换到另一个租户/Message，都会认证失败。`key_id`
不匹配当前 keyring 被归类为“密钥暂不可用”，而认证失败被归类为不可重试的数据损坏。

解密以后仍要把 Payload 与数据库中可安全查询的不可变列交叉校验，包括：

- sender identity、template key/version、locale；
- recipient 重新脱敏后的结果；
- category、priority、duplicate risk policy；
- scheduled time、dispatch deadline；
- canonical metadata。

AES-GCM 能证明“密文是受信密钥产生的”，交叉校验进一步证明“这份密文仍属于当前这条数据库
记录”。若攻击或故障只修改了旁路查询列，Worker 不会带着不一致身份继续发送。

完整纵向测试在这里发现过一个真实边界：Protobuf Timestamp 和 Go `time.Time` 能携带纳秒，
PostgreSQL `timestamptz` 只有微秒精度。如果加密前不规范化，密文中的 deadline 与数据库读回值
会相差不足一微秒，严格身份校验仍会正确地拒绝它。最终在受理边界把 scheduled/deadline 统一
截断到微秒，再同时用于加密、幂等指纹和持久化；没有在发送端用模糊比较掩盖数据模型不一致。

## 5. 模板渲染与 MIME 安全

第一版目录只发布不可变的 `verification_code.v1@1/zh-CN`。受理时已经固定版本并校验变量；
投递时再次按精确版本解析，使一次重试不会突然使用后来发布的新模板。

模板同时生成：

- UTF-8 Subject；
- Plain Text 正文；
- HTML 正文，使用 Go `html/template` 自动转义变量。

MIME Encoder 生成 `multipart/alternative`，正文使用 quoted-printable，并加入 Date、Message-ID、
From、To、Subject、MIME-Version 和 Content-Type。安全边界包括：

- Header 禁止 CR/LF，阻止 `Bcc:` 等 Header Injection；
- SMTP Envelope 只接受 bare address，不把 display name 混入 RCPT 地址；
- display name 和正文限制 UTF-8 与大小；
- 完整 MIME 当前限制 2 MiB；
- multipart boundary 从 Attempt UUID 确定，同一 Attempt 的编码稳定；
- HTML 变量默认转义，V1 不允许调用方提交 Raw HTML。

## 6. 明文生命周期

明文只存在于 `Build → Provider.Submit` 的同步调用窗口：

- 解密得到的 byte slice 在 Build 返回前 best-effort `clear`；
- MIME 直接写入最终缓冲区，避免再复制一份完整邮件；
- Provider 返回后 Worker 立即 `clear(MIMEMessage)`；
- Fake Provider 只记录 Attempt/Message/Tenant 和字节数，不保留地址或 MIME；
- `DeliveryMaterialError.Error()` 只暴露稳定 code，私有 cause 不进入 unwrap 错误链；
- Outbox、Delivery Attempt、Journal 和普通日志都不保存明文。

Go 的 string、模板引擎和垃圾回收不保证密码学意义的内存擦除，因此这里的准确保证是“减少副本、
缩短生命周期并禁止持久化/日志扩散”，不是声称进程内存中从未出现明文。更高安全级别需要独立
密钥服务、进程隔离和专门的敏感内存方案。

## 7. 主要文件

- `internal/application/ports/delivery_material.go`：Material、Renderer、Sender、MIME Ports 与安全错误；
- `internal/security/payload/protector.go`：AES-GCM authenticated Open；
- `internal/application/delivery/delivery_material_builder.go`：解密、交叉校验和内容编排；
- `internal/template/catalog/catalog.go`：固定版本验证码模板与 HTML/Text 渲染；
- `internal/sender/static/resolver.go`：开发阶段租户级发件身份注册表；
- `internal/content/mimebuilder/builder.go`：安全 multipart/alternative MIME；
- `internal/application/delivery/dispatch_worker.go`：事务外构建、失败归一化和明文清理；
- `internal/provider/fake/provider.go`：不保留明文的测试 Provider；
- `internal/bootstrap/app.go`：Composition Root 装配。

## 8. 验证

测试覆盖：

- Seal/Open 往返、密文篡改、错误 AAD、历史 key 缺失；
- 模板变量校验、Text/HTML 渲染和 HTML escaping；
- MIME 双 part 解码、UTF-8 Subject、CRLF 规范和 Header Injection；
- 密文到真实 Envelope/MIME 的完整 Material 构建；
- 数据库安全列与密文身份不一致时拒绝发送；
- 纳秒级请求时间先规范化为 PostgreSQL 微秒精度，持久化往返后身份仍一致；
- 可重试/永久 Material 失败均完成 Attempt，且 Provider 调用次数为零；
- Fake Provider 的观测数据不保留验证码或 MIME。

验证命令：

```bash
go test ./...
go test -race ./...
go vet ./...
make migrate-validate
go test -tags=integration ./internal/integration/...
```

## 9. 面试表达

### 30 秒版本

> 我在 SMTP 前增加了 Delivery Material 边界。Worker 领取任务并提交短事务后，才在事务外用
> AES-GCM 认证解密 Payload，交叉校验数据库不可变字段，按受理时固定的模板版本渲染 HTML/Text
> 并生成安全 MIME。构建失败会按可恢复性完成 Attempt 并进入重试或永久失败，明文不进入 MQ、
> Outbox、Journal 和日志，Provider 返回后还会清空 MIME 缓冲区。

### 2 分钟版本

> 之前可靠链路只传派发元数据，因为收件地址和验证码都在加密 Submission 中。我的设计没有让
> SMTP Adapter 负责解密和模板，而是抽出 DeliveryMaterialBuilder：密码学、模板、Sender Identity
> 和 MIME 都通过 Port 组合，Provider 只负责提交。因此以后换 HTTP API Provider 不会修改状态机。
>
> 构建发生在 `QUEUED → SENDING` 的领取事务之后，避免解密和模板工作占数据库连接。但这意味着
> 构建失败时 Attempt 已是 STARTED，不能直接 return。我把稳定、脱敏的 Material 错误归一化成
> Provider failed result，再走原有 finalize 短事务：缺 key 等暂态进入 RETRY_SCHEDULED，GCM 认证
> 失败和固定模板错误进入 PERMANENTLY_FAILED，Provider 不会被调用。
>
> MIME 是 UTF-8 multipart/alternative，正文 quoted-printable，Header 禁 CR/LF，HTML 用自动转义
> 模板。明文只在同步构建和 Provider 调用窗口存在，数据库、RabbitMQ 和 Fake Provider 都不复制。
> 我没有夸大内存擦除：Go GC 下只能减少副本和缩短生命周期，无法保证 string 被物理清零。

### 可能追问

**为什么解密不放在第一段事务里？**

事务只保护必须原子提交的状态。解密、模板和未来网络/缓存访问不需要数据库原子性，放进去只会
延长行锁和连接占用。第一段先留下 SENDING + STARTED Attempt，任何后续失败都有可审计证据。

**为什么密钥不存在可重试，认证失败不可重试？**

密钥不存在可能是滚动发布时某个实例 keyring 未刷新，换实例或刷新配置后可以恢复；认证失败表示
同一密文、AAD 和密钥组合不可信，重复相同输入不会改变结果，应隔离和告警。

**既然 AES-GCM 已认证，为什么还要交叉校验明文字段？**

GCM 认证的是密文和 AAD，不会自动约束同一数据库行旁边的 `template_version`、category 等查询列。
交叉校验能发现部分列被错误迁移或篡改，避免查询身份与实际投递内容不一致。

**为什么 Fake Provider 不保存完整请求？**

测试替身也属于数据扩散面。保存完整请求会让测试内存、失败快照或 debug 输出成为第二份验证码
仓库，所以只记录无敏感元数据和 MIME 字节数；需要断言正文时由调用栈内的 handler 同步检查。

## 10. 尚未解决

- QQ SMTP implicit TLS 连接与授权码认证；
- SMTP command/response 阶段的 4xx、5xx 和 DATA 后断线分类；
- 连接复用、并发舱壁、Provider 限速与熔断；
- 多 key version 的生产 keyring/KMS 获取和轮换；
- 数据库模板发布工作流、多 Sender Identity 与 Provider Router；
- 终态后的 encrypted payload 清理策略。

下一步 09-A2 会先实现可测试的 SMTP Transport/Provider，再通过显式 opt-in 的真实测试连接 QQ
SMTP；不会默认读取本地凭据或自动发信。
