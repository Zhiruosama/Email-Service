# 09-A2A：SMTP Provider 核心与错误分类

- 状态：已完成
- 阶段目标：在不依赖真实 QQ 凭据的情况下，实现可装配、可超时、可精确测试的 implicit TLS SMTP Provider，并把协议结果转换成领域状态。

## 1. 为什么 SMTP 不是简单调用 `SendMail`

SMTP 是有状态协议：

```text
TCP → TLS → 220 Greeting → EHLO → AUTH
    → MAIL FROM → RCPT TO → DATA/354
    → 邮件内容 + <CRLF>.<CRLF> → 最终 250 → QUIT
```

如果只拿到一个普通 error，就无法回答失败发生在哪一步，也无法决定是否安全重试。例如连接失败
说明对方一定没有收到邮件；完整 DATA 后等待最终 250 时断线，则可能是邮件已入队但响应丢失。
这两者都叫“网络错误”，业务动作却完全不同。

本阶段没有使用标准库的一站式 `smtp.SendMail`，而是封装底层 `smtp.Client`，保留 TLS、AUTH、
MAIL、RCPT、DATA 写入和 DATA 最终确认这些阶段。`net/smtp` 已冻结、不再增加新特性，所以它只
作为 Adapter 内部的 RFC 5321 协议原语；应用 Port、状态机和配置不会依赖它，未来替换库时不会
改动 Worker。

## 2. 分层设计

```text
Dispatch Worker
  → EmailProvider.Submit(ProviderRequest)
      → SMTP Provider：校验请求、归一化结果
          → SMTP Transport：TLS 与 SMTP 会话
              → SMTP Server
```

职责分别是：

- Worker：控制 Attempt、事务边界和消息状态；
- Provider：把协议结果变成 `ProviderResult`；
- Transport：执行网络会话并返回 `ExchangeError{phase, status}`；
- SMTP Server：QQ 或未来其他标准 SMTP 服务。

Transport 是接口，因此 Provider 分类测试不需要网络；同时用进程内 TLS SMTP Server 验证真实
命令顺序，避免所有测试都停留在 mock 层。

## 3. 为什么第一版一封邮件一条连接

09-A2A 每封邮件执行独立 TCP/TLS/SMTP Session。它不是性能终局，但故障边界最清晰：

- 不会复用已经被服务器关闭或进入未知状态的 Session；
- AUTH、MAIL 和 DATA 的错误天然属于当前 Attempt；
- 不需要同时解决连接池健康检查、并发借还、NOOP 和 idle timeout；
- 可以先用真实证据固定 `SUBMISSION_UNKNOWN` 语义。

代价是 TLS handshake 开销更高。09-B 会在指标和并发边界明确后增加有界连接池；不能为了省一次
握手就无界复用连接，否则一个坏 Session 会污染多封邮件。

## 4. TLS 与认证

当前只允许 `implicit_tls`：先建立 TLS，再读取 SMTP Greeting。TLS 最低版本是 1.2，并使用系统
根证书和配置 host 完成证书链、主机名校验，不提供 `InsecureSkipVerify` 开关。

认证支持：

- `LOGIN`：QQ 默认配置；
- `PLAIN`：供其他明确支持的 TLS SMTP 服务使用。

两种认证都只能在已经验证的 TLS 连接上执行。用户名和授权码禁止 CR/LF/NUL，配置错误只输出字段
名称，不输出值。QQ 配置使用完整邮箱作为 username、客户端授权码作为认证秘密，而不是网页登录
密码。腾讯云当前公开配置也列出 `smtp.qq.com`、465/SSL；真实能力仍以人工 smoke test 为准。
[腾讯云 SMTP 配置说明](https://intl.cloud.tencent.com/zh/document/product/1266/71700)

## 5. 阶段化错误模型

Transport 不保留远端 response text，因为服务器可能回显收件地址或内容。它只保留：

```go
type ExchangeError struct {
    Phase      ExchangePhase
    StatusCode int
    TimedOut   bool
    Canceled   bool
    Network    bool
    Protocol   bool
}
```

Provider 再转换成稳定领域 Failure：

| SMTP 结果 | 领域类别 | Retryable | Message 结果 |
| --- | --- | ---: | --- |
| 连接/TLS/发送前网络错误 | `NETWORK` | 是 | `RETRY_SCHEDULED` 或到上限后 DLQ |
| 发送前超时 | `TIMEOUT_BEFORE_SEND` | 是 | 同上 |
| 421/451 等暂时服务故障 | `PROVIDER_UNAVAILABLE` | 是 | 同上 |
| 450/452 暂时容量压力 | `RATE_LIMITED` | 是 | 同上 |
| AUTH 5xx | `AUTHENTICATION` | 否 | `PERMANENTLY_FAILED` |
| MAIL FROM 5xx | `VALIDATION` | 否 | `PERMANENTLY_FAILED` |
| 503 Bad sequence of commands | `VALIDATION` | 否 | `PERMANENTLY_FAILED` |
| RCPT TO 5xx | `RECIPIENT_REJECTED` | 否 | `PERMANENTLY_FAILED` |
| DATA 明确 5xx | `CONTENT_REJECTED` | 否 | `PERMANENTLY_FAILED` |
| DATA 最终响应前断线 | `SUBMISSION_UNKNOWN` | 否 | `SUBMISSION_UNKNOWN` |

稳定 code 形如 `SMTP_AUTH_535`、`SMTP_RECIPIENT_550`。没有保存诸如 “mailbox not found for
user@example.com” 的原始文本，因此 Attempt、Journal、Callback 和日志不会扩散地址。

## 6. DATA 为什么会产生不确定结果

SMTP Server 只有在收到完整 DATA 和终止符后，才用最终 `250` 表示已经接收责任。如果客户端发送
终止符后连接断开，存在两个物理世界：

```text
世界 A：Server 没接收完整 DATA → 邮件未入队
世界 B：Server 已入队并发出 250，但 250 在网络中丢失
```

客户端看到的都是 EOF/timeout，无法证明是哪一个。对 `AVOID_DUPLICATE` 验证码直接重发可能让
用户收到两封，因此 Provider 返回 `SUBMISSION_UNKNOWN`，而不是伪装成普通重试错误。

如果 DATA Close 得到明确 4xx/5xx，结果仍然已知，可以按状态码处理。只有最终响应缺失才进入
不确定状态。QUIT 发生在最终 250 之后，QUIT 失败不会撤销 Server 已经承担的投递责任，所以忽略
QUIT error。

标准库内部存在缓冲，Close 阶段的网络错误无法可靠证明终止符是否完整送达。本实现选择保守地
归入 UNKNOWN，优先避免重复；未来若要对 `PREFER_DELIVERY` 采用不同策略，应由显式 Reconciler/
策略层决定，不能在 Transport 中偷偷重发。

## 7. 配置与 Composition Root

`MAIL_PROVIDER` 现在只接受：

- `fake`：默认本地模式，使用 `.invalid` 发件地址，不建立外网连接；
- `smtp`：使用 `MAIL_SMTP_*` 配置创建 SMTP Provider 和对应 Sender Identity。

SMTP 配置只在选择 `smtp` 时要求存在。构造 Provider 不连接服务器，第一次 Worker Submit 才按需
连接。这使 QQ 暂时不可达时 Submission API 仍可以依靠 PostgreSQL 可靠受理；故障通过重试积压、
状态和后续告警体现，而不是让启动过程失败。

当前配置包括 host、port、security、auth method、username、auth code、from address/name 和
session timeout。Provider 还会验证 MIME Envelope From 必须等于已配置身份，防止数据库内容或
错误装配绕过发件身份授权。

对于 `smtp.qq.com`，配置还要求 `MAIL_SMTP_FROM_ADDRESS` 与 `MAIL_SMTP_USERNAME` 相同。
通用 SMTP 可能允许已验证 alias，但 QQ 的客户端配置要求认证账户与发件地址一致；在本地启动
边界拒绝不一致配置，比等到 RCPT 阶段收到模糊的 503 更容易定位，也避免无意义地反复认证。

## 8. 双重保护的真实测试

真实 QQ 测试不属于普通测试套件。必须同时满足：

1. 使用 `real_smtp` build tag；
2. `MAIL_SMTP_REAL_TEST_ENABLED=true`。

`make test-smtp-real` 固定 build tag，并在开关不是精确 `true` 时直接拒绝。测试只向
`MAIL_SMTP_TEST_RECIPIENT` 发送一封固定、无验证码的连通性邮件。日常 `go test ./...`、CI、
Testcontainers 集成测试和 `make run` 都不会触发它。

## 9. 主要文件与验证

- `internal/provider/smtp/config.go`：SMTP 配置和 fail-closed 校验；
- `internal/provider/smtp/transport.go`：TLS、AUTH 和 SMTP 会话阶段；
- `internal/provider/smtp/provider.go`：领域结果归一化；
- `internal/provider/smtp/transport_test.go`：进程内 TLS SMTP 协议测试；
- `internal/provider/smtp/real_qq_test.go`：双重显式授权 smoke test；
- `internal/bootstrap/config.go`：`MAIL_SMTP_*` 加载；
- `internal/bootstrap/app.go`：Fake/SMTP Provider 与 Sender Identity 选择；
- `.env.example`：不含真实凭据的配置契约；
- `Makefile`：受保护的真实测试入口。

自动测试覆盖：

- implicit TLS 与证书主机名验证；
- AUTH LOGIN 用户名/授权码交互；
- SMTP Envelope 和 DATA dot-stuffing；
- RCPT 550 保留阶段和状态；
- DATA 接收后断线成为 UNKNOWN；
- 4xx/5xx、超时、取消、协议不匹配分类；
- response text 和授权码不进入错误；
- Fake/SMTP Composition Root 选择；
- real SMTP build tag 编译且默认跳过。

验证命令：

```bash
go test ./...
go test -race ./...
go vet ./...
make migrate-validate
go test -tags=real_smtp ./internal/provider/smtp -run 'TestReal' -count=1
```

09-A2A 完成时，最后一条只证明带 tag 的测试能够编译、固定 MIME 测试通过且真实发信默认跳过。
随后 09-A2B 在人工显式开启开关后完成真实 QQ SMTP 接受验证，排障和证据记录见
[QQ SMTP 真实验证](09a2b-qq-smtp-smoke.md)。

## 10. 面试表达与下一步

### 30 秒版本

> 我没有直接调用 SendMail，而是保留 SMTP 的 TLS、AUTH、MAIL、RCPT、DATA 和最终确认阶段，
> 再由 Provider 把状态码转换成统一 Failure。4xx 可重试，AUTH/RCPT/DATA 的明确 5xx 分别变成
> 鉴权、收件人和内容永久失败；完整 DATA 后丢失最终响应则进入 SUBMISSION_UNKNOWN，避免验证码
> 被盲目重复发送。协议通过进程内 TLS SMTP Server 测试，真实 QQ 测试还有 build tag 和环境开关
> 双重保护。

### 可能追问

**为什么 QUIT 失败仍算成功？**

最终 DATA `250` 表示 SMTP Server 已承担投递责任。QUIT 只是优雅关闭 Session，之后的网络故障
不能倒退已经发生的接受事实。

**为什么认证失败不重试当前邮件？**

同一凭据重试不会自行恢复，只会放大请求和封禁风险。当前任务永久失败；09-B 还会按
provider/endpoint/credential 粒度熔断并告警，避免每封邮件都重新撞认证。

**SMTP Provider 为什么不加入 readiness？**

Provider 短暂不可达应由 durable queue 和数据库重试吸收。若让它直接拉低整个实例 readiness，
可能连 PostgreSQL 可靠受理和 Callback 都一起停止。持续故障应看分类指标、积压和熔断状态。

### 尚未解决

- Provider/credential 粒度并发限制、Token Bucket 和熔断器；
- 有界连接池、NOOP 健康检查和 idle connection 回收；
- enhanced status code（如 4.7.x）与 QQ 特有错误的细化映射；
- `SUBMISSION_UNKNOWN` Reconciler，以及 `PREFER_DELIVERY` 的人工策略；
- DKIM、SPF/DMARC 运维验证和生产密钥存储。
