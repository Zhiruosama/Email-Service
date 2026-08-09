# 09-A2B：QQ SMTP 真实验证与 503 排障

- 状态：已完成
- 阶段目标：在人工授权、无业务数据的前提下，证明第一版 SMTP Provider 能被真实 QQ SMTP 接受，并记录一次可复现的配置排障过程。

## 1. 为什么真实测试仍然必要

09-A2A 已经通过进程内 TLS SMTP Server 验证命令顺序和错误语义，但测试替身无法完全代表真实
Provider：

- QQ 实际支持的 TLS、AUTH capability 和策略可能不同；
- 授权码是否有效只能由 QQ 认证服务确认；
- QQ 对 Envelope From、频率和账户状态有自己的限制；
- 本地证书、DNS 和出口网络只有真实连接才能覆盖。

因此需要一封受控 smoke test，但它不是普通自动化测试，更不能在 CI 中默认运行。

## 2. 双重显式授权

真实测试同时要求：

```text
编译条件：-tags=real_smtp
运行条件：MAIL_SMTP_REAL_TEST_ENABLED=true
```

`make test-smtp-real` 会自动加入 build tag，但环境开关不是精确 `true` 时会直接退出。即使有人误把
真实凭据放进开发环境，普通 `go test ./...`、`make test` 和 Testcontainers 集成测试也不会发信。

测试邮件具备以下边界：

- 只发送一封；
- 收件人来自本地 `MAIL_SMTP_TEST_RECIPIENT`；
- Subject/正文固定为连通性说明；
- 不包含验证码、Token 或业务 Metadata；
- 不输出 username、recipient 或 auth code；
- `.env` 权限必须是 `0600`。

## 3. 第一次测试：为什么失败是好证据

第一次真实测试的结果是：

```text
TLS             成功
AUTH LOGIN      成功
MAIL FROM       表面成功
RCPT TO         503
DATA            未进入
邮件发送        未发生
```

原始实现把所有 RCPT 5xx 都归类为 `RECIPIENT_REJECTED`，得到
`SMTP_RECIPIENT_503`。这个结论不准确，因为 RFC 5321 定义 `503` 为 Bad sequence of commands；
它表示服务器认为 SMTP Transaction 状态不正确，而不是标准的 mailbox-not-found。

预检同时发现：

```text
MAIL_SMTP_FROM_ADDRESS != MAIL_SMTP_USERNAME
```

实际原因是本地 `MAIL_SMTP_FROM_ADDRESS` 写错。QQ 使用认证账户发送时要求 Envelope From 与账户
身份一致；错误 From 使后续 RCPT 无法在有效发件事务上继续。

这次失败发生在 DATA 之前，因此可以严格证明：邮件正文和结束标记没有提交给 QQ，不存在“其实
已接受但响应丢失”的不确定窗口，也不存在再次测试造成重复邮件的风险。

## 4. 从故障反推的代码改进

没有只修改 `.env` 然后重试，而是把真实故障反馈到通用实现：

1. `503` 现在稳定映射为：

   ```text
   Category  = VALIDATION
   Code      = SMTP_PROTOCOL_503
   Retryable = false
   ```

2. `smtp.qq.com` 配置增加 fail-fast 规则：

   ```text
   MAIL_SMTP_FROM_ADDRESS == MAIL_SMTP_USERNAME
   ```

3. `.env` 中含空格的 display name 必须加引号，避免 Shell 把第二个单词当命令；
4. 新增单元测试，确保 RCPT 503 不再误报为收件人不存在；
5. 配置错误仍只输出字段语义，不输出真实地址或凭据。

通用 SMTP Config 仍允许其他 host 使用已验证 alias；只有已确认该身份约束的 QQ endpoint 才启用
相等校验，避免把某个 Provider 的策略错误提升成 SMTP 通用协议规则。

## 5. 第二次测试结果

修正 From Address 后，以同一双重授权入口再次执行一封固定 smoke test，结果通过：

```text
DNS/TCP             成功
TLS handshake       成功
证书链/主机名       成功
EHLO capability     成功
AUTH LOGIN          成功
MAIL FROM           成功
RCPT TO             成功
DATA + MIME         成功
最终 SMTP response  成功
ProviderResult      ACCEPTED
```

这证明 QQ SMTP Server 已对该 Message 承担投递责任。系统生成的 `smtp/<attempt_id>` 是本地稳定
Attempt correlation，不是假装成 QQ 返回的 Provider Message ID；标准库没有从最终 response 中
提取可安全使用的远端 ID。

测试执行后，收件方又人工确认：邮件出现在普通收件箱而非垃圾邮件目录，配置的发件显示名称、
UTF-8 中文 Subject 和固定正文均正确显示。文档只记录验证结论，不保存截图中的真实发件或收件
地址，避免把测试证据变成新的个人信息扩散面。

## 6. `PROVIDER_ACCEPTED` 不等于 Inbox

SMTP 最终成功响应只代表当前 SMTP Server 接受消息，后面仍可能发生：

- 垃圾邮件过滤；
- 收件域二次策略；
- 延迟投递；
- Bounce；
- 用户规则移动目录。

本次人工检查能够证明这一封测试邮件可见，但不能成为每封生产邮件的机器证据。因此状态机仍只
推进到 `PROVIDER_ACCEPTED`，不伪造 `DELIVERED`。最终送达需要 Provider Webhook、
DSN/Bounce Ingress 或业务侧确认；QQ 个人 SMTP 本身不提供与 API Provider 等价的逐封状态查询。

## 7. 验证证据

本阶段实际执行：

```bash
go test -race ./internal/provider/smtp ./internal/bootstrap
go vet ./internal/provider/smtp ./internal/bootstrap
MAIL_SMTP_REAL_TEST_ENABLED=true \
  go test -tags=real_smtp ./internal/provider/smtp \
  -run '^TestRealQQSMTP$' -count=1
```

第一次真实测试得到脱敏的 `SMTP_RECIPIENT_503`，未进入 DATA；修复配置和分类后，第二次真实测试
通过，耗时小于一秒。测试输出没有包含邮箱地址或授权码；收件方随后人工确认邮件位于普通
收件箱，中文 Header 和正文渲染正常。

## 8. 面试表达

### 30 秒版本

> SMTP 核心通过本地 TLS Server 后，我又设计了 build tag 和环境开关双重保护的 QQ smoke test。
> 第一次真实测试在 RCPT 阶段返回 503，定位到 From Address 配错；因为还没进入 DATA，可以安全
> 重试。我同时把 503 从“收件人拒绝”修正为协议状态错误，并为 QQ 增加 From 与认证账户一致的
> fail-fast 校验。修复后真实 TLS、AUTH、Envelope、MIME DATA 和最终接受响应全部通过。

### 可能追问

**为什么第一次失败可以安全重试？**

RCPT 发生在 DATA 前，邮件正文和终止符还没有发送。只有 DATA 完成后最终响应丢失才存在“对方
可能已接受”的物理歧义。

**为什么不把 503 当作永久收件人错误？**

RFC 5321 将 503 定义为 Bad sequence of commands。收件人不存在通常是 550/551/553；把协议状态
错当成收件人错误，会误导运营和调用方，也掩盖 Provider 配置故障。

**为什么测试成功还不标记 DELIVERED？**

最终 SMTP 成功响应只证明上游 Server 接受责任，无法证明最终收件箱位置。状态名必须表达已有
证据，不能把“已提交给 Provider”夸大成“用户已看到”。

## 9. 下一步

进入 09-B Provider 可靠性保护。其中并发舱壁和本地 Token Bucket 已在 09-B1 完成，详见
[Provider 舱壁与限速](09b1-provider-bulkhead-rate-limit.md)。后续继续：

- 熔断器 CLOSED/OPEN/HALF_OPEN；
- 区分计入熔断的基础设施故障与不计入的收件人错误；
- 为限流、熔断拒绝和恢复增加 Metrics 与测试；
- 再评估有界 SMTP 连接池，而不是直接无界复用 Session。
