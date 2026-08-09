# 08-A2：gRPC Submission/Query、租户身份边界与端到端链路

## 1. 这一阶段解决什么问题

08-A1 已有可靠受理内核，但没有外部入口。08-A2 注册通用 Proto 中的 `DeliveryService`，实现
第一组可用 RPC：

```text
SubmitEmail
  → 固定开发租户身份
  → Proto Adapter
  → EmailSubmissionService
  → PostgreSQL Message + encrypted Submission + Outbox
  → Relay → RabbitMQ → Worker → Fake Provider

GetEmail
  → 固定开发租户身份
  → EmailQueryService
  → tenant-scoped safe status view
```

`BatchSubmitEmail`、`CancelEmail`、`ListEmailEvents` 继续由生成代码返回 `UNIMPLEMENTED`，避免
提供只有接口外形、没有完整语义的半成品。

## 2. 为什么 Handler 必须很薄

gRPC Server 只负责四类事情：

1. 从认证 Context 取得 tenant；
2. 把 Proto 转成应用 Command/Query；
3. 调用 Application Service；
4. 把结果或内部错误映射成稳定的公开协议。

邮箱规范化、模板授权、幂等、加密、数据库时间和事务不放在 Handler 中。这样未来新增 HTTP
Gateway、AI-Nexus 兼容 Adapter 或内部批量入口时，都调用同一个 Application Service，不会
形成第二套受理规则。

## 3. 租户为何不在请求体中

如果客户端能提交 `tenant_id`，只要修改一个字段就可能访问其他租户模板、幂等键和状态。
因此通用 Proto 没有 tenant 字段，Application Command 的 tenant 只能来自认证 Context。

生产目标是：

```text
mTLS client certificate / service identity
  → authenticated principal
  → registered tenant mapping
  → context tenant identity
```

当前 gRPC 仍是 plaintext，不能假装已经有生产认证。08-A2 使用
`FixedDevelopmentTenantInterceptor`：进程启动时从 `MAIL_DEV_TENANT_ID` 固定一个 tenant，
客户端无法在请求中覆盖。`MAIL_GRPC_ALLOW_INSECURE=true` 必须显式配置，变量名明确标注这是
临时开发模式。Health RPC 不要求业务 tenant。

这不是多租户生产认证，只是一个安全边界清晰、后续可替换的开发 Adapter。mTLS 完成后替换
Interceptor，Proto、Handler 和 Application Service 都不需要改变。

## 4. 类型化密钥配置

新增必需环境变量：

```text
MAIL_GRPC_ALLOW_INSECURE=true
MAIL_DEV_TENANT_ID=<uuid>
MAIL_PAYLOAD_KEY_ID=<non-secret-version-id>
MAIL_PAYLOAD_ENCRYPTION_KEY_BASE64=<32-byte-key>
MAIL_PAYLOAD_FINGERPRINT_KEY_BASE64=<different-32-byte-key>
```

配置层使用 strict standard base64 解码，要求恰好 32 bytes，并拒绝加密与 HMAC 使用同一把
key。错误只包含变量名和格式要求，不回显输入值。

`.env.example` 的 key 仅用于本地开发，不能用于生产。生产需要 KMS/Vault、密钥版本和轮换
流程。`payload_key_id` 可以公开，但实际 key 只能存在进程秘密配置或密钥服务中。

## 5. 第一版模板与发件身份目录

Bootstrap 装配了可替换的 `SubmissionCatalog`，当前注册：

```text
tenant       MAIL_DEV_TENANT_ID
sender       ainexus.default
template     verification_code.v1@1
locale       zh-CN
variables    code + purpose + valid_for_seconds
```

验证码必须是六位数字；purpose 只允许 `REGISTER/RESET_PASSWORD/LOGIN`；有效期为 60～1800
秒；未知 JSON 字段会被拒绝。模板省略版本时固定活动版本 1，显式请求其他版本返回不存在。

目录实现应用端口，而不是写死在 gRPC Handler。后续可以替换为 PostgreSQL 发布模板目录与
缓存，而受理用例不变。模板授权和 sender identity 授权都使用已经认证的 tenant。

## 6. SubmitEmail 的转换细节

Adapter 先检查必需嵌套对象和 Protobuf Timestamp，再转换枚举和变量。一个典型陷阱是：

```text
Proto priority: uint32
Application priority: uint8
```

如果先转换再校验，`265` 会截断成 `9`，非法输入反而通过。因此 Adapter 必须在 narrowing
conversion 前检查 `priority <= 9`。

`google.protobuf.Struct` 转成 JSON 后仍由 Template Catalog 和 Application Service 做对象
深度、大小和 Schema 校验。Handler 不打印 request，也不把 variables 放入错误详情。

同步 `ACCEPTED` 只表示 PostgreSQL 事务已经提交，不表示 RabbitMQ、Provider 或收件箱完成。
同 key 同 Payload 返回 `DUPLICATE` 和原 message ID。

## 7. GetEmail 与租户隔离

查询必须且只能选择：

```text
message_id XOR idempotency_key
```

按幂等键查询天然带 tenant 条件。按 message ID 查询后还会校验记录 tenant；如果 UUID 属于其他
租户，服务返回 `NOT_FOUND`，而不是 `PERMISSION_DENIED`。这样调用方无法根据错误差异确认
另一个租户是否存在该任务。

响应只包含：

- message/idempotency ID；
- 脱敏邮箱；
- sender/template/locale；
- 类别、优先级和状态；
- 时间、sequence 和脱敏 failure。

它不解密或返回原始邮箱、模板变量、正文、Provider 原始响应和密钥信息。

## 8. gRPC 错误映射

| 内部错误 | gRPC Code | 客户端语义 |
| --- | --- | --- |
| 输入、变量 Schema、selector 错误 | `INVALID_ARGUMENT` | 修正请求，不重试 |
| sender/template 未授权 | `PERMISSION_DENIED` | 配置或权限错误 |
| 模板/Message 不存在 | `NOT_FOUND` | 停止或对账 |
| 同 key 不同 Payload | `ALREADY_EXISTS` | 不得换内容重试 |
| PostgreSQL/事务暂时失败 | `UNAVAILABLE` | 同 key 有界重试 |
| Context 超时 | `DEADLINE_EXCEEDED` | 结果未知，查询或同 key 重试 |
| 不变量和未知错误 | `INTERNAL` | 告警，不暴露内部细节 |

公开错误使用稳定、无敏感信息的文本，并切断内部 error chain。SQL、密文、邮箱、验证码和模板
变量不会通过 `status.Error` 返回。

## 9. Composition Root 如何变化

`bootstrap.NewApp` 新增装配：

```text
Security Config
  → AES-GCM/HMAC PayloadProtector
  → Verification SubmissionCatalog
  → EmailSubmissionService
  → EmailQueryService
  → DeliveryServer
  → grpc.Server registration + tenant interceptor
```

gRPC Endpoint 使用注册函数扩展服务，同时继续提供标准 Health。进程监督和 graceful shutdown
顺序不变：停止 API 时 `GracefulStop` 等待在途 RPC，超时才强制停止。

## 10. 本地运行

```bash
cp .env.example .env
make infra-up
make migrate-up
make db-dev-seed

set -a
. ./.env
set +a
make run
```

`db-dev-seed` 只插入 `.env.example` 对应的固定开发 tenant，使用 `ON CONFLICT DO NOTHING`。
生产 tenant 必须由控制面创建，不能运行开发 seed。

## 11. 如何验证

普通验证：

```bash
go test ./...
go test -race ./...
go vet ./...
make migrate-validate
```

真实纵向测试：

```bash
TEST_POSTGRES_IMAGE=postgres:18.4-alpine \
TEST_RABBITMQ_IMAGE=rabbitmq:4.3.4-management-alpine \
go test -tags=integration ./internal/integration \
  -run '^TestRuntimeComposition$' -count=1 -v -timeout=4m
```

现在 Runtime 测试不再直接写 `mail_messages`，而是：

1. 启动 PostgreSQL 18.4 与 RabbitMQ 4.3.4；
2. 验证未迁移时 App 拒绝启动；
3. Migration、RabbitMQ Policy 和开发 tenant 就绪；
4. 启动完整 App 并等待 readiness；
5. 通过真实 gRPC `SubmitEmail` 提交验证码邮件；
6. Message + encrypted Payload + Outbox 原子落库；
7. Relay、RabbitMQ Consumer、Worker、Fake Provider 完成投递；
8. 通过真实 gRPC `GetEmail` 查询到 `PROVIDER_ACCEPTED` 与脱敏邮箱；
9. 检查恰好一个 Attempt、没有 Pending Outbox；
10. 验证 graceful shutdown。

## 12. 面试表达

### 30 秒版本

> 我给可靠受理内核接入了 gRPC Submit/Get，但 Handler 只做身份、Proto 映射和公开错误转换。
> tenant 不允许从请求体传，当前 plaintext 阶段用显式固定开发租户 Interceptor，未来直接替换
> mTLS 身份解析。模板和发件身份通过 Application Port 授权，查询只返回脱敏状态视图。最终
> Testcontainers 从真实 gRPC Submit 跑到 RabbitMQ、Worker、Fake Provider，再由 gRPC Get
> 验证最终状态。

### 2 分钟版本

> 我把 gRPC 当成 Adapter，而不是业务层。Interceptor 把认证租户放入 Context，Handler 将
> Proto 转成 Application Command，上一阶段继续负责模板、幂等、加密和数据库事务。当前没有
> mTLS，所以配置必须显式确认 insecure，并固定一个开发 tenant；客户端无法传 tenant ID。
>
> 第一版 Submission Catalog 同时校验 tenant 对模板和 sender identity 的授权，把验证码模板
> 固定到不可变版本并严格校验变量。Get 支持 message ID 或幂等键二选一，跨租户 message ID
> 返回 NotFound，响应不解密敏感 Payload。
>
> Adapter 还集中做标准 gRPC Code 映射并切断内部错误链。实现时专门防了 uint32 priority 转
> uint8 前的截断绕过。最终我把原先直接写数据库的 Runtime 测试升级成真实 gRPC Submit →
> PostgreSQL Outbox → RabbitMQ → Worker → Fake Provider → gRPC Get 的完整纵向测试。

## 13. 尚未解决

- 生产 TLS/mTLS、证书身份到 tenant 的注册映射；
- 数据库模板/发件身份控制面、发布工作流与缓存；
- BatchSubmit、Cancel、ListEvents；
- 租户配额、API 限流、最大在途任务容量保护；
- 真实 SMTP 前的 Payload 解密、模板渲染与 MIME；
- Metrics、Trace、审计日志和公开错误的 RetryInfo details；
- KMS/Vault、密钥轮换与敏感数据清理。

下一阶段进入 08-B：可靠状态通知与 AI-Nexus 回调适配，让 AI-Nexus 在
`PROVIDER_ACCEPTED` 后幂等激活验证码。
