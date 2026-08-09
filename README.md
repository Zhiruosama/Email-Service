# Mail Service

Mail Service 是一个面向多个业务系统的可靠邮件投递服务。它负责邮件任务的可靠受理、异步调度、
模板渲染、供应商投递、失败恢复和状态通知；接入方继续拥有自己的业务状态。

例如，AI-Nexus 负责验证码的生成、验证和消费，Mail Service 只负责把验证码邮件可靠地发送出去。
AI-Nexus 是第一个接入方，但不是本服务的架构边界。

## 工作流程

```text
业务服务
  │ gRPC SubmitEmail
  ▼
PostgreSQL: Message + Transactional Outbox
  │ Scheduler / Outbox Relay
  ▼
RabbitMQ Quorum Queue
  │ Dispatch Worker
  ▼
Template + MIME ──> Fake Provider / SMTP Provider
  │
  └── Lifecycle Event ──gRPC Callback──> 业务服务
```

邮件提交和 Outbox 事件在同一个数据库事务中保存，保证任务不会出现“数据库写成功但消息没有发布”的
中间状态。Worker、Provider 或业务回调暂时失败时，任务会通过持久化状态、延迟重试和死信机制恢复。

## 当前能力

### 接口

| 方向 | RPC | 状态 |
| --- | --- | --- |
| Client → Mail Service | `DeliveryService.SubmitEmail` | 已实现 |
| Client → Mail Service | `DeliveryService.GetEmail` | 已实现 |
| Mail Service → Client | `DeliveryEventReceiverService.ReportDeliveryEvent` | 已实现 |
| Client → Mail Service | `BatchSubmitEmail/CancelEmail/ListEmailEvents` | 仅声明，暂未实现 |

### 可靠性与投递

- PostgreSQL Message、Submission、Delivery Attempt、Event Journal 与 Transactional Outbox；
- 数据库 Scheduler，以及带 Lease/Fencing 的 Outbox Relay；
- RabbitMQ Publisher Confirm、Quorum Queue、Manual ACK、延迟重试和 DLQ；
- 幂等受理、乐观锁、稳定事件 ID，以及至少一次状态回调；
- 固定版本模板、AES-GCM Payload 加密、HMAC 幂等指纹和 UTF-8 MIME；
- 可控 Fake Provider 与真实 SMTP Provider；
- SMTP implicit TLS、LOGIN/PLAIN、阶段化超时和错误归一化；
- Provider 并发舱壁、Token Bucket、熔断器和 OpenTelemetry Metrics 埋点；
- 标准 gRPC liveness/readiness 与优雅停机。

当前 QQ SMTP 已完成真实连通性和收件箱验证。SMTP Server 接受邮件表示投递已被供应商受理，
并不等同于服务可以自动证明邮件最终进入收件箱；最终送达、退信和投诉需要供应商 Webhook 支持。

## 技术栈

- Go 1.25
- gRPC + Protobuf
- PostgreSQL 18
- RabbitMQ 4.3.4 Quorum Queues
- OpenTelemetry
- SMTP Provider / Fake Provider

## 快速启动

### 1. 准备环境

本地需要 Go、Docker、Docker Compose 和 `protoc >= 3.21`。

```bash
cp .env.example .env
```

`.env.example` 默认使用 `MAIL_PROVIDER=fake`，不会连接真实 SMTP。按需修改端口和本地密钥后，
把配置导出到当前终端：

```bash
set -a
. ./.env
set +a
```

### 2. 启动依赖并初始化

```bash
make infra-up
make migrate-up
make db-dev-seed
make migrate-status
```

`infra-up` 会启动 PostgreSQL 和 RabbitMQ，并应用 dispatch/lifecycle queue 的 retry、delivery limit
和 dead-letter Policy。应用启动时只检查 Migration，不会由多个应用副本自动修改 Schema。

### 3. 启动服务

```bash
make run
```

默认 gRPC 地址是 `:8080`。本地开发租户由服务端根据 `MAIL_DEV_TENANT_ID` 注入，不能由请求体覆盖。

停止依赖但保留数据卷：

```bash
make infra-down
```

## Provider 模式

### Fake Provider

```dotenv
MAIL_PROVIDER=fake
```

Fake Provider 会执行完整的受理、调度、解密、模板渲染、MIME 构建和状态流转，但不会连接外部服务器，
适合日常开发和自动化测试。

### 真实 SMTP

在本地 `.env` 中设置：

```dotenv
MAIL_PROVIDER=smtp
MAIL_SMTP_HOST=smtp.qq.com
MAIL_SMTP_PORT=465
MAIL_SMTP_SECURITY=implicit_tls
MAIL_SMTP_AUTH_METHOD=login
MAIL_SMTP_USERNAME=your-address@qq.com
MAIL_SMTP_AUTH_CODE=your-authorization-code
MAIL_SMTP_FROM_ADDRESS=your-address@qq.com
MAIL_SMTP_FROM_NAME="AI Nexus"
```

修改 Provider 后必须重启 Mail Service。SMTP Adapter 按任务建立连接，单纯启动服务不会发送邮件。

显式发送一封真实连通性测试邮件：

```bash
export MAIL_SMTP_TEST_RECIPIENT=recipient@example.com
MAIL_SMTP_REAL_TEST_ENABLED=true make test-smtp-real
```

真实测试同时受到 build tag 和环境开关保护，普通 `make test`、`make test-integration` 和
`make run` 不会触发测试邮件。SMTP 授权码只能放在被 Git 忽略的本地 `.env` 或 Secret Store 中。

## AI-Nexus 接入

AI-Nexus V0.1 只使用 `SubmitEmail`、`GetEmail` 和 `ReportDeliveryEvent`。请求映射、幂等语义、
回调状态机、本地端口和验收用例见：

- [AI-Nexus V0.1 对齐与对接手册](docs/integrations/ai-nexus-v01-handoff.md)

唯一可信的接口定义位于 `api/proto/mailservice/delivery/v1/`，`gen/go/` 是生成代码，不得手工修改。

## 开发与验证

```bash
make test                 # 普通单元测试，不依赖 Docker
make test-integration     # Testcontainers 集成测试
make migrate-validate     # 校验 Migration
make check                # Proto 编译、代码生成、格式化和单元测试
make check-all            # 在 check 基础上执行 Buf format/lint
```

常用基础设施命令：

```bash
make infra-status
make db-up
make db-down
make mq-up
make mq-down
make mq-policy-status
```

执行 `make help` 可以查看全部命令。

## 文档导航

### 接入与协议

- [AI-Nexus V0.1 对接手册](docs/integrations/ai-nexus-v01-handoff.md)
- [gRPC V1 协议](docs/protocol/README.md)
- [AI-Nexus 字段适配](docs/protocol/ai-nexus-adapter.md)
- [错误模型](docs/protocol/error-model.md)

### 架构与设计

- [产品边界与设计原则](docs/product-scope.md)
- [系统架构](docs/architecture.md)
- [可靠性、调度与故障语义](docs/reliability.md)
- [领域模型与数据模型](docs/data-model.md)
- [安全、多租户与可观测性](docs/security-and-observability.md)
- [架构决策记录](docs/adr/README.md)
- [实施路线](docs/roadmap.md)

### 学习与面试

- [秋招讲解档案](docs/interview/README.md)
- [术语表](docs/interview/glossary.md)
- [设计依据](docs/references.md)

## 部署状态与后续范围

当前 `compose.yaml` 已容器化 PostgreSQL 和 RabbitMQ，Mail Service 应用镜像尚未加入。后续会补充
多阶段 Dockerfile、非 root 运行、Migration Job、gRPC Health Check 和 Compose 服务网络。

以下能力也不阻塞当前 AI-Nexus V0.1 接入：

- Batch、Cancel 和事件分页查询；
- 多供应商路由与供应商 Webhook；
- OTel SDK、Prometheus/OTLP Exporter 和监控面板；
- 生产 mTLS、KMS、终态敏感 Payload 清理和租户控制面。

本地允许显式使用明文 gRPC 和固定开发租户；这些配置不得直接用于生产环境。
