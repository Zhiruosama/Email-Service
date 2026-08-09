# Mail Service

Mail Service 是一个面向多个业务系统的通用、可靠邮件投递服务。

它负责可靠受理、定时调度、异步投递、模板管理、供应商路由、失败重试、熔断、
死信、状态通知和可观测性。业务系统仍然拥有自己的业务状态，例如验证码生成、
验证和消费。

AI-Nexus 是第一个接入方，但不是 Mail Service 的架构边界。

## 当前阶段

项目已完成架构基线、通用 gRPC V1 契约、领域状态机、PostgreSQL 18 Migration、
Repository 与 version 乐观锁、Message + Transactional Outbox 原子持久化、数据库
Scheduler、Outbox Relay 的 Lease/Fencing，以及带 mandatory、Publisher Confirm、Quorum
Queue 和按需重连的 RabbitMQ Publisher Adapter；Dispatch Worker、Delivery Attempt、
Fake Provider，以及带 Manual ACK、prefetch、延迟重试、DLQ、重连和优雅停机的 RabbitMQ
Consumer Adapter 也已完成。Composition Root 已将这些组件装配为可运行后台进程，并提供
动态 gRPC liveness/readiness。可靠受理应用内核也已完成：模板版本固定、规范化 Payload
HMAC 幂等指纹、AES-GCM 加密，以及 Message + Submission + Outbox 同事务持久化。
`SubmitEmail/GetEmail` gRPC、固定开发租户身份边界、验证码模板目录和标准错误映射也已接入，
真实纵向测试从 gRPC Submit 跑到 Fake Provider 再由 gRPC Get 查询。Delivery Event Journal
也已完成，Message、不可变状态历史与 lifecycle Outbox 在同一事务提交，并共享稳定 event ID。
Notification Worker 核心和 gRPC Callback Client 现已完成：它按 event ID 回查权威 Journal，
限制回调超时，并把成功、可重试和永久失败转换成稳定语义。Lifecycle RabbitMQ Consumer、
通知延迟重试、独立 DLQ、双 Consumer readiness 与运行时装配也已完成，真实纵向测试能够从
SubmitEmail 一直运行到四条状态 gRPC Callback。投递前的 Delivery Material 链路现也已完成：
Worker 在数据库事务外认证解密 Payload、校验不可变身份、渲染固定模板并生成 UTF-8
multipart/alternative MIME，明文不进入数据库、Outbox、Fake Provider 观测记录或错误码。
SMTP Provider 核心也已完成：支持 implicit TLS、LOGIN/PLAIN 授权、阶段化超时和 4xx/5xx
错误归一化，并把 DATA 最终响应丢失建模为 `SUBMISSION_UNKNOWN`。Composition Root 可显式选择
`fake` 或 `smtp`，普通测试永远不会连接真实服务器。QQ SMTP smoke test 现已通过：真实
implicit TLS、AUTH LOGIN、Envelope、MIME DATA 和最终接受响应均已验证；这只证明 SMTP Server
接受，不等同于系统可自动证明最终进入收件箱。本次测试邮件已由人工确认出现在普通收件箱，
中文发件名称、Subject 和正文显示正常。09-B1 又在 SMTP 外层接入了非阻塞并发舱壁和本地
Token Bucket：过载请求不会堆在进程内，而是返回稳定、可重试的失败并回到持久化调度链路。
09-B2 又完成本地 Provider 熔断器：连续基础设施故障达到阈值后进入 `OPEN`，冷却到期只放行
一个 `HALF_OPEN` 探针，成功恢复、失败重新打开；认证失败会立即熔断。下一阶段进入 Provider
可观测性与恢复运维能力。

## 设计文档

1. [产品边界与设计原则](docs/product-scope.md)
2. [系统架构](docs/architecture.md)
3. [可靠性、调度与故障语义](docs/reliability.md)
4. [领域模型与数据模型](docs/data-model.md)
5. [安全、多租户与可观测性](docs/security-and-observability.md)
6. [分阶段实施路线](docs/roadmap.md)
7. [架构决策记录](docs/adr/README.md)
8. [设计依据](docs/references.md)
9. [gRPC V1 协议](docs/protocol/README.md)
10. [秋招讲解档案](docs/interview/README.md)

## 暂定技术基线

- Go 1.25
- gRPC + Protobuf
- PostgreSQL 18
- RabbitMQ 4.3.4 Quorum Queues
- OpenTelemetry
- SMTP Provider 和可控 Fake Provider

版本只是当前设计基线，实施时通过依赖锁定和兼容性测试固定具体小版本。

## 本地验证

日常离线检查使用 `protoc`，Go 代码生成插件由 `go.mod` 固定：

```bash
make check
```

完整检查会按 Makefile 中固定的 Buf 版本执行 lint：

```bash
make check-all
```

`gen/go/` 是生成代码，不得手工修改。

本地 `protoc` 最低版本为 3.21，且需要能够找到标准
`google/protobuf/*.proto` include 文件。

## 本地基础设施

本地开发使用固定的 PostgreSQL 18.4 与 RabbitMQ 4.3.4 镜像：

```bash
cp .env.example .env
make infra-up
make migrate-up
make db-dev-seed
make migrate-status
```

启动后台投递进程：

```bash
set -a
. ./.env
set +a
make run
```

默认开发模式必须显式配置 `MAIL_PROVIDER=fake`、`MAIL_GRPC_ALLOW_INSECURE=true` 和
`MAIL_CALLBACK_GRPC_ALLOW_INSECURE=true`；它们只用于本地
投递编排。Fake Provider 会完整执行解密、模板渲染与 MIME 构建，但不会建立 SMTP 连接或真实
发送邮件，也不构成生产认证。`MAIL_DEV_TENANT_ID` 由进程固定注入，不能
从请求体覆盖。服务启动时只验证 Migration 是否完整，不会由应用副本自动修改 Schema。

也可以只启动或停止其中一个组件：

```bash
make db-up
make mq-up
make db-down
make mq-down
```

停止全部容器但保留数据卷：

```bash
make infra-down
```

RabbitMQ Management UI 默认为 `http://localhost:15672`，本地账号为
`email_service / email_service_dev`。

`make mq-up` 和 `make infra-up` 会自动应用 dispatch/lifecycle queue 的 delayed retry、
delivery limit 和 at-least-once dead-letter Policy；可用 `make mq-policy-status` 查看实际策略。

Migration 文件校验和真实 PostgreSQL + RabbitMQ 集成测试：

```bash
make migrate-validate
make test-integration
```

普通 `go test ./...` 不依赖 Docker；只有 integration build tag 会启动一次性 PostgreSQL
或 RabbitMQ 容器。RabbitMQ 集成测试会验证 Publisher Confirm、消息持久化、Consumer
Manual ACK、延迟重试、DLQ，以及 Broker 应用重启后的客户端重连。

### 显式 SMTP 模式

只有把 `MAIL_PROVIDER` 改为 `smtp` 时，进程才会要求并使用 `.env.example` 中的
`MAIL_SMTP_*` 配置。`MAIL_PROVIDER_MAX_CONCURRENT`、`MAIL_PROVIDER_RATE_PER_SECOND` 和
`MAIL_PROVIDER_RATE_BURST` 分别控制单实例 SMTP 并发、持续速率和短时突发；它们是本地保护，
不是多实例共享配额。`MAIL_PROVIDER_CIRCUIT_FAILURE_THRESHOLD` 和
`MAIL_PROVIDER_CIRCUIT_OPEN_DURATION` 控制本地熔断阈值与冷却时间。SMTP Adapter 采用按需连接，
应用启动不会连接或发送邮件。真实 smoke test
还受到 build tag 和环境开关双重保护：必须主动导出本地 `.env`，把
`MAIL_SMTP_REAL_TEST_ENABLED` 改为 `true`，再执行 `make test-smtp-real`。普通 `make test`、
`make test-integration` 和 `make run` 不会触发这个测试。授权码不得提交到 Git。
