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
下一阶段为 Notification Worker 与 AI-Nexus 回调联调。

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

当前必须显式配置 `MAIL_PROVIDER=fake` 和 `MAIL_GRPC_ALLOW_INSECURE=true`；它们只用于本地
投递编排，不会真实发送邮件，也不构成生产认证。`MAIL_DEV_TENANT_ID` 由进程固定注入，不能
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

`make mq-up` 和 `make infra-up` 会自动应用 dispatch queue 的 delayed retry、delivery limit
和 at-least-once dead-letter Policy；可用 `make mq-policy-status` 查看实际策略。

Migration 文件校验和真实 PostgreSQL + RabbitMQ 集成测试：

```bash
make migrate-validate
make test-integration
```

普通 `go test ./...` 不依赖 Docker；只有 integration build tag 会启动一次性 PostgreSQL
或 RabbitMQ 容器。RabbitMQ 集成测试会验证 Publisher Confirm、消息持久化、Consumer
Manual ACK、延迟重试、DLQ，以及 Broker 应用重启后的客户端重连。
