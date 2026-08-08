# Mail Service

Mail Service 是一个面向多个业务系统的通用、可靠邮件投递服务。

它负责可靠受理、定时调度、异步投递、模板管理、供应商路由、失败重试、熔断、
死信、状态通知和可观测性。业务系统仍然拥有自己的业务状态，例如验证码生成、
验证和消费。

AI-Nexus 是第一个接入方，但不是 Mail Service 的架构边界。

## 当前阶段

项目已完成架构基线、通用 gRPC V1 契约、领域状态机、PostgreSQL 18 Migration、
Repository 与 version 乐观锁、Message + Transactional Outbox 原子持久化，以及数据库
Scheduler。下一阶段为 Outbox Relay 的 lease 领取与发布边界。

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
- RabbitMQ 4.x Quorum Queues
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

## 本地 PostgreSQL

本地开发数据库使用固定的 PostgreSQL 18.4 镜像：

```bash
cp .env.example .env
make db-up
make migrate-up
make migrate-status
```

停止容器但保留数据卷：

```bash
make db-down
```

Migration 文件校验和真实数据库集成测试：

```bash
make migrate-validate
make test-integration
```

普通 `go test ./...` 不依赖 Docker；只有 integration build tag 会启动一次性 PostgreSQL
容器。
