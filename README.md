# Mail Service

Mail Service 是一个面向多个业务系统的通用、可靠邮件投递服务。

它负责可靠受理、定时调度、异步投递、模板管理、供应商路由、失败重试、熔断、
死信、状态通知和可观测性。业务系统仍然拥有自己的业务状态，例如验证码生成、
验证和消费。

AI-Nexus 是第一个接入方，但不是 Mail Service 的架构边界。

## 当前阶段

项目处于架构设计阶段，尚未开始业务实现。

## 设计文档

1. [产品边界与设计原则](docs/product-scope.md)
2. [系统架构](docs/architecture.md)
3. [可靠性、调度与故障语义](docs/reliability.md)
4. [领域模型与数据模型](docs/data-model.md)
5. [安全、多租户与可观测性](docs/security-and-observability.md)
6. [分阶段实施路线](docs/roadmap.md)
7. [架构决策记录](docs/adr/README.md)
8. [设计依据](docs/references.md)

## 暂定技术基线

- Go 1.26
- gRPC + Protobuf
- PostgreSQL 18
- RabbitMQ 4.x Quorum Queues
- OpenTelemetry
- SMTP Provider 和可控 Fake Provider

版本只是当前设计基线，实施时通过依赖锁定和兼容性测试固定具体小版本。
