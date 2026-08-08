# 设计依据

本文档记录架构决策所依据的上游官方资料。实现阶段仍需固定具体软件版本并重新验证
相应行为。

## RabbitMQ

- [Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)：Quorum Queue 的复制、
  Publisher Confirm、Manual Ack、Delivery Limit 和 Dead Letter 语义。
- [Reliability Guide](https://www.rabbitmq.com/docs/reliability)：网络故障下重发可能
  产生重复，因此 Consumer 必须幂等。
- [Publishers](https://www.rabbitmq.com/docs/publishers)：Publisher Confirm 和
  `mandatory` 不可路由处理，以及共享 Channel 并发发布的风险。
- [Publisher Confirms Go Tutorial](https://www.rabbitmq.com/tutorials/tutorial-seven-go)：
  Go 客户端的 Confirm correlation 和 Deferred Confirmation。
- [amqp091-go API](https://pkg.go.dev/github.com/rabbitmq/amqp091-go)：官方维护的 AMQP
  0.9.1 Go 客户端、Confirm、Return 和 Channel API；当前实现固定为 `v1.13.0`。
- [Queues](https://www.rabbitmq.com/docs/queues)：Queue 声明属性一致性与优先使用 Policy
  管理可变参数。
- [Release Information](https://www.rabbitmq.com/release-information)：RabbitMQ 版本支持
  窗口；当前本地与测试基线固定为 `4.3.4`。
- [Time-To-Live and Expiration](https://www.rabbitmq.com/docs/ttl)：过期消息的队首
  处理语义，是不把 RabbitMQ 作为长期调度权威来源的原因之一。

## gRPC

- [Authentication](https://grpc.io/docs/guides/auth/)：TLS、mTLS 和调用凭据。
- [Health Checking](https://grpc.io/docs/guides/health-checking/)：标准
  `grpc.health.v1` 服务。
- [Retry](https://grpc.io/docs/guides/retry/)：透明重试和配置化重试语义。

## PostgreSQL

- [SELECT](https://www.postgresql.org/docs/current/sql-select.html)：
  `FOR UPDATE SKIP LOCKED` 用于多实例批量领取 Scheduler 和 Outbox 工作项。

## 可观测性

- [OpenTelemetry Go](https://opentelemetry.io/docs/languages/go/)：Go 的 Trace 和
  Metrics 稳定性及 OTLP 集成。

## Go

- [Go Release History](https://go.dev/doc/devel/release)：实现开始时选择仍在官方支持
  窗口内的 Go 版本。
