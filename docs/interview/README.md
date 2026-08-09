# Mail Service 秋招讲解档案

本目录以“我为什么这样设计、实际做了什么、如何验证、面试时如何表达”为主线，记录
Mail Service 从零到可上线的每一个开发阶段。

它不是设计文档的复制品：

- `docs/` 下的其他文档面向系统规范和开发协作；
- 本目录面向复盘、学习和面试表达；
- 只把已经验证的内容标为“已完成”；
- 每完成一个可独立提交的阶段，就新增或更新一篇记录。

## 当前进度

| 序号 | 阶段 | 状态 | 面试记录 |
| ---: | --- | --- | --- |
| 01 | 系统定位与架构基线 | 已完成 | [架构基线](01-architecture-baseline.md) |
| 02 | 通用 gRPC V1 契约与工程骨架 | 已完成 | [gRPC 契约](02-grpc-contract.md) |
| 03 | 领域模型与邮件状态机 | 已完成 | [领域状态机](03-domain-state-machine.md) |
| 04 | PostgreSQL Migration 与 Repository | 已完成 | [PostgreSQL 基础](04-postgresql-foundation.md) / [Repository 与乐观锁](04b-postgresql-repository.md) |
| 05 | Transactional Outbox 原子持久化 | 已完成 | [Transactional Outbox](05-transactional-outbox.md) |
| 06-A | 数据库 Scheduler | 已完成 | [数据库 Scheduler](06a-database-scheduler.md) |
| 06-B | Outbox Relay 与 Lease Fencing | 已完成 | [Outbox Relay](06b-outbox-relay.md) |
| 06-C | RabbitMQ Publisher Adapter | 已完成 | [RabbitMQ Publisher](06c-rabbitmq-publisher.md) |
| 07-A | Worker 核心、Delivery Attempt 与 Fake Provider | 已完成 | [Worker 核心](07a-dispatch-worker-core.md) |
| 07-B | RabbitMQ Consumer、Manual ACK 与 DLQ | 已完成 | [RabbitMQ Consumer](07b-rabbitmq-consumer.md) |
| 07-C | Composition Root、进程生命周期与健康检查 | 已完成 | [运行时装配](07c-composition-root-runtime.md) |
| 08-A1 | 可靠受理、幂等指纹与 Payload 加密 | 已完成 | [可靠受理内核](08a1-reliable-submission-core.md) |
| 08-A2 | Submission/Query gRPC Adapter | 已完成 | [gRPC Submission/Query](08a2-grpc-submission-query.md) |
| 08-B1 | Delivery Event Journal 与通知事实 | 已完成 | [Delivery Event Journal](08b1-delivery-event-journal.md) |
| 08-B2 | Notification Worker 与 AI-Nexus 回调 | 未开始 | 完成后新增 |
| 09 | SMTP Provider、重试与熔断 | 未开始 | 完成后新增 |
| 10 | 多租户、模板和生产化 | 未开始 | 完成后新增 |

## 推荐阅读顺序

1. [项目全貌](00-project-overview.md)
2. [系统定位与架构基线](01-architecture-baseline.md)
3. [通用 gRPC V1 契约](02-grpc-contract.md)
4. [领域模型与邮件状态机](03-domain-state-machine.md)
5. [PostgreSQL 基础与 Migration](04-postgresql-foundation.md)
6. [PostgreSQL Repository 与乐观锁](04b-postgresql-repository.md)
7. [Transactional Outbox 原子持久化](05-transactional-outbox.md)
8. [数据库 Scheduler](06a-database-scheduler.md)
9. [Outbox Relay 与 Lease Fencing](06b-outbox-relay.md)
10. [RabbitMQ Publisher Adapter](06c-rabbitmq-publisher.md)
11. [Worker 核心、Delivery Attempt 与 Fake Provider](07a-dispatch-worker-core.md)
12. [RabbitMQ Consumer、Manual ACK 与 DLQ](07b-rabbitmq-consumer.md)
13. [Composition Root、进程生命周期与健康检查](07c-composition-root-runtime.md)
14. [可靠受理、幂等指纹与 Payload 加密](08a1-reliable-submission-core.md)
15. [gRPC Submission/Query 与租户身份边界](08a2-grpc-submission-query.md)
16. [Delivery Event Journal 与通知事实](08b1-delivery-event-journal.md)
17. [核心术语速查](glossary.md)

## 每阶段固定记录结构

后续每个阶段至少回答：

1. 这一阶段解决什么问题？
2. 为什么现在做，而不是更早或更晚？
3. 有哪些替代方案，为什么没有选择？
4. 修改了哪些核心文件？
5. 最重要的实现细节和故障场景是什么？
6. 如何通过测试证明它有效？
7. 面试时怎样在 30 秒和 2 分钟内说明？
8. 这一阶段还没有解决什么？

新增记录时使用 [阶段记录模板](stage-template.md)，避免只记录“写了哪些文件”而没有
记录设计原因。
