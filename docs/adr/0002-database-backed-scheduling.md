# ADR-0002：数据库是调度真相，RabbitMQ 是传输通道

- 状态：Proposed
- 日期：2026-07-28

## 决策

`scheduled_at`、`next_attempt_at` 和 `dispatch_deadline` 保存在 PostgreSQL。
Scheduler 领取到期任务并通过 Transactional Outbox 发布到 RabbitMQ。

## 原因

- 支持从数秒到数月的调度；
- 支持取消、查询、租户暂停和重启恢复；
- 避免 RabbitMQ TTL 消息在队首过期语义下占用资源；
- 避免依赖额外的延迟消息插件；
- 让任务状态只有一个权威来源。

## 后果

- 需要高效时间索引和有界批量领取；
- Scheduler 只做短事务状态推进，崩溃由事务回滚恢复，不为 Message 设置 lease；
- Outbox Relay 跨事务等待 Broker Confirm，需要独立的 lease 恢复；
- Scheduler 延迟成为必须监控的 SLI；
- RabbitMQ 可以短时不可用，Outbox 会积压；
- 数据库容量规划必须包含未来计划任务。
