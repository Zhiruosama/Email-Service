# ADR-0004：采用至少一次和端到端幂等

- 状态：Proposed
- 日期：2026-07-28

## 决策

数据库到 RabbitMQ、RabbitMQ 到 Worker、状态通知到订阅方均采用至少一次语义，
并通过稳定 ID、唯一约束、状态机和 payload fingerprint 实现幂等。

## 原因

网络超时和进程崩溃使跨系统 Exactly Once 无法成立。隐藏重复风险会造成更难诊断
的数据丢失或重复邮件。

## 后果

- 所有 Consumer 都必须能处理重复；
- Publisher Confirm 超时时允许重发；
- 外部 SMTP 的不确定结果必须成为显式状态；
- API 文档不能宣称绝对不会产生外部重复邮件。
