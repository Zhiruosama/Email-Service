# 核心术语速查

## Message

一封逻辑邮件的权威业务记录，保存状态、收件人密文、模板版本、调度时间、截止时间和
重试信息。一个 Message 只对应一个收件人。

## Attempt

一次具体的 Provider 投递尝试。一封 Message 可以有多个 Attempt，但成功消费旧 MQ
消息不能凭空创建新 Attempt。

## Outbox

数据库里的待发布事件记录。它不是邮件正文，也不是 RabbitMQ。它通常只包含事件 ID、
Message ID、事件类型和 dispatch generation 等必要信息。

## Transactional Outbox

在同一个数据库事务中写入 Message 和 Outbox，解决“数据库成功但 MQ 没发布”的双写
不一致。Relay 后续发布可能重复，因此仍需幂等消费者。

## Transactor 与 Unit of Work

Transactor 负责 Begin、Commit、Rollback；Unit of Work 暴露绑定到同一事务的多个
Repository。本项目用它们让应用层表达“Message + Outbox 必须原子提交”，同时不直接
依赖 pgx。

## Scheduler

Mail Service 内部的后台角色，扫描已到 `scheduled_at` 或 `next_attempt_at` 的 Message，
使用数据库事务时间推进状态并创建 Outbox。它不直接发送邮件，也不直接发布 RabbitMQ，
因此只需要短事务行锁，不需要 Message Lease。

## Outbox Relay

领取 PENDING Outbox，发布 RabbitMQ，等待 Publisher Confirm，再记录发布结果的后台
角色。它使用短事务设置 Lease，网络调用不持有数据库事务。

## Worker

消费 RabbitMQ 命令、读取 Message、检查状态和 generation、创建 Attempt，并调用
Provider 的执行角色。

## `FOR UPDATE SKIP LOCKED`

`FOR UPDATE` 锁住当前事务领取的行；`SKIP LOCKED` 让其他实例跳过已锁行继续领取剩余
任务。它提高并发领取效率，不提供端到端 Exactly Once。

## Lease

事务提交后有期限的处理权，例如 `lease_owner + lease_until`。处理者崩溃后，其他实例
可以在 Lease 过期后恢复任务。它适合 Outbox Relay 这类跨网络工作；Scheduler 的状态
推进在一个短事务内完成，由 PostgreSQL 回滚和行锁恢复，不额外设置 Lease。

## Fencing Token

每次领取生成的唯一所有权 token。Outbox 结果更新必须匹配当前 token；Lease 过期并被
重新领取后，旧 Publisher 即使晚返回也只能得到 LeaseLost，不能覆盖新 owner 的状态。

## Publisher Confirm

RabbitMQ 向 Publisher 确认 Broker 已经接管消息的机制。Confirm 丢失时 Publisher 无法
判断结果，可能重发，因此 Consumer 必须幂等。

## Consumer Ack

Worker 完成持久化处理后向 RabbitMQ 确认消息可以删除。应在数据库事务提交后 ACK，
否则 Worker 崩溃可能导致消息已经删除但状态尚未保存。

## At Least Once

允许同一消息被处理一次或多次，通过幂等消除重复副作用。比“可能静默丢失”更适合
可靠投递系统。

## Exactly Once

严格只执行一次。数据库、RabbitMQ 和外部 SMTP 之间没有统一事务，项目不虚假承诺
端到端 Exactly Once。

## `dispatch_generation`

Message 每次重新进入发送队列时递增的代次。Worker 收到旧 generation 的延迟 MQ 消息
时直接忽略，避免旧命令再次发送邮件。

## `sequence`

同一 Message 的状态事件单调序号。业务订阅方不能让较小 sequence 的旧事件覆盖较新
状态。

## Circuit Breaker

供应商持续失败时暂时停止新的调用，经过冷却后用少量探测请求判断是否恢复。本项目按
Provider、Endpoint/Region 和 Credential 隔离熔断状态。

## Bulkhead

用独立队列、连接池和并发限制隔离不同类别或 Provider，防止某一部分故障耗尽整个
服务资源。

## DLQ

Dead Letter Queue。处理超过重试上限或遇到 poison message 的任务进入死信，供告警、
诊断和受约束的人工重放，不能静默丢弃。
