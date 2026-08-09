# 核心术语速查

## Message

一封逻辑邮件的权威业务记录，保存状态、收件人密文、模板版本、调度时间、截止时间和
重试信息。一个 Message 只对应一个收件人。

## Attempt

一次具体的 Provider 投递尝试。一封 Message 可以有多个 Attempt，但成功消费旧 MQ
消息不能凭空创建新 Attempt。Worker 在 Provider I/O 前原子创建 `STARTED` Attempt；最终
只归一化为明确接受、明确失败或提交结果未知，供状态机和 Reconciler 使用。

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

## Composition Root

程序中唯一知道全部具体实现并负责依赖组装的位置。本项目由 `internal/bootstrap.NewApp`
创建 PostgreSQL、RabbitMQ、Scheduler、Relay、Worker 和 Provider；领域/应用层不读取环境
变量，也不依赖具体 SDK。

## Supervisor

管理多个长期运行组件的生命周期。组件意外退出时取消 peers，正常关机时按阶段停止并设置
总超时，避免进程处于“端口还活着但核心 goroutine 已死”的半失效状态。

## Liveness 与 Readiness

Liveness 回答“进程是否仍在运行”，失败通常意味着应重启；Readiness 回答“当前是否应接收
流量”，依赖暂时不可用时可以变为 NOT_SERVING 而不立即杀进程。本项目 Worker readiness
要求 PostgreSQL 可达且 RabbitMQ Consumer lanes 已就绪。

## Graceful Shutdown

收到 SIGTERM 后先撤销 readiness，再停止产生新工作，等待有界在途处理，最后关闭网络和
数据库资源。超过总时限时返回失败并由外部编排系统终止，不能无限阻塞发布。

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

## `mandatory` Publish

AMQP 发布标志。Exchange 存在但没有 Queue 匹配 routing key 时，RabbitMQ 通过
`basic.return` 把消息退回 Publisher；如果不启用，消息可能被静默丢弃。Return 只说明
路由失败，Confirm 说明 Broker 对这次 Publish 的处理结果，二者不能互相替代。

## AMQP Connection 与 Channel

Connection 是昂贵、长生命周期的 TCP 连接；Channel 是复用该连接的轻量逻辑会话。
Publisher 使用一条长连接和有界 Channel 池，一次并发发布独占一个 Channel，不能让多个
goroutine 共享同一 Channel 做 Publish/Confirm correlation。

## Persistent Message

AMQP 消息的 delivery mode。Persistent message 与 durable queue 配合，使 Broker 重启后
能够恢复消息；它不代表发布方已经知道消息落盘，仍需要 Publisher Confirm。

## Quorum Queue

RabbitMQ 基于 Raft 的复制队列类型，面向数据安全和高可用。声明时需要
`x-queue-type=quorum` 且队列天然 durable。单节点开发环境只能验证协议和重启持久化，不能
证明三节点多数派故障容忍。

## Consumer Ack

Worker 正常完成结果事务后向 RabbitMQ 确认消息可以删除。旧 generation、重复事件也可以
在数据库证明其不能再次执行后 ACK；若数据库已有 `SENDING + STARTED` 而结果未完成，
由 Reconciler 对账，不能靠 MQ 重投盲目再次调用 Provider。任何 ACK 都必须建立在已提交
的数据库事实之上。

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
