# 项目全貌

## 一句话介绍

Mail Service 是一个面向多个业务系统的通用可靠邮件投递平台。它负责邮件任务的可靠
受理、定时调度、异步投递、模板渲染、供应商路由、重试、熔断、死信和状态通知。

AI-Nexus 是第一个使用方，但验证码生成、验证和消费仍由 AI-Nexus 自己负责。

## 为什么这个项目不只是 SMTP 封装

直接调用 SMTP 只能回答“当前这次网络请求是否看起来成功”，不能解决：

- API 进程崩溃后任务会不会丢；
- 同一个请求重试会不会发送两封；
- 邮件需要半小时后发送怎么办；
- Provider 暂时故障时如何退避；
- 多个 Worker 如何避免同时处理同一任务；
- RabbitMQ 或业务回调暂时不可用时如何恢复；
- SMTP 接受和用户真正收到有什么区别。

因此项目的核心不是发送一段 SMTP 命令，而是构建一个可恢复的异步任务系统。

## 系统主链路

```text
业务系统
  │ SubmitEmail
  ▼
Submission API
  │ PostgreSQL 本地事务
  ├── Message
  └── Transactional Outbox
          │
          ▼
     Outbox Relay
          │
          ▼
       RabbitMQ
          │
          ▼
   Delivery Worker
          │
          ▼
  SMTP / API Provider
          │
          ▼
  Delivery Event / Subscriber
```

定时发送和重试会在 Message 与 Outbox 之间经过 Scheduler：

```text
SCHEDULED / RETRY_SCHEDULED
          │ 到期
          ▼
      Scheduler
          │ 同事务推进状态并创建 Outbox
          ▼
     Outbox Relay → RabbitMQ
```

## 最重要的可靠性观点

```text
PostgreSQL 是任务状态和调度时间的权威来源；
RabbitMQ 是异步传输通道；
系统使用至少一次处理和端到端幂等；
不宣称跨数据库、MQ 和 Provider 的 Exactly Once。
```

## 面试展开顺序

介绍项目时可以按以下顺序：

1. 先讲业务问题：同步 SMTP 不可靠、不可调度、不可恢复；
2. 再讲职责边界：业务拥有业务状态，邮件服务拥有投递状态；
3. 再讲可靠受理：PostgreSQL + Transactional Outbox；
4. 再讲异步执行：RabbitMQ + 幂等 Worker；
5. 再讲定时与重试：Scheduler 扫描数据库到期任务；
6. 最后讲故障治理：熔断、舱壁、DLQ、对账和可观测性。

不要一开始就罗列技术名词。先把“为什么需要这些组件”讲清楚，再讲技术实现。
