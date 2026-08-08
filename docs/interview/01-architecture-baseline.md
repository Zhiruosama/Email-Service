# 阶段 01：系统定位与架构基线

- 状态：已完成
- 阶段目标：确定通用邮件服务的边界、组件、可靠性语义和实施路线

## 1. 解决的问题

AI-Nexus 原有邮件流程依赖外部 gRPC 服务生成验证码，并可能共享 Redis。验证码生成、
存储、发送和验证没有形成清晰边界，也没有幂等、回调、对账和异步投递状态。

本阶段先回答：

- Mail Service 应该负责什么；
- AI-Nexus 应该负责什么；
- 系统如何从 QQ SMTP 扩展到多个业务和 Provider；
- 数据库、RabbitMQ、Scheduler、Worker 如何分工；
- 面对超时、重复和崩溃时采用什么一致性模型。

## 2. 做出的核心决策

### 2.1 通用核心与业务适配器分离

核心只认识：

```text
Tenant、Message、Template、Provider、Attempt、DeliveryEvent
```

它不认识注册、登录、订单等业务。AI-Nexus 验证码通过适配器转换成：

```text
verification_code.v1 模板 + 受约束变量
```

原因是避免邮件核心随着每个业务需求修改状态机，同时保留业务侧强类型语义。

### 2.2 模块化单体

先使用一个 Go 仓库和一个二进制，但支持按角色运行：

```text
api / scheduler / relay / worker / notifier / reconciler
```

原因：领域仍在形成，过早拆微服务会增加协议、部署和一致性成本。按角色运行仍允许
生产环境独立扩容 Worker 或 API。

### 2.3 PostgreSQL 是权威状态

PostgreSQL 保存 Message、调度时间、Attempt、Outbox 和事件。选择它是因为：

- 本地事务适合 Message + Outbox 原子写入；
- 唯一约束适合实现租户级幂等；
- `FOR UPDATE SKIP LOCKED` 适合多实例领取任务；
- Partial Index 适合从大量历史任务中查找少量活跃任务；
- `RETURNING`、时间类型和 JSONB 便于任务系统建模。

MySQL 8 也可以实现。选择 PostgreSQL 是工程体验和能力组合的取舍，不是因为只有它
能完成任务。

### 2.4 RabbitMQ 是传输通道，不是长期调度器

未来几分钟到几个月的任务保存在 PostgreSQL。到期后 Scheduler 才创建 Outbox，
Outbox Relay 再发布 RabbitMQ。

这样可以支持取消、修改、查询、重启恢复，并避免 MQ 同时成为第二份业务状态。

### 2.5 至少一次 + 幂等

网络故障下 Publisher Confirm、Consumer Ack 或 RPC 响应可能丢失。系统允许安全重试
和重复消息，通过唯一键、状态机、事件 ID、sequence 和 dispatch generation 幂等。

没有宣称 Exactly Once，因为 SMTP 等外部系统并不参与本地事务。

## 3. Outbox、Scheduler 和 Relay 的关系

```text
Scheduler：判断 Message 是否到期；推进状态并创建 Outbox
Outbox：数据库里的待发布命令记录，不保存完整邮件正文
Relay：领取 Outbox，发布 RabbitMQ，等待 Publisher Confirm
Worker：消费 MQ 命令，读取 Message，调用 Provider
```

立即邮件直接创建 `QUEUED Message + Outbox`；定时邮件先进入 `SCHEDULED`，到期后由
Scheduler 创建 Outbox。

## 4. 可靠性设计

### Transactional Outbox

```sql
BEGIN;
INSERT INTO mail_messages (...);
INSERT INTO outbox_events (...);
COMMIT;
```

保证业务任务和待发布事件同时存在或同时回滚，消除“数据库成功但 MQ 没发布”的永久
丢失窗口。

Relay 可能在发布成功、标记成功之前崩溃，因此可能重复发布，Worker 必须幂等。

### `FOR UPDATE SKIP LOCKED`

多个实例并发领取数据库任务时：

- `FOR UPDATE` 锁住自己领取的行；
- `SKIP LOCKED` 跳过别人已经领取的行；
- Lease 在事务提交后表示有期限的处理权；
- Lease 过期允许其他实例恢复崩溃任务。

它只避免同时领取，不能保证整个投递链路 Exactly Once。

### 熔断和舱壁

熔断按照：

```text
Provider + Endpoint/Region + Credential
```

划分，避免单个租户凭据故障拖垮所有租户。Critical、Transactional、Notification、
Bulk 使用不同队列和并发池，防止批量邮件挤占验证码。

## 5. 主要文档产出

- `docs/product-scope.md`
- `docs/architecture.md`
- `docs/reliability.md`
- `docs/data-model.md`
- `docs/security-and-observability.md`
- `docs/roadmap.md`
- `docs/adr/`

## 6. 如何验证

本阶段属于架构冻结，验证方式是检查：

- 组件职责是否唯一；
- 故障窗口是否有恢复路径；
- 是否存在隐式共享数据库或 Redis；
- 是否错误承诺 Exactly Once；
- AI-Nexus 和 Mail Service 是否拥有两套验证码状态。

## 7. 面试表达

### 30 秒版本

> 我先将邮件系统定位为通用可靠投递平台，而不是 SMTP 封装。PostgreSQL 保存任务和
> 调度状态，Transactional Outbox 解决数据库与 RabbitMQ 双写问题；Scheduler 负责
> 到期任务，Relay 负责可靠上 MQ，Worker 负责 Provider 投递。系统采用至少一次和
> 幂等，不承诺跨系统 Exactly Once，并通过队列隔离、熔断和 DLQ 处理故障。

### 可能追问

**为什么不用 RabbitMQ 直接保存一个月后的消息？**

因为取消、修改、查询和重启恢复都需要业务状态；长期计划保存在 PostgreSQL，只把
已经到期的工作送入 MQ，职责更清晰。

**Outbox 是否保证绝不重复？**

不保证。它保证待发布事件不会因双写永久丢失；发布确认丢失时可能重复，因此消费者
仍需幂等。

## 8. 尚未解决

本阶段没有实现代码、数据库、MQ 或 SMTP，只确定了可实施边界。下一阶段通过公共
协议和可执行测试把这些术语固定下来。
