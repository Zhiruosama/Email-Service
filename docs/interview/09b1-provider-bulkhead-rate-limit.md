# 09-B1：Provider 并发舱壁与本地 Token Bucket

- 状态：已完成
- 阶段目标：限制单实例 SMTP 的并发会话和发送速率，并让过载安全回到已有持久化重试链路

## 1. 解决的问题

SMTP Provider 能真实发送后，Consumer 的并发可能同时放大到外部 Provider。假设 RabbitMQ 有 8 个
消费 lane，而 SMTP 变慢到每次 10 秒，没有独立边界时就可能同时占用大量网络连接、goroutine 和
Provider 配额。继续加 Worker 只会把压力推给最脆弱的外部依赖。

本阶段解决两个不同问题：

- **并发舱壁**：限制同一时刻正在执行的 SMTP 调用数；
- **Token Bucket**：限制一段时间内允许开始的 SMTP 调用数，同时容许有界突发。

## 2. 为什么现在做

只有 SMTP 的成功、明确失败和 `SUBMISSION_UNKNOWN` 语义稳定后，保护层才知道怎样包装 Provider，
又不破坏结果分类。更早实现会围绕尚未稳定的接口返工；更晚实现则会让真实 Provider 暴露在无界
并发压力下。

现有 Worker、Message 状态机、数据库 Scheduler 和 full-jitter retry 已经提供可靠重试，因此
Guard 只负责快速拒绝，不需要再发明一套内存排队系统。

## 3. 整体设计

```text
RabbitMQ Consumer / Dispatch Worker
                │
                ▼
      Provider Resilience Guard
        1. 检查 Context
        2. 尝试获取并发槽位
        3. 尝试消费速率令牌
                │
                ▼
           SMTP Provider
```

Guard 实现同一个 `ports.EmailProvider` 接口，所以 Worker 不知道也不依赖具体保护算法。这是装饰器
模式：原 SMTP Provider 专注协议，外层 Guard 专注流量治理，Composition Root 负责组装。

### 为什么舱壁不等待

本实现用带缓冲的 channel 表示并发槽位，并通过非阻塞 `select` 尝试获取。槽位满时立即返回：

```text
RATE_LIMITED / LOCAL_PROVIDER_BULKHEAD_FULL / retryable=true
```

如果在 Guard 内等待，会出现第二条不可观测、进程重启即丢失的内存队列，而且等待时间会继续消耗
Worker context。当前邮件已经可靠保存在 PostgreSQL，因此更合理的做法是快速释放 Worker，由状态机
记录失败，再让 Scheduler 按 full jitter 安排重试。

### Token Bucket 怎样工作

桶容量是 `Burst`，创建时装满。每次允许请求消耗一个令牌；经过时间 `t` 后补充：

```text
新增令牌 = t × RatePerSecond
桶内令牌 = min(Burst, 原令牌 + 新增令牌)
```

例如 `rate=2/s, burst=2`：空闲后可以立即通过 2 个请求，桶空后每 500ms 恢复一个令牌。相比固定
时间窗口，Token Bucket 不会在窗口边界突然允许两倍流量，也能表达“稳定速率 + 小突发”。

执行顺序是“先舱壁、后令牌”。如果连执行槽位都拿不到，就不应消耗一个宝贵令牌。

## 4. 配置与边界

新增单实例配置：

| 环境变量 | 默认值 | 含义 |
| --- | ---: | --- |
| `MAIL_PROVIDER_MAX_CONCURRENT` | `2` | 同时进入 SMTP Provider 的最大调用数 |
| `MAIL_PROVIDER_RATE_PER_SECOND` | `1` | 每秒补充的令牌数，支持小数 |
| `MAIL_PROVIDER_RATE_BURST` | `2` | 桶容量及允许的短时突发 |

这些值是保守的本地开发基线，不代表 QQ 公布的精确配额。它们不会形成全局限速：若部署 3 个副本且
每个都是 `1/s`，理论总速率约为 `3/s`。严格全局配额需要共享限流器，或由控制面把总配额切分给
各副本。

配置会在启动时校验零值、上界、`NaN` 和无穷值，错误只暴露稳定说明，不回显凭据。

## 5. Consumer Prefetch 和舱壁的区别

这是很容易被追问的点：

| 机制 | 限制对象 | 保护对象 |
| --- | --- | --- |
| RabbitMQ `prefetch` | Consumer 尚未 ACK 的消息数 | Broker/Consumer 的在途消息和内存 |
| Provider 并发舱壁 | 正在调用 SMTP 的请求数 | 外部连接、Provider 和本机资源 |
| Token Bucket | 单位时间内开始的 Provider 调用数 | Provider 速率配额 |

三者不能互相替代。prefetch 可以大于 SMTP 并发，因为部分消息正在数据库领取或收尾；但差距过大
会增加本地快速拒绝和数据库重试次数，需要通过指标持续调参。

## 6. 故障语义

| 场景 | 结果 | 是否调用 SMTP | 后续 |
| --- | --- | ---: | --- |
| Context 进入前已取消 | `TIMEOUT_BEFORE_SEND/LOCAL_PROVIDER_CONTEXT_DONE` | 否 | 可安全重试 |
| 并发槽位满 | `RATE_LIMITED/LOCAL_PROVIDER_BULKHEAD_FULL` | 否 | full-jitter 重试 |
| 令牌不足 | `RATE_LIMITED/LOCAL_PROVIDER_RATE_LIMITED` | 否 | full-jitter 重试 |
| Guard 放行 | 原样返回 SMTP 结果 | 是 | 沿用已有分类 |

Guard 不保存 `ProviderRequest`、收件地址或 MIME。槽位通过 `defer` 释放，即使 SMTP 返回失败也不会
永久泄漏容量。令牌桶用 mutex 保护浮点令牌和上次补充时间，可被多个 Worker 安全共享。

## 7. 主要文件

- `internal/provider/resilience/guard.go`：配置、非阻塞舱壁、Token Bucket 和稳定失败结果；
- `internal/provider/resilience/guard_test.go`：并发、补充、取消和令牌不误扣测试；
- `internal/bootstrap/config.go`：环境变量加载、边界校验和默认值；
- `internal/bootstrap/app.go`：只在真实 SMTP 外组装 Guard，Fake 测试路径保持不变；
- `.env.example`：可提交的非敏感配置示例。

## 8. 验证

已覆盖：

- 2 个槽位占满后，第 3 个请求不进入底层 Provider；
- 舱壁拒绝不消费 Token Bucket 令牌；
- 使用可控时钟验证 `2/s` 在 500ms 后准确补回 1 个令牌；
- 100 个 goroutine 同时竞争 10 个令牌时只放行 10 个；
- 已取消 Context 不调用 Provider，也不消费令牌；
- SMTP Composition Root 实际返回带 Guard 的 Provider；
- 配置覆盖、非法数值和敏感信息不回显。

实际通过的验证命令：

```bash
go test ./...
go test -race ./...
go vet ./...
make migrate-validate
go test -tags=real_smtp -run '^$' ./internal/provider/smtp
make test-integration
```

`real_smtp` 命令只做带标签代码的编译检查，并用 `-run '^$'` 明确不运行真实发送测试；容器集成测试
继续使用 Fake Provider，验证 PostgreSQL/RabbitMQ 原有纵向链路没有回归。

本阶段未连接 QQ SMTP，也未发送真实邮件。

## 9. 面试表达

### 30 秒版本

> 我在 SMTP Provider 外加了一个实现相同接口的 Resilience Guard。它先用非阻塞 semaphore 限制
> 同时 SMTP 会话数，再用本地 Token Bucket 限制启动速率。过载不会进入新的内存队列，而是返回
> 稳定、可重试的 RATE_LIMITED，由 PostgreSQL 状态机和 Scheduler 做持久化重试。并发测试、可控
> 时钟和 race detector 用来证明它不会超发或泄漏槽位。

### 2 分钟版本

> RabbitMQ prefetch 只限制 Consumer 未确认消息，不能直接保护 SMTP 连接，所以我在 Provider
> 边界增加装饰器。并发舱壁用 buffered channel 做非阻塞 try-acquire；满了就快速失败，因为任务
> 已经可靠落在 PostgreSQL，没必要在进程内再排一遍。拿到槽位后才消费令牌，避免无法执行的请求
> 浪费速率额度。令牌桶允许稳定速率加有界突发，状态由 mutex 保护。拒绝结果进入现有 full-jitter
> retry，Guard 本身不保存 MIME。当前限流是每实例的，所以多副本会放大总速率；严格全局限流是
> 后续控制面或共享存储的职责。

### 可能追问

**为什么不用 `time.Ticker` 发令牌？**

Ticker 需要后台 goroutine、停止生命周期和 channel 容量管理。按请求根据经过时间惰性补充，状态
更少，也不会因调度暂停丢 tick；mutex 临界区只做常数次计算。

**为什么被限流后不直接在 Guard 里等？**

等待会占用 Worker、MQ 未确认消息和 MIME，并形成不可恢复的内存队列。数据库已经能可靠保存下一次
执行时间，快速失败再持久化重试更符合本系统的职责划分。

**为什么用 channel 做 semaphore？**

Go 的 buffered channel 天然表达固定容量，通过 `select + default` 可以原子地 try-acquire，释放操作
简单且 race detector 可验证。这里不需要公平等待队列，因为设计目标就是过载时快速卸载。

**本地限流会不会多副本超额？**

会，所以文档明确只承诺单实例保护。部署时可以按实例数切分额度；需要动态、严格全局配额时再使用
Redis/控制面等共享协调方案，同时评估共享组件故障时的降级策略。

## 10. 尚未解决

- 09-B2 的 `CLOSED/OPEN/HALF_OPEN` 熔断器；
- provider/endpoint/credential 维度的动态隔离键；
- 限流与拒绝 Metrics、Trace 和告警；
- 严格多实例全局配额；
- SMTP 连接池和安全复用；
- 多 Provider Router 与健康路由。
