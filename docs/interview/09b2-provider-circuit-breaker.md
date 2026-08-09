# 09-B2：Provider 熔断器与半开探测

- 状态：已完成
- 阶段目标：SMTP 持续故障时停止重复撞击外部依赖，并通过单探针安全判断恢复

## 1. 解决的问题

并发舱壁和限速只能限制压力大小，不能识别“Provider 已经持续不可用”。例如 QQ SMTP 网络中断或
授权码失效时，即使限制为每秒一个请求，系统仍会为每封邮件重复 TLS 握手、认证和失败。这会浪费
连接与 Worker 时间，还可能放大 Provider 封禁风险。

熔断器解决的是“现在是否值得继续尝试调用”，不是邮件业务重试本身。邮件是否重试仍由 Failure、
Message 状态机和数据库 Scheduler 决定。

## 2. 状态机

```text
                    连续故障达到阈值
          ┌────────────────────────────────┐
          │                                ▼
       CLOSED                           OPEN
          ▲                                │
          │ 探针成功                       │ 冷却到期且有新请求
          │                                ▼
          └────────────────────────── HALF_OPEN
                                           │
                                           └── 探针失败 → OPEN（重新计时）
```

- `CLOSED`：正常放行，统计连续基础设施故障；
- `OPEN`：快速拒绝，不调用 SMTP，也不消耗舱壁槽位和速率令牌；
- `HALF_OPEN`：只允许一个探针，其他请求仍快速失败；
- 认证失败不等待累计阈值，立即进入 `OPEN`。

默认阈值是连续 5 次故障，开放 30 秒。`OPEN → HALF_OPEN` 是惰性转换：没有后台 goroutine 或
Ticker；冷却到期后的第一个请求触发转换并获得探针资格。

## 3. 为什么半开只放一个探针

如果恢复瞬间直接关闭熔断，数据库积压和 RabbitMQ 在途消息会同时冲向刚恢复的 SMTP。单探针先用
一个真实结果确认恢复：成功才恢复正常流量，失败则重新打开。这相当于给恢复过程单独设置容量为 1
的舱壁。

若探针在调用 SMTP 前被本地舱壁、Token Bucket 或 Context 拒绝，它会释放探针资格，不会让熔断器
永久卡在 `HALF_OPEN`，也不会把本地限流误判成 Provider 再次故障。

## 4. 哪些失败计入熔断

| 结果 | 处理 | 原因 |
| --- | --- | --- |
| `ACCEPTED` | 清零连续故障 | Provider 正常接受 |
| `AUTHENTICATION` | 立即打开 | 同一凭据继续尝试不会自行恢复 |
| `RATE_LIMITED` | 累加 | Provider 明确要求降低压力 |
| `PROVIDER_UNAVAILABLE` | 累加 | 外部服务不可用 |
| `NETWORK` | 累加 | 无法可靠访问外部依赖 |
| `TIMEOUT_BEFORE_SEND` | 累加 | 调用未能在边界内完成 |
| `SUBMISSION_UNKNOWN` | 累加 | Provider 链路健康已出现严重异常 |
| `RECIPIENT_REJECTED` | 清零 | 是单封邮件业务结果，不代表 Provider 故障 |
| `CONTENT_REJECTED` | 清零 | Provider 能明确响应，只是拒绝内容 |
| `VALIDATION` | 清零 | 当前 Worker 已预校验请求，SMTP 明确响应不代表基础设施坏 |
| `INTERNAL`/非法结果 | 忽略 | 本地 invariant 不应污染外部健康统计 |

“不计入”不等于吞掉错误。原始 ProviderResult 仍原样交给 Worker；熔断器只旁观并更新健康状态。
`SUBMISSION_UNKNOWN` 虽然计入熔断，但当前邮件仍进入不确定状态，不能因为熔断器想重试就改变其
防重复语义。

## 5. 认证失败后的任务语义

第一次真实认证失败保持 SMTP 原结果：`AUTHENTICATION` 且不可重试，因此当前邮件按原有规则失败；
同时熔断器立即打开。开放期间后续邮件得到：

```text
PROVIDER_UNAVAILABLE / LOCAL_PROVIDER_CIRCUIT_OPEN / retryable=true
```

这样后续任务留在持久化重试链路，而不是用同一个坏凭据逐封调用 SMTP、再把整批邮件永久失败。
当前配置不支持进程内热更新凭据，修复授权码后重启实例会创建新的 `CLOSED` 熔断器；动态凭据轮换
属于后续控制面能力。

## 6. 并发正确性：epoch fencing

考虑两个并发调用 A 和 B：

```text
A、B 都在 CLOSED 时进入
A 先失败并达到阈值 → OPEN
B 稍后成功返回
```

如果 B 无条件记录成功，就会错误地把刚打开的熔断器关闭。实现为每次状态轮次维护递增 `epoch`，
调用入场时拿到 ticket；结果返回时只有 ticket epoch 仍等于当前 epoch 才能修改状态。A 打开熔断时
递增 epoch，B 的旧 ticket 随即失效。这与数据库 Lease 使用 fencing token 阻止旧 Worker 回写是
同一个并发设计思想，只是这里发生在单进程内存中。

## 7. 配置和当前粒度

| 环境变量 | 默认值 | 含义 |
| --- | ---: | --- |
| `MAIL_PROVIDER_CIRCUIT_FAILURE_THRESHOLD` | `5` | 连续健康故障达到多少次后打开 |
| `MAIL_PROVIDER_CIRCUIT_OPEN_DURATION` | `30s` | OPEN 冷却时间 |

阈值限制在 `1..100000`，开放时间限制在 `100ms..24h`，启动时 fail-fast 校验。

当前一个服务进程只配置一组 SMTP endpoint 和 credential，所以一个 Guard 就自然对应：

```text
smtp + endpoint + credential
```

它仍是本地状态，多副本之间不会同步。未来多 Provider/多凭据 Router 必须按 route key 分别维护
Guard；若把所有凭据放在同一熔断器中，一个租户授权失败会拖垮其他租户。

## 8. 主要文件

- `internal/provider/resilience/circuit_breaker.go`：状态机、ticket/epoch 和失败分类；
- `internal/provider/resilience/guard.go`：熔断准入与舱壁、Token Bucket 的组合顺序；
- `internal/provider/resilience/circuit_breaker_test.go`：阈值、认证、半开、过期结果和并发探测；
- `internal/bootstrap/config.go`：熔断环境变量加载和启动校验；
- `.env.example`：安全默认配置。

## 9. 验证

已覆盖：

- 达到连续故障阈值后打开，并且不再调用底层 Provider；
- 认证失败一次立即打开；
- 开放时间到期后单探针成功关闭、失败重新计时；
- 半开并发只允许一个调用；
- 收件人拒绝会中断连续基础设施故障计数；
- 旧 epoch 的在途成功不能提前关闭新一轮 OPEN；
- 本地限流拒绝会释放半开探针资格；
- 所有返回结果都满足统一 ProviderResult 校验。

实际通过的验证命令：

```bash
go test -race -count=10 ./internal/provider/resilience ./internal/bootstrap
go test ./...
go test -race ./...
go vet ./...
make migrate-validate
go test -tags=real_smtp -run '^$' ./internal/provider/smtp
make test-integration
```

带 `real_smtp` 标签的命令使用 `-run '^$'`，只验证真实测试代码能够编译，不执行发送；容器集成测试
继续使用 Fake Provider。本阶段没有连接 QQ SMTP，也没有发送真实邮件。

## 10. 面试表达

### 30 秒版本

> 我在 Provider Guard 里增加了 CLOSED、OPEN、HALF_OPEN 熔断状态机。连续网络、超时或 Provider
> 故障达到阈值就快速拒绝，认证失败立即熔断；冷却后只放一个探针，成功恢复、失败重新打开。
> 业务拒绝不计入熔断，SUBMISSION_UNKNOWN 只影响健康统计而不改变当前邮件语义。并发旧请求用
> epoch fencing 防止过期结果错误覆盖新状态。

### 2 分钟版本

> 舱壁解决同时有多少调用，熔断解决当前还值不值得调用。我把熔断检查放在舱壁和令牌之前，所以
> OPEN 时不会消耗本地容量。状态转换由请求惰性驱动，不需要后台 goroutine。CLOSED 统计连续基础
> 设施故障，认证失败立即打开；冷却后 HALF_OPEN 只允许一个探针。Provider 的收件人和内容拒绝
> 说明服务仍能明确响应，所以它们清零连续故障，而不是打开熔断。并发场景中，打开熔断前已经进入
> 的请求可能晚返回，因此每轮状态有 epoch，旧 ticket 不能修改新状态。当前状态按实例维护，未来
> Router 需要按 endpoint 和 credential 拆分。

### 可能追问

**熔断和重试有什么区别？**

重试决定某封邮件以后是否再执行；熔断决定当前是否调用一个整体不健康的 Provider。前者是任务
状态和时间调度，后者是依赖健康保护，二者不能互相替代。

**为什么收件人拒绝反而清零基础设施故障？**

因为它证明 SMTP 能建立会话并返回明确业务结果。把坏地址算作 Provider 故障，会让攻击者或脏数据
通过大量无效地址把整条发送链路熔断。

**为什么不用现成熔断库？**

当前状态机很小，但项目需要自定义 Failure 分类、认证立即熔断、`SUBMISSION_UNKNOWN` 语义以及与
本地舱壁/令牌的准入取消。自实现减少依赖并能用确定性测试覆盖。若以后加入动态路由、滑动窗口和
分布式状态，应重新评估成熟库或独立控制面。

**为什么不是到时间自动变成 HALF_OPEN？**

没有请求时转换状态没有业务价值。由第一个到达请求惰性转换，可以避免定时 goroutine 和停机管理，
而且仍严格保证开放窗口内不调用 Provider。

## 11. 尚未解决

- 熔断状态变化和拒绝次数 Metrics 已在 [09-B3](09b3-provider-observability.md) 完成；
- 进程级 OTel Exporter、Trace 和告警规则；
- 运维查询、告警和人工强制 open/close；
- 多实例共享健康状态；
- 多 Provider/endpoint/credential Router；
- 动态凭据轮换和不同故障类型的独立开放时长；
- SMTP 连接池与连接健康检查。
