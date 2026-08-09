# 09-B3：Provider OpenTelemetry Metrics 与安全标签

- 状态：已完成
- 阶段目标：让 Provider 调用、局部拒绝和熔断恢复可度量，同时不让 Metrics 成为敏感信息泄漏点

## 1. 解决的问题

舱壁、限流和熔断器已经能保护 SMTP，但没有指标时只能看到“邮件变慢了”，无法回答：

- 请求是否真的进入 SMTP？
- 是 Provider 慢，还是本地舱壁/令牌拒绝？
- 熔断器何时打开，是否进入半开，探针是否恢复？
- 某类失败的比例和 Provider 延迟分位数是多少？

仅增加日志也不够。日志适合单次事件排查，Metrics 适合聚合速率、分布、状态和告警。

## 2. 三层边界：Instrumentation、SDK、Exporter

```text
Provider Guard
  → Observer Port
  → OpenTelemetry Instruments
  → SDK / Reader                 （进程级，后续装配）
  → OTLP 或 Prometheus Exporter  （部署级，后续装配）
```

本阶段完成前两层，并在测试中使用 OTel SDK ManualReader 验证真实聚合数据。主进程目前没有配置
SDK/Exporter，所以全局 OTel Meter 默认安全 no-op，不会新增端口、后台 goroutine 或外部网络连接。

这不是“指标没用”，而是将职责拆清：Provider 负责产生稳定观测，进程入口负责 Resource、Exporter、
采样和 Shutdown。以后切换 OTLP/Prometheus 不需要修改熔断状态机。

## 3. 为什么先定义 Observer Port

Resilience Guard 依赖的是项目自己的小接口：

```text
RecordProviderCall
RecordProviderRejection
RecordCircuitState
RecordCircuitTransition
```

它不知道 Counter、Histogram、Prometheus 或 OTLP。这是依赖倒置：核心模块描述“发生了什么”，外层
Adapter 决定“用什么监控技术记录”。普通单元测试注入内存 Recorder，运行时注入 OTel Adapter；
Observer 方法没有返回错误，因此采集结果不能篡改邮件投递结果。

## 4. 指标设计

| Instrument | 类型 | 属性 | 回答的问题 |
| --- | --- | --- | --- |
| `mail.provider.calls` | Counter | `provider,outcome,failure_category` | 实际调用量和结果比例 |
| `mail.provider.duration` | Histogram，单位秒 | 同上 | p50/p95/p99 外部调用耗时 |
| `mail.provider.rejections` | Counter | `provider,reason` | 被 context/舱壁/限流/熔断拒绝多少 |
| `mail.provider.circuit.state` | Gauge | `provider` | 当前 CLOSED=0、HALF_OPEN=1、OPEN=2 |
| `mail.provider.circuit.transitions` | Counter | `provider,from,to,reason` | 打开、探测和恢复次数 |

`calls` 和 `duration` 只在真正调用底层 SMTP Provider 后记录。熔断 OPEN 的快速失败不能算成 SMTP
调用，也不能以接近 0 的耗时污染延迟直方图；它进入 `rejections{reason=circuit_open}`。

为什么耗时用 Histogram：平均值会掩盖尾延迟，例如 99 次 100ms 和 1 次 10s 的平均值看起来仍不
明显；Histogram 可以由后端计算 p95/p99 和 SLO 分布。

## 5. 标签安全与基数控制

允许的标签只有稳定、低基数集合：

```text
provider
outcome
failure_category
reason
from / to
transition_reason
```

明确禁止：

- `tenant_id`、`message_id`、attempt ID；
- 收件地址、发件地址和邮箱域的原文；
- Subject、模板变量、验证码和 MIME；
- SMTP 授权码、用户名和 Provider 原始响应；
- 任意错误字符串或稳定集合之外的枚举。

原因不只是安全。Metrics 后端会为每组标签值创建时序；把 message ID 放进去会让每封邮件产生新
时序，造成高基数、内存增长和查询退化。

OTel Adapter 对未知 outcome、failure category、rejection reason 和 transition 值再次归一化为
`unknown`，即使内部 Adapter 将来返回非法动态值，也不会原样进入标签。测试会读取真实 OTel 聚合
数据并逐个检查属性 Key 白名单和恶意 Marker 不泄漏。

## 6. 为什么 Gauge 还需要 epoch 序号

熔断状态机用 epoch 防止旧 Provider 结果回写，新问题是观测回调在状态锁外执行：

```text
goroutine A 生成 OPEN(epoch=1)，尚未记录 Gauge
goroutine B 生成 HALF_OPEN(epoch=2)，先记录 Gauge=1
goroutine A 随后记录 Gauge=2（OPEN）
```

状态机本身已经 HALF_OPEN，但监控会倒退到 OPEN。不能为了指标把 OTel 调用放进状态锁，否则监控
延迟会阻塞发送。实现把 epoch 作为 `Sequence` 交给 OTel Adapter，Adapter 按 Provider 丢弃旧序号
Gauge。Sequence 只做本地顺序判断，不作为 Label，所以不会增加时序数量。Transition Counter 仍会
完整记录每次真实转换。

## 7. 调用耗时边界

计时点严格包围：

```text
started = clock.Now()
result  = smtpProvider.Submit(...)
ended   = clock.Now()
```

因此不包含等待 RabbitMQ、数据库领取、MIME 构建、熔断拒绝或本地限流。若墙上时钟回拨，负数耗时
会归零；后续可以切换单调时钟实现，但 Go `time.Time` 在同一进程通常携带 monotonic reading。

## 8. 主要文件

- `internal/provider/resilience/observer.go`：SDK 无关的安全 Observation 和 Observer Port；
- `internal/provider/resilience/guard.go`：调用、拒绝、状态与转换观测点；
- `internal/observability/providermetrics/observer.go`：OpenTelemetry Metrics Adapter；
- `internal/provider/resilience/observer_test.go`：Guard 观测语义与并发 Recorder 测试；
- `internal/observability/providermetrics/observer_test.go`：ManualReader、属性白名单与乱序 Gauge 测试；
- `internal/bootstrap/app.go`：SMTP 运行时注入全局 OTel Meter。

## 9. 验证

已覆盖：

- Accepted/Failed 调用数量、Failure Category 和纯 Provider Duration；
- context、bulkhead、rate limit、circuit open 四类局部拒绝；
- CLOSED→OPEN→HALF_OPEN→CLOSED 状态与原因；
- 并发 100 次提交下 Recorder 不丢事件、无数据竞争；
- OTel ManualReader 能收集五个预期 Instrument；
- 每个 Instrument 的属性 Key 严格匹配白名单；
- 非法动态枚举和邮箱形态 Marker 不进入属性值；
- 旧 epoch Gauge 不能覆盖较新的熔断状态。

实际通过的验证命令：

```bash
go test -race -count=10 ./internal/provider/resilience ./internal/observability/providermetrics ./internal/bootstrap
go test ./...
go test -race ./...
go vet ./...
make migrate-validate
go test -tags=real_smtp -run '^$' ./internal/provider/smtp
make test-integration
```

带 `real_smtp` 标签的命令只编译、不运行真实发送；容器集成测试继续使用 Fake Provider。本阶段没有
连接 QQ SMTP，也没有发送真实邮件。

## 10. 面试表达

### 30 秒版本

> 我没有让熔断器直接依赖 OTel SDK，而是定义 Observer Port，由 Adapter 输出调用 Counter、耗时
> Histogram、局部拒绝 Counter、熔断 Gauge 和 Transition Counter。标签只允许 Provider 和稳定
> 结果枚举，禁止邮箱、消息 ID 和错误原文。并发状态指标用 epoch Sequence 丢弃过期 Gauge，测试
> 通过 OTel ManualReader 验证真实聚合结果和标签白名单。

### 2 分钟版本

> 可观测性分成 Instrumentation、SDK 和 Exporter。本阶段让 Provider Guard 产生 SDK 无关的安全
> Observation，再由 OTel Adapter 映射到五个 Instrument；主进程尚未配置 Exporter，因此不会私自
> 新增网络连接。真实 SMTP 调用和本地拒绝分开统计，避免快速拒绝污染延迟 Histogram。标签是严格
> 白名单，未知枚举归一为 unknown，防止敏感信息和高基数。状态回调放在锁外避免监控阻塞熔断器，
> 同时携带 epoch Sequence，让 Adapter 忽略乱序 Gauge。ManualReader 测试检查了名称、属性集合和
> 恶意值不泄漏，race detector 验证并发安全。

### 可能追问

**Counter、Histogram、Gauge 分别用在什么地方？**

Counter 记录只增事件数量，适合调用、拒绝和转换；Histogram 记录耗时分布，支持分位数；Gauge
表达某一时刻的最新状态，适合熔断状态编码。

**为什么不把 error code 也做标签？**

当前稳定 Failure Category 足够告警聚合；错误码集合会随着 Provider 和 SMTP 阶段扩张。未经治理就
加 code 容易形成高基数，也可能把 Provider 原始字符串误带进来。需要时应先建立稳定枚举白名单。

**Metrics 记录失败会不会导致邮件失败？**

Observer 方法没有错误返回，不能修改 ProviderResult。运行期全局 Meter 无 SDK 时是 no-op；而
Instrument 创建错误属于部署配置错误，在 Composition Root 启动阶段 fail-fast，不会在已受理邮件
中途改变业务结果。

**为什么现在还看不到 Prometheus 页面？**

因为本轮完成的是标准 OTel 埋点，不是 Exporter 部署。下一阶段需要统一配置 MeterProvider、Resource、
Reader/Exporter 和 Shutdown；不能为了一个 Provider 指标偷偷开启未认证的 `/metrics` 端口。

## 11. 尚未解决

- 进程级 OTel SDK、service/resource 属性和 OTLP/Prometheus Exporter；
- MeterProvider Flush/Shutdown 与应用生命周期；
- Trace、异步 Span Link 和日志 Trace ID；
- Provider 告警阈值与 Dashboard；
- 多凭据 Router 后安全的 `credential_group` 标签；
- Scheduler lag、Outbox lag、queue depth 和端到端 SLO 指标。
