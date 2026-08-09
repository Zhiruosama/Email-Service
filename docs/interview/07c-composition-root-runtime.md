# 07-C：Composition Root、进程监督与健康检查

## 1. 这一阶段解决什么问题

07-B 结束时，数据库 Repository、Scheduler、Outbox Relay、RabbitMQ Publisher/Consumer、
Dispatch Worker 和 Fake Provider 都已分别通过测试，但 `cmd/mail-service/main.go` 还是空的。
这些是经过验证的零件，还不是一个能持续运行的服务。

07-C 把它们组装成第一条可启动的后台投递进程：

```text
main
  → typed environment config
  → PostgreSQL Pool + schema preflight
  → TransactionManager
  → Scheduler ─┐
  → Relay ─────┼─ staged supervisor
  → Consumer ──┤
  → gRPC Health┘

Outbox → RabbitMQ → Dispatch Worker → Fake Provider → PostgreSQL final state
```

本阶段不新增业务状态和发送规则，重点是依赖组装、持续运行、异常传播、健康状态和安全
退出。

## 2. Composition Root 是什么

Composition Root 是程序中唯一知道全部具体实现的地方。当前位于
`internal/bootstrap.NewApp`：

```text
ports.Transactor       ← postgres.TransactionManager
ports.OutboxPublisher  ← rabbitmq.Publisher
ports.EmailProvider    ← fake.Provider
DispatchProcessor      ← delivery.DispatchWorker
```

领域层和应用层仍然只依赖接口，不读取环境变量，也不知道 pgx、RabbitMQ 或 gRPC。未来把
Fake Provider 换成 SMTP Provider，只修改 Bootstrap 的构造选择，不需要修改 Worker。

### 为什么不用 Wire、Fx 等 DI 框架

当前构造图规模有限，手工构造具有以下优点：

- 编译器直接检查构造参数；
- 创建顺序和清理顺序一眼可见；
- 启动失败时哪些资源需要回收非常清楚；
- 不需要运行时代码生成、反射或额外容器概念。

当依赖图大到手工装配反而难维护时再评估 DI 工具，而不是为了“看起来工程化”提前引入。

## 3. 类型化配置

所有环境读取集中在 `bootstrap.LoadConfig`。业务包不能直接调用 `os.Getenv`。

当前启动必需：

```text
DATABASE_URL
RABBITMQ_URL
MAIL_PROVIDER=fake
```

`MAIL_INSTANCE_ID` 默认使用 hostname，也可以显式覆盖。连接池、batch、并发、poll delay、
lease、publish/provider/finalize timeout、重试范围、consumer lanes/prefetch 和 shutdown
timeout 都有类型化默认值与范围校验。

解析错误只报告变量名和期望格式，不回显原始值。因此包含 PostgreSQL/RabbitMQ 密码的 URL
不会进入配置错误日志。

### 为什么 Fake Provider 必须显式配置

Fake Provider 会返回“Provider 已接受”，但不会真的发邮件。若把它作为未配置时的默认值，
生产环境漏配 Provider 就会产生最危险的假成功。因此当前必须显式设置：

```text
MAIL_PROVIDER=fake
```

后续 SMTP 完成后会扩展为明确的 Provider/Router 配置，仍不会静默回退到 Fake。

## 4. 数据库启动检查

`pgxpool.NewWithConfig` 本身不保证数据库当前可用，所以 Bootstrap 会：

1. 安全解析 `DATABASE_URL`；
2. 设置 min/max connections、connect timeout、lifetime、idle time 和 lifetime jitter；
3. `Ping` PostgreSQL；
4. 使用 `to_regclass` 确认 `tenants`、`mail_messages`、`outbox_events`、
   `delivery_attempts` 四张表都存在，并确认 Goose schema version 至少为 3。

数据库可以 Ping 但 migration 未执行时，服务拒绝启动。这避免 Scheduler/Relay 启动后不停
报告“relation does not exist”，同时 readiness 却错误显示健康。

应用不会自动执行 Migration。生产环境应由独立部署步骤执行 `make migrate-up`，否则多个
应用副本同时修改 Schema，会把数据库发布权限、锁等待和应用启动绑定在一起。

## 5. Poll Runner：把 RunOnce 变成持续服务

Scheduler 和 Relay 保持单批次 `RunOnce`，运行时使用通用 Poll Runner 包装：

```text
RunOnce 有工作
  → 立即领取下一个有界 batch

RunOnce 返回空 batch
  → 等待 IdleDelay

RunOnce 返回瞬时错误
  → exponential full jitter
  → 重试

RunOnce 返回不变量/腐败数据错误
  → 退出 Poller
  → Supervisor 取消整个进程
```

有积压时立即续跑是为了吞吐；空闲时延迟轮询是为了避免持续打空 SQL；基础设施错误退避是
为了防止数据库故障时形成日志和连接热循环。

错误并非一律重试：Repository/Transaction 暂时失败通常可恢复；`SchedulerInvariant`、
`OutboxRelayInvariant`、腐败记录和非法持久化数据说明代码或数据约束已被破坏。继续无限
重试只会制造噪声，所以这些错误会让组件失败并交给 Supervisor。

## 6. Staged Supervisor

所有组件正常运行时并发启动；收到 SIGINT/SIGTERM 后按 stage 取消：

```text
Stage 0  readiness monitor
         └── 先变成 NOT_SERVING，停止接收新流量

Stage 1  Scheduler + Outbox Relay
         └── 停止产生和发布新 dispatch 工作

Stage 2  RabbitMQ Consumer
         └── 停止领取，等待有界 Worker finalize

Stage 3  gRPC Health Server
         └── 最后停止健康端点

Close    RabbitMQ Publisher → PostgreSQL Pool
```

Supervisor 使用 `context.WithoutCancel` 保留 context values，但自己控制各 stage 的取消时机。
这不是忽略关机，而是把一次父 context cancellation 转换成有顺序的 drain。

如果任意组件在正常运行期间意外返回，无论 error 是否为 nil，都视为异常：立即取消所有
组件，等待有界收尾并让进程返回失败。否则可能出现“进程还活着，但 Scheduler 已经死了”的
半失效状态。

总关机受 `MAIL_SHUTDOWN_TIMEOUT` 限制；它必须大于 Consumer 自身的 shutdown timeout。

## 7. Liveness 与 Readiness

进程启动了标准 gRPC Health Checking Service，目前有三个 service name：

| Service | 含义 |
| --- | --- |
| `mailservice.liveness.v1` | gRPC 进程事件循环仍存活 |
| `mailservice.worker.v1` | PostgreSQL 可达且 RabbitMQ Consumer 已启动全部 lanes |
| 空字符串 `""` | 当前进程的总体 readiness，与 Worker readiness 相同 |

RabbitMQ Consumer 在 topology 声明和 lanes 启动后将 `Ready()` 设为 true，连接断开、重连间隙
和关机时变回 false。Readiness Monitor 还会使用有界 timeout Ping PostgreSQL。

Provider 故障暂时不影响进程 readiness：邮件仍可持久化和稍后重试。后续有真实 Provider
Router 后，Provider health 会进入路由、熔断和降级，而不是粗暴杀死整个 Submission API。

07-C 当时 gRPC Endpoint 只注册 Health Service；08-A2 已注册 Submit/Get。当前仍使用明确配置
的 plaintext 开发身份，生产 mTLS 仍属于后续安全阶段。

## 8. main 的职责

`cmd/mail-service/main.go` 只做操作系统边界工作：

1. 创建 JSON `slog` logger；
2. 监听 SIGINT/SIGTERM；
3. 加载配置；
4. `bootstrap.NewApp`；
5. `app.Run`；
6. 将启动或运行失败转换为进程退出码。

它不包含 SQL、RabbitMQ settlement、状态机或 Provider 逻辑。

## 9. 启动与健康检查

```bash
cp .env.example .env
make infra-up
make migrate-up

set -a
. ./.env
set +a
make run
```

可选使用 `grpcurl` 检查：

```bash
grpcurl -plaintext \
  -d '{"service":"mailservice.worker.v1"}' \
  localhost:8080 grpc.health.v1.Health/Check
```

构建二进制：

```bash
make build
```

产物位于被 Git 忽略的 `bin/mail-service`。

## 10. 主要代码

- `cmd/mail-service/main.go`：signal、日志和退出码；
- `internal/bootstrap/config.go`：环境配置、默认值和交叉校验；
- `internal/bootstrap/app.go`：Composition Root、数据库 preflight、资源关闭；
- `internal/bootstrap/poller.go`：batch drain、idle wait 和错误退避；
- `internal/bootstrap/supervisor.go`：异常联动和分阶段停机；
- `internal/bootstrap/grpc_endpoint.go`：标准 gRPC Health Server；
- `internal/bootstrap/readiness.go`：PostgreSQL + Consumer 动态 readiness；
- `internal/consumer/rabbitmq/consumer.go`：新增线程安全 `Ready()` 状态；
- `internal/integration/runtime_composition_test.go`：完整进程级纵向测试；
- `.env.example`、`Makefile`：本地启动入口。

## 11. 如何验证

单元与静态检查：

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/mail-service
```

运行时纵向集成测试：

```bash
TEST_POSTGRES_IMAGE=postgres:18.4-alpine \
TEST_RABBITMQ_IMAGE=rabbitmq:4.3.4-management-alpine \
go test -tags=integration ./internal/integration \
  -run '^TestRuntimeComposition$' -count=1 -v -timeout=4m
```

真实测试验证：

1. PostgreSQL 可连接但未迁移时，`NewApp` 返回 `ErrStartup`；
2. 迁移完成并应用 RabbitMQ Policy；
3. 在数据库原子创建一封立即邮件及 Outbox；
4. App 启动后 Worker Health 变为 `SERVING`；
5. Relay 发布、Consumer 消费、Fake Provider 接受；
6. Message 最终为 `PROVIDER_ACCEPTED`；
7. 恰好有一个 Delivery Attempt；
8. 该消息没有遗留 Pending Outbox；
9. context cancellation 后进程在时限内退出。

## 12. 面试表达

### 30 秒版本

> 我通过 Composition Root 把之前独立测试的 PostgreSQL、Scheduler、Outbox Relay、
> RabbitMQ Publisher/Consumer、Dispatch Worker 和 Fake Provider 装配成一个可运行进程。
> Scheduler/Relay 用 Poll Runner 做空闲轮询和错误退避，Supervisor 负责组件异常联动与
> 分阶段优雅停机。服务提供标准 gRPC Health：只有数据库正常且 Consumer lanes 就绪才
> readiness serving。真实集成测试跑通了从数据库 Outbox 到 Fake Provider 的完整链路。

### 2 分钟版本

> 我把所有具体依赖限制在 internal/bootstrap 的 Composition Root，领域和应用层仍然只依赖
> ports，没有引入 DI 框架。启动时先构造有界 pgxpool、Ping 数据库，并确认核心迁移表存在；
> Migration 仍由独立部署步骤执行。
>
> Scheduler 和 Relay 保留可测试的 RunOnce，再由通用 Poll Runner 包装：有 backlog 就连续
> drain，空 batch 才 idle wait，基础设施错误 full-jitter 退避，腐败数据或不变量错误则退出。
> Staged Supervisor 在组件意外退出时取消所有 peers；正常 SIGTERM 时先撤 readiness，再停
> Scheduler/Relay、Consumer、gRPC，最后关闭 Publisher 和 DB。
>
> 标准 gRPC Health 区分 liveness 和 worker readiness。Rabbit Consumer 在连接与 lanes
> 就绪时暴露原子状态，readiness 还会有界 Ping PostgreSQL。Testcontainers 测试先证明未迁移
> 会拒绝启动，然后让一封邮件真实经过 Outbox、RabbitMQ、Worker、Fake Provider，检查最终
> 状态、Attempt、Outbox 清空和安全停机。

## 13. 可能追问

**为什么数据库连不上时启动失败，RabbitMQ 连不上却可以启动后重连？**

PostgreSQL 是任务和状态真相，当前后台角色没有数据库就无法做任何安全工作；RabbitMQ 是
可恢复的传输层，Outbox 能保存待发布事件，Consumer/Publisher 也实现了重连。进程可存活，
但 RabbitMQ Consumer 未就绪时 readiness 会是 NOT_SERVING。

**为什么应用不自动 Migration？**

生产 Schema 变更需要独立权限、审计、回滚和发布顺序。多个 App 副本竞争 Migration 还可能
延长启动或持锁。应用只做兼容性 preflight，部署流水线负责迁移。

**所有错误都让进程退出吗？**

不会。数据库连接等基础设施错误由 Poll Runner 退避；持久化数据腐败和应用不变量破坏才
视为 fatal。前者通常等待恢复，后者重试不会变好，需要告警和人工介入。

**一个进程同时运行所有角色，怎么水平扩展？**

Scheduler/Relay 的 `SKIP LOCKED` 和 Outbox lease、Consumer 的 competing consumers 都支持
多实例。当前先提供 `all` 运行形态跑通纵向链路；后续再增加 role 开关，让不同职责独立
扩缩容，而不拆业务代码。

## 14. 尚未解决

- Batch、Cancel、ListEvents gRPC handler 尚未实现；
- lifecycle 状态事件的通知 Consumer 与 AI-Nexus 联调；
- 真实 SMTP、Payload 解密、模板渲染与 MIME；
- TLS/mTLS、认证授权和密钥管理；
- OpenTelemetry Metrics/Trace 与运行告警；
- 全局数据库/Provider 故障的暂停消费与熔断控制；
- DLQ 管理、安全重放和陈旧 Attempt Reconciler；
- `--role` 分角色启动，目前固定运行后台投递链路的 `all` 形态。

下一步进入 08-A1：先实现可靠受理用例与安全 Payload 持久化，再由 08-A2 接入 gRPC，
把当前后台投递链路开放给第一个真实调用方，而不是继续依赖测试直接写数据库。
