# 阶段 04-B：PostgreSQL Repository 与乐观锁

- 状态：已完成
- 阶段目标：让 Message 状态机可以可靠地创建、恢复和并发保存到 PostgreSQL，同时不把数据库细节泄漏到应用层

## 1. 解决的问题

04-A 只有表和约束，Go 代码还不能把 Message Snapshot 写入或读回。直接在 API、
Scheduler 和 Worker 中散写 SQL 会造成：

- 每个模块各自解释 nullable 字段和状态字符串；
- PostgreSQL 错误码泄漏到 gRPC 层；
- 幂等创建存在先查后插的竞态；
- 两个请求都从旧版本更新，后提交者覆盖先提交者；
- Repository 提前清空领域事件，事务失败时丢失后续 Outbox 输入；
- Repository 只能使用连接池，下一阶段无法复用到同一事务。

本阶段增加应用端口和 PostgreSQL Adapter，把这些规则集中在一个持久化边界。

## 2. 分层结构

```text
Application
    │ 依赖 MessageRepository 接口和稳定错误
    ▼
internal/application/ports
    ▲
    │ 实现接口
internal/storage/postgres
    │
    ▼
pgxpool.Pool 或 pgx.Tx
```

应用层定义自己需要什么，PostgreSQL Adapter 实现它。应用层不导入 pgx，也不识别
SQLSTATE 或 constraint name。

## 3. MessageRecord

`Message` 聚合只管理生命周期。租户、幂等键、category 等受理身份不参与状态转换，
因此放在应用层 `MessageRecord`：

```text
TenantID
IdempotencyKey
PayloadFingerprint [32]byte
Category
Priority
DuplicateRiskPolicy
Message *message.Message
```

Fingerprint 使用固定 `[32]byte`，在 Go 类型层表达 HMAC-SHA256 长度，不允许调用方
传任意长度切片。它不是邮件正文，也不是可以离线枚举的普通邮箱哈希。

边界校验包括：

- Tenant 和 Message ID 必须是 UUID；
- 幂等键必须为 1..255 bytes 且没有首尾空白；
- category、priority 和 duplicate risk policy 合法；
- Create 时 Message version 必须为 0。

数据库 CHECK 和唯一约束仍然保留，形成应用校验与数据库约束两道防线。

## 4. Repository 契约

```text
Create
GetByID
GetByIdempotencyKey
Save
```

`Create` 返回：

```text
CREATED
DUPLICATE
```

稳定错误包括：

```text
ErrMessageNotFound
ErrConcurrentUpdate
ErrIdempotencyConflict
ErrMessageIDConflict
ErrInvalidMessageRecord
ErrCorruptMessageRecord
ErrMessageRepository
```

应用层通过 `errors.Is` 判断语义，不依赖 PostgreSQL 驱动错误。

## 5. 幂等创建为什么使用 ON CONFLICT

错误方案是：

```text
SELECT 是否存在
    ↓ 不存在
INSERT
```

两个请求可能同时 SELECT 到“不存在”，随后都 INSERT，仍然必须依赖数据库唯一约束。
因此 Repository 直接尝试原子插入：

```sql
INSERT INTO mail_messages (...)
VALUES (...)
ON CONFLICT ON CONSTRAINT mail_messages_tenant_idempotency_unique
DO NOTHING
RETURNING version;
```

结果分三类：

1. 返回 version：本次成功创建；
2. 没有返回行：幂等键已存在，读取已有记录并比较 fingerprint；
3. Message 主键冲突：返回独立的 Message ID conflict。

没有采用“让 INSERT 报 23505 再查询”的方式，因为 PostgreSQL 事务内一条语句发生
约束异常后，事务会进入 aborted 状态，不能继续查询已有记录。`DO NOTHING` 不会让
事务失败，04-C 可以在同一个事务里继续写 Outbox。

Fingerprint 比较结果：

```text
相同 tenant + key + fingerprint → DUPLICATE，返回已有 Message
相同 tenant + key，不同 fingerprint → ErrIdempotencyConflict
不同 tenant，相同 key → 两条独立 Message
```

## 6. Snapshot 映射

查询结果先映射为 `message.Snapshot`，然后调用：

```go
message.Restore(snapshot)
```

Repository 不直接组装私有 Message 字段。这样数据库记录即使绕过 CHECK 被人为改坏，
领域层仍会拒绝未知状态、无效 Attempt、缺失 generation 或错误时间关系，并返回
`ErrCorruptMessageRecord`。

Nullable PostgreSQL 字段使用 pgx 类型显式处理：

```text
pgtype.Timestamptz
pgtype.Text
pgtype.Bool
```

这覆盖 scheduled time、retry time、Provider 信息和三列一组的 Failure。数据库 BIGINT
和 INTEGER 在转换成 Go `uint64/uint32` 前检查负数与范围，防止静默溢出。

`Restore` 不产生领域事件，因此一次查询不会被误认为一次新的状态变化。

## 7. 乐观锁

Save 使用：

```sql
UPDATE mail_messages
SET
    status = $1,
    ...,
    version = version + 1
WHERE id = $15
  AND version = $16
RETURNING version;
```

如果没有返回行，说明当前数据库 version 已经不是调用方读取时的 version，返回
`ErrConcurrentUpdate`。

真实并发测试使用两个独立聚合：

```text
都读取 SCHEDULED version 0
        ├── Candidate A: Queue
        └── Candidate B: Cancel

两个 goroutine 同时 Save
        ├── 一个成功，version 变成 1
        └── 一个 ErrConcurrentUpdate
```

最终数据库状态必须与成功者一致，失败者不能覆盖它。业务层以后处理冲突时要重新
Load 并重新执行状态机，不能盲目重复旧 UPDATE；例如重新读取后发现已经 CANCELED，
Queue 就应该被状态机拒绝。

## 8. 为什么 Save 不修改传入 Message 的 version

Message 是请求级对象，每个命令通常只保存一次。Save 返回新的数据库 version，但不
修改聚合中的旧 version：

- Repository 不需要获得修改领域对象内部持久化元数据的特殊权限；
- 并发失败时对象不会出现“看起来已经保存”的假状态；
- 下一次命令重新从数据库 Load；
- 04-C 提交完成后可以返回保存结果，原请求对象随请求结束被丢弃。

如果调用方再次使用同一个旧对象 Save，会得到 `ErrConcurrentUpdate`，不会覆盖数据。

## 9. Repository 不处理领域事件

Create 和 Save 都不会调用：

```go
PullEvents()
```

原因是 Repository 只保存 Message，不知道 Outbox 是否成功。如果 Message 保存后立刻
清空事件，而同一事务里的 Outbox 写入失败，内存事件就已经丢失。

04-C 的事务协调者会按顺序执行：

```text
读取 PendingEvents
→ 保存 Message
→ 写 Outbox
→ COMMIT
→ 提交成功后再清理请求级事件
```

本阶段测试明确检查成功和冲突 Save 都不会清空 pending events。

## 10. Pool 与 Tx 复用

PostgreSQL Repository 只依赖：

```go
type DBTX interface {
    QueryRow(context.Context, string, ...any) pgx.Row
}
```

`*pgxpool.Pool` 和 `pgx.Tx` 都满足这个接口，因此：

```text
NewMessageRepository(pool) → 普通独立操作
NewMessageRepository(tx)   → 参与调用方事务
```

集成测试验证了：

- Tx Repository 可以读到自己的未提交写入；
- Pool Repository看不到未提交行；
- Rollback 后行不存在；
- 事务中的幂等冲突返回 DUPLICATE 后，事务仍可继续执行 SQL。

## 11. 错误隔离

意外数据库错误映射为 `ErrMessageRepository`。底层 cause 保留给内部观测代码，但没有
放进 `errors.Unwrap` 链，也没有进入稳定错误文本。因此：

```text
errors.Is(err, ErrMessageRepository) == true
errors.As(err, *pgconn.PgError)      == false
```

context cancellation/deadline 不会被吞掉，调用方仍可以识别 `context.Canceled` 和
`context.DeadlineExceeded`。

## 12. 主要文件

- `internal/application/ports/message_repository.go`
- `internal/application/ports/message_repository_test.go`
- `internal/storage/postgres/message_repository.go`
- `internal/storage/postgres/message_queries.go`
- `internal/storage/postgres/message_mapping.go`
- `internal/storage/postgres/message_repository_test.go`
- `internal/integration/postgres_repository_test.go`
- `internal/testkit/postgrescontainer/postgres.go`

## 13. 验证

实际执行并通过：

```text
go test ./...
make test-integration
make check-all
go vet ./...
go vet -tags=integration ./internal/integration/...
go test -race ./...
go test -race -tags=integration ./internal/integration/...
go test -tags=integration -coverpkg=./internal/application/ports,./internal/storage/postgres ./internal/integration/...
```

应用端口普通单元测试语句覆盖率为 `92.6%`；由 integration package 驱动的应用端口与
PostgreSQL Adapter 跨包语句覆盖率为 `67.7%`。剩余分支主要是整数极限和数据库异常
防御路径，不能用 SQLite 或伪造成功路径代替真实 PostgreSQL 语义。

真实 PostgreSQL 测试覆盖：

- Scheduled Snapshot 完整往返；
- Retry Failure 和 nullable 字段往返；
- ID 和幂等键查询；
- 同 fingerprint 重复提交；
- 不同 fingerprint 幂等冲突；
- 跨租户相同 key；
- Message ID 冲突；
- Queue/Cancel 的真实并发 version 竞争；
- Pool/Tx 可见性、Rollback 和事务存活；
- 绕过 CHECK 后领域 Restore 拒绝损坏快照；
- pending events 不被 Repository 清空；
- 驱动错误不通过 unwrap 链泄漏。

## 14. 面试表达

### 30 秒版本

> 我在应用层定义 MessageRepository 端口，PostgreSQL Adapter 负责 Snapshot 映射、
> 租户级幂等和稳定错误。创建使用 `INSERT ON CONFLICT DO NOTHING`，避免先查后插竞态，
> 并保证幂等冲突不会让事务 aborted；更新使用 `WHERE id AND version` 乐观锁。真实并发
> 测试让 Queue 和 Cancel 同时保存，只有一个成功。Repository 同时兼容 Pool 和 Tx，
> 但不清空领域事件，为下一阶段 Transactional Outbox 保留事务边界。

### 2 分钟展开重点

1. 解释依赖倒置和为什么应用层不识别 pgx；
2. 解释 SELECT-then-INSERT 的竞态；
3. 解释唯一异常为什么会让 PostgreSQL 事务 aborted；
4. 用 Queue/Cancel 说明乐观锁防止 Lost Update；
5. 解释冲突后为什么必须重新运行状态机；
6. 解释为什么 Repository 不 PullEvents；
7. 用 Pool/Tx 同一 DBTX 接口说明为 Outbox 预留的扩展点。

### 可能追问

**为什么不用 `SELECT FOR UPDATE`？**

普通单消息命令冲突较少，乐观锁不需要在内存执行业务规则期间持有数据库锁。Scheduler
批量抢不同任务时才使用 `FOR UPDATE SKIP LOCKED`，两者解决的问题不同。

**乐观锁失败后能不能自动重试 UPDATE？**

不能直接重放旧 Snapshot。必须重新读取并重新调用状态机，因为另一个请求可能已经让
原操作失去业务合法性。

**为什么 Repository 返回 version 却不更新 Message？**

请求级对象保存后不复用，下一次命令重新 Load。这样失败对象不会被标记成已保存，也
不需要给基础设施层开放修改领域对象内部字段的权限。

**为什么不用 ORM？**

当前 SQL 数量很少，但幂等 conflict target、乐观锁和后续 SKIP LOCKED 都需要明确 SQL
语义。手写 SQL 的审查成本低于引入 ORM 抽象；查询规模增长后可以评估 sqlc，但事务
设计仍由应用负责。

## 15. 尚未解决

- 领域事件到 Outbox record 的映射；
- Message 与 Outbox 同一事务提交；
- 事务提交后的事件清理策略；
- gRPC SubmitEmail 应用服务；
- 加密收件地址、模板变量和模板版本；
- Scheduler、Relay、RabbitMQ 和 Worker。

下一阶段是 Transactional Outbox：用同一个 `pgx.Tx` 保存 Message 和 Outbox，并通过
故障测试证明两者不会出现半成功。
