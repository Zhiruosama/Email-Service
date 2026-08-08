# 阶段 02：通用 gRPC V1 契约与工程骨架

- 状态：已完成
- 阶段目标：冻结各模块和调用方共同依赖的公共协议，并建立可重复生成和验证的 Go 骨架

## 1. 为什么先做协议

数据库、Scheduler、Worker 和 AI-Nexus 都需要共享以下语义：

- 什么叫邮件已受理；
- 幂等键如何使用；
- 有哪些投递状态；
- 什么时候可以取消；
- 批量提交是否原子；
- 状态事件如何处理重复和乱序。

如果先分别开发，最终容易出现同名状态含义不同、字段无法兼容或客户端需要猜测行为。

## 2. 实现内容

### 2.1 中立协议包

使用：

```text
mailservice.delivery.v1
```

而不是 `ainexus.mail.v1`，使核心协议能够服务多个业务系统。

### 2.2 DeliveryService

提供：

```text
SubmitEmail
BatchSubmitEmail
GetEmail
CancelEmail
ListEmailEvents
```

`SubmitEmail` 返回 `ACCEPTED` 只表示 Message 和 Outbox 已可靠持久化，不表示已经进入
RabbitMQ，更不表示 Provider 或收件箱已经接受。

### 2.3 单收件人模型

一个逻辑 Message 只对应一个收件人。批量提交会拆成独立消息，因为不同地址可能有
不同重试、退信、投诉和状态，放在同一 SMTP Envelope 会造成状态和隐私耦合。

### 2.4 模板协议

调用方传递：

```text
template key + immutable version + locale + validated variables
```

默认不允许传任意 Subject、HTML、SMTP 参数或 callback URL。模板变量由发布时的
Schema 校验，降低注入和接口失控风险。

### 2.5 幂等语义

```text
(authenticated tenant, idempotency_key)
```

是逻辑唯一键：

- 相同 key、相同 Payload：返回原 Message 和 `DUPLICATE`；
- 相同 key、不同 Payload：返回 `ALREADY_EXISTS`；
- RPC 超时：调用方必须用相同 key 查询或重试。

### 2.6 通用状态通知

业务系统通过预注册的 `DeliveryEventReceiverService` 接收事件。请求不能携带动态回调
URL，避免 SSRF、凭据混乱和任意目标攻击。

事件通过：

```text
event_id + message_id + sequence
```

实现重复接收幂等和乱序保护。

## 3. 工程工具链

当前固定：

- Go 1.25.12；
- Buf 1.72.0；
- `protoc-gen-go` 1.36.11；
- `protoc-gen-go-grpc` 1.5.1。

`go.mod` 的 `tool` 声明固定生成插件，Makefile 同时提供 `protoc` 离线生成和 Buf 完整
lint/format 检查。

生成代码位于：

```text
gen/go/mailservice/delivery/v1/
```

生成代码不手工修改。

## 4. 可执行兼容守卫

`internal/contract/schema_test.go` 通过 Protobuf Descriptor 检查：

- RPC 名称和顺序；
- 核心字段编号；
- DeliveryStatus 数字；
- 所有枚举必须有 `UNSPECIFIED = 0`；
- 核心请求不得出现 `tenant_id`、动态 callback URL、Raw HTML 或 SMTP 配置。

这些测试不能替代 Buf breaking check，但可以让关键协议边界在普通 `go test` 中立即
暴露。

## 5. AI-Nexus 映射

AI-Nexus 继续生成验证码并保存 HMAC 摘要，映射为：

```text
request_id              → idempotency_key
verification_code.v1    → template key
验证码变量               → content.variables
CRITICAL                → category
两分钟                  → dispatch deadline
AVOID_DUPLICATE         → ambiguous-result policy
```

`PROVIDER_ACCEPTED` 或可信 `DELIVERED` 首次到达后，AI-Nexus 幂等激活验证码；后续事件
不得延长有效期。

## 6. 主要文件

- `api/proto/mailservice/delivery/v1/common.proto`
- `api/proto/mailservice/delivery/v1/delivery.proto`
- `api/proto/mailservice/delivery/v1/event.proto`
- `docs/protocol/`
- `internal/contract/schema_test.go`
- `buf.yaml`
- `buf.gen.yaml`
- `Makefile`
- `go.mod`

## 7. 验证结果

完成时通过：

```text
make check-all
go test ./...
go vet ./...
Buf format
Buf lint
protoc compile
Go bindings generation
```

## 8. 面试表达

### 30 秒版本

> 我没有让邮件协议绑定 AI-Nexus，而是设计了中立的 `mailservice.delivery.v1`。协议
> 明确区分可靠受理、Provider 接受和最终送达，支持租户级幂等、定时发送、截止时间、
> 批量拆分、查询、取消和至少一次状态通知。关键字段编号与状态枚举通过 Descriptor
> 测试保护，代码生成工具版本也被固定，避免不同环境生成结果漂移。

### 可能追问

**为什么租户 ID 不放请求体？**

租户必须来自 mTLS 或 Token 对应的可信身份。若只相信请求体，调用方可能伪造其他
租户身份。

**为什么批量接口不是一个大事务？**

每个收件人是独立 Message，需要独立幂等、状态和重试。批量接口只是减少调用次数，
不是把所有邮件绑定成一个失败或成功单元。

**为什么有 `SUBMISSION_UNKNOWN`？**

SMTP 提交正文后连接中断时，Provider 可能已经接受，也可能没有。把它明确建模，才能
根据消息类别选择避免重复或偏向送达，而不是把未知错误当成确定失败。

## 9. 尚未解决

当前只有协议和工程骨架，尚未实现状态转换、数据库持久化、MQ 和 Provider。下一步
先实现不依赖基础设施的领域状态机，再让数据库和 Worker 复用同一套规则。
