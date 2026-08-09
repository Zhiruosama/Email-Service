# 分阶段实施路线

## 阶段 0：架构冻结

交付：

- 确认技术基线；
- 冻结通用 Proto V1；
- 确认 AI-Nexus 适配方式；
- 数据库 ERD 和迁移策略；
- 状态机与错误码表；
- 安全和密钥方案；
- 本地开发拓扑。

退出标准：

- 所有协议术语与状态语义唯一；
- 没有共享 AI-Nexus 数据库或 Redis；
- 明确 SMTP 不确定结果策略；
- 明确模板版本与变量 Schema。

## 阶段 1：可靠受理内核

实现：

- Go 项目骨架；
- 配置、日志、Tracing、Metrics；
- PostgreSQL migrations；
- gRPC Submission 和 Query；
- 租户认证；
- 幂等任务持久化；
- Transactional Outbox；
- Scheduler；
- RabbitMQ Relay；
- Fake Provider；
- 基础 Worker 和状态机。

验证：

- API 落库前后崩溃；
- Outbox 重复发布；
- Worker 重复消费；
- 定时任务重启恢复；
- 相同 key 不同 payload 被拒绝。

## 阶段 2：状态通知和 AI-Nexus 联调

实现：

- Notification Outbox；
- gRPC Subscriber；
- 状态查询和对账；
- `verification_code.v1` 模板；
- AI-Nexus 协议适配；
- 回调重复、延迟和乱序处理。

验证：

- AI-Nexus 全链路验证码；
- Provider 接受后激活；
- 回调暂时不可用后恢复；
- 超过截止时间不再投递。

## 阶段 3：真实 SMTP

实现：

- 已完成：加密 Payload 认证解密与不可变身份交叉校验；
- 已完成：固定版本模板渲染、HTML + Plain Text MIME；
- 已完成：构建失败的可重试/永久失败分流；
- 已完成：implicit TLS SMTP Provider 与 LOGIN/PLAIN 授权；
- 已完成：QQ SMTP 配置加载与运行时显式 Provider 选择；
- 已完成：协议阶段超时、4xx/5xx 与 DATA 不确定结果分类；
- 已完成：双重显式授权的 QQ SMTP smoke test；
- 已完成：单实例 Provider Token Bucket 限速；
- 已完成：非阻塞 Provider 并发舱壁；
- 已完成：本地 Provider `CLOSED/OPEN/HALF_OPEN` 熔断器；
- 已完成：Provider OpenTelemetry 调用、拒绝与熔断指标埋点；
- 进程级 OpenTelemetry SDK 与 Exporter；
- Retry Scheduler；
- DLQ 和安全重放；
- 终态敏感 payload 清理。

验证：

- 临时失败后成功；
- 认证失败熔断；
- SMTP 4xx/5xx 分类；
- DATA 后连接断开的不确定结果；
- QQ SMTP 限速和连接复用行为。

## 阶段 4：通用多租户能力

实现：

- 控制面 API；
- 模板发布工作流；
- 多发件身份；
- 多 Provider 路由；
- 租户级配额和抑制列表；
- 批量提交拆分；
- Provider Webhook；
- `DELIVERED/BOUNCED/COMPLAINED`。

验证：

- 租户隔离；
- Provider 故障转移；
- Critical 与 Bulk 舱壁；
- 模板回滚；
- Webhook 签名、重复和乱序。

## 阶段 5：生产化

实现：

- mTLS 和证书轮换；
- KMS/Vault；
- HA PostgreSQL；
- 三节点 RabbitMQ Quorum Queue；
- SLO Dashboard 和告警；
- 数据保留与合规删除；
- 压测、故障注入和灾难恢复演练；
- Runbook。

## 暂不提前实现

以下能力只保留扩展点：

- 可视化模板编辑器；
- 营销活动编排；
- 附件；
- Raw Email；
- 跨区域主动主动部署；
- Kafka/Stream 事件出口；
- 用户邮件偏好中心。

## 建议的第一条纵向切片

第一条可运行链路应当是：

```text
SubmitEmail
→ PostgreSQL Message + Outbox
→ Relay
→ RabbitMQ
→ Worker
→ Fake Provider
→ Message 状态
→ Notification Outbox
→ Fake Subscriber
```

这条链路跑通并通过崩溃恢复测试以后，再接 QQ SMTP。这样 SMTP 只是一个边缘
Adapter，不会反过来塑造整个系统。
