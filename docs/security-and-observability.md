# 安全、多租户与可观测性

## 1. 威胁边界

主要风险：

- 未授权服务滥发邮件；
- 租户越权使用模板、发件身份或 Provider；
- 日志和 Trace 泄露邮箱、验证码或正文；
- 动态模板和 callback URL 引入注入、SSRF；
- Provider Webhook 伪造或重放；
- SMTP 凭据泄露；
- 单租户流量耗尽全局容量；
- 模板渲染产生超大消息或恶意链接；
- 运维重放已失效或已清理的敏感任务。

## 2. 身份认证与授权

服务间默认使用 mTLS：

- 证书身份映射到租户和应用；
- 服务端验证 CA、SAN、有效期和吊销策略；
- 请求中的租户字段只用于审计一致性检查；
- 本地开发必须显式允许 insecure 模式。

如需要面向无法使用 mTLS 的客户端，再增加：

- 短期 OAuth2 access token；或
- 可轮换、可撤销、仅保存哈希的 API key。

授权至少检查：

- 可用模板和版本；
- 可用发件身份；
- 可用邮件类别和优先级；
- 单次/每日配额；
- 是否允许计划发送、批量发送或 Raw Email；
- 可用状态订阅目标。

## 3. 数据保护

- 传输：外部连接使用 TLS，内部生产环境使用 mTLS；
- 静态：邮箱、Reply-To 和模板变量使用 envelope encryption；
- 主密钥：来自 KMS/Vault，数据库只保存 key version 和密文；
- 指纹：使用独立 HMAC 密钥，不能复用加密密钥；
- 日志：邮箱只允许不可逆指纹或有限脱敏；
- 清理：一旦不再需要后续发送就按保留策略擦除敏感 payload；
- 备份：数据库备份同样加密并受保留策略约束；
- 模板：HTML 使用受限模板引擎，变量默认转义；
- 附件：V1 不支持；后续使用对象存储引用、病毒扫描和大小限制。

## 4. 防滥用

限流维度：

- 调用方身份；
- 租户；
- 发件身份；
- 收件邮箱指纹；
- 收件域；
- 模板；
- Provider 凭据；
- 全局类别。

限流采用两层：

- API 层快速 Token Bucket，保护入口；
- 数据库/Redis 中的共享配额，保证多实例总量。

容量不足时优先保障 `CRITICAL`，但所有租户仍有硬上限，避免“高优先级”成为绕过
配额的入口。

## 5. Provider Webhook

- 每个 Provider 独立入口和签名验证器；
- 校验签名、时间戳和重放窗口；
- Provider event ID 建唯一约束；
- 请求体限制大小；
- 原始内容不进入普通日志；
- 先可靠落库再返回成功；
- 解析失败进入隔离队列，不能无限快速重试；
- Webhook 不直接调用租户系统。

## 6. 可观测性

采用 OpenTelemetry 生成 Trace 和 Metrics，日志使用结构化 JSON。

### 6.1 Trace

跨以下边界传播 W3C Trace Context：

- gRPC Submission；
- Outbox 事件；
- RabbitMQ 消息；
- Worker Attempt；
- Provider 请求；
- Notification Delivery。

异步阶段使用 Span Link 关联原始提交，不人为制造持续数小时的单个 Span。

### 6.2 Metrics

关键指标：

```text
mail_submissions_total{tenant_tier,category,result}
mail_submission_duration_seconds
mail_scheduled_lag_seconds{category}
mail_outbox_lag_seconds
mail_queue_depth{category}
mail_queue_oldest_age_seconds{category}
mail_attempts_total{provider,category,result,error_category}
mail_provider_duration_seconds{provider}
mail_delivery_end_to_end_seconds{category}
mail_callbacks_total{transport,result}
mail_callback_lag_seconds
mail_dead_letter_total{category,error_category}
mail_circuit_state{provider,credential_group}
mail_sensitive_payload_pending_cleanup
```

禁止使用 tenant ID、message ID、完整错误消息和邮箱作为 Metrics Label。

### 6.3 Logs

允许字段：

- message ID；
- tenant key（若租户数量受控，否则用内部 tier）；
- attempt number；
- provider；
- route version；
-状态；
- 稳定错误码；
- Trace/Span ID。

禁止字段：

- 验证码；
- 邮件正文和模板变量；
- 完整收件邮箱；
- SMTP/API 凭据；
- Provider 原始响应；
- mTLS 证书内容。

## 7. SLO 初稿

先以事务邮件为目标：

| 指标 | 初始目标 |
| --- | --- |
| Submission API 可用性 | 99.9% |
| Submission API p99 | < 200 ms |
| 已受理立即任务进入可消费队列 p99 | < 2 s |
| 健康 Provider 下 CRITICAL 首次尝试 p99 | < 5 s |
| 状态通知延迟 p99 | < 10 s |
| 已接受任务内部丢失率 | 0（以审计和对账验证） |

Provider 最终送达率不能直接作为本服务 SLO，因为它受地址质量和外部供应商影响；
但应作为分 Provider、域名和租户的业务指标监控。

## 8. 健康检查

- Liveness：进程事件循环和关键 goroutine 未失效；
- Readiness/API：数据库可写、密钥可用、容量保护未触发；
- Readiness/Worker：数据库、RabbitMQ、必要密钥可用；
- gRPC 使用标准 Health Checking Service；
- Provider 故障不直接让 Submission API 失去 readiness；
- 控制面故障时使用最后一个有效配置快照继续运行。

## 9. 审计

以下操作需要不可抵赖的审计记录：

- 模板发布和回滚；
- Provider 凭据和路由变更；
- 租户暂停/恢复；
- 配额变更；
- DLQ 查看和重放；
- 敏感数据解密；
- Raw Email 权限授予；
- 数据删除和保留策略变更。

审计日志与业务日志分离，并有更严格的访问控制和保留周期。
