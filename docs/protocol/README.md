# gRPC V1 协议

规范源位于：

```text
api/proto/mailservice/delivery/v1/
```

阅读顺序：

1. [V1 调用语义与约束](grpc-v1.md)
2. [错误模型](error-model.md)
3. [AI-Nexus 适配](ai-nexus-adapter.md)

生成的 Go 文件位于 `gen/go/`，不得手工修改。

V1 发布后：

- 字段编号不得复用；
- 删除字段必须同时保留字段编号和名称；
- 枚举编号不得重新解释；
- 新字段应保证旧客户端忽略后仍能安全运行；
- 破坏性变更进入新的 Protobuf package 版本。
