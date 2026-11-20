# HTTP API（仅标准库，默认监听 127.0.0.1:8080）

所有请求/响应均为 JSON；错误响应体形如
`{"code": "...", "message": "...", "details": {...}}`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查与工作区路径 |
| POST | `/api/samples/import` | 导入样本（空 body 时读取工作区样本文件） |
| POST | `/api/analyze` | 触发分阶段分析，body 可带 `doc_id`；受停止标记控制 |
| POST | `/api/stop` | 写入停止标记，请求优雅停止 |
| POST | `/api/resume` | 清除停止标记并恢复全部文档 |
| GET | `/api/status` | 全部文档状态；`?doc_id=<id>` 查单文档（含指纹与失效事件） |
| GET | `/api/documents/<id>/sequence` | 块序列、裁决边、置信度 |
| GET | `/api/documents/<id>/spanning` | 跨栏统计 |
| GET | `/api/documents/<id>/result` | 最终汇总结果（含 result_digest） |
| POST | `/api/documents/<id>/update` | 单文档增量更新并与全量构建逐字节比对 |
| GET | `/api/invalidations` | 失效传播事件流 |

并发分析由进程内运行锁串行化；文档级发布由 `fcntl` 文件锁与序号规则保护。
