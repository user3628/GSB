"""资源编排依赖图诊断工作台。

模块划分：
- errors:    业务错误码与异常类型
- canonical: 确定性 JSON 规范化与摘要
- models:    资源、快照、增量片段数据模型
- validation: 样本规格校验（E_SPEC_INVALID / E_SNAPSHOT_INVALID）
- graph:     拓扑分层、环检测、影响面、批次、权重一致度
- storage:   SQLite 持久化与意图日志（单写者）
- service:   提交/重建/查询的事务边界与恢复逻辑
- api:       本地回环 HTTP 接口（FastAPI）
"""

__all__ = ["__version__"]
__version__ = "1.0.0"
