"""本地日志增量索引与崩溃恢复组件。

模块划分：
- errors:   统一业务错误码
- models:   批次状态机与数据模型
- parser:   确定性日志行解析
- indexing: 基于 numpy 的区间合并与统计聚合
- store:    SQLite 连接与 schema 管理
- engine:   单写者引擎（提交、级联失效、恢复）
- api:      FastAPI HTTP 接口
"""

from .engine import LogIndexEngine
from .models import BatchState

__all__ = ["LogIndexEngine", "BatchState"]
__version__ = "1.0.0"
