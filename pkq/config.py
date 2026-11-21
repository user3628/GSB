"""服务配置：三重预算、分页上限、切分阈值。所有值确定性生效。"""
from __future__ import annotations

from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class Config:
    # 点查切分
    max_keys_per_query: int = 64          # 主键集合数量阈值，超过返回 E_QUERY_TOO_LARGE
    # 三重预算
    max_concurrent_tasks: int = 2         # 全局并发上限
    task_memory_budget_bytes: int = 4096  # 单任务内存预算（解压后估计）
    round_cost_budget: int = 10_000       # 整轮时间预算（确定性解压代价单位）
    scan_queue_capacity: int = 8          # 有界等待队列
    # 分页
    max_page_size: int = 128              # 服务端页大小硬上限
    default_page_size: int = 16
    # 首轮结果截止（秒），提交后须在此时间内给出首页或 NOT_READY
    first_page_deadline_sec: float = 5.0
    # 验证钩子：每个任务提交前延迟（秒），模拟慢速解压以便构造强制终止窗口
    task_commit_delay_sec: float = 0.0

    def effective_page_size(self, client_page_size: int | None) -> int:
        if client_page_size is None or client_page_size <= 0:
            return min(self.default_page_size, self.max_page_size)
        return min(client_page_size, self.max_page_size)

    def to_dict(self) -> dict:
        return asdict(self)


DEFAULT_CONFIG = Config()
