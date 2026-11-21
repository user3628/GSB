# 需求覆盖矩阵

| # | 业务要求 | 实现位置 | 验证测试 / 演示 |
|---|----------|----------|------------------|
| 1 | 页模型、WAL 记录字段与 CRC、同页版本连续、损坏返回 E_WAL_CORRUPT 且整段不回放 | `models.py`, `wal.py` | `tests/test_wal.py::WalCorruptionTest`, `tests/test_replay.py::test_page_versions_are_per_page_contiguous` |
| 2 | 追加按段大小轮转、LSN 跨段连续无缺口、段头起始 LSN 与首条一致、原子轮转、按 LSN 范围检索并返回段标识 | `wal.py` | `tests/test_wal.py::WalRotationTest`，演示 `wal_append_rotation` / `range_query` |
| 3 | 检查点携带起点 LSN、页版本集合、整体校验摘要与单调序号；整体替换不混合；校验失败/序号回退 E_CHECKPOINT_INVALID 且原状态可用 | `engine.py`, `sqlite_store.py`, `recovery.py` | `tests/test_checkpoint.py` |
| 4 | 从起点 LSN 升序回放、摘要与版本一致性校验、跳号 E_REPLAY_MISMATCH 附期望/实际 LSN、与基准一致、重复回放幂等 | `replay.py` | `tests/test_replay.py` |
| 5 | 小规模精确路径、大规模批量+分组刷页；两路径逐页一致；输出路径、批量参数与选择依据 | `replay.py` | `tests/test_adaptive.py`，演示 `adaptive_equivalence` |
| 6 | 增量检查点只刷脏页；截断前确认段已被检查点覆盖，否则 E_TRUNCATE_UNSAFE 附未覆盖区间 | `engine.py`, `wal.py` | `tests/test_truncate.py`，演示 `truncate_safety` / `truncate_after_coverage` |
| 7 | 单写者串行、多读者并发、只可见完整提交版本、同请求标识只执行一次并复用结果 | `util.py::RWLock`, `engine.py` | `tests/test_concurrency.py` |
| 8 | 写意图先持久化、强杀后扫描未完成意图、回滚到最后完整检查点或标记中止、报告基于持久化事实 | `recovery.py`, `sqlite_store.py` | `tests/test_recovery.py`（子进程 `os._exit` 强杀） |
| 附 | 后台刷页、可中断续算、numpy 向量化校验 | `engine.py`, `replay.py` | `tests/test_background_flush.py`，演示 `interruptible_replay` |

## 业务错误码

| 代码 | 触发场景 |
|------|----------|
| `E_WAL_CORRUPT` | 段头/记录/页脚 CRC 错误、JSON 解析失败、LSN 缺口、段头起始 LSN 不一致 |
| `E_CHECKPOINT_INVALID` | 检查点 SHA256 校验失败、序号回退、无检查点却要求回放/截断 |
| `E_REPLAY_MISMATCH` | LSN 跳号、before_digest 与当前页摘要不符，details 含 expected/actual LSN |
| `E_TRUNCATE_UNSAFE` | 仍存在未被检查点覆盖、回放依赖的段，details 含未覆盖 LSN 区间 |
