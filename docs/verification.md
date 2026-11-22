# 验证结果记录

环境：Python 3.14.4 / NumPy 2.2.1 / PyYAML（仅本地，无 Docker、GPU、外部网络）。
所有数据由固定种子生成器产出，结果可重复。复现命令：

```bash
python3 -m unittest discover -s tests -v   # 单元/集成测试
python3 scripts/demo.py                    # 一键端到端验收
```

## 1. 自动化测试

`python3 -m unittest discover -s tests` 结果：**Ran 30 tests … OK**（全部通过）。

| 测试模块 | 覆盖验收点 |
| --- | --- |
| `test_normalize_dispatch` | NFKC/全半角/小写/空白折叠、切分偏移、`E_EMPTY_QUERY`、派发计划逐字节确定性 |
| `test_candidates` | 三路候选字段完整性、同义词条目版本与偏移、UNK 显式回退、路线候选上限、候选确定性 |
| `test_budget_scheduler` | 全局/通道预算拒绝、累计永不超预算、`E_LATENCY_BUDGET`、波次并发上限、预估成本上界 |
| `test_adjudication` | 对数域评分、确定性同分仲裁、空/重复改写剔除、淘汰依据保留、重叠度有界 |
| `test_pipeline_recovery` | 并行/串行逐字节一致、停止恢复与全新运行一致、配置变更增量失效、稳定错误码、序号连续 |
| `test_checkpoints_locking` | 原子写无半写残留、flock 互斥、多进程 `E_STALE_CHECKPOINT`、检查点序号 1..N 连续 |
| `test_http_and_manifest` | HTTP 全端点冒烟、分片清单篡改检测、跨分片重复声明 |

## 2. 端到端演示（scripts/demo.py，退出码 0，all_checks_passed=true）

- **多路改写**：21 条记录（含 3 条空查询），样例 `q01-0000` 三路全命中，
  路线候选数 `{synonym:2, glyph:2, bridge:2}`，桥接 UNK 显式回退候选 2 条。
  同义词候选携带 `entry_version=syn-v1-e011` 与 `replaced_offset=[7,12]`。
- **并行 vs 串行**：`run_fingerprint` 相同；`plans/candidates/orchestration/
  validation/summary` 五个状态文件 `cmp` 逐字节一致（全 true）。
- **预算编排**（全局/通道预算=20）：未处理原因码 `E_BUDGET_EXHAUSTED`、
  `E_EMPTY_QUERY`；全局评估 19/20，通道消耗 `{mini:7, web:12}`，未超限。
- **延迟预算**（per_request_latency_ms=0）：4 条有效查询全部返回
  `E_LATENCY_BUDGET`，诊断含 `elapsed_ms` 与 `budget_ms`，无工作线程致命异常。
- **优雅停止/恢复**：处理 4 个查询单元后停止（17 条登记 STOP_REQUESTED）；
  恢复后状态 completed，与全新运行 `run_fingerprint` 一致，所有检查点序号连续。
- **分层汇总**：10 个 `lang|channel` 分层；总计 92 候选、36 采纳，
  路命中 `{bridge:35, glyph:26, synonym:31}`，有采纳查询占比 0.857。
- **多进程锁**：双进程竞争发布，观察到 `E_STALE_CHECKPOINT`，合法发布后序号严格为 2。

## 3. 关键产物示例

裁决轨迹（对数域分量，NumPy 计算）：

```json
{
  "candidate_id": "synonym-00", "decision": "adopted", "rank": 0,
  "confidence": 0.82, "route_weight": 0.9, "overlap": 0.714285714,
  "log_confidence": -0.198450939, "log_route_weight": -0.105360516,
  "score": -0.036845482
}
```

分片清单显式声明跨分片重复（生成器强制注入）：

```json
[{"query_id": "q02-0010", "same_as": "q01-0002"}]
```

## 4. 版本失效传播验证

修改 `adjudication.overlap_weight` 后重跑：`first_invalid_stage` 报告最早受影响阶段为
`validation`（仅 `adjudication_fingerprint` 不匹配），该阶段及下游被重建；
dispatch/candidate/orchestration 检查点保留复用，日志仍以 seq=1 开头且四阶段齐全，
失效原因写入 `state/invalidations.jsonl`，`status` 接口可观测版本指纹与失效原因。

## 5. 已知边界

- 文件锁使用 POSIX `fcntl.flock`，目标运行环境为 Linux/macOS；Windows 退化为进程内保护。
- 延迟预算为确定性模拟计量（无真实网络/翻译调用），便于离线复现与一致性验证。
