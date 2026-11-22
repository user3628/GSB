# 验证报告

本报告记录在本地离线环境中的实际验证结果。所有数据来自 `scripts/generate_data.py`
生成的确定性文件，重复运行逐位一致；不访问任何外部目标。

## 1. 环境

- 操作系统：Linux（代码跨平台，仅依赖标准库路径与回环 socket，可在 Windows 直接运行）
- Python：3.14.4（要求 >= 3.10）
- 依赖：FastAPI、uvicorn、pytest、httpx（见 `requirements.txt`）
- 存储：本地 SQLite 文件，WAL 模式，无独立数据库进程
- HTTP：uvicorn 仅绑定 `127.0.0.1`

## 2. 确定性数据

`python scripts/generate_data.py` 产物（位于 `data/`）：

- `sli_samples.json`：4 个增量批次，每批每服务每指标 42 条；批次 1 含 1 条确定性
  重复记录。场景：健康 → payment 降级 → 持续降级 → 恢复。
- `topology_snapshot.json`：版本 1 全量快照，7 服务 6 边，含 SHA-256 校验摘要。
- `topology_increment.json`：版本 2 增量，新增 `payment -> risk` 边。
- `topology_snapshot_v3.json`：版本 3 全量替换，删除 `cache` 服务及其边。

## 3. 单元/集成测试

命令：`python -m pytest -v`
实际结果：**24 passed**

覆盖映射：

| 测试文件 | 覆盖的业务要求 |
| --- | --- |
| `test_samples_topology.py` | 要求 1、2：解析失败整批回滚、未声明服务、批内去重、stale base、幂等重放、冲突、校验和、快照替换不混合 |
| `test_aggregation_alerts.py` | 要求 3、4、5：最近秩分位、版本范围、样本不足业务码、FIRING/SUPPRESSED/RESOLVED、非法迁移、原子级联 |
| `test_rca_numeric.py` | 要求 6、8：半偶舍入、越界检查、确定性、稳定排序、失败回滚、意图中止 |
| `test_concurrency.py` | 要求 7：单写者串行 + 重复提交复用、并发同基版本仅一个成功另一个 stale |
| `test_recovery.py` | 要求 8：子进程 SIGKILL 后重启扫描 pending 意图、回滚到最后完整版本、派生索引重建、恢复后继续工作 |
| `test_api.py` | 全部 HTTP 路径与统一 JSON 错误信封 |

## 4. 端到端演示（无 HTTP）

命令：`python scripts/demo.py`，退出码 0。关键实测输出：

```
samples-b1: version=1 records=588 duplicates=1
samples-b2: version=2 records=588 duplicates=0
samples-b3: version=3 records=588 duplicates=0
samples-b4: version=4 records=588 duplicates=0
alert lifecycle: FIRING 0 / SUPPRESSED 0 / RESOLVED 4   # 终态
suppression index: 4 条，payment WARNING/CRITICAL，suppressed_count=1
RCA: rank1 payment hops=0 score=1.000000; rank2 notify hops=1 score=0.700000
stale: E_STALE_BASE {'base_version': 1, 'current_version': 2}
fail_rebuild: E_REBUILD_FAILED -> version still 4
snapshot v3: cache removed: True
counts: samples=2352, alerts=4, rca_scores=8, suppression_index=4
```

## 5. 真实 HTTP 验证

启动：`python -m uvicorn rca_pipeline.app:create_app --factory --host 127.0.0.1 --port 8011`

通过 HTTP 客户端实测（节选）：

```
POST /topology/changes            -> 200 topology_version=1
POST /samples/batches (4 批)      -> 200 new_version=1..4, 首批 duplicate_count=1
重复提交相同 commit_id             -> 200 replayed=true
错误 base_version                 -> 409 E_STALE_BASE
POST 拓扑增量                     -> 200 affected_services=['payment','risk']
GET  /alerts?service=payment      -> 4 条告警
GET  /rca/{id}                    -> 候选评分 + 数值契约
POST /faults/inject fail_rebuild  -> 200，随后批次 500 E_REBUILD_FAILED，版本保持 4
GET  /recovery                    -> pending_intents=0（失败意图已中止）
```

## 6. 强制终止恢复一致性

`test_recovery_after_hard_kill` 在独立子进程中：

1. 主进程提交拓扑 v1 与样本批次 1（样本版本=1）。
2. 注入 `crash_after_intent`，子进程在阶段一意图提交后对自身发 `SIGKILL`，
   子进程返回码为 -9（shell 语义 137）。
3. 重启后 `run_startup_recovery()` 扫描到 `pending` 意图：
   - `pending_intents_found >= 1`，动作 `aborted_unfinished_intent`；
   - 样本版本仍为 1（崩溃批次未前进）；
   - 分位索引从最后完整版本重建，`version_range.hi=1`；
   - `recovery_events` 记录 `derived_rebuild`，`pending_intents=0`；
   - 恢复后再次提交批次 2 成功，`new_version=2`。

实际结果：**PASSED**。恢复报告仅包含已持久化事实，不承诺强制终止后生成退出报告。

## 7. 复现步骤

```bash
pip install -r requirements.txt
python scripts/generate_data.py
python -m pytest
python scripts/demo.py
python -m uvicorn rca_pipeline.app:create_app --factory --host 127.0.0.1 --port 8000
```
