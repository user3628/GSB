# 超标告警根因定位管线 — 设计说明

面向本地、确定性、可离线运行的 SLI 可观测性管线。无外部监控系统、无真实网络、
无容器；持久化仅使用 Python 内置 `sqlite3`，HTTP 仅监听回环地址 `127.0.0.1`。

## 1. 模块结构

```
rca_pipeline/
  config.py      阈值、窗口、抑制窗口、定点权重与安全边界
  errors.py      业务错误码与统一 JSON 错误信封
  fixedpoint.py  定点整数运算（每步越界检查、半偶舍入、相对容差比较）
  db.py          SQLite 连接（WAL）与全部关系表 schema
  topology.py    拓扑校验和、版本、快照/增量、BFS 跳数
  samples.py     SLI 解析、批内确定性去重、批次入库
  stats.py       最近秩分位、良好事件率
  aggregates.py  分位聚合索引、告警状态机、抑制索引
  rca.py         根因候选定点评分与稳定排序
  cascade.py     失效 + 同事务原子重建
  recovery.py    意图持久化、故障注入、强制终止恢复
  service.py     单写者串行化、幂等/冲突、只读查询
  api.py / app.py  FastAPI 路由与 uvicorn 入口
scripts/         确定性数据生成、端到端演示
tests/           24 个 pytest 用例（含子进程 SIGKILL 恢复）
data/            固定样本与拓扑文件（生成器产物，可重复生成）
```

## 2. 数据模型与版本

- 样本版本：`sample_batches.new_version` 单调递增，每条 `samples` 行记录其批次版本。
- 拓扑版本：`topology_versions.version` 单调递增，边以 `added_version/removed_version`
  做时态标记；快照替换先把所有活动边置为失效，再写入新边，杜绝新旧内容混合。
- 校验和：对规范化（排序、紧凑 JSON）后的 `{services, edges}` 计算 SHA-256；
  增量的校验和只覆盖其受影响子集，快照覆盖全量。声明值与计算值不符返回
  `E_CHECKSUM_MISMATCH`。

## 3. 样本解析与去重（业务要求 1、2）

- 字段缺失、类型错误、负延迟、非布尔 `is_good`、服务未在当前拓扑声明，均返回
  `E_SAMPLE_INVALID`，且在任何写事务之前校验，整批不入库。
- 批内 `(service, metric, ts_ms)` 重复时按输入顺序保留首条，重复数写入
  `sample_batches.duplicate_count` 与批次报告。
- 增量必须携带与当前样本版本相等的 `base_version`，否则 `E_STALE_BASE`，状态不变。

## 4. 分位聚合索引（业务要求 3）

- 窗口为每个 `(service, metric)` 最近 `WINDOW_SIZE=40` 条样本；样本数低于
  `MIN_SAMPLES=4` 时不产出索引，查询返回 `E_INSUFFICIENT_SAMPLES`，绝不静默给零。
- 最近秩法（确定性，无插值）：`rank=ceil(p*n)`，取升序序列 `sorted[rank-1]`。
- 良好事件率以 10^6 定点整数存储。
- 每个条目记录 `sample_lo/sample_hi`，即窗口数据覆盖的样本版本范围。

## 5. 告警与抑制（业务要求 4）

- 两类规则：`GOOD_RATE`（良好率低于阈值）与 `LATENCY_P99`（p99 超延迟上限）；
  指纹 = `sha256(service|metric|rule)` 前 16 位。
- 合法迁移：
  - `FIRING -> {SUPPRESSED, RESOLVED}`
  - `SUPPRESSED -> {FIRING, RESOLVED}`
  - `RESOLVED -> FIRING`
  其余迁移返回 `E_INVALID_TRANSITION` 且不修改持久化状态。
- 同服务、同级、同指纹告警在抑制窗口（5 分钟）内合并，累计 `suppressed_count`
  写入抑制索引；窗口外再次违规回到 `FIRING`；窗口恢复健康则 `RESOLVED`。
- 抑制索引是按指纹累积的降噪产物，重建时保留并持续更新。

## 6. 级联失效与原子重建（业务要求 5）

`service_refs` 维护每个服务对 `quantile / suppression / rca` 三类产物的引用。
一次重建在单个写事务内完成：

1. 失效：删除受影响服务的分位索引、RCA 评分与服务引用（抑制索引累积保留）。
2. 重建分位索引、重评告警与抑制。
3. 全量重算 RCA 评分，确保不存在“分位已新、RCA 仍旧”的中间可见状态。
4. 快照替换删除的服务只失效不重建；增量新增服务纳入重建。

读连接使用 WAL 只读快照，只能看到已提交的完整版本。

## 7. 根因评分数值契约（业务要求 6）

全部为有符号定点整数，刻度 `SCALE=10^6`：

```
hop_score(h)  = 1 / (1 + h)                 # 半偶舍入整数除法
time_score(d) = clamp(1 - d / halo, 0, 1)   # halo = 10 分钟
score         = 0.6 * hop_score + 0.4 * time_score
```

- 跳数限制 `[0,32]`、时间差限制 `[0, 24h]`，每步加减乘除都做显式越界检查，
  越界抛 `FixedPointError`（在重建路径映射为 `E_REBUILD_FAILED`）。
- 舍入：仅在重缩放时使用 round-half-to-even；最终渲染在 6 位小数处截断，
  因存储值本身至多 6 位小数，截断无损。
- 累计误差上界 `2 ulp = 0.000002`，随每个 RCA 响应返回。
- 比较：先按相对容差 `1e-4` 判等，再以 `(跳数升序, 服务名)` 稳定打破平局；
  纯整数运算保证同输入逐位一致。

## 8. 并发一致性（业务要求 7）

- 进程内单写者锁 `RLock` 串行化所有提交与重建；只读查询走独立 WAL 连接，
  可并发读取某个完整提交版本。
- 幂等：相同 `commit_id` + 相同 payload 只执行一次，后来者等待并复用结果，
  响应标记 `replayed=true`。
- 冲突：相同 `commit_id` 但 payload 哈希不同，返回 `E_CONFLICTING_BATCH`。
- 基础版本检查先于在途幂等等待，因此两个并发的同基版本增量只有一个成功，
  另一个收到 `E_STALE_BASE`。

## 9. 意图持久化与强制终止恢复（业务要求 8）

每个提交分两阶段：

1. 在独立事务写入 `intents(status=pending)` 并提交（同时登记 `commits=inflight`）。
2. 在第二个事务执行数据写入 + 原子级联重建，成功后把意图标记 `done` 并提交。

故障注入：

- `crash_after_intent`：阶段一提交后对当前进程发 `SIGKILL`，模拟 `kill -9`。
  阶段二事务未提交，SQLite 原子性保证无部分行；重启时恢复流程扫描到
  `pending` 意图，标记 `aborted`，并从最后完整版本重建全部派生索引。
- `fail_rebuild`：阶段二内主动失败并回滚，版本不前进；随后在独立事务把意图
  标记 `aborted`，无悬挂 pending。一次性故障位在恢复时清除，避免重启自毁。

恢复报告只包含 `recovery_events` 中已持久化的事实；被 SIGKILL 的进程不可能
产生完整退出报告，系统不做此类承诺。

## 10. HTTP 接口（前缀 `/api/v1`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/health` | 健康检查 |
| POST | `/samples/batches` | 提交 SLI 增量批次 |
| POST | `/topology/changes` | 提交拓扑快照/增量 |
| GET  | `/alerts` | 查询告警（可按 status/service 过滤） |
| POST | `/alerts/{id}/transitions` | 手动告警状态迁移 |
| GET  | `/aggregations/quantiles` | 分位聚合索引 |
| GET  | `/suppressions` | 抑制索引 |
| GET  | `/rca/{alert_id}` | 根因候选评分 |
| GET  | `/diagnostics` | 版本、计数、服务引用、数值契约 |
| GET  | `/recovery` | 恢复事实报告 |
| POST | `/faults/inject` | 异常注入（`crash_after_intent`/`fail_rebuild`/null） |

错误响应统一为 `{"ok": false, "error": {"code", "message", "context"}}`，
覆盖 `E_SAMPLE_INVALID / E_STALE_BASE / E_INSUFFICIENT_SAMPLES /
E_INVALID_TRANSITION / E_CONFLICTING_BATCH / E_TOPOLOGY_INVALID /
E_CHECKSUM_MISMATCH / E_REBUILD_FAILED / E_NOT_FOUND`。

## 11. 运行方式

```bash
pip install -r requirements.txt
python scripts/generate_data.py     # 生成 data/ 下确定性文件
python -m pytest                    # 全部测试
python scripts/demo.py              # 无 HTTP 的端到端演示
python -m uvicorn rca_pipeline.app:create_app --factory --host 127.0.0.1 --port 8000
```
