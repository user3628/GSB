# 本地日志增量索引与崩溃恢复组件 — 技术说明

## 1. 范围与运行形态

面向本地日志检索客户端的嵌入式组件，无外部服务依赖：

- 存储：Python 标准库自带 SQLite（WAL 模式，`synchronous=FULL`），数据库文件默认在 `runtime/logindex.db`。
- 接口：Python 引擎 `LogIndexEngine` + 本地回环 FastAPI（默认 `127.0.0.1:8765`）。
- 日志行格式：`毫秒时间戳|服务名|级别|消息`，消息内允许管道符；级别限定
  `DEBUG/INFO/WARN/ERROR/FATAL`。
- 样本：`samples/` 下 5 个确定性文件，覆盖 5 个服务、5 个级别、毫秒时间戳、
  迟到记录、可重打记录与非法行。

## 2. 模块结构

| 模块 | 职责 |
| --- | --- |
| `logindex/parser.py` | 确定性行解析，非法行抛 `E_LINE_INVALID`，整批不入库 |
| `logindex/models.py` | 五态状态机、数据模型、迁移合法性校验 |
| `logindex/indexing.py` | numpy 区间合并、分区摘要、批次聚合等纯算子 |
| `logindex/store.py` | SQLite 连接、schema、WAL、数据库重置 |
| `logindex/engine.py` | 单写者引擎：提交、两条路径、级联失效、版本可见性、恢复 |
| `logindex/api.py` | FastAPI 路由与统一 JSON 错误码 |
| `scripts/demo.py` | 四段流程端到端演示（真实 uvicorn + 真实子进程崩溃） |
| `tests/` | 53 个 pytest 用例（含子进程强制终止恢复） |

## 3. 持久化模型

- `batches`：批次标识、状态、基础版本、提交版本、路径模式、失效版本。
- `records`：`(batch_id, line_no)` 联合主键，重复提交天然幂等。
- `record_ts_history`：重打时间戳的新旧值与事件版本，支撑按版本回看。
- `classification_index`：`(service, level, batch_id)` 分类计数。
- `timeline_index`：`(service, ordinal)` 合并后的不相交区间，带
  `built_version / rebuilt_count / last_reason`。
- `batch_stats`：批次级 `(service, level)` 统计摘要。
- `service_watermark`：服务级最大时间戳水位，是仅追加判定的唯一依据。
- `version_events`：全局单调版本事件流（COMMIT/RETIMESTAMP/INVALIDATE）。
- `intents`：意图日志，提交任何派生工作前先落盘。
- `write_lock`：跨进程单写者锁；`recovery_events`：恢复动作审计。

## 4. 状态机

合法迁移：

```
PENDING   -> INDEXING | ABORTED
INDEXING  -> COMMITTED | ABORTED
INDEXING  -> PENDING            # 仅恢复器可走的回滚路径
COMMITTED -> INVALIDATED
INVALIDATED / ABORTED           # 终态，无出边
```

非法迁移抛 `E_INVALID_TRANSITION`，在事务内校验，失败即回滚，不修改任何
持久化状态。

## 5. 两类索引路径与等价性

判定只依赖服务水位，与业务功能无关：

- 本批某服务所有新时间戳都严格晚于 `service_watermark.max_ts` →
  `APPEND_ONLY`：不增加 `rebuilt_count`，`last_reason=APPEND_ONLY`。
- 任一新时间戳不晚于水位（迟到/重放）→ `REBUILD`：仅受影响服务分区重建，
  返回重建原因与受影响分区清单。

两条路径最终都调用同一个 `indexing.merge_intervals`（numpy 排序后合并相邻/
重叠区间，端点相接即合并），因此对同一数据集产出完全一致的区间；分类计数
与批次统计同样来自同一套聚合。`tests/test_engine.py` 中的
`test_append_and_rebuild_paths_produce_identical_index` 用两个独立数据库
交叉验证区间、分类与统计的一致性。

## 6. 依赖失效的原子性

重打时间戳与批次删除都遵循“意图先落盘 → 单事务完成所有派生变更”：

1. 写 `intents` 并独立提交（崩溃后恢复器可见）。
2. 在一个事务内完成记录修改/批次失效、删除受影响服务的旧分类行与统计行、
   重建分类索引、时间轴区间、批次统计，并推进水位与版本事件。

因为三类派生产物在同一事务提交，SQLite WAL 下只读连接只能看到事务前或
事务后的完整版本，不可能观察到“分类已更新而区间仍旧”的中间状态。

## 7. 版本可见性

- 当前版本直接读物化表；只读连接与写者通过 WAL 并发，不受写事务阻塞。
- 历史版本 `as_of_version` 不依赖历史物化副本，而是依据
  `version_events`、`record_ts_history` 与批次 `invalidated_version`
  重放当时可见的记录，再用同一套 numpy 算子即时重算索引（响应中
  `materialized=false` 标明为重算结果）。
- 重打事件的历史时间戳取“目标版本之后最早一个事件”的 `old_ts`；被删除
  批次在其失效版本之前仍可回看，之后不可见。

## 8. 并发控制与幂等

- 进程内：一把可重入写锁串行化所有写操作；同批次并发提交由
  `threading.Event` 合并，只执行一次，等待者复用同一结果（`reused=true`）。
- 跨进程：`write_lock` 单行表兜底，持有者死亡后由恢复器清理陈旧锁。
- 基础版本不匹配抛 `E_STALE_BASE`，在任何写动作之前校验，状态零变更。
- 同 `(batch_id, line_no)` 内容不一致抛 `E_CONFLICTING_RECORD` 并保留首值；
  内容一致的重复提交直接返回首次版本。

## 9. 崩溃恢复与报告真实性

提交被拆成三个崩溃安全的事务边界：

1. 插入 PENDING 批次 + SUBMIT 意图；
2. PENDING -> INDEXING 并更新意图状态；
3. 记录与三类派生产物 + COMMITTED 一次性原子提交。

崩溃注入点在边界 2 之后、边界 3 之前（`arm_crash_after_indexing`），
此时磁盘上只有 INDEXING 意图，没有任何派生写入。重启时 `recover()`：

- 清理陈旧 `write_lock`；
- 扫描 `finished_at_ms IS NULL` 的意图；
- INDEXING 批次按策略 `requeue`（回 PENDING 等待重排，默认）或 `abort`；
- 以 `MAX(version) WHERE state='COMMITTED'` 计算最后完整批次边界。

报告只引用数据库中的已提交事实，并把恢复动作追加到 `recovery_events`。
被强杀的进程不会也不可能生成“完整退出报告”——演示中崩溃子进程以非零码
退出，恢复结论完全由重启后的持久化扫描产生。

## 10. HTTP API

| 方法与路径 | 说明 |
| --- | --- |
| `GET /health` | 存活与当前版本 |
| `POST /batches` | 提交批次（`batch_id/lines/base_version`） |
| `POST /retimestamp` | 重打时间戳，触发级联重建 |
| `POST /batches/invalidate` | 删除（失效）批次并级联重建 |
| `POST /batches/abort` | 手动中止 PENDING/INDEXING 批次 |
| `GET /batches` | 批次清单与状态 |
| `GET /records/query` | 服务/级别/时间范围查询，支持 `version` 回看 |
| `GET /index-state` | 三类派生产物，支持 `version` 回看 |
| `GET /version-events` | 版本事件流 |
| `GET /recovery-report` | 启动恢复报告与持久化恢复事件 |
| `POST /test/arm-crash` | 武装下一次提交的崩溃注入点 |

错误统一为 `{"error": {"code", "message", "detail"}}`，业务码：
`E_LINE_INVALID / E_CONFLICTING_RECORD / E_INVALID_TRANSITION /
E_STALE_BASE / E_BATCH_NOT_FOUND / E_RECORD_NOT_FOUND / E_LOCKED /
E_INTERNAL`。

## 11. 验证方式

```bash
# Windows PowerShell 仅激活脚本不同：.venv\Scripts\Activate.ps1
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests/          # 53 passed
.venv/bin/python scripts/demo.py           # 四段端到端，输出 runtime/demo_output.txt
```

演示固定输出（节选自最近一次运行）：

- 段1 B1：v1，APPEND_ONLY，5 个服务 12 条，重建分区 0；
- 段2 B2：v2，APPEND_ONLY，重建分区 0；
- 段3a B3：v3，REBUILD，仅 `gateway` 分区重建，附原因；
- 段3b 重打：v4，仅 `auth-svc` 分区三类派生产物同事务重建；
- 段4 崩溃子进程 exit_code=1，重启后恢复报告显示
  `last_complete_version=3`、CRASH1 回 PENDING，重排提交到 v5，
  最终查询命中 24 条。

完整 JSON 见 `runtime/demo_output.txt`。
