# 本地对象存储分段上传元数据索引

这是一个可直接嵌入本地文件同步客户端的领域组件。它以 SQLite 为唯一事实源，持久化上传会话、分段和对象元数据，并在内存中增量维护三类派生索引：

- 按会话的连续序号区间：快速定位乱序上传缺口。
- 按对象键的会话代际：裁决同键并发上传和过期合并。
- 按 SHA-256 的内容去重引用计数：相同摘要只保留一份内容记录。

组件另提供 FastAPI 本地 HTTP 适配层、固定样本、端到端脚本、强制终止演练和 pytest 回归。HTTP 默认只绑定 `127.0.0.1`，不使用容器。

## 快速运行

环境要求：Python 3.11+、pip、本地回环端口。本次验证使用 Python 3.14.4、FastAPI 0.115.6、uvicorn 0.34.0、NumPy 2.5.5、pytest 8.3.4。

```bash
python -m pip install -r requirements.txt
python -m pip install -e .

PYTHONPATH=src python -m pytest
PYTHONPATH=src python examples/domain_client.py
PYTHONPATH=src python scripts/validate_e2e.py
PYTHONPATH=src python scripts/scaling_probe.py
```

Windows PowerShell 可将路径前缀写成 `$env:PYTHONPATH="src"` 后执行同样命令；服务仍绑定 `127.0.0.1`，SQLite 默认位于 `data\`。

启动本地 HTTP 服务（示例端口 `18080` 已确认空闲）：

```bash
UPLOAD_INDEX_DB=data/upload_index.db \
UPLOAD_INDEX_HOST=127.0.0.1 \
UPLOAD_INDEX_PORT=18080 \
PYTHONPATH=src \
python -m uvicorn upload_index.api:app --host 127.0.0.1 --port 18080
```

浏览 `http://127.0.0.1:18080/docs` 可查看 OpenAPI。服务关闭时会写入 `clean_shutdown=1`；强制终止不会获得完整退出报告，下一次启动只依据 SQLite 已提交事实生成恢复报告。

## 目录

```text
src/upload_index/          领域模型、SQLite 事务服务、版本回放、HTTP API
samples/                   固定批次、并发同键、去重回收、强制终止计划
scripts/validate_e2e.py    HTTP 端到端验证 + SIGKILL 重启演练
scripts/scaling_probe.py   NumPy 固定种子 600 段探针
examples/domain_client.py  最小进程内集成示例
tests/                     pytest 回归
artifacts/                 已实际运行产生的验证结果
data/                      SQLite、WAL 和本地日志（运行产物）
```

## HTTP API

所有成功响应为 `{"ok": true, "data": ...}`。所有失败响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_PART_MISMATCH",
    "message": "the same seq was already committed with a different digest",
    "session_id": "...",
    "seq": 3,
    "stored_sha256": "...",
    "submitted_sha256": "..."
  }
}
```

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/sessions` | 发起会话，接受 `object_key`、可选 `session_id`、`lower_bound`、`conflict_policy` |
| `POST` | `/sessions/{id}/parts` | 提交单段 |
| `POST` | `/sessions/{id}/parts/batch` | 原子提交批次，请求含 `batch_id` 与 `parts` |
| `DELETE` | `/sessions/{id}/parts/{seq}` | 删除单个已提交分段并递减去重引用 |
| `GET` | `/sessions/{id}/view?version=N` | 按版本读取分段清单、连续区、缺口、去重引用和代际索引 |
| `POST` | `/sessions/{id}/merge` | 按代际合并对象，校验连续性、摘要、总大小 |
| `DELETE` | `/sessions/{id}` | 取消会话、删除其分段并递减去重引用 |
| `GET` | `/sessions/{id}/segments` | 查看按会话聚簇的物理索引段 |
| `GET` | `/objects/view?object_key=...&version=N` | 查询任意对象键的对象元数据和代际索引 |
| `GET` | `/objects/{object_key}` | 不含 `/` 的对象键便捷查询 |
| `GET` | `/content/{sha256}?version=N` | 查询摘要内容记录、引用计数和历史回收状态 |
| `GET` | `/versions/current` | 当前索引版本与保留下界 |
| `GET` | `/versions?since=N&limit=M` | 版本事件轨迹 |
| `POST` | `/maintenance/gc` | 物理回收 `pending_gc` 内容记录 |
| `POST` | `/maintenance/compact` | 压缩指定会话或全部会话的索引段 |
| `POST` | `/query-leases` | 声明活跃查询版本，防止版本回收越过它 |
| `DELETE` | `/query-leases/{id}` | 释放查询租约 |
| `POST` | `/versions/retire` | 推进版本保留下界并生成检查点 |
| `GET` | `/recovery/report` | 最近一次启动恢复报告 |
| `GET` | `/diagnostics/current` | 当前内存索引诊断快照 |
| `POST` | `/admin/crash-drill` | 仅显式开启时可用，供独立脚本强制终止进程 |

### 请求示例

```json
POST /sessions
{"session_id":"sess-1","object_key":"sync/a.bin","conflict_policy":"reject"}
```

```json
POST /sessions/sess-1/parts/batch
{
  "batch_id": "client-batch-17",
  "parts": [
    {"seq": 2, "size": 20, "sha256": "<64 hex>", "arrival_ms": 1760000000002},
    {"seq": 1, "size": 10, "sha256": "<64 hex>", "arrival_ms": 1760000000001}
  ]
}
```

```json
POST /sessions/sess-1/merge
{
  "generation": 1,
  "total_size": 30,
  "parts": [
    {"seq": 1, "size": 10, "sha256": "<64 hex>", "arrival_ms": 1760000000001},
    {"seq": 2, "size": 20, "sha256": "<64 hex>", "arrival_ms": 1760000000002}
  ]
}
```

`total_size` 与 `parts` 清单都可省略。省略 `total_size` 时由已存分段大小求和；提供清单时会逐段比对摘要与大小，清单项可省略 `arrival_ms`。缺失或多余序号返回 `E_PART_MISSING`。对象摘要并非拼接字节内容，而是按下式对已存分段清单做域分离哈希，避免客户端传输整个对象：

```text
SHA-256("LOCAL-OBJECT-V1\0" + 每段 "seq:size:sha256\n" 的 ASCII 拼接)
```

## 数据模型与一致性设计

SQLite 表：

- `sessions`：对象键、代际、序号下界、状态、会话级重复提交计数。
- `parts`：`(session_id, seq)` 主键、大小、摘要、到达毫秒、首次版本、分段重复次数。
- `content_blobs`：摘要唯一内容记录、引用计数、`active/pending_gc` 状态。
- `objects`：对象键唯一元数据、总大小、分段数、对象摘要、代际、合并版本。
- `version_events`：每次逻辑状态变更的严格单调递增版本事件。
- `index_segments`：按会话聚簇的版本事件索引段；压缩只改物理布局。
- `recovery_intents`：批次/压缩意图；主事务与意图完成标记分别提交。
- `version_checkpoints`：版本回收时保留的状态基线。
- `recovery_reports`：启动恢复审计。

### 提交事务

单段、批次、取消、合并、GC 都使用 `BEGIN IMMEDIATE`。分段写入、去重引用增减、版本事件和索引段写入在同一事务提交。批次中每个有效段获得独立版本；批次中任一业务错误会回滚整批，不会留下部分分段或引用计数。解析错误在打开写事务前完成，返回 `E_PART_INVALID` 且整批不入库。

去重引用在同一事务边界内变化。引用降为 0 时先迁移为 `pending_gc`，之后的 `/maintenance/gc` 再删除 `content_blobs` 行并写入 `content_purged` 版本事件。历史版本仍可看到 `pending_gc` 或删除前状态，当前物理表不再保留该内容行。

### 三类增量索引

- 连续区：提交新序号时用有序插入方式合并相邻区间，不扫描重建全部区间；重复提交不新增区间。
- 代际：同键 `reject` 策略遇到活跃会话返回 `E_SESSION_CONFLICT`；`allow_generation` 允许并存并把代际加一。合并要求请求代际等于会话代际且等于对象键当前代际。
- 去重：首次摘要写一条内容记录，后续分段只递增引用；取消按实际已提交分段逐一递减。

同号同摘要重复提交返回成功，分段与会话级重复计数加一，不增加去重引用；同号不同摘要返回 `E_PART_MISMATCH` 并保留首次元数据。

### 版本视图

当前内存索引只服务新写入裁决和诊断；带版本查询从不读取当前可变索引。查询从最近的检查点恢复状态，再顺序应用 `(检查点版本, 查询版本]` 内的已提交事件。因此后续增量提交不会改变旧版本结果。

版本回收由活跃查询租约约束：回收候选下界不会高于最小活跃查询版本。推进保留下界时：

1. 生成 `floor - 1` 的完整检查点。
2. 删除更早版本事件。
3. 从索引段中裁掉更早事件或删除空段。
4. 后续查询 `< floor` 返回 `E_VERSION_RETIRED`，`>= floor` 仍由检查点加后续事件重建。

### 索引段压缩

每个会话事件先写入一个索引段。压缩把同一会话的多个段合并为一个事件序列，写入意图后在同一事务内插入新段并删除旧段；没有逻辑版本变化，因此所有版本区间查询语义保持不变。若压缩中途进程死亡，SQLite 回滚未提交变更，启动扫描将意图标记为回滚。

### 强制终止恢复

批次和压缩先单独提交 `recovery_intents(status=pending)`，主事务在提交时把意图改为 `complete`。进程在意图后、主事务提交前被 SIGKILL 时：

- 未提交事务由 SQLite 自动回滚。
- 启动扫描 pending 意图和 `staging` 索引段。
- 若无完整批次证据，意图标记为 `aborted_rolled_back`。
- 恢复报告只包含 SQLite 已持久化计数、最后完整版本和恢复决策。
- `clean_exit_report_available=false` 明确表示强制终止时不存在完整退出报告。

## 业务错误码

| 业务码 | 含义 |
|---|---|
| `E_PART_INVALID` | JSON 类型、正整数序号、摘要格式、非负大小或 int64 溢出校验失败 |
| `E_PART_OUT_OF_RANGE` | 序号低于会话 `lower_bound` |
| `E_PART_MISMATCH` | 同号不同摘要、摘要关联大小冲突或清单摘要不符 |
| `E_PART_MISSING` | 合并清单缺段或序号不连续 |
| `E_SIZE_MISMATCH` | 声明总大小不等于已存分段大小和 |
| `E_SESSION_CONFLICT` | `reject` 策略下同键存在活跃会话 |
| `E_SESSION_STALE` | 合并代际不是会话/对象键当前代际，响应附 `current_generation` |
| `E_SESSION_NOT_ACTIVE` | 已完成或已取消会话不接受该操作 |
| `E_OBJECT_EXISTS` | 同键对象已经成功合并一次 |
| `E_VERSION_RETIRED` | 查询版本低于保留下界 |
| `E_VERSION_NOT_FOUND` | 查询版本高于当前版本 |
| `E_LEASE_NOT_FOUND` | 查询租约已释放或过期 |
| `E_CONTENT_NOT_FOUND` | 指定版本不存在摘要记录 |

## 固定样本与验证结果

样本：

- `full_batch.json`：6 段全量提交，其中两段摘要相同，随后原子合并。
- `incremental_batches.json`：先到 `[5,3,3重复]`，再补 `[1,6,4,2]`，形成乱序、幂等重复和缺口填充。
- `concurrent_sessions.json`：同键两个 `allow_generation` 会话，旧代际合并被拒，新代际成功。
- `dedup_gc.json`：两个会话共享摘要，引用 `2 -> 1 -> 0`，再进入 `pending_gc` 与物理移除。
- `forced_termination_plan.json`：基础 3 段后，在追加 `[4,5,6]` 的批次事务中被 SIGKILL。
- `large_batch_spec.json`：固定 NumPy 种子生成 600 个唯一分段和 6 个重复事件。

本次实际运行：pytest `14 passed`，`scripts/validate_e2e.py` 38 项检查全部通过。

- 全量提交：版本 7，区间 `[[1,6]]`，对象摘要 `59831293164b39216240e221409370c16f5b24fd7f2db93e8e4daa4279fa0271`。
- 增量：版本 12 区间 `[[3,3],[5,5]]`、缺口 `[[1,2],[4,4]]`；版本 16 合并为 `[[1,6]]`，重复提交 1 次，跨会话同摘要引用为 3。
- 压缩：8 个索引段压为 1 个，版本 12 与 16 的查询结果逐字段不变。
- 代际：会话代际 `[1,2]`，旧代际返回 `E_SESSION_STALE(current_generation=2)`，最终状态为旧会话 `cancelled`、新会话 `completed`。
- 去重回收：引用 `2 -> 1 -> 0`，状态 `pending_gc -> removed`，GC 物理删除目标共享内容行。
- 版本回收：租约把保留下界限制在版本 12；释放后推进到版本 16，版本 12 返回 `E_VERSION_RETIRED`。
- 批次强制终止：崩溃前后版本均为 4，分段均为 `[1,2,3]`，恢复检测到 1 条 pending 意图并判定 `aborted_rolled_back`；重启后可重新提交 `[4,5,6]` 并形成 `[[1,6]]`。
- 压缩强制终止：在索引段压缩事务内再次 SIGKILL，重启后版本与源索引段均与压缩前一致，pending 压缩意图判定 `aborted_rolled_back`。

`scripts/scaling_probe.py` 最近一次独立运行：606 次提交（600 唯一段 + 6 重复）约 `0.032s` 完成（约 1.9 万事件/秒），生成版本 607，区间 `[[1,600]]`，600 条去重内容；607 个索引段压缩为 1 个，压缩前后分段与区间一致。此前空载运行约 `0.022s`；该计时受本机负载影响，不作为生产容量承诺。

完整机器可读结果：

- `artifacts/validation_result.json`
- `artifacts/force_termination_recovery.json`
- `artifacts/scaling_result.json`

## 范围与取舍

- 组件面向单进程、本地 SQLite 写者；跨进程并发写应交给 SQLite 锁和上层串行化，不是分布式协调器。
- 只校验客户端声明的 SHA-256 格式与同号一致性，不读取对象字节，因此不替代内容级密码学校验。
- 版本事件支持精确历史视图，但旧版本读取成本为检查点之后的事件数；版本回收和段压缩用于约束长期成本。
- 去重引用计数保证事务内双向一致，`pending_gc` 提供了可审计的删除窗口，但 GC 需要上层周期性触发。
- 索引段压缩减少物理段数量；查询仍按版本过滤事件，适合数百到较大本地批次，未实现列式或外部搜索引擎。
