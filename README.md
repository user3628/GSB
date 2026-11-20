# 本地确定性事件流窗口聚合引擎

本项目从零实现一个只依赖本地文件的窗口计算服务：读取毫秒时间戳事件，按滚动窗口（tumbling）、滑动窗口（sliding）和会话窗口（session）计算分组聚合，并通过 SQLite 意图日志与原子 JSON 快照支持强制终止后的断点恢复。

服务只监听本地回环地址，不访问外部账号、数据库或网络服务。运行时状态全部写入工作区目录。

## 快速开始（Windows PowerShell）

要求 Python 3.11 或更高版本。项目已在 Python 3.14 上执行验证，代码只使用跨平台标准库持久化原语。

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

python -c "import sqlite3, fastapi, uvicorn, numpy; print(sqlite3.sqlite_version)"
python -m pytest -q
python scripts\verify.py

python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

如果 PowerShell 禁止激活脚本，可直接使用虚拟环境解释器：

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts\verify.py
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Linux/macOS/WSL：

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
python scripts/verify.py
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

可用环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DWE_CONFIG` | `samples/config.json` | 窗口与资源限制配置 |
| `DWE_RUNTIME_DIR` | `runtime` | SQLite 与快照持久化目录 |
| `DWE_CRASH_POINT` | 未设置 | 验证专用强制终止点 |
| `DWE_CRASH_BATCH_ID` | 未设置 | 只对指定批次触发终止 |
| `DWE_CRASH_KIND` | 未设置 | `batch` 或 `snapshot` |

`DWE_CRASH_*` 使用 `os._exit(137)` 真正终止进程，不执行异常处理、FastAPI shutdown 或退出报告。

## 目录结构

```text
app/
  config.py         配置校验与指纹
  crash.py         确定性强制终止钩子
  engine.py         窗口、聚合、状态机、查询与容量控制
  errors.py         统一业务错误码
  main.py           FastAPI HTTP 接口
  models.py         请求模型
  persistence.py    SQLite 意图日志与原子快照
samples/
  config.json       固定窗口配置
  phase1.json       崩溃前确定性批次
  crash_batch.json  强制终止目标批次
  phase2.json       恢复后续摄入批次
  invalid_event.json 缺失分组键的非法样本
  snapshot_sample.json 固定合法快照样本
scripts/
  verify.py         三场景强制终止恢复对比工具
  build_sample_snapshot.py 重新生成固定快照样本
tests/              单元、HTTP、并发、恢复测试
docs/
  DESIGN.md         机制与一致性设计
  VALIDATION.md     实际验证记录
artifacts/
  verification/verification_report.json 最近一次验证报告
runtime/            默认运行目录（运行时生成）
```

## 窗口与边界语义

所有时间单位为毫秒。`watermark_ms` 是已成功提交批次中见过的最大事件时间戳。一个批次先在当前已提交 watermark 下判断迟到，再完整合并批次内全部事件，最后推进 watermark 并关闭到期窗口。因此同一批次内的乱序事件不会互相制造迟到。

| 窗口 | 起点/终点 | 事件归属 | 关闭条件 |
| --- | --- | --- | --- |
| 滚动 | `[start, end)`，`start = floor(ts / width) * width` | 恰好一个窗口 | `watermark >= end + allowed_lateness_ms` |
| 滑动 | `[start, start + width)`，起点按 `step` 对齐纪元网格 | 时间戳覆盖的每一个窗口 | 同上 |
| 会话 | `[first_event_ts, last_event_ts]`，事件跨度端点均包含 | 同一分组内相邻时间差 `<= session_gap_ms` 的事件 | `watermark > last_event_ts + session_gap_ms` |

边界结论：

- `999ms` 属于 `[0,1000)`，`1000ms` 属于 `[1000,2000)`。
- 会话间隔恰好等于 `session_gap_ms` 时仍属同一会话；只有 `>` 才切分。
- 会话关闭使用严格大于，因此 `last_event + gap` 时刻的新事件仍可延长该会话。
- 空批次合法，但不会推进 watermark。

窗口状态机：

```text
OPEN --关闭--> CLOSED --发射--> EMITTED
```

只有 `OPEN` 窗口允许合并新事件。`CLOSED` 后到达的事件按 `late_policy` 处理：

- `queue`：进入迟到队列，不修改窗口聚合。
- `reject`：返回 `E_LATE_EVENT`，整批不提交。

滑动窗口中一个事件可能同时命中多个窗口，因此迟到判定和队列记录按“事件-窗口分配”粒度执行。若一条事件的部分窗口已关闭、部分仍开放，开放窗口继续更新，关闭窗口的分派进入迟到队列。

## 聚合与分位

- `count` 是事件数，包含度量值为 `null` 的事件。
- `non_null_count` 是参与数值聚合的事件数。
- `sum` 使用 Neumaier 补偿求和；输出时转为 `Decimal`，按 `ROUND_HALF_EVEN` 保留配置的固定小数位（默认 6），JSON 中是字符串，例如 `"5.000000"`。
- 全部为空值时返回确定空聚合：`non_null_count=0`、`sum="0.000000"`、`min=null`、`max=null`、分位样本规模为 0。
- 分位使用 nearest-rank：排序后 `rank = clamp(ceil(p * n), 1, n)`，并同时返回 `rank` 与 `sample_size`。

## 持久化与恢复

每个会修改状态的批次在合并前向 SQLite `intents` 表写入 `pending` 意图。快照流程：

1. 生成完整新状态，版本号单调递增。
2. 写入同目录临时文件并 `fsync`。
3. 使用 `os.replace` 原子安装为 `runtime/snapshots/vNNNNNNNNNNNN.json`。
4. `fsync` 快照目录。
5. 更新内存可见状态，再标记意图为 `committed`。

启动时只选择一个完整、版本合法、配置指纹一致、SHA-256 校验通过的快照整体加载，绝不把多个快照或旧内存状态合并。随后扫描 SQLite `pending` 意图：

- 若最新快照的 `last_intent_id` 或批次注册表证明该意图已经持久化，标记 `committed`。
- 否则标记 `aborted`，状态保持最后一个完整快照。
- 半成品 `.tmp` 快照被忽略，但写入恢复报告。

恢复报告只陈述磁盘与 SQLite 中的已持久化事实，不承诺强制终止时还能生成完整退出报告。

## 并发与查询可见性

- 所有状态变更由全局单写者锁串行化。
- 同批次标识并行提交时只有一个执行者；其他请求等待并复用结果。相同 `batch_id` 但规范化内容不同返回 `E_CONFLICTING_BATCH`。
- 每次成功提交发布一个完整不可变状态版本；查询只读取某一个已发布版本。
- 分页查询创建短期内存查询视图并固定 `as_of_version`。后续分页即使遇到新摄入，也不会看到旧视图中的重复或遗漏行。

批次内容哈希会先按 `(timestamp_ms, group, value)` 规范化事件多重集，因此相同内容的不同输入顺序仍视为同一个批次。

## 资源边界

活跃窗口数或迟到队列长度超过 soft limit 时，按 `(window_end_ms, window_start_ms, group, window_type)` 从旧到新淘汰 `EMITTED` 窗口，并在批次响应及查询中输出淘汰清单。淘汰只移除窗口状态，不伪造或静默丢弃输入。

若淘汰后仍超过 hard limit，或者单个批次事件数超过 `batch_event_hard_limit`，返回 `E_CAPACITY_EXCEEDED`，整批不提交。

## HTTP 接口

服务默认地址：`http://127.0.0.1:8000`。OpenAPI 文档：`http://127.0.0.1:8000/docs`。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/health` | 健康与状态摘要 |
| `GET` | `/v1/config` | 生效配置、边界与舍入说明 |
| `POST` | `/v1/batches` | 提交事件批次 |
| `POST` | `/v1/snapshots` | 触发显式快照 |
| `GET` | `/v1/snapshots` | 列出快照 |
| `POST` | `/v1/snapshots/load` | 按 `version` 或本地 `path` 整体加载快照 |
| `POST` | `/v1/windows/query` | 范围/分组/窗口类型过滤与游标分页 |
| `GET` | `/v1/late-events` | 查询迟到队列 |
| `GET` | `/v1/recovery/report` | 最近一次启动恢复报告 |
| `GET` | `/v1/state/summary` | 当前状态版本与计数 |
| `GET` | `/v1/state/transitions` | 合法状态迁移表 |
| `POST` | `/v1/state/transitions/validate` | 只验证迁移，不修改持久化状态 |

批次示例：

```bash
curl -X POST http://127.0.0.1:8000/v1/batches \
  -H "Content-Type: application/json" \
  -d '{
    "batch_id": "demo-1",
    "events": [
      {"timestamp_ms": 1700000000999, "group": "gA", "value": 1.0},
      {"timestamp_ms": 1700000001000, "group": "gA", "value": null}
    ]
  }'
```

查询示例：

```bash
curl -X POST http://127.0.0.1:8000/v1/windows/query \
  -H "Content-Type: application/json" \
  -d '{
    "start_ms": -10000000,
    "end_ms": 10000000000000,
    "group": "gA",
    "window_types": ["tumbling", "sliding", "session"],
    "quantiles": [0.5, 0.95],
    "limit": 20
  }'
```

加载固定样本快照：

```bash
curl -X POST http://127.0.0.1:8000/v1/snapshots/load \
  -H "Content-Type: application/json" \
  -d '{"path": "samples/snapshot_sample.json"}'
```

失败响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_EVENT_INVALID",
    "message": "Event batch failed schema validation",
    "details": {}
  }
}
```

## 业务错误码

| 错误码 | HTTP 状态 | 含义 |
| --- | --- | --- |
| `E_EVENT_INVALID` | 400 | 非法时间戳、缺失/空分组、非有限数值或请求结构非法；整批不摄入 |
| `E_INVALID_TRANSITION` | 409 | 非法窗口状态迁移；不修改状态 |
| `E_LATE_EVENT` | 409 | 关闭窗口收到事件且策略为 `reject`；整批不摄入 |
| `E_SNAPSHOT_INVALID` | 409 | 版本不单调、校验摘要失败、结构或配置指纹不符；原状态保持可用 |
| `E_CONFLICTING_BATCH` | 409 | 相同批次标识对应不同内容 |
| `E_CAPACITY_EXCEEDED` | 429 | 批次或状态将超过硬上限 |
| `E_CURSOR_EXPIRED` | 410 | 分页视图过期或过滤条件不匹配 |
| `E_REQUEST_INVALID` | 400 | 非事件类请求参数错误 |
| `E_NOT_FOUND` | 404 | 路径不存在 |
| `E_INTERNAL` | 500 | 未预期内部错误 |

## 固定样本和验证

固定数据位于 `samples/`：

- `phase1.json`：边界时间戳 `999/1000/1499/1500/2000`、乱序事件、空值组 `empty`，以及 `1e15` 与 `1.0` 混合量级。
- `crash_batch.json`：恢复验证目标批次。
- `phase2.json`：恢复后的继续摄入，包含 `1e-9`、`1e12`、`1e15` 和更多迟到事件。
- `snapshot_sample.json`：版本 100、38 个窗口、14 条迟到队列记录的固定快照样本。

执行完整验证：

```bash
python scripts/verify.py
```

工具会启动本地 Uvicorn 进程，执行一次未中断路径和三次真实强制终止路径，逐项比较窗口记录与迟到队列，并输出：

```text
artifacts/verification/verification_report.json
```

本次实际结果见 `docs/VALIDATION.md`：19 项 pytest 全部通过；未中断路径 82 个窗口、20 条迟到记录；三个恢复场景差异清单均为空。

## 当前验证环境说明

本次交付在提供的本地 WSL/Linux 环境执行，Python 3.14.4、SQLite 3.46.1、NumPy 2.5.5、FastAPI 0.165.0、Uvicorn 0.54.0、pytest 9.2.0。代码使用 `pathlib`、原子替换及跨平台 SQLite，并针对 Windows 跳过目录 `fsync`；Windows PowerShell 启动命令已在上文给出，但本次未在原生 Windows 终端重复执行。
