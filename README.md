# 本地 SLI/SLO 错误预算组件

一个可在 Windows 本机运行的 SLI/SLO 错误预算计算服务。组件从固定 JSONL 样本或 HTTP
事件批次摄入数据，使用 SQLite 保存版本化窗口，提供滑动窗口 SLI、错误预算、采样置信区间、
多窗口燃烧率告警、迟到事件审计，以及强制终止后的恢复报告。

核心约束均已落地：

- 1 分钟基础桶按 epoch 边界对齐，5 分钟、30 分钟、1 小时窗口由同一批基础事实推导。
- 每个提交版本同时包含事件、桶计数、关闭快照、告警和 ingest 幂等记录。
- 迟到批次整体拒绝并写审计记录；已关闭桶的计数和预算快照不变。
- 超过精确上限的窗口使用确定性蓄水池采样，输出采样率、方法和 Wilson 置信区间。
- 预算可加性通过非重叠 1 分钟账本及 5m/30m/1h 平铺分区校验；不把重叠滚动窗口直接相加。
- 写操作先提交 `write_intents`，再以单个 SQLite 事务提交全部业务状态。
- 单写者串行摄入，多读者通过 WAL 和固定 version 读取完整快照；同窗口计算使用 single-flight。

## 快速开始（PowerShell）

要求 Python 3.11+，并从工作区根目录执行：

```powershell
python --version
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

python scripts\generate_samples.py
python -m pytest -q
python scripts\e2e_demo.py
```

启动本地 HTTP 服务：

```powershell
.\scripts\start_server.ps1 -Port 8000
```

等价的原生命令如下。服务有意只使用一个 worker；进程内并发和跨线程 single-flight
依赖单写者模型。

```powershell
$env:PYTHONUTF8 = "1"
$env:SLO_DATA_DIR = "data"
$env:SLO_DB_PATH = "data\slo.db"
python -m uvicorn app:app --host 127.0.0.1 --port 8000 --workers 1
```

接口文档：`http://127.0.0.1:8000/docs`。所有运行时 SQLite、恢复报告和日志均写入工作区
`data` 目录。

## 固定样本

`scripts/generate_samples.py` 不使用随机数，生成结果可逐字节复现。

| 文件 | 服务 | 事件数 | 时间范围 | 错误率 | 用途 |
|---|---|---:|---|---:|---|
| `samples/events_small.jsonl` | `checkout-small` | 6,000 | 2026-01-15 10:00–12:00Z | 0.5% | 低于 10,000 精确上限 |
| `samples/events_large.jsonl` | `checkout-large` | 60,000 | 2026-01-15 10:00–12:00Z | 1.0% | 30m/1h 超过精确上限 |

SHA-256 和生成参数记录在 `samples/manifest.json`。每个事件包含 `event_id`、ISO-8601
时间戳、服务、SLI 类型和 `good` 布尔值。

## HTTP API

### 摄入事件

```powershell
$event = @{
  timestamp = "2026-01-15T10:00:00Z"
  service = "checkout"
  sli_type = "availability"
  good = $true
  event_id = "evt-0001"
}
$body = @{
  batch_id = "example-0001"
  source = "powershell"
  events = @($event)
} | ConvertTo-Json -Depth 6

Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/v1/events" `
  -ContentType "application/json" -Body $body
```

`batch_id` 可重复提交同一 payload，服务返回原结果并标记 `replayed=true`；同 ID 不同
payload 返回 `E_CONFLICT`。无法通过相邻窗口建立一致预算的数据应分块提交，每批时间跨度
不得大于迟到容忍期。

### 查询窗口和预算

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/v1/services/checkout-small/windows?windows=5m,30m,1h&sli_type=availability"
Invoke-RestMethod "http://127.0.0.1:8000/v1/services/checkout-large/budget-summary?sli_type=availability"
Invoke-RestMethod "http://127.0.0.1:8000/v1/alerts?service=checkout-large"
Invoke-RestMethod "http://127.0.0.1:8000/v1/recovery"
Invoke-RestMethod "http://127.0.0.1:8000/v1/rejections?service=checkout"
```

单窗口查询也可使用 `/v1/services/{service}/sli?window=5m` 或
`/v1/services/{service}/budget?window=5m`。查询参数 `version` 可锁定某个已提交快照，
从而在后续摄入发生后仍重复得到同一窗口和同一确定性采样结果。

事件别名：`timestamp|ts|time`、`sli_type|sli`、`good|is_good|good_event`。时间戳可为
ISO-8601、epoch 秒或 epoch 毫秒；统一转为 UTC 毫秒并输出以 `Z` 结尾的 ISO 时间。

### 统一错误格式

业务码同时位于顶层 `code` 和 `error.code`，兼容简单脚本与结构化客户端。

```json
{
  "code": "E_LATE_EVENT",
  "message": "The batch was rejected atomically because one or more events are late or out of order.",
  "details": {"rejection_count": 1},
  "error": {
    "code": "E_LATE_EVENT",
    "message": "The batch was rejected atomically because one or more events are late or out of order.",
    "details": {
      "rejection_count": 1,
      "late_tolerance_seconds": 600,
      "accepted_events": 0,
      "bucket_counts_unchanged": true
    }
  }
}
```

主要业务码：

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_LATE_EVENT` | 409 | 批次含超出容忍期或落于关闭桶的事件，整批不改变桶 |
| `E_INSUFFICIENT_DATA` | 422 | 窗口样本量低于下限，响应含 `sample_count` |
| `E_INVALID_REQUEST` / `E_VALIDATION` | 422 | 请求结构、时间戳、窗口或配置错误 |
| `E_DUPLICATE_EVENT` | 409 | `event_id` 在批次内或已提交数据中重复 |
| `E_CONFLICT` | 409 | 幂等键冲突或并发版本冲突 |
| `E_SERVICE_NOT_FOUND` | 404 | 无该服务/SLI 的已提交事实 |
| `E_VERSION_NOT_FOUND` | 404 | 指定快照版本不存在 |

## 计算语义

### 窗口与关闭

- 基础桶：`bucket_start = floor(timestamp_ms / 60_000) * 60_000`，半开区间
  `[bucket_start, bucket_start + 60s)`。
- 逻辑水位：服务已接受事件的最大时间戳，不使用墙钟。这样历史固定样本可直接回放。
- 单事件迟到：相对批次最终水位超过 `late_tolerance_seconds` 即拒绝。
- 关闭规则：`bucket_end + late_tolerance <= watermark` 后桶进入 `CLOSED`。
- 迟到策略：选择确定性的“整批拒绝并审计”，而不是改记下一窗口。任意一个事件迟到都会
  使整个批次返回 `E_LATE_EVENT`；`rejected_events` 保存原因、水位和目标桶。
- 查询窗口默认结束于当前水位之后的下一分钟边界，包含最新部分分钟；显式 `as_of` 则向下
  对齐到分钟边界。窗口起点和终点均为半开边界。

### 多窗口 SLI、预算和采样

```text
SLI = good_events / total_events
allowed_bad_fraction = 1 - SLO_target
allowed_bad_events = total_events * allowed_bad_fraction
budget_consumption_ratio = bad_events / allowed_bad_events
burn_rate = (1 - SLI) / (1 - SLO_target)
```

当窗口总事件数低于 `SLO_MINIMUM_SAMPLES` 时，不计算 SLI，单窗口 API 返回
`E_INSUFFICIENT_DATA` 和当前样本量。

当窗口总事件数超过 `SLO_EXACT_EVENT_LIMIT` 时：

- 使用 `blake2b-256(id|timestamp|good)` 优先级的确定性蓄水池抽取固定上限事件；
- 输出 `population_size`、`sample_size`、`sample_rate`、方法和 Wilson 置信区间；
- 预算估计区间由 SLI 区间反向映射；
- 同一 `version + window + as_of` 的结果被缓存，等待中的并发请求复用同一次计算；
- 同版本重复查询逐字节一致。

告警计算直接使用低成本的基础桶精确汇总，不受采样抖动影响。预算汇总也使用精确桶账本，
从而保持可加性。

### 多窗口燃烧率告警

默认短窗口为 5m、长窗口为 1h，30m 作为同版本诊断指标：

- `CRITICAL`：短、长窗口燃烧率均 `>= critical_burn_rate`（默认 6.0）；
- `WARNING`：短、长窗口燃烧率均 `>= warning_burn_rate`（默认 2.0）；
- 否则 `OK`；样本不足时状态为 `OK`，`evaluation_status=INSUFFICIENT_DATA`；
- 所有窗口必须来自同一 version。

### 跨窗口预算一致性

滚动 5m/30m/1h 窗口相互重叠，因此它们的 `budget_consumption_ratio` 不可相加。服务提供两种
可加的规范视图：

1. `ledger`：每个 1 分钟基础桶一行，事件只出现一次；
2. `tumbling_windows`：把同一对齐范围分别按 5m、30m、1h 完整平铺，每个分区不重叠。

`consumed_error_budget` 表示实际坏事件数（错误预算消耗单位），`error_budget_units` 表示允许
坏事件数。二者都可加；比例只能按重算公式 `sum(bad) / sum(allowed)` 得到。
`consistency_checks` 会验证账本、总账和平铺分区逐项相等。

## 持久化、并发和恢复

SQLite 使用 WAL、`synchronous=FULL` 和 `BEGIN IMMEDIATE`。写入流程：

1. 在独立事务中提交 `write_intents(state=RUNNING, target_version)`；
2. 在内存中完成窗口与告警计算；
3. 以一个事务提交事件、新桶版本、关闭快照、`versions`、告警和 ingest 幂等结果；
4. 同一事务把 intent 标为 `COMPLETE`。

因此查询只能看到旧的完整版本或新的完整版本。若进程在步骤 2 被强制终止，重启扫描到
`RUNNING` intent，将其标记为 `ABANDONED`，保留 `last_complete_version`；若在步骤 3 的
SQLite 事务内崩溃，SQLite 原子回滚整笔事务。恢复报告明确说明其只基于持久化事实，不承诺
强杀前未提交的工作或完整的优雅退出报告。

单进程内写者由 `RLock` 串行化；查询不取写锁。必须使用一个 uvicorn worker。

## 配置

| 环境变量 | 默认值 | 说明 |
|---|---:|---|
| `SLO_DATA_DIR` | `data` | 运行时数据目录 |
| `SLO_DB_PATH` | `data/slo.db` | SQLite 文件 |
| `SLO_BUCKET_SECONDS` | `60` | 基础桶长度 |
| `SLO_WINDOWS` | `5m,30m,1h` | 查询与告警窗口 |
| `SLO_TARGET` | `0.999` | SLO 目标 |
| `SLO_MINIMUM_SAMPLES` | `100` | 最小样本量 |
| `SLO_EXACT_EVENT_LIMIT` | `10000` | 超过后采样 |
| `SLO_LATE_TOLERANCE_SECONDS` | `120` | 关闭和迟到容忍期 |
| `SLO_WARNING_BURN_RATE` | `2.0` | WARNING 阈值 |
| `SLO_CRITICAL_BURN_RATE` | `6.0` | CRITICAL 阈值 |
| `SLO_CONFIDENCE_LEVEL` | `0.95` | 采样置信水平 |
| `SLO_STALL_AFTER_INTENT_MS` | `0` | 测试强杀用的意图提交后暂停 |

## 验证

当前实际执行结果见 `VERIFICATION.md`，机器可读端到端输出见
`artifacts/e2e_report.json`。测试覆盖窗口计算、最小样本、迟到拒绝、关闭快照、预算可加性、
确定性采样、CI、告警、幂等、single-flight 和恢复扫描。

设计与不变量说明见 `docs/design.md`。
