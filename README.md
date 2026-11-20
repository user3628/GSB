# 本地列式 Join / Shuffle 执行引擎

面向本地确定性列式样本的 Join 与 Shuffle 执行引擎。项目只使用 Python 3.11+、NumPy 与 Python 标准库 `sqlite3`，不依赖 Docker、外部服务或真实账号，HTTP 服务只允许绑定回环地址。

本次交付的实测环境为当前工作区：Python 3.14.4、NumPy 2.5.5、SQLite 3.46.1。实现只使用 Python 标准库和跨平台 NumPy API，Windows 命令与路径已在下文给出。

核心交付：

- 不可变列式快照：整数键、双精度度量、逐行空值位图、版本、生成时间与 SHA-256 内容摘要。
- 确定性 SplitMix64 哈希分区：同键必同分区；输出每个分区的记录数、字节估计、偏斜标记与热点键。
- 内连接与左外连接：分区内按 `(key, row locator)` 稳定排序，分组输出 `sum / mean / count`。
- 高精度路径：Kahan 补偿求和 + 固定规范的全局归并顺序，使结果与分区数无关、逐位一致。
- 快速路径：朴素顺序求和；精度目标越界返回 `E_PRECISION_BUDGET`，不会静默降级。
- SQLite 单写者状态机：请求幂等、FIFO 排队、分区边界持久化、未完成意图恢复。
- 本地 HTTP API、固定种子样本生成器、可重复实验脚本与自动化测试。

## 目录

```text
join_shuffle_engine/       引擎、快照、SQLite、服务与 HTTP API
scripts/generate_samples.py 固定随机种子的样本与快照生成器
scripts/run_experiments.py  分区一致性、精度、偏斜、预算与恢复实验
scripts/crash_drill.py      强制终止与恢复演练
tests/                      unittest 自动化测试
data/snapshots/             已生成或可重新生成的本地快照
artifacts/                  实验 JSON、恢复报告与运行输出
docs/TECHNICAL.md           算法、事务、误差合同与验收对照
```

## Windows 快速开始

在 PowerShell 中执行：

```powershell
cd <项目目录>
py -3.11 -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt

py scripts\generate_samples.py --force
py -m unittest discover -s tests -v
py scripts\run_experiments.py
py scripts\crash_drill.py --phase all
```

启动 API：

```powershell
py -m join_shuffle_engine --db artifacts\engine.db --host 127.0.0.1 --port 8765
```

仅接受 `127.0.0.1`、`localhost`、`::1`。指定其它主机时服务拒绝启动。端口可用 `--port` 修改，默认 `8765`。

## 固定样本

`scripts/generate_samples.py` 使用固定种子 `20261121`，并在 Python 3.11+ / NumPy 整数与 IEEE-754 双精度运算上确定性生成：

| 快照 | 行数 | 用途 |
|---|---:|---|
| `demo-left-v1` | 9000 | 常规左表，重复键、空值、混合量级 |
| `demo-right-v1` | 2212 | 常规右表，1–3 条匹配记录，部分键缺失 |
| `demo-left-v2` / `demo-right-v2` | 9000 / 2212 | 版本单调换版样本 |
| `precision-left-v1` | 1005 | 含 `1e16 + 1000 × 1.0` 混合量级组 |
| `precision-right-v1` | 3 | 未匹配键、右度量全空样本 |
| `skew-left-v1` | 8500 | 热点键 `7` 占 7000 行 |
| `skew-right-v1` | 751 | 偏斜 Join 右表 |

快照目录格式：

```text
manifest.json    格式、side、version、generated_at、dtype、数组摘要、content_sha256
keys.bin         little-endian signed int64
measures.bin     little-endian IEEE-754 float64
nulls.bin        逐行 u8 位图，1 表示 NULL
```

加载时会检查：

- `format`、字节序、dtype、文件长度和每个数组 SHA-256；
- 整个 manifest 核心内容的 `content_sha256`；
- 空值位图只能取 0/1；NULL 位置的度量必须为规范 `0.0`；
- 非 NULL 度量必须有限；度量维度必须为 `dimensionless`；单位必须在允许集合内；
- 同一数据集的左、右快照版本都必须严格递增。

任一失败返回 `E_SNAPSHOT_INVALID`，不会激活新版本。

## HTTP API

所有成功响应：

```json
{"ok": true, "data": {}}
```

所有错误响应：

```json
{"ok": false, "error": {"code": "E_SNAPSHOT_INVALID", "message": "...", "details": {}}}
```

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/v1/health` | 回环服务健康检查 |
| GET | `/v1/datasets` | 当前活动快照对 |
| POST | `/v1/snapshots/load` | 校验并原子替换左右快照 |
| POST | `/v1/shuffle` | 提交 Shuffle 统计请求 |
| POST | `/v1/join` | 提交 Join + 聚合请求 |
| GET | `/v1/requests/{id}` | 请求状态、排队语义、绑定版本 |
| GET | `/v1/requests/{id}/partitions` | 已提交分区统计、热点键、字节估计 |
| GET | `/v1/requests/{id}/result` | 已提交分组聚合结果 |
| GET | `/v1/requests/{id}/precision` | 已提交误差声明与路径结论 |
| GET | `/v1/requests/{id}/partials` | 分区部分和审计记录 |
| GET | `/v1/recovery/report` | SQLite 持久化事实推导出的恢复报告 |

### PowerShell 调用示例

加载演示快照：

```powershell
$root = (Get-Location).Path
$body = @{
  dataset_id = "demo"
  left_path  = "$root\data\snapshots\demo-left-v1"
  right_path = "$root\data\snapshots\demo-right-v1"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8765/v1/snapshots/load `
  -ContentType "application/json" -Body $body
```

提交高精度 Join：

```powershell
$body = @{
  request_id    = "demo-p7"
  dataset_id    = "demo"
  partitions    = 7
  join_type     = "inner"
  measure       = "left"
  skew_limit    = 0.35
  skew_action   = "report"
  precision     = @{ mode = "high" }
} | ConvertTo-Json -Depth 5

$response = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8765/v1/join `
  -ContentType "application/json" -Body $body
$response.data.result.results | Select-Object -First 5
```

查询：

```powershell
Invoke-RestMethod http://127.0.0.1:8765/v1/requests/demo-p7/partitions
Invoke-RestMethod http://127.0.0.1:8765/v1/requests/demo-p7/result
Invoke-RestMethod http://127.0.0.1:8765/v1/requests/demo-p7/precision
Invoke-RestMethod http://127.0.0.1:8765/v1/recovery/report
```

异步提交时设置 `"wait": false`，接口返回 HTTP 202；随后使用 `GET /v1/requests/{id}?wait=1` 等方式等待。相同 `request_id` 与相同 payload 重复提交只登记一次执行，其余调用等待并复用同一结果。相同 request id 携带不同 payload 返回 `E_REQUEST_CONFLICT`。

### 请求字段

Join：

```json
{
  "request_id": "demo-p7",
  "dataset_id": "demo",
  "partitions": 7,
  "join_type": "inner",
  "measure": "left",
  "skew_limit": 0.35,
  "skew_action": "report",
  "precision": {
    "mode": "auto",
    "target_abs_error": null,
    "max_operations": null
  },
  "wait": true,
  "timeout": 60
}
```

- `join_type`: `inner` 或 `left`。
- `measure`: `left`、`right`、`both`；`both` 在任一侧 NULL 时结果 NULL，否则相加。
- `skew_action`: `report` 返回偏斜标记与热点键；`reject` 返回 `E_SKEW_LIMIT`。
- `precision.mode`: `auto`、`high`、`fast`。
- `target_abs_error`: 任意分组求和绝对误差目标。
- `max_operations`: 计算预算；成本包含分区数、偏斜惩罚、连接行数与归并组数。

Shuffle 请求使用相同分区/偏斜字段，不需要精度字段。

## 数值精度合同

`u = 2^-53`，`N` 为参与求和的有度量记录数，`S = Σ|x_i|`。

- 高精度路径：Kahan 补偿求和；固定 4096 条 merge chunk；误差界为
  `(4u + 16·N·u²) · S`。
- 快速路径：顺序朴素求和；误差界为
  `N·u/(1-N·u) · S`。
- 均值由 `sum_value / non_null_count` 推导，不累加均值。
- `count` 是连接输出行数，包含 NULL 度量；`non_null_count` 是求均值的分母。
- 某组全部度量为 NULL 时确定返回 `sum_value=0.0`、`mean_value=null`、`non_null_count=0`。

`auto` 的选择顺序：

1. 目标足够宽松且快速路径误差界满足目标时选快速路径；
2. 否则高精度路径满足目标且预算足够时选高精度路径；
3. 两条路径都不能满足目标或预算时返回 `E_PRECISION_BUDGET`；
4. 显式 `fast` 但误差界越界时绝不静默降级，返回 `E_PRECISION_BUDGET`。

高精度路径的规范归并顺序固定为 `(group_key, left_row_index, right_row_index)`，与请求分区数无关，因此同一输入在 1、2、3、7、17、64 等任意分区数下结果逐位一致。

## 并发与持久化

- 一个进程内使用单线程 FIFO writer 串行执行 Shuffle/Join；多个查询可并发读取 SQLite WAL 中已提交版本。
- 请求在提交时绑定活动快照对。之后加载新快照只影响后续请求，不会混用旧统计。
- 分区统计、部分和、聚合结果只有在最终事务提交后才可查询；执行中的请求返回 `E_REQUEST_NOT_COMMITTED`。
- 每次写入先提交 pending intent，再执行工作，最后在同一事务中提交完整分区或最终结果。
- 进程重启时扫描 `starting/running` 执行与 pending intent，将执行标记为 `aborted`，保留最后一个完整分区边界并生成恢复报告。
- 恢复报告只陈述 SQLite 已持久化事实，明确 `exit_report_complete=false`；不承诺强制终止后存在完整退出报告。

## 中断恢复演练

```powershell
py scripts\crash_drill.py --phase partition --partitions 8 --crash-at 3
py scripts\crash_drill.py --phase merge --partitions 8
py scripts\crash_drill.py --phase all
```

- `partition`：在第 3 分区已写 pending intent、但分区未完成时 `os._exit(96)`。重启后最后完整分区为 2，持久化分区为 0/1/2。
- `merge`：8 个分区全部完成，最终归并意图已写、聚合尚未提交时 `os._exit(98)`。重启后无聚合结果，执行标记 aborted。
- 输出写入 `artifacts/crash_recovery_report.json`。

## 实测结果

运行 `py scripts/run_experiments.py` 后的实测输出摘要：

| 分区数 | 分组数 | 连接行数 | 聚合签名前缀 | 与 p=1 逐位一致 |
|---:|---:|---:|---|---|
| 1 | 1126 | 16623 | `558caba1d3a8fff0` | 是 |
| 2 | 1126 | 16623 | `558caba1d3a8fff0` | 是 |
| 3 | 1126 | 16623 | `558caba1d3a8fff0` | 是 |
| 7 | 1126 | 16623 | `558caba1d3a8fff0` | 是 |
| 17 | 1126 | 16623 | `558caba1d3a8fff0` | 是 |
| 64 | 1126 | 16623 | `558caba1d3a8fff0` | 是 |

混合量级固定样本（`1e16` 后接 1000 个 `1.0`）：

- `math.fsum` 精确参考：`1.0000000000001e16`
- 高精度：`1.0000000000001e16`，绝对误差 `0.0`，声明界 `4.440892098503045`
- 快速：`1e16`，绝对误差 `1000.0`，声明界 `1111.3332476500163`

其它实测：

- 左外连接未匹配组：`sum_value=0.0`、`mean_value=null`、`count=2`、`non_null_count=0`。
- 偏斜：8 分区中分区 7 占比 `0.789861`，热点键 `7` 在分区内占 `0.958122`；`reject` 返回 `E_SKEW_LIMIT`。
- 精度预算：快速路径目标 `10.0` 返回 `E_PRECISION_BUDGET`；`auto` + 2000 操作预算同样返回 `E_PRECISION_BUDGET`。
- 重复并发提交：执行次数 `1`，其余调用复用结果。
- 快照同版本重载：`E_SNAPSHOT_INVALID`。
- 中断恢复：分区中断最后完整分区 `2`、3 个完整分区、0 个聚合；归并中断 8 个完整分区、0 个聚合。
- 自动化测试：`Ran 8 tests ... OK`。

完整机器可读输出见 `artifacts/experiment_report.json`、`artifacts/crash_recovery_report.json`；控制台输出见 `artifacts/experiment_output.txt` 与 `artifacts/test_output.txt`。

## 错误码

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_INVALID_REQUEST` | 400 | 字段、类型、范围或路径非法 |
| `E_SNAPSHOT_INVALID` | 400 | 摘要、版本、dtype、空值位图、量纲或文件校验失败 |
| `E_DATASET_NOT_FOUND` | 404 | 数据集未加载 |
| `E_REQUEST_NOT_FOUND` | 404 | 请求不存在 |
| `E_REQUEST_CONFLICT` | 409 | 同 request id 不同 payload 或状态冲突 |
| `E_REQUEST_NOT_COMMITTED` | 409 | 请求尚未提交可查询结果 |
| `E_EXECUTION_ABORTED` | 409 | 执行在恢复扫描中被标记中止 |
| `E_SKEW_LIMIT` | 422 | 偏斜分区超过声明阈值且策略为 reject |
| `E_PRECISION_BUDGET` | 422 | 精度目标或计算预算无法由可用路径满足 |
| `E_NUMERIC_OVERFLOW` | 422 | 聚合超出有限 float64 范围 |
| `E_INTERNAL` | 500 | 未预期内部失败 |

## 已知边界

- 哈希共用只在单进程 writer 模型内保证；不要对同一 SQLite 文件启动多个 writer 进程。
- 高精度路径是 Kahan 合同与规范归并，不声称优于正确舍入的 `math.fsum`；误差界是保守上界。
- 快照文件生成后应视为不可变。篡改会由摘要拒绝；执行中要求 pin 的快照内容保持一致。
- 键为有符号 64 位整数，度量为有限 IEEE-754 double；字面 `NaN/Inf` 的度量不允许进入快照。

更深入的算法和事务说明见 `docs/TECHNICAL.md`。
