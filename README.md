# 本地可观测性采样协调器

这是一个完全离线、可重复验证的日志/追踪采样协调应用。它面向 SRE 在固定本地数据集上评估头部采样、尾部采样、偏置校正和崩溃恢复，不连接任何真实账号、线上服务或外部采集系统。

实现栈：Python 3.11+、FastAPI、Uvicorn、NumPy、pytest、SQLite（标准库 `sqlite3`）。本工作区实际验证环境为 Python 3.14.4、FastAPI 0.165.0、Uvicorn 0.54.0、NumPy 2.5.5、pytest 9.2.0、SQLite 3.46.1。

## 快速开始

Windows PowerShell：

```powershell
py -3.11 -m venv --system-site-packages .venv
.\.venv\Scripts\python.exe scripts\bootstrap_offline.py
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m obs_sampler.demo
```

Linux/WSL：

```bash
python3 -m venv --system-site-packages .venv
./.venv/bin/python scripts/bootstrap_offline.py
./.venv/bin/python -m pytest
./.venv/bin/python -m obs_sampler.demo
```

`bootstrap_offline.py` 只使用 `--no-index` 和本机已有包；如果另有离线 wheel 目录，可传入 `--wheelhouse <目录>`。固定版本见 `requirements.txt`。现有 `.venv` 即按该方式创建并验证。

启动 HTTP 服务：

```powershell
$env:OBS_DB_PATH = "$PWD\runtime\api.db"
$env:OBS_LOG_DIR = "$PWD\logs"
.\.venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8000
```

```bash
OBS_DB_PATH=./runtime/api.db OBS_LOG_DIR=./logs ./.venv/bin/python -m uvicorn main:app --host 127.0.0.1 --port 8000
```

单进程建议使用 `workers=1`：应用内部由单写者锁和 SQLite `BEGIN IMMEDIATE` 串行写，多个查询可从 WAL 中并发读取。跨实例的同批次提交也会等待并由后到者复用已提交结果。

## 固定离线数据

`samples/` 中的文件全部由 `scripts/generate_samples.py` 确定性生成：

| 文件 | 内容 |
| --- | --- |
| `samples/logs.jsonl` | 712 条日志，按毫秒时间戳非递减排列 |
| `samples/spans.jsonl` | 704 条跨度，按毫秒时间戳非递减排列 |
| `samples/policy_v1.json` | 版本 1 策略快照与 SHA-256 摘要 |
| `samples/policy_v2.json` | 版本 2 策略快照与 SHA-256 摘要 |
| `samples/naive_expected.json` | 两个版本的精确去重总体与仅头部采样的预期集合摘要 |
| `samples/manifest.json` | 以上文件的 SHA-256 文件摘要 |

日志行示例：

```json
{"timestamp_ms":1735689600000,"service":"checkout","level":"INFO","message_fingerprint":"fp-checkout-info-00","trace_id":"trace-checkout-info-0000","span_id":"span-log-checkout-info-0000","parent_span_id":"span-root-checkout-info-0000","sample_weight":1.0}
```

跨度行示例：

```json
{"timestamp_ms":1735689600007,"service":"checkout","level":"INFO","message_fingerprint":"span-op-00","trace_id":"trace-checkout-info-0000","span_id":"span-child-checkout-info-0000","parent_span_id":"span-root-checkout-info-0000","latency_us":275000,"error":false,"sample_weight":2.0}
```

批次提交接口分别接收 `logs` 和 `spans` 数组。每个流内部必须按 `timestamp_ms` 非递减；任一行非法、重复 `(kind, trace_id, span_id)` 或任一流时间戳回退，都返回 `E_RECORD_INVALID`，且不会写入任何批次、记录、决策、计数或偏置行。时间戳是整数毫秒，不使用浮点秒。

## 策略快照

策略文件包含：

- 单调版本号 `version`、生成时间、固定 `seed`。
- 按服务/级别的 `strata` 头部采样率。
- 尾部规则：`latency_threshold_us`、`base_rate`、`error_multiplier`。
- `dedup_window_ms`、`min_samples`、`error_bound`、`bias_correction`。
- 规范 JSON 的 SHA-256 `digest`。计算时对除 `digest` 外的完整文档做排序键、紧凑分隔符、UTF-8 编码哈希。

示例：

```json
{
  "version": 1,
  "seed": 20261120,
  "dedup_window_ms": 500,
  "dedup_scope": "trace_per_kind",
  "min_samples": 12,
  "error_bound": 0.4,
  "bias_correction": true,
  "strata": {"checkout": {"INFO": 0.65, "ERROR": 1.0}},
  "tail_sampling": {
    "latency_threshold_us": 250000,
    "base_rate": 0.35,
    "error_multiplier": 2.0
  },
  "digest": "<sha256>"
}
```

提交策略时先验证摘要，再按当前版本执行单调性检查。`version <= current_version` 返回 `E_STALE_SNAPSHOT`。升版时，应用读取全部已持久化原始记录，在同一事务内重建新版本的决策、分层计数、偏置行和提交摘要；旧版本保留为独立只读快照。

重新生成样本与摘要：

```bash
./.venv/bin/python scripts/generate_samples.py
```

## 采样语义

### 确定性头部与尾部

每条记录的头部随机量由快照版本、固定种子、批次标识、信号类型和行内序号确定：

```text
u_head = first64(blake2b("head|version|seed|batch_id|kind|source_index")) / 2^64
head_selected = u_head < head_rate(service, level)
```

跨度尾部随机量使用独立的域分隔，避免与头部相关：

```text
u_tail = first64(blake2b("tail|version|seed|record_uid")) / 2^64
eligible = error or latency_us >= latency_threshold_us
p_tail = min(1, base_rate * (error_multiplier if error else 1))
tail_selected = eligible and u_tail < p_tail
```

冲突裁决：

1. 头部先产生 `PENDING -> SAMPLED|DROPPED`。
2. 尾部只做保留或提升，绝不把已头部抽中的记录降级。若尾部命中且记录已头部抽中，路径为 `TAIL_RETAINED`；若头部未抽中而尾部命中，路径为 `TAIL_PROMOTED`。
3. 尾部未命中时保留原头部结果。若最终发出，状态再进入 `EMITTED`；否则保持 `DROPPED`。
4. 对触发尾部的记录，联合入选概率为 `p = head_rate + (1-head_rate) * p_tail`；未触发尾部时为 `p = head_rate`。偏置校正开启时最终权重为 `upstream_weight / p`。被提升记录使用该联合概率，不复用 `1/head_rate` 的头部权重。

日志不参与跨度尾部规则；日志仅走分层头部决策。层级与延迟阈值边界均为包含式（`>=`）。

### 去重窗口

默认 `dedup_scope=trace_per_kind`：日志和跨度是两类独立信号，各自以 `(kind, trace_id)` 去重。样本策略也可显式设为 `trace_global`，此时一个追踪窗口跨日志与跨度只保留一个规范记录。

每个窗口从规范记录时间戳 `t0` 开始，到 `t0 + dedup_window_ms` 结束，两端都包含；时间戳为整数毫秒。窗口内后续同键记录标为 `DROPPED/DEDUP_WINDOW`，窗口结束后下一条记录开启新窗口并成为新的规范记录。规范记录排序列为 `(timestamp_ms, kind, batch_id, source_index, record_uid)`。

去重、头部、尾部、分层计数和偏置报告都在同一策略版本的同一事务中生成，因此日志与跨度不会出现一侧已更新而另一侧仍旧值的可见中间状态。

### 状态与计数

每条决策保存最终状态以及 `PENDING/SAMPLED/DROPPED/EMITTED` 迁移历史：

- 头部抽中、最终发出：`PENDING -> SAMPLED -> EMITTED`。
- 头部未抽中、尾部提升：`PENDING -> DROPPED -> EMITTED`。
- 未发出：`PENDING -> SAMPLED|DROPPED`。
- 窗口内重复：`PENDING -> DROPPED`，路径 `DEDUP_WINDOW`。

分层计数只统计去重后的规范记录。每条运行使用 Kahan 补偿求和累加总体权重、发出权重与抽样方差项，并输出原始补偿值、数值误差界、Horvitz-Thompson 方差估计、标准误、95% 置信界以及相对误差界。估计总量为加权发出总量，朴素总体为去重后的本机精确权重和。

`min_samples` 是发出样本阈值。若某层总体存在但从未抽样，报告保留真实总体计数与权重，`weighted_total` 为 `null`，状态为 `INSUFFICIENT`；HTTP 查询返回 `E_INSUFFICIENT_SAMPLES`，不会用零计数冒充估计。发出样本少于阈值时同样返回该业务码。

## HTTP API

失败响应统一为：

```json
{"code":"E_RECORD_INVALID","message":"...","details":{}}
```

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/v1/policies` | 提交策略快照 |
| `GET` | `/api/v1/policies/current` | 当前策略 |
| `GET` | `/api/v1/policies/{version}` | 指定版本策略 |
| `GET` | `/api/v1/policies/{version}/sampling-rate?service=&level=` | 查询服务/级别采样率 |
| `GET` | `/api/v1/policies/{version}/bias?service=&level=` | 查询偏置报告 |
| `GET` | `/api/v1/policies/{version}/state` | 完整版本派生状态与摘要 |
| `POST` | `/api/v1/batches` | 批次入库与整版重算 |
| `GET` | `/api/v1/batches/{batch_id}` | 批次决策与幂等结果 |
| `GET` | `/api/v1/recovery-report` | 最近一次恢复报告 |
| `POST` | `/api/v1/recovery/scan` | 显式扫描未完成意图 |
| `GET` | `/health` | 进程、数据库与当前版本 |

同时提供 `/api/v1/bias` 和 `/api/v1/sampling-rate` 别名，`policy_version` 缺省时使用当前版本。

业务码至少包括：

| 业务码 | HTTP | 含义 |
| --- | --- | --- |
| `E_RECORD_INVALID` | 422 | 行非法、请求模式错误、流内时间戳非单调或批次为空 |
| `E_CONFLICTING_RECORD` | 409 | 相同 `batch_id` 对应不同内容 |
| `E_STALE_SNAPSHOT` | 409 | 策略或批次引用了非当前版本 |
| `E_INSUFFICIENT_SAMPLES` | 422 | 未抽样或样本数低于阈值 |
| `E_POLICY_INVALID` | 422 | 策略字段或摘要无效 |
| `E_POLICY_NOT_FOUND` | 404 | 策略版本不存在 |
| `E_BATCH_NOT_FOUND` | 404 | 批次不存在 |

## 持久化与恢复

SQLite 数据库使用 WAL 与 `synchronous=FULL`。主要事实/派生表：

- `policies`、`batches`、`records`：输入事实与版本快照。
- `decisions`、`decision_transitions`、`dedup_windows`：逐条决策、状态演进与去重窗口。
- `strata_counts`、`bias_reports`：服务/级别计数与偏置快照。
- `derived_commits`：每个版本的原子派生提交标记、修订号与决策/计数/报告摘要。
- `intents`：先于业务事务独立提交的意图日志。
- `recovery_reports`：最近启动扫描得到的已落盘事实报告。

每次策略提交或批次提交都先独立提交一条 `RUNNING` 意图，然后在 `BEGIN IMMEDIATE` 主事务中完成事实、整版派生状态、提交标记和意图完成状态。主事务未提交时，SQLite 回滚全部写入；重启扫描会把遗留 `RUNNING` 意图标为 `ABORTED`，并验证当前版本指针指向存在的策略和派生提交。若指针损坏，会回退到数据库中版本最高的完整派生提交。

恢复报告明确标记 `scope=COMMITTED_SQLITE_FACTS_ONLY`、`forced_termination_exit_report_guaranteed=false`，只列出已提交策略、记录、决策状态、完整版本等进程失败后仍可证明的事实，不承诺强制终止时本应生成的完整退出报告。

## 演示结果（实际运行）

`./.venv/bin/python -m obs_sampler.demo` 会清空并重建 `runtime/demo.db`，输出 `runtime/demo_summary.json`。固定参数为 seed `20261120`、v1/v2 去重窗口 `500/350 ms`、误差上界 `0.40`、最小样本 `12`、尾部延迟阈值 `250000/300000 us`，崩溃退出码 `87`。

实际结果：

- 输入：712 条日志 + 704 条跨度 = 1416 条记录。
- 去重后规范记录 1288 条，窗口内重复 128 条。
- v1 最终状态：`EMITTED=1020`，`DROPPED=396`；路径为 `HEAD=825`、`TAIL_RETAINED=186`、`TAIL_PROMOTED=9`、`HEAD_DROPPED=268`、`DEDUP_WINDOW=128`。9 条提升记录的权重均按联合入选概率调整，测试显式验证其不等于头部权重。
- 仅头部采样的规范记录集合与固定预期摘要核对：v1 为 1011 条、v2 为 963 条，`sampled_uids_sha256` 完全一致。
- v1 偏置：8 个分层全部满足配置误差上界；校正估计平均绝对相对偏差 2.62%，最大 8.55%；未校正样本计数平均绝对偏差 20.81%。
- 强制终止：子进程在 v2 的决策、计数与偏置行写入后、提交标记前执行 `os._exit(87)`。重启后当前版本仍为 1，v2 不可见，v1 的决策/计数/报告摘要完全不变；恢复报告列出 1 条被中止的 `POLICY_COMMIT` 意图，外键检查为空。
- 恢复后重新提交 v2 成功：1416 条记录重算，`EMITTED=986`、`DROPPED=430`；v2 校正估计平均绝对相对偏差 3.48%，最大 9.97%，8 个分层均在配置误差上界内。
- 重复提交同一批次返回 `REPLAYED_COMMITTED_RESULT`，当前 v2 决策被复用，记录不重复入库。

## 验证结果

```text
$ ./.venv/bin/python -m pytest
................................                                         [100%]
32 passed, 1 warning in ~2.3s
```

唯一警告来自本机 FastAPI 0.165.0 对 `starlette.testclient`/`httpx` 组合的上游弃用提示，不影响测试。`pyflakes` 对应用、测试和脚本无输出。测试覆盖：

- 非单调、非法或重复标识批次整批拒绝；批次幂等与内容冲突。
- 策略摘要、版本单调性、升版全量重算与旧版本保留。
- 固定版本/种子/序号的可重复决策；头部/尾部优先级；错误放大；提升权重调整。
- 包含式去重窗口边界、按信号去重与可选全局追踪去重。
- Kahan 补偿、加权估计、置信误差界、未抽样与样本不足业务码。
- 仅头部采样的规范记录集合与 `naive_expected.json` 摘要逐条一致。
- 单服务与双服务实例并发同批次只执行一次，其余等待并复用。
- 未提交写事务期间 8 个并发读者只看到上一完整提交。
- 子进程真实强制终止后的意图中止、事务回滚、摘要一致性与恢复后重试。
- HTTP 全流程及统一业务错误 JSON。
- 真实 Uvicorn 进程冒烟：`/health`、策略提交、1416 条批次、采样率、偏置与恢复报告查询均返回 200。

## 目录

```text
obs_sampler/        应用、采样引擎、SQLite、API、演示与崩溃 worker
samples/            固定日志/跨度样本、两份策略、朴素预期结果、manifest
scripts/            样本生成与离线环境引导
tests/              32 项 pytest 验证
runtime/            演示数据库与 demo_summary.json（运行产物被 git 忽略）
logs/               本地轮转运行日志（被 git 忽略）
main.py             Uvicorn 入口
```
