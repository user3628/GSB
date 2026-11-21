# 分布式块存储副本修复领域组件

这是一个完全本地、确定性的 Python 3.10+ 示例组件。它把一个副本组内的副本上报汇总为组级估算，输出待修复字节量、定点进度和带相对容差的收敛结论，并通过 SQLite 意图日志支持强制终止后的恢复。

项目不依赖 Docker、外部服务或网络目标。HTTP 服务只监听 `127.0.0.1:18765`。

## 快速开始（Windows PowerShell）

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts\init_db.py --db data\repair.sqlite3
.\.venv\Scripts\python.exe scripts\demo.py
.\.venv\Scripts\python.exe scripts\crash_demo.py
.\.venv\Scripts\python.exe -m unittest discover -s tests -t . -v
```

Linux/macOS 下把 `.\.venv\Scripts\python.exe` 换成 `.venv/bin/python` 即可。

生产式启动（真实系统时钟）：

```powershell
python -m replica_repair.server --db data/repair.sqlite3 --host 127.0.0.1 --port 18765
```

固定样本演示需要可复现的“接收时刻”，因此 `demo.py` 和 `crash_demo.py` 会给子进程设置：

```text
REPAIR_CLOCK_BASE_ISO=2000-01-01T00:00:03Z
```

该环境变量只用于本地固定样本，服务仍使用单调推进的逻辑时钟，所以严格等待耗时真实可测。不设置时服务使用真实 UTC 时间。

## 目录

```text
replica_repair/       领域模型、Decimal/Kahan 数值、SQLite、服务、HTTP
samples/              固定副本组、轮次、上报和数值病态样本
scripts/              初始化、端到端演示、SIGKILL 恢复演示
tests/                16 个本地 unittest 测试
artifacts/            最近一次实际运行输出
TECHNICAL.md          数值、一致性与恢复状态机设计说明
```

## HTTP API

所有接口为 JSON，成功响应包含 `"ok": true`，业务失败包含稳定 `code`。

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` | `/health` | 健康检查，并返回服务逻辑时间 |
| `POST` | `/v1/groups` | 创建或幂等确认副本组 |
| `GET` | `/v1/groups/{group_id}` | 查询副本组 |
| `POST` | `/v1/rounds` | 创建修复轮次 |
| `GET` | `/v1/rounds/{round_id}` | 查询轮次状态，不返回部分估算 |
| `POST` | `/v1/rounds/{round_id}/reports` | 原子提交一批上报 |
| `POST` | `/v1/rounds/{round_id}/aggregate` | 显式指定归一化方案并封版聚合 |
| `GET` | `/v1/rounds/{round_id}/conclusion` | 读取完整已提交结论 |
| `GET` | `/v1/recovery/report/latest` | 读取最近一次启动恢复报告 |

聚合请求示例：

```json
{
  "normalization": "REPORTED_RATIO",
  "strict_wait": false,
  "grace_ms": 0,
  "max_wait_ms": 30000
}
```

`normalization` 缺失时返回 `E_NORMALIZATION_REQUIRED`，不会静默选择方案。允许值为：

- `ALL_EQUAL`：四个副本各取 `1/4`，缺失副本权重为零。
- `REPORTED_RATIO`：仅对已上报副本的配置权重重新归一，使已上报权重合计为 1。
- `MISSING_ZERO`：按全部副本配置权重归一，缺失副本强制为零，因此已上报权重合计可能小于 1。

## 固定样本实际结果

副本组权重为 `0.4000000000000001 / 0.3 / 0.2 / 0.1`，块大小 4 MiB。基础轮次中 `replica-1` 迟到、`replica-3` 缺失。最近一次运行结果：

| 方案 | 组级估算字节 | 待修复字节 | 进度（12 位） | 容差 `1e-8` |
|---|---:|---:|---:|---|
| `ALL_EQUAL` | 2251799821025280 | 1688849860997939 | 0.250000002119 | 收敛 |
| `REPORTED_RATIO` | 6004799512947371 | 4503599628349167 | 0.250000001059 | 收敛 |
| `MISSING_ZERO` | 3602879707768423 | 2702159777009501 | 0.250000001059 | 收敛 |

同一 `ALL_EQUAL` 估算改用 `1e-12` 容差后，进度仍为 `0.250000002119`，但允许差仅 `0.000000000000250000002119`，结论稳定翻转为不收敛。完整数据见 `artifacts/demo_output.json`。

迟到上报进入下一轮后，`REPORTED_RATIO` 的待修复字节从 `4503599628349167` 变为 `3302639727390812`，差值为 `-1200959900958355`；进度从 `0.250000001059` 变为 `0.400000000593`。

强制终止演示中，SIGKILL 前可观察到：

```text
intent_state = PREPARED
round_status = AGGREGATING
conclusion_count = 0
```

重启后启动恢复把轮次标记为 `ABORTED`，恢复报告动作是 `MARKED_ABORTED`；同一意图参数重试后提交成功，执行次数变为 2，再次聚合复用同一摘要。完整输出见 `artifacts/crash_recovery_output.json`。

## 业务码

| 业务码 | 含义 |
|---|---|
| `E_REPORT_INVALID` | 上报字段、副本标识、整数范围或批次结构非法；整批不入库 |
| `E_NORMALIZATION_REQUIRED` | 聚合未显式声明归一化方案 |
| `E_NORMALIZATION_UNKNOWN` | 归一化方案不在允许集合 |
| `E_GROUP_VERSION_MISMATCH` | 副本组版本与请求或轮次快照不一致 |
| `E_CONFLICTING_ROUND` | 同一轮次以不同输入重复聚合 |
| `E_CONFLICTING_REPORT` | 上报标识、副本或迟到队列发生冲突 |
| `E_PROGRESS_OUT_OF_RANGE` | 进度超出 `[0,1]` 或估算为零但已同步为正；不截断、不产生 NaN |
| `E_ROUND_NOT_READY` / `E_ROUND_ABORTED` | 尚无完整提交结论 |
| `E_WAIT_TOO_LONG` | 严格等待超过 `max_wait_ms` |

## 语义要点

- 上报在满足任一条件时进入迟到队列：`reported_at > deadline`、服务收到时刻 `> deadline`、或轮次已经封版。迟到项不会进入当前轮次。
- 若迟到项到达时已存在下一个未封版轮次，立即归入该轮；否则等待下一轮创建时领养。
- `strict_wait=true` 会先等到 `deadline`；仍缺副本时再等待 `grace_ms`。结论文档中输出 `WAIT_FOR_DEADLINE`、`WAIT_FOR_MISSING_REPLICAS` 和实际 `delay_ms`。
- 组级估算为 `Σ(normalized_weight_i × block_count_i) × block_size_bytes`；已同步量为 `Σ(normalized_weight_i × synced_bytes_i)`。
- 字节先按 `ROUND_HALF_EVEN` 舍入到整数，再相减得到待修复量；进度固定 12 位小数。每一步的输入、输出、舍入模式和误差都在结论的 `rounding.steps` 中。
- 收敛公式为 `abs(actual-target) <= tolerance * max(abs(actual),abs(target))`。`delta == allowed_delta` 时按包含边界判为收敛；参与者按 `(replica_id, report_id)` 字典序固定。
- 报告、权重和估算按副本标识排序并以 80 位 Decimal 计算，重复聚合直接读取同一持久化 JSON，避免运算顺序漂移。

## 验证

```powershell
python -m unittest discover -s tests -t . -v
python scripts/demo.py
python scripts/crash_demo.py
```

最近一次测试结果：16 个测试全部通过。演示脚本会通过固定回环地址 `http://127.0.0.1:18765` 访问真实 HTTP 子进程。`crash_demo.py` 使用 `Popen.kill()` 强制终止聚合子进程，再从同一 SQLite 文件重启并重试。

更深的设计与误差上界说明见 `TECHNICAL.md`。

