# 本地列式向量化表达式执行引擎

面向本地分析管线的零外部服务执行组件。它从固定种子的列式样本构建带空值位图的批次，按 `scalar`（精确逐行）或 `vector`（NumPy 整数组）路径执行表达式计划，并通过本地 HTTP API 暴露会话、结果、批次统计和恢复报告。会话、检查点、批统计与意图日志持久化在 SQLite；进程被强制终止后，重启只依据已提交事实返回中止报告。

详细语义见 [docs/CONTRACT.md](docs/CONTRACT.md)，实际验收数据见 [docs/VERIFICATION.md](docs/VERIFICATION.md)。

## 功能概览

- 表达式：列引用、三类常量、算术、比较、逻辑、条件、Filter、Project、Aggregate。
- 数据类型：`int64`、`float64`、`bool`；严格类型匹配，不做隐式数值提升。
- 双执行路径：同一计划可显式跑 `scalar` 或 `vector`，也可由行数、列数与计划复杂度自动选择。
- 数值契约：整型回绕/饱和、除零为 `null`、IEEE-754 比较、固定小数位 JSON、NPZ 全精度下载、Neumaier 补偿求和。
- 规模自适应：批大小与内存预算联动；超预算在执行前返回 `E_MEMORY_BUDGET`。
- 并发与幂等：单写者串行执行、WAL 并发只读、同请求 ID 只执行一次并等待复用。
- 崩溃恢复：意图先落库，批次检查点原子提交，结果与 `READY` 状态原子发布；强制终止后只会报告已持久化的最后完整批次。

## 目录

```text
vecengine/
  model.py          列式模型、空值位图、样本注册表
  plan.py           严格计划解析与类型推导
  evaluator.py      标量/向量内核、聚合、补偿求和
  selection.py      路径、批大小与内存预检
  persistence.py    SQLite 状态机、意图、检查点、恢复报告
  engine.py         单写者调度、幂等复用、结果发布与查询
  api.py / server.py 本地 HTTP 服务
  samples.py        固定种子三档样本生成
  examples.py       实验用表达式与聚合计划
scripts/
  check_environment.py
  generate_samples.py
  run_experiment.py
  recovery_drill.py
  reset_local_state.py
tests/              15 项 unittest（API、双路径、并发、恢复）
docs/               语义契约与验证记录
```

## Windows 环境准备

要求 Windows 10/11、Python 3.11 或更高版本、内置 SQLite。以下命令在 PowerShell 中执行：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python scripts/check_environment.py
```

依赖清单固定为 `numpy==2.3.5`、`requests==2.32.5`。执行引擎只用 Python 标准库与 NumPy；`requests` 用于实验/恢复脚本。代码不使用 Docker、外部服务、网络数据或平台专属进程 API；强制终止演练使用 `Popen.kill()`，在 Windows 上对应 `TerminateProcess`。

当前交付环境的实际验证是 Linux/WSL2 + Python 3.14.4 + SQLite 3.46.1；代码与脚本按 Python 3.11+ 和 Windows 路径规则编写。若要在 Windows 做同等验收，重复本文命令即可。

## 生成样本

```powershell
python scripts/generate_samples.py
```

默认写入 `data/samples/`：

| 档位 | 行数 | 文件 |
| --- | ---: | --- |
| small | 5,000 | `small.npz` |
| medium | 250,000 | `medium.npz` |
| large | 2,000,000 | `large.npz` |

固定种子为 `20261120`。每档包含 `id`、`i64_a`、`i64_b`、`f64_a`、`f64_b`、`flag`，每个数组配 `__valid` 位图。前 15 行注入空值、`INT64_MIN/MAX`、乘法溢出、除零、负数除法和浮点边界；随机区域也按固定步长分布空值。

可覆盖行数或重建：

```powershell
python scripts/generate_samples.py --rows small=2000 --rows medium=100000 --rows large=1000000 --force
```

## 启动服务

```powershell
python -m vecengine.server --host 127.0.0.1 --port 8765 --db artifacts/vecengine.db --data-dir data/samples --results-dir artifacts/results
```

可用环境变量：

| 变量 | 默认值 |
| --- | ---: |
| `VECENGINE_HOST` / `VECENGINE_PORT` | `127.0.0.1` / `8765` |
| `VECENGINE_SCALAR_MAX_ROWS` | `50000` |
| `VECENGINE_SCALAR_MAX_CELLS` | `400000` |
| `VECENGINE_SCALAR_MAX_NODES` | `48` |
| `VECENGINE_BATCH_MIN` / `VECENGINE_BATCH_MAX` | `512` / `65536` |
| `VECENGINE_MEMORY_BUDGET_BYTES` | `536870912` |
| `VECENGINE_DUPLICATE_WAIT_SECONDS` | `600` |
| `VECENGINE_TEST_HOOKS` | 未设置；恢复演练子进程设为 `1` |

`--reset` 只清理 DB/WAL/SHM 与 `--results-dir`，用于可重复重建。也可单独运行：

```powershell
python scripts/reset_local_state.py --db artifacts/vecengine.db --results-dir artifacts/results
```

## HTTP API

所有响应使用统一包装：成功为 `{"ok":true,"data":...}`，失败为 `{"ok":false,"error":{"code","message","details"}}`。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `GET` | `/datasets` | 样本元数据 |
| `POST` | `/sessions` | 提交计划，同步等待可复用结果 |
| `GET` | `/sessions/{id}` | 会话状态与选择依据 |
| `GET` | `/sessions/{id}/result?offset=0&limit=100` | 分页 JSON 结果 |
| `GET` | `/sessions/{id}/result?format=npz` | 全精度 NPZ 下载 |
| `GET` | `/sessions/{id}/batches` | 已提交批次统计 |
| `GET` | `/sessions/{id}/intents` | 意图日志（诊断） |
| `GET` | `/recovery/report` | 全部恢复报告 |
| `GET` | `/sessions/{id}/recovery` | 单会话恢复报告 |

提交示例：

```python
import requests

plan = {
    "version": 1,
    "dataset": "small",
    "operators": [
        {
            "op": "filter",
            "predicate": {
                "op": "compare", "fn": "ge",
                "left": {"op": "column", "name": "id"},
                "right": {"op": "literal", "dtype": "int64", "value": 0},
            },
        },
        {
            "op": "project", "name": "ratio",
            "expr": {
                "op": "binary", "fn": "div",
                "left": {"op": "column", "name": "f64_a"},
                "right": {"op": "column", "name": "f64_b"},
            },
        },
    ],
}
response = requests.post(
    "http://127.0.0.1:8765/sessions",
    json={
        "request_id": "demo-001",
        "plan": plan,
        "execution": {
            "path": "auto",                 # auto | scalar | vector
            "integer_overflow": "wrap",     # wrap | saturate
            "memory_budget_bytes": 536870912,
        },
    },
    timeout=600,
)
data = response.json()["data"]
print(data["path"], data["batch_size"], data["selection"]["reasons"])
print(data["result_url"])
```

JSON 浮点值为了保留固定小数位而输出为字符串，例如 `"1.234500"`；`float_format` 字段说明小数位、round-half-even 与“仅展示舍入”。需要参与后续计算时使用 NPZ 下载，其中保存完整 binary64 值和 `__valid` 位图。结果数组中的无效槽位规范化为零，空值语义以位图为准。

## 验证

```powershell
python -m unittest discover -s tests -v
python scripts/run_experiment.py --work-dir artifacts/experiment-final
```

实验脚本会：

1. 生成或复用固定样本；
2. 启动进程内本地 HTTP API（随机回环端口）；
3. 对每档样本分别以 `scalar`、`vector` 执行投影与聚合计划；
4. 通过 HTTP 下载两份 NPZ，比较 schema、行数、空值位图、整数/布尔位级相等，以及浮点 `rtol=1e-12, atol=1e-12` 与最大绝对/相对偏差；
5. 输出自动路径/批大小推断、提交耗时、批统计；
6. 另起子进程执行强制终止演练，核对重启后的 `aborted` 状态、最后完整批次、pending 意图和无完整结果承诺；
7. 写入 `artifacts/experiment-final/experiment_report.json`。本次验收报告已固化在 `docs/experiment_report.json`，强制终止报告在 `docs/recovery_report.json`。

## 恢复策略

- 正常执行：每批先写 `execute_batch` pending 意图；随后批次统计、`completed_batches`、`processed_rows` 与意图完成在同一事务中提交。
- 结果发布：先写 finalize 意图，NPZ 临时文件原子替换，再由同一事务发布 `READY`、结果路径、哈希和意图完成状态。
- 强制终止：重启扫描 `queued/running` 会话和遗留 pending 意图，统一标记 `aborted`，报告最后已提交批次和行检查点；不会读取或承诺内存中的半成品。
- 孤儿结果文件不会被查询；启动时清理没有 `READY` 会话引用的 NPZ。

## 主要业务码

`E_PLAN_INVALID`、`E_MEMORY_BUDGET`、`E_SESSION_NOT_FOUND`、`E_DUPLICATE_REQUEST`、`E_SESSION_NOT_READY`、`E_SESSION_ABORTED`、`E_RESULT_NOT_FOUND`、`E_EXECUTION_FAILED`。完整映射见语义契约。

## 已知边界

- 当前只实现全局聚合，不含分组聚合或 Join。
- `scalar` 路径为语义基准与审计路径，不追求大规模性能；`auto` 会在阈值外选择向量路径。
- 恢复策略是保守中止，不自动续跑；这保证报告只引用持久化事实。
- 结果 NPZ 是 SQLite 外部的不可变文件，其路径、大小、哈希与 `READY` 状态在 SQLite 中原子登记。
