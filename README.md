# RateGuard：本地自适应限流与熔断领域组件

RateGuard 是一个只依赖 Python 标准库与 NumPy 的本地微服务测试组件。它从确定性的请求事件批次中计算滑动窗口分位延迟、成功率、自适应配额和熔断状态，并通过只监听回环地址的 HTTP API 暴露给网关、告警和 SRE 工具链。

本项目不访问真实站点、账号或外部服务。所有演示数据由固定随机种子生成，数据库、样本与验证证据都留在工作区。

## 交付内容

- `rateguard/numerics.py`：Decimal 分位、补偿求和、DKW/Wilson 置信区间、误差界、定点配额与阻尼。
- `rateguard/engine.py`：事件摄入、幂等冲突、窗口版本、复合快照、限流裁决、熔断状态机、恢复查询。
- `rateguard/storage.py`：SQLite schema、意图日志、事务协议与启动恢复。
- `rateguard/http_api.py`：标准库 `http.server` API，统一 JSON 错误信封。
- `rateguard/samples.py`：三档固定种子样本生成器。
- `scripts/demo.py`：真实启动/终止本地 HTTP 进程的端到端演示，包含一次强制退出恢复。
- `tests/`：24 项自动化测试。
- `docs/`：数值契约、API、恢复协议与技术说明。
- `artifacts/`：最近一次测试、演示、数据库、日志和验证摘要。

## Windows 快速开始

要求 Python 3.11 或更高版本。以下命令在 PowerShell 中执行：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python --version
python -c "import numpy, sqlite3; print(numpy.__version__, sqlite3.sqlite_version)"
```

生成固定样本并运行完整演示：

```powershell
python scripts/generate_samples.py --out artifacts/samples
python scripts/demo.py --db artifacts/demo.sqlite3 --out artifacts
```

运行测试：

```powershell
python -m unittest discover -s tests -v
```

也可用一条命令重跑测试、演示并生成验收证据：

```powershell
python scripts/verify.py
```

启动仅监听回环地址的 HTTP 服务：

```powershell
python -m rateguard.server --db artifacts/rateguard.sqlite3 --host 127.0.0.1 --port 8765
```

`--host` 只接受 `127.0.0.1`、`::1` 或 `localhost`。健康检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8765/health
```

## API 摘要

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/events/batches` | 摄入事件批次，按 `batch_id + sequence` 幂等 |
| `GET` | `/v1/events/batches/{batch_id}/{sequence}` | 查询批次状态与首次提交结果 |
| `POST` | `/v1/limit/decision` | 读取一个复合版本并给出一致性限流裁决 |
| `GET` | `/v1/circuit/state` | 查询当前持久化熔断状态与冷却信息 |
| `POST` | `/v1/circuit/transition` | 手动状态迁移，非法迁移返回业务码 |
| `GET` | `/v1/circuit/transitions` | 查询熔断轨迹 |
| `GET` | `/v1/windows` | 查询窗口摘要与两种插值结果 |
| `GET` | `/v1/windows/stats` | 查询完整统计与误差契约，可严格校验样本下限 |
| `GET` | `/v1/quota/derivation` | 查询持久化配额推导明细与中间量 |
| `GET` | `/v1/recovery/report` | 查询启动恢复报告 |

详细请求与响应见 `docs/api.md`。

最小事件批次示例：

```json
{
  "batch_id": "local-b001",
  "sequence": 1,
  "events": [
    {
      "timestamp_ms": 1700000000100,
      "route": "/checkout",
      "success": true,
      "latency_us": 28000
    }
  ]
}
```

统一错误信封：

```json
{
  "ok": false,
  "error": {
    "code": "E_STALE_WINDOW",
    "message": "requested window version is not current",
    "details": {}
  }
}
```

至少覆盖 `E_CONFLICTING_REPORT`、`E_INVALID_TRANSITION`、`E_INSUFFICIENT_SAMPLES`、`E_STALE_WINDOW`、`E_BATCH_NOT_FOUND`。

## 数值契约要点

- 窗口锚定 Unix epoch 毫秒，边界固定为半开区间 `[start_ms, end_ms)`；时间戳分辨率 1 ms，耗时分辨率 1 μs。
- 固定窗口长度与滑动步长共同决定重叠窗口。一个事件可属于多个滑动窗口。
- P50/P95/P99 同时输出最近秩与线性插值：
  - 最近秩：`rank = ceil(p * n)`，一基索引。
  - 线性：R type 7，`position = (n - 1) * p`，在相邻顺序统计量间插值。
- 统计与裁决使用 50 位 Decimal 上下文，绝不使用二进制浮点参与持久化数值或准入判断。
- 成功率由精确成功数与样本数的分数传播，0/1 良好事件指标使用 Neumaier 补偿求和；输出值统一 `ROUND_HALF_EVEN`。
- 分位使用 DKW 分布无关置信带；成功率使用 Wilson 区间。样本数不足时，严格查询/裁决返回 `E_INSUFFICIENT_SAMPLES`。
- 配额因子为百万分之一整数定点。每一步都校验边界，阻尼系数为 2500 bp，死区为 1 个配额单位。
- 熔断合法迁移只有 `CLOSED→OPEN`、`OPEN→HALF_OPEN`、`HALF_OPEN→CLOSED`、`HALF_OPEN→OPEN`。
- 限流与熔断读取同一个不可变复合快照，快照显式链接同一窗口、熔断和配额版本。

完整公式与误差说明见 `docs/numerical-contract.md`。

## SQLite 与恢复

所有业务写操作先提交 `PREPARED` 意图，再在同一个 `BEGIN IMMEDIATE` 事务内完成业务写入和 `COMMITTED` 意图。强制终止时，SQLite 回滚未提交事务；重启恢复扫描仍为 `PREPARED` 的意图，将对应批次标记为 `ABORTED`，并把所有头指针校正到最后一个完整不可变版本。

本组件不承诺被强制终止的进程生成退出报告。恢复报告只记录重启后数据库中可验证的事实，例如：

- 找到的未完成意图数量与种类；
- 被恢复标记为中止的批次；
- 每个路由最后完整的复合版本；
- SQLite `quick_check` 与头指针修复记录；
- 恢复前后完整版本是否保持。

详细协议见 `docs/recovery-protocol.md`。

## 实际验证结果

当前验证机的实际运行环境为 Python 3.14.4、NumPy 2.5.5、SQLite 3.46.1（Linux/WSL）。实现不使用 Docker，也不使用平台专用依赖；Windows PowerShell 启动方式见上文，仅需 Python 3.11+ 与 NumPy。

自动化测试：

```text
Ran 24 tests
OK
```

三档固定样本：

| 规模 | 种子 | 事件数 | 批次 | checkout 当前窗口数 | 最终熔断状态 |
|---|---:|---:|---:|---:|---|
| small | 37001 | 72 | 16 | 15 | CLOSED |
| medium | 37002 | 720 | 16 | 15 | CLOSED |
| large | 37003 | 4800 | 16 | 15 | CLOSED |

三档均覆盖故障期和恢复期熔断轨迹；例如 large 的轨迹为：

```text
CLOSED -> OPEN              reason=CONSECUTIVE_FAILURES
OPEN -> HALF_OPEN           reason=COOLDOWN_ELAPSED
HALF_OPEN -> OPEN           reason=FAILURE_WHILE_HALF_OPEN
OPEN -> HALF_OPEN           reason=COOLDOWN_ELAPSED
HALF_OPEN -> OPEN           reason=FAILURE_WHILE_HALF_OPEN
OPEN -> HALF_OPEN           reason=COOLDOWN_ELAPSED
HALF_OPEN -> CLOSED         reason=HALF_OPEN_PROBE_SUCCEEDED
```

small 的最新窗口样本数不足以计算 P95，严格 P99 查询和限流裁决都返回 `E_INSUFFICIENT_SAMPLES`，没有静默给出夸大置信度的结果。

强制退出恢复实测：

```text
crash_request_disconnected = true
child_exit_code = 87
incomplete_intents_found = 1
batch_status = ABORTED
abort_reason = RECOVERED_INCOMPLETE_INTENT
previous_complete_versions_preserved = true
composite_versions_before_crash = composite_versions_after_restart
```

原始证据位于 `artifacts/test-results.txt`、`artifacts/demo_console.txt`、`artifacts/demo_report.json`、`artifacts/validation-summary.md` 与 `artifacts/verification.json`。

## 项目结构

```text
rateguard/          领域组件与 HTTP 服务
scripts/            样本生成与端到端演示
tests/              数值、服务、HTTP、样本、恢复测试
docs/               技术契约与集成说明
artifacts/          最近一次运行证据与 SQLite 数据库
```

## 使用边界

- 这是本地确定性测试组件，不是生产级分布式限流器；并发一致性依赖单机 SQLite 事务。
- DKW 与 Wilson 区间依赖窗口内样本近似独立同分布。窗口组成变化带来的统计不确定性无法仅靠数值舍入消除。
- `GET /v1/circuit/state` 只读持久化状态；`OPEN→HALF_OPEN` 的实际推进发生在摄入或裁决等评估点。
- 强制终止恢复保证针对 SQLite 已提交事实，不保证外部调用方已收到响应。
