# 单机梯度通信故障注入与降级聚合验证台

这是一个只使用回环网络、SQLite、本地确定性张量和 Python 进程的单机验证台。三个工作节点按轮次与参数分片提交梯度块；注入器在提交路径上制造五类真实通信故障；验证台对故障分类、重试、缩短参与集合或中止，并把注入规格、观测结果、重试序列、状态迁移与最终聚合结论绑定后原子提交。

## 目录

```text
grad_fault_lab/
  config.py          配置与回环地址约束
  models.py          注入规格、故障码分类、梯度块与校验
  injection.py       原始 socket/HTTP 故障注入与节点块拉取
  aggregation.py     序列复验、Neumaier 补偿加权聚合、误差上界
  state_machine.py   INJECTING/RETRYING/DEGRADED/VERIFIED/ABORTED
  store.py           SQLite 意图、最终事务、WAL 查询、重启恢复
  orchestrator.py    单批次单写者、重试/降级策略与结论绑定
  node_app.py        本地测试节点（固定监听端口）
  injector_app.py    本地注入端点
  validator_app.py   本验证台 API
  demo.py            端到端演示及 SIGKILL 恢复
config/default.json  固定回环端口、种子、形状与轮次策略
tests/               28 项单元/集成/端到端测试
artifacts/runtime/demo_output.json
artifacts/verification_report.json
```

## 环境准备（不访问外网）

当前环境实测 Python 3.14.4，满足 Python >= 3.11；SQLite 3.46.1 由标准库提供。FastAPI、Uvicorn、NumPy、pytest 已在机器本地可用。虚拟环境使用 `--system-site-packages` 继承这些本地包，不触发 pip 下载：

```bash
python3 -m venv --system-site-packages .venv
.venv/bin/python -c "import fastapi,uvicorn,numpy,pytest,sqlite3; print('ok')"
```

也可执行：

```bash
bash scripts/bootstrap_venv.sh
```

`requirements.txt` 记录本次实际使用的版本。没有使用 Docker、外部数据库、真实数据集、账号或加速硬件。

由于本地依赖通过 `--system-site-packages` 继承，脚本还会在 `.venv/bin` 生成 `pytest` 与 `uvicorn` 的本地入口，二者均调用虚拟环境解释器。

## 固定端口与启动

`config/default.json` 固定以下回环端口：

| 服务 | 地址 |
|---|---|
| node-0 | `127.0.0.1:18101`，未监听端口 `18191` |
| node-1 | `127.0.0.1:18102`，未监听端口 `18192` |
| node-2 | `127.0.0.1:18103`，未监听端口 `18193` |
| 注入端点 | `127.0.0.1:18104` |
| 验证台 | `127.0.0.1:18105` |

一键启动并保持运行：

```bash
bash scripts/run_services.sh
```

也可分开启动：

```bash
.venv/bin/python -m grad_fault_lab.node_app --config config/default.json --node-id node-0
.venv/bin/python -m grad_fault_lab.node_app --config config/default.json --node-id node-1
.venv/bin/python -m grad_fault_lab.node_app --config config/default.json --node-id node-2
.venv/bin/python -m grad_fault_lab.injector_app --config config/default.json
.venv/bin/python -m grad_fault_lab.validator_app --config config/default.json
```

## 运行验收

```bash
.venv/bin/pytest -q
.venv/bin/python -m grad_fault_lab.demo
```

演示会自动启动全部服务、执行六个注入/降级场景、对验证台进程发送 SIGKILL、重启并查询恢复报告。完整结果写入 `artifacts/runtime/demo_output.json`。

## HTTP API

创建并同步执行一个批次：

```json
POST /v1/batches
{
  "batch_id": "demo-timeout-retry",
  "round": 0,
  "injection": {
    "type": "response_timeout",
    "target_node": "node-1",
    "target_shard": 0,
    "round": 0,
    "retry": {
      "max_retries": 1,
      "backoff_seconds": 0.03,
      "backoff_multiplier": 2.0
    },
    "params": {
      "timeout_seconds": 0.1,
      "apply_attempts": [1]
    }
  }
}
```

`params.apply_attempts` 决定故障作用于哪些尝试。默认仅作用于第 1 次，因此重试可在第 2 次正常提交；把 `[1,2]` 与 `max_retries=1` 组合可真实耗尽重试并触发降级。

查询端点：

```text
GET /health
GET /v1/config
GET /v1/batches/{batch_id}
GET /v1/batches/{batch_id}/faults
GET /v1/batches
GET /v1/recoveries
GET /v1/diagnostics/error-codes
```

批次尚未最终提交时，`GET /v1/batches/{batch_id}` 只返回 `E_BATCH_NOT_READY`，不会暴露故障已写入但结论未提交的中间状态。

## 业务码

每类故障都有不同业务码，所有错误响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_FAULT_RESPONSE_TIMEOUT",
    "message": "Batch finished in ABORTED with business code ...",
    "retryable": false,
    "details": {"batch_id": "...", "fault_codes": ["..."]}
  }
}
```

| 业务码 | HTTP | 分类 | 是否重试 |
|---|---:|---|---|
| `E_FAULT_CONNECTION_REFUSED` | 502 | RETRYABLE | 是 |
| `E_FAULT_RESPONSE_TIMEOUT` | 504 | RETRYABLE | 是 |
| `E_FAULT_CONTENT_TRUNCATED` | 502 | FATAL | 否 |
| `E_FAULT_DUPLICATE_SUBMISSION` | 409 | FATAL | 否 |
| `E_FAULT_ORDER_MISMATCH` | 409 | FATAL | 否 |
| `E_BLOCK_VALIDATION_FAILED` | 422 | FATAL | 否 |
| `E_RETRY_EXHAUSTED` | 503 | 降级结论 | 否 |
| `E_INJECTION_SPEC_INVALID` | 422 | 输入错误 | 否，且不创建批次意图 |
| `E_INVALID_TRANSITION` | 409 | 状态机错误 | 否 |
| `E_RECOVERED_AFTER_RESTART` | 409 | 恢复结论 | 否 |
| `E_BATCH_NOT_READY` | 409 | 查询隔离 | 否 |
| `E_BATCH_PAYLOAD_CONFLICT` | 409 | 幂等冲突 | 否 |
| `E_DEGRADED_AGGREGATION` | 200 | 已验证降级结论 | 不适用 |
| `E_OK` | 200 | 成功 | 不适用 |

## 五类真实故障

1. **连接拒绝**：注入器对配置中的未监听 `refused_port` 发起真实 TCP `connect()`，捕获 `ConnectionRefusedError`。
2. **响应超时**：原始 socket 连接节点 `/debug/hang`，发送真实 HTTP 请求，在声明超时内等待 `recv()`；超时后通过上下文管理器关闭 socket，观测记录含 `socket_closed=true`。
3. **内容截断**：节点真实返回大于实际响应体的 `Content-Length`，客户端读取到 EOF 后比较声明长度和实收字节，判定短读；服务端 h11 会记录预期的协议截断错误，这是被注入现象的副作用。
4. **重复提交**：先通过真实 HTTP 拉取节点块，再把目标分片块实际追加到提交序列；聚合端独立复验并返回 `E_FAULT_DUPLICATE_SUBMISSION`。
5. **顺序错乱**：真实拉取节点块后交换目标分片与相邻分片的顺序；聚合端复验分片顺序并返回 `E_FAULT_ORDER_MISMATCH`。

## 重试、降级与误差上界

- 初始状态为 `INJECTING`。首个可重试故障触发 `INJECTING -> RETRYING`。
- 退避时间按 `backoff_seconds * backoff_multiplier ** (retry_number - 1)` 计算；每次尝试的代码、耗时和进入本次尝试前的退避时间均写入 `retry_attempts`。
- 致命故障立即进入 `DEGRADED`，不进入 `RETRYING`。
- 轮次策略 `on_exhaustion` 决定 `shorten` 或 `abort`；`min_participants` 是缩短后允许的最小节点数。
- 缩短后按分片块 `sample_weight` 加权。分子、分母均使用 Neumaier 补偿求和。
- 误差上界同时包含保守的缺失节点贡献界与浮点舍入界：

```text
missing_bound = (W_omitted / W_all) * (max_norm(included) + max_norm(omitted))
rounding_bound = L2 bound from n * eps * sum(abs(weight * tensor))
upper_bound = missing_bound + rounding_bound
```

记录中还给出 `observed_deviation_l2` 与 `within_bound`，用于逐项复核。最终聚合的 `fault_ids` 引用同一批次内每个 `fault_events.fault_id`。

## 状态机

合法迁移：

```text
INJECTING -> RETRYING | DEGRADED | VERIFIED | ABORTED
RETRYING  -> RETRYING | DEGRADED | VERIFIED | ABORTED
DEGRADED  -> VERIFIED | ABORTED
VERIFIED  -> 终止
ABORTED   -> 终止
```

非法迁移抛 `E_INVALID_TRANSITION`，既不改变内存状态，也不写入 SQLite。最终提交时存储层再次校验整条迁移链。

## SQLite、事务与恢复

数据库默认位于 `var/lab.sqlite3`，启用 WAL、外键和 `synchronous=FULL`。主要表：

```text
batches             批次意图、请求哈希、最终状态与业务码
fault_events        注入规格、分类、观测结果
retry_attempts      每次尝试的代码、耗时与退避
transitions         状态迁移链
aggregation_results 聚合张量、参与节点、误差界与绑定 fault_ids
recovery_reports    重启恢复依据与差异
```

流程严格分为：

1. 校验规格和注册节点；失败直接返回 `E_INJECTION_SPEC_INVALID`，不创建 `batches` 行。
2. 先提交 `RUNNING` 意图。
3. 在内存中执行真实故障、重试和聚合，不写中间结果。
4. 用一个 `BEGIN IMMEDIATE` 事务原子写入迁移、故障、重试、聚合结果并更新批次为最终状态。
5. 查询使用只读事务；WAL 保证已提交批次要么全部可见，要么不可见。

验证台启动时扫描 `is_final=0` 的意图。由于中间结果不在独立事务中落盘，恢复只能基于已提交意图和已有正式记录。当前策略是 `MARK_ABORTED_FROM_COMMITTED_INTENT`，恢复报告明确写出 `exit_report_complete=false`，不虚构退出前未落盘的故障观测或聚合结论。也允许未来实现“恢复到可重算状态”，但本实现选择可审计的中止。

## 并发与幂等

- 同一 `batch_id` 使用独立进程内锁串行执行；SQLite 写事务再全局串行化所有最终写入。
- 相同请求重复触发会等待首个写入完成后直接复用已提交批次；故障不会重复注入。
- 相同 `batch_id` 携带不同请求哈希返回 `E_BATCH_PAYLOAD_CONFLICT`。
- 多个读取请求使用独立连接和只读快照，可与写批次并发运行。

## 实际验证结果

最近一次执行：

```text
.venv/bin/pytest -q
28 passed, 87 warnings in 14.88s

.venv/bin/python -m grad_fault_lab.demo
exit=0
```

演示结果摘要（精确数值见 `artifacts/runtime/demo_output.json`）：

| 场景 | HTTP | 最终状态/业务码 | 关键观测 |
|---|---:|---|---|
| 超时后重试成功 | 200 | `VERIFIED` / `E_OK` | 第 1 次 `E_FAULT_RESPONSE_TIMEOUT`，30ms 退避，第 2 次成功 |
| 内容截断致命失败 | 502 | `ABORTED` / `E_FAULT_CONTENT_TRUNCATED` | 1 次尝试，无重试 |
| 重复提交冲突 | 409 | `ABORTED` / `E_FAULT_DUPLICATE_SUBMISSION` | 聚合端独立检出重复 |
| 顺序错乱 | 409 | `ABORTED` / `E_FAULT_ORDER_MISMATCH` | 聚合端独立检出乱序 |
| 连接拒绝后重试成功 | 200 | `VERIFIED` / `E_OK` | 真实未监听端口，30ms 退避后成功 |
| 重试耗尽后缩短集合 | 200 | `VERIFIED` / `E_DEGRADED_AGGREGATION` | 两次超时后省略 `node-1`，实际偏差 `0.4147193280`，上界 `0.9176381092` |
| SIGKILL 后重启恢复 | — | `ABORTED` / `E_RECOVERED_AFTER_RESTART` | 意图保留；终止前未提交的故障/重试/聚合均为 0 条；恢复后写入 ABORTED 结论；`exit_report_complete=false` |

上述结果由独立验证台进程、真实 HTTP/socket 和真实 SQLite 文件产生。

## 边界与安全

- 配置加载时拒绝非 `127.0.0.1` 主机；注入器只访问配置中已注册节点的固定端口，不接收任意 URL。
- 进程不访问外部网络；演示环境显式设置本地 `NO_PROXY`，HTTP 客户端使用 `trust_env=False`。
- 张量由 `seed + node_id + round + shard` 派生随机流生成，不依赖数据集。
- 恢复不做无依据补写；强制终止期间未提交的观测不会出现在最终报告里。
