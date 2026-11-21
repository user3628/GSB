# 实际验证记录

验证日期：2026-11-21（Asia/Shanghai）

本次代码在交付沙箱中实际执行。沙箱为 Linux，Python 版本高于任务下限 3.11；实现只使用跨平台标准库与 FastAPI/SQLite，Windows 启动脚本与命令见 `README.md`。Windows 专用 PowerShell 脚本未在当前 Linux 沙箱执行。

## 环境

```text
Python 3.14.4
sqlite3 3.46.1
fastapi 0.165.0
uvicorn 0.54.0
pytest 9.2.0
httpx 0.28.1
```

虚拟环境位于 `.venv/`（已在 `.gitignore` 中排除）。SQLite 使用标准库 `sqlite3`，默认数据库路径 `data/pdb_engine.sqlite3`，演示数据库位于 `demo_output/`。

## 自动化测试

命令：

```text
python -m pytest -q
```

原始输出：`demo_output/pytest_stdout.txt`。

```text
.......................                                                  [100%]
=============================== warnings summary ===============================
.venv/lib/python3.14/site-packages/fastapi/testclient.py:1
  ... StarletteDeprecationWarning: Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead.
-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
25 passed, 1 warning in 0.93s
```

测试覆盖：

- CPU/内存量纲、非法值、溢出、PDB 单一预算字段。
- `minAvailable` 百分比 `ceil`、`maxUnavailable` 百分比 `floor`。
- 非 Running、evicting、terminating 副本从健康可用数剔除。
- `E_SELECTOR_EMPTY`。
- 单 Pod 多 PDB 全部满足才允许。
- 整节点朴素顺序与约束感知顺序的放行数对比。
- 放置容量、节点选择器、污点容忍与 `E_NO_FEASIBLE_NODE`。
- 同请求并发等待与幂等复用。
- 重复请求在首批已进入 `RUNNING` 后到达时仍会等待并复用最终结果。
- 不同节点并发提交共享 PDB 时不突破健康下限。
- 子进程强杀后的 `ABORTED` 恢复与 PDB 重建。
- HTTP 业务码、明细查询与统一错误 JSON。

编译检查：

```text
python -m compileall -q pdb_engine scripts tests
compileall: OK
```

## 端到端演示

命令：

```text
python scripts/demo.py
```

完整标准输出：`demo_output/demo_stdout.txt`。

机器可读结果：`demo_output/demo_result.json`。

最终断言：

```json
{
  "single_blocked_code": true,
  "no_feasible_node_code": true,
  "naive_allowed": true,
  "optimized_not_less_than_naive": true,
  "actual_allowed": true,
  "recovery_available": true,
  "recovery_aborted": true
}
```

```text
DEMO PASSED
```

### 1. 单 Pod 被阻塞

请求 `ledger-0`。PDB `ledger-full-availability` 使用整数 `minAvailable: 2`，当前健康可用数为 2：

```json
{
  "code": "E_PDB_BLOCKED",
  "pdb": "ledger-full-availability",
  "current_available": 2,
  "required_available": 2,
  "after_available": 1,
  "gap": 1
}
```

HTTP 状态为 `409`。

### 2. 无可行放置节点

请求 `web-b-offsite` 时 PDB 预算允许，但 `worker-a` 为待腾空节点，`worker-c` 的 `dedicated=ops:NoSchedule` 污点不被该 Pod 容忍，因此：

```text
HTTP 409
E_NO_FEASIBLE_NODE
```

驱逐在裁决阶段即降级为阻塞，不存在“先放行、后无节点”的事后状态。

### 3. 整节点跨两个 PDB 部分放行

样本两份百分比 PDB：

```text
web-a-budget:          minAvailable 50%  -> ceil(4 * 50 / 100) = 2
web-critical-budget:   maxUnavailable 50% -> 4 - floor(4 * 50 / 100) = 2
```

初始可用数均为 3。`worker-a` 上的 Pod：

- `web-a-shared`：同时属于两份 PDB；
- `web-a-alpha`：只属于 `web-a-budget`；
- `web-a-beta`：只属于 `web-critical-budget`。

实际输出：

```json
{
  "actual_allowed": 2,
  "actual_blocked": 1,
  "naive_allowed": 1,
  "optimized_not_less_than_naive": true,
  "optimized_preview_allowed": 2
}
```

```text
朴素顺序：web-a-shared -> web-a-alpha -> web-a-beta  放行 1
优化顺序：web-a-alpha -> web-a-beta -> web-a-shared  放行 2
```

实际允许：

```text
web-a-alpha -> worker-b
web-a-beta  -> worker-c（容忍 ops 污点）
```

实际阻塞：

```text
web-a-shared: E_PDB_BLOCKED
  web-a-budget:         current=2, required=2, gap=1
  web-critical-budget:  current=2, required=2, gap=1
```

最终 `web-a-budget` 与 `web-critical-budget` 当前可用数均为 2；`ledger-full-availability` 保持 2。

同请求重复提交返回相同 `drain_id`，`replayed=true`，步骤数仍为 3，PDB 不再扣减。

### 4. 强制终止与恢复

演示用子进程执行真实 drain，在第一个步骤 `web-a-alpha` 完成 `COMMITTED` 后直接 `os._exit(97)`，不执行清理和最终报告。父进程重启服务后得到：

```json
{
  "facts_scope": "PERSISTED_ONLY",
  "complete_exit_report": false,
  "aborted_drains": [
    {
      "request_id": "crash-drain-demo",
      "node": "worker-a",
      "status_before": "RUNNING",
      "status_after": "ABORTED",
      "committed_step_count": 1,
      "pending_intent_count": 0,
      "last_complete_step": {
        "sequence": 1,
        "pod": "web-a-alpha",
        "outcome": "ALLOWED",
        "availability_after": {
          "web-a-budget": 2,
          "web-critical-budget": 3,
          "ledger-full-availability": 2
        }
      }
    }
  ]
}
```

恢复后的 PDB 可用数：

```text
web-a-budget:          initial=3, restored=2
web-critical-budget:   initial=3, restored=3
ledger-full-availability: initial=2, restored=2
```

`GET /api/v1/drains/crash-drain-demo` 返回 `status=ABORTED` 和唯一一个已提交步骤；不存在伪造的完整退出报告。

## 真实 HTTP 冒烟

使用真实 `uvicorn` 监听 `127.0.0.1:28081`，通过 `curl` 调用：

```text
GET /health                          -> 200
POST /api/v1/samples/load            -> 200
POST /api/v1/evictions/check         -> 409 E_PDB_BLOCKED
POST /api/v1/drains                  -> 200 COMPLETED
GET  /api/v1/drains/http-drain       -> 200 COMPLETED, 3 steps
```

关键响应：

```json
{
  "status": "COMPLETED",
  "release_summary": {
    "actual_allowed": 2,
    "actual_blocked": 1,
    "naive_allowed": 1,
    "optimized_not_less_than_naive": true,
    "optimized_preview_allowed": 2
  },
  "allowed": [
    ["web-a-alpha", "worker-b"],
    ["web-a-beta", "worker-c"]
  ],
  "blocked": [
    ["web-a-shared", "E_PDB_BLOCKED"]
  ]
}
```

## 结果说明

- 验收目标中的固定样本、两份百分比 PDB、整节点部分放行、放置可行性与强杀恢复均已实际验证。
- 持久化恢复报告只引用 SQLite 中已提交的 drain、step 与 PDB 事实。
- 未使用 Docker、容器、线上集群或未授权外部目标。

