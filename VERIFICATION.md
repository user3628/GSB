# 实际验证记录

验证日期：2026-11-23（Asia/Shanghai）。当前执行环境为 WSL2/Linux，不是 Windows 原生内核；应用代码、命令和 PowerShell 脚本均使用跨平台 Python/SQLite API。

## 环境

- Python：3.14.4
- FastAPI：0.165.0
- Uvicorn：0.54.0
- NumPy：2.5.5
- pytest：9.2.0
- httpx：0.28.1
- 平台：Linux 6.18.58.1-microsoft-standard-WSL2-x86_64
- 版本锁定文件：`requirements-lock.txt`

## 自动化测试

执行：

```text
.venv/bin/python -m pytest -q
..................................                                [100%]
34 passed, 1 warning
```

唯一警告是 FastAPI `TestClient` 对 httpx 的 Starlette 弃用提示，不影响本应用运行时路径。测试覆盖：

- 非法规格/轨迹整批回滚、数值类型和溢出检查；
- 冷启动、热启动、预热、初始化并发对启动排队的影响；
- 优先级队列、队列满、预留额度隔离和越权拒绝；
- 并发去重、内容冲突；
- 真实启动超时、执行超时、连接主动拒绝和资源释放；
- 五类异常注入及逐项复查；两套全新数据库中的异常注入回放一致；
- DRAINING 在途调用完成后原子终止；
- 多写者与多读者快照不变量；
- 子进程强制终止后的 EXECUTING 与 CREATING 两种恢复；
- HTTP API 业务码和响应结构；
- 固定输入在全新数据库中的逐次一致回放。

## 样本验证

执行：

```text
.venv/bin/python -m tools.verify_samples
{
  "passed": true,
  "assertions": {
    "trace_replays_identical": true,
    "all_trace_calls_succeeded": true,
    "cold_and_warm_observed": true,
    "all_injection_rechecks_consistent": true,
    "five_distinct_injection_codes": true,
    "recovery_reports_consistent": true
  }
}
```

完整机器可读结果：`artifacts/verification.json`。

### 固定样本

- 种子：`20261123`
- 函数：4 个
- 轨迹调用：24 个
- 回放前预热 `fn-orders` 1 个实例
- 两次全新数据库回放：业务判定逐项相同

第一次回放的累计判定：

| 函数 | 冷启动 | 热启动 | 启动超时 | 执行超时 |
|---|---:|---:|---:|---:|
| `fn-audit` | 1 | 2 | 0 | 0 |
| `fn-image` | 1 | 3 | 0 | 0 |
| `fn-orders` | 0 | 10 | 0 | 0 |
| `fn-report` | 1 | 6 | 0 | 0 |

总调用结果：3 次冷启动、21 次热启动，24 次全部成功；额度占用最终为 0，不变量审计通过。

### 五类异常注入

| 注入 | 返回码 | 复查 |
|---|---|---|
| `INSTANCE_CREATE_TIMEOUT` | `E_START_TIMEOUT` | 冷启动 +1、启动超时 +1、额度归零、实例 TERMINATED |
| `EXECUTION_INTERRUPT` | `E_EXEC_TIMEOUT` | 冷启动增量匹配、执行超时 +1、额度归零、调用 FAILED |
| `DUPLICATE_CALL_ID` | `E_CONFLICTING_CALL` | 同 ID 仅一条调用、无额外冷启动、额度归零 |
| `QUOTA_BURST` | `E_QUOTA_DENIED` | 超出预留额度 1 次、预留额度最终归零 |
| `TRACE_OUT_OF_ORDER` | `E_TRACE_OUT_OF_ORDER` | 无冷启动/超时变化、轨迹批次数不变 |

五个业务码互不相同，五次 `recheck.consistent` 均为 `true`。

### 强制终止恢复

两种场景均在子进程进入目标状态后使用 `Popen.kill()` 强制终止：

| 场景 | PENDING 意图 | 恢复调用终态 | 额度释放 | 实例终态 | 审计 |
|---|---:|---|---:|---|---|
| 执行中终止 | 1 | `E_RECOVERED_INTERRUPTED` | 按需 1 -> 0 | `TERMINATED` | 通过 |
| 创建中终止 | 1 | `E_RECOVERED_INTERRUPTED` | 按需 1 -> 0 | `TERMINATED` | 通过 |

两份报告均为 `persisted_facts_only=true`、`exit_report_available=false`。第二次重启无新增恢复调用和 PENDING 意图。

## 解释边界

- 超时表示客户端关闭 HTTP 连接并完成本地原子释放，不表示远端测试端点内的代码已被强制终止。
- 强制终止进程不会有完整退出报告；恢复报告只在重启后根据 SQLite 已提交记录生成。
- 当前验证在 WSL2 完成。Windows 原生运行方式见 `README.md`，应使用相同的 Python 3.11+ 与 `requirements.txt`/`requirements-lock.txt`。

