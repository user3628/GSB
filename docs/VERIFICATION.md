# 实际验证记录

验证日期：2026-11-20（Asia/Shanghai）。执行环境为 WSL2 Linux，Python 3.14.4，但代码只使用 Python 3.11+ 可用语法/API，并提供 Windows PowerShell 安装命令。

## 环境与依赖

```text
python_version: 3.14.4
python_supported: true
platform: Linux-6.18.58.1-microsoft-standard-WSL2-x86_64-with-glibc2.43
sqlite_version: 3.46.1
numpy: 2.3.5
requests: 2.32.5
virtualenv: .venv
```

独立虚拟环境已创建，`requirements.txt` 中版本安装成功。`python scripts/check_environment.py` 返回 `ok: true`。

## 单元与集成测试

命令：

```bash
.venv/bin/python -m unittest discover -s tests -v
```

结果：`Ran 15 tests ... OK`。覆盖：

- 非法计划、未知列和类型不匹配在执行任何批次之前返回 `E_PLAN_INVALID`。
- 投影与聚合在 `scalar`/`vector`、`wrap`/`saturate` 下数组和空值位图完全一致。
- 边界行验证 `INT64_MAX+1` 回绕、饱和、乘法溢出、除零空值、条件空值选 `else`。
- `[1e16, 1, -1e16]` 的补偿求和结果为 `1.0`。
- 自动路径阈值切换、内存预算错误、同请求并发去重、不同请求统计隔离。
- 重启扫描未完成会话，恢复报告给出最后批次/检查点且无结果。
- HTTP API 成功/错误统一响应、NPZ 下载、重复请求、缺会话和内存错误。

另做了 200,000 组随机整数属性测试：`add/sub/mul/div` 在 `wrap` 与 `saturate` 下，标量与向量逐元素差异均为 0；100,000 组随机浮点 `add/sub/mul/div` 差异也为 0。

## 三档正式样本

`python scripts/generate_samples.py --force` 生成：

| 档位 | 行数 | 文件大小 |
| --- | ---: | ---: |
| small | 5,000 | 236 KB |
| medium | 250,000 | 12 MB |
| large | 2,000,000 | 90 MB |

投影计划含 Filter、常量、列引用、算术、负数、比较、严格逻辑与条件；聚合计划含 `sum/avg/min/max/count/count_star`。投影使用 `integer_overflow=wrap`，聚合使用 `saturate`。

## 双路径一致性

命令：

```bash
.venv/bin/python scripts/run_experiment.py --work-dir artifacts/experiment-final
```

所有 12 次执行（3 档 × 2 计划 × 2 路径）均 `consistent=True`。整数/布尔位级相等、空值位图相等；所有浮点列最大绝对偏差 `0.0`，最大相对偏差 `0.0`（仍按 `rtol=1e-12, atol=1e-12` 审计）。

| 档位 | 计划 | 自动选择 | 标量耗时 | 向量耗时 | 批次数 | 输出行 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| small (5k) | projection | scalar / 5000 | 85.0 ms | 54.9 ms | 1 | 4,995 |
| small (5k) | aggregate | scalar / 5000 | 64.2 ms | 49.3 ms | 1 | 1 |
| medium (250k) | projection | vector / 65536 | 2,401.6 ms | 122.0 ms | 4 | 249,752 |
| medium (250k) | aggregate | vector / 65536 | 1,085.6 ms | 376.6 ms | 4 | 1 |
| large (2M) | projection | vector / 65536 | 18,984.3 ms | 1,851.7 ms | 31 | 1,998,017 |
| large (2M) | aggregate | vector / 65536 | 12,536.3 ms | 3,492.0 ms | 31 | 1 |

自动选择依据按阈值与复杂度记录在会话 `selection` 中：小档满足行数、单元格数和复杂度阈值，所以选 `scalar`；中/大档超过 `scalar_max_rows` 与 `scalar_max_cells`，切到 `vector`。自适应批大小在默认 512 MiB 预算下为小档 5000、其余 65536。

完整 JSON 报告已固化在 `docs/experiment_report.json`（原始运行路径为 `artifacts/experiment-final/experiment_report.json`）。

## 强制终止恢复演练

恢复脚本在子进程启动本地服务，提交 medium 投影计划，使用 `batch_size=512` 和测试专用批延迟。等到数据库已持久化批次 0 且批次 1 意图为 `pending` 时，调用 `process.kill()` 强制终止；随后用同一 SQLite/结果目录重启。

实际结果：

```text
kill 前持久化事实: completed_batches=1, processed_rows=512,
                  pending intent=batch_index 1
重启后会话状态: aborted
恢复报告: last_completed_batch=0, last_persisted_row=512,
          pending_intent_count=1, result_available=false
结果查询: HTTP 409, E_SESSION_ABORTED
批次统计: 批次 0 仍可查询
recovery_drill.passed=true
```

报告只包含 `facts` 中的已提交批次、检查点和意图事实，并明确 `no complete result is promised after forced termination`。未尝试承诺恢复内存中的完整结果。完整演练报告已固化在 `docs/recovery_report.json`。

## 内存与错误码抽查

- 默认计划请求 `memory_budget_bytes=131072`（128 KiB）在创建会话前返回 `E_MEMORY_BUDGET`，无批次执行。
- 非法列、类型混用返回 `E_PLAN_INVALID`；未知会话返回 `E_SESSION_NOT_FOUND`；同请求 ID 不同执行选项返回 `E_DUPLICATE_REQUEST`。
- 除零、全空列、空输入均返回确定的空值或计数结果，不抛出未分类异常。
- 自动化测试另验证了 0 行空批次与全空列的 `sum/count/count_star` 确定结果。

