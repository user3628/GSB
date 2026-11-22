# Capacity Lab

本地、确定性的容量评估与压测应用。应用维护带版本的压测计划，按并发档位执行轮次，向本地确定性测试服务发起真实 HTTP 请求，采集延迟与错误，使用定点数与精确有理数计算分位、吞吐、错误率及阈值裁决，并持久化完整审计证据。

项目只访问 `localhost` / `127.0.0.0/8` / `::1` 回环端点，不使用 Docker 或其他容器。

本次自动化验收实际运行在 WSL2 Linux、Python 3.14.4、NumPy 2.2.1；代码只使用跨平台标准库和 NumPy，Windows 硬杀分支使用 `Popen.kill()` / `TerminateProcess`。原生 Windows 命令与路径已在下方给出。

## 交付内容

- 压测计划、档位、阈值、升降档规则与固定随机种子校验。
- `PENDING -> SAMPLING -> CONVERGED` 与 `PENDING/SAMPLING -> ABORTED` 轮次状态机。
- 有界并发的 HTTP 采样器，支持独立请求超时、超时连接关闭、错误注入和确定性延迟。
- 最近秩与线性插值两种分位算法，精确有理数计算，定点阈值比较。
- 比例指标逐步定点舍入账本、累计舍入误差、两种 P99 的差异说明。
- 停止、回退最后通过档、降档重测三种否决决策；已提交结论不可修改。
- SQLite 单写者事务、多读者一致快照、全局结论版本、意图记录与崩溃恢复。
- 本地 HTTP API、CLI、验收计划、超时/错误计划、强制终止演练与自动化测试。

## 环境准备

要求 Python 3.10+ 和 NumPy，SQLite 与 HTTP 服务使用 Python 标准库。

PowerShell：

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Linux/macOS：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## 快速启动

终端 1，启动确定性本地测试服务：

```bash
python -m capacity_lab.deterministic_service --port 18081
```

终端 2，启动应用 HTTP 接口：

```bash
python -m capacity_lab.cli serve --db capacity_lab.db --port 18080
```

PowerShell 提交固定计划并执行完整容量序列：

```powershell
$plan = Get-Content fixtures/sample_plan.json -Raw
Invoke-RestMethod -Method Put `
  -Uri http://127.0.0.1:18080/api/plans/capacity-demo `
  -ContentType 'application/json' -Body $plan

Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:18080/api/plans/capacity-demo/runs `
  -ContentType 'application/json' -Body '{"async":false}'
```

查询最新拐点报告与恢复报告：

```powershell
Invoke-RestMethod http://127.0.0.1:18080/api/plans/capacity-demo/report
Invoke-RestMethod http://127.0.0.1:18080/api/recovery/latest
```

也可以直接使用 CLI：

```bash
python -m capacity_lab.cli submit --db capacity_lab.db \
  --plan-file fixtures/sample_plan.json --plan-id capacity-demo
python -m capacity_lab.cli run --db capacity_lab.db --plan-id capacity-demo
python -m capacity_lab.cli report --db capacity_lab.db --plan-id capacity-demo
```

## HTTP API

所有成功响应为 `{"ok":true,"data":...}`；所有业务失败为 `{"ok":false,"error":{"code":"E_...","message":"...","details":...}}`。

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` | `/api/health` | 健康检查与启动恢复摘要 |
| `PUT` | `/api/plans/{plan_id}` | 提交新计划版本；非法计划不改变最新版本 |
| `POST` | `/api/plans/{plan_id}/validate` | 只校验，不持久化 |
| `GET` | `/api/plans/{plan_id}?version=` | 查询计划版本 |
| `GET` | `/api/plans/{plan_id}/versions` | 查询版本与指纹 |
| `POST` | `/api/plans/{plan_id}/runs` | 启动完整序列；`async` 控制同步/后台 |
| `POST` | `/api/plans/{plan_id}/stages/{index}/rounds` | 手工触发单个档位；可传 `run_id` |
| `GET` | `/api/plans/{plan_id}/report` | 最新轮次、拐点与决策依据 |
| `GET` | `/api/runs/{run_id}` | 查询轮次集合与提交结论 |
| `GET` | `/api/rounds/{round_id}?include_samples=false` | 查询轮次证据；非 `CONVERGED` 不暴露样本 |
| `GET` | `/api/recovery/latest` | 最新恢复报告 |
| `GET` | `/api/recovery/reports?limit=` | 恢复报告历史 |

主要业务码：`E_PLAN_INVALID`、`E_INVALID_TRANSITION`、`E_ROUND_ACTIVE`、`E_INSUFFICIENT_SAMPLES`、`E_PROCESS_INTERRUPTED`、`E_ROUND_ABORTED`、`E_NOT_FOUND`、`E_BAD_REQUEST`、`E_INTERNAL`。

## 固定样本与验收结果

`fixtures/sample_plan.json` 声明四个并发档位 `2/4/8/16`、固定种子、复核阈值、比例定点小数位和 `nearest_rank` 插值方法。当前本地验收结果：

| 并发 | 判定 | P50 ms | P95 ms | P99 ms | 吞吐 rps | 错误率 |
|---:|---|---:|---:|---:|---:|---:|
| 2 | PASS | 7.731 | 14.362 | 17.767 | 235.734 | 0.000000 |
| 4 | PASS | 8.374 | 14.695 | 25.336 | 447.763 | 0.000000 |
| 8 | PASS | 21.867 | 41.177 | 46.630 | 341.732 | 0.000000 |
| 16 | REJECT | 53.132 | 107.155 | 144.353 | 272.444 | 0.000000 |
| 12（降档重测） | PASS | 38.977 | 67.712 | 92.949 | 286.929 | 0.000000 |

拐点：`BETWEEN_PASS_AND_REJECT`，并发 `12`。`16` 档否决后按 `500000 ppm` 比例降到 `12` 重测，重测通过。

同一样本、同一阈值边界下：

- 最近秩 P99 = `144353` 定点单位，超过允许边界 `143844`，裁决 `REJECT`。
- 线性插值 P99 = `143336` 定点单位，位于允许边界内。
- 最近秩减线性 = `1.017 ms`。
- 该档位会因插值方式产生相反阈值结论；计划显式声明最近秩，其他方式只作为解释证据，不能覆盖已提交结论。

完整结果见 `artifacts/acceptance_report.md` 与 `artifacts/acceptance_run_report.json`。

## 验证命令

```bash
python -m unittest discover -s tests -v
python scripts/run_acceptance.py
python scripts/force_kill_drill.py
```

强制终止演练会异步启动一个全部请求挂起的 `SAMPLING` 轮次，在观察到 `SAMPLING` 后硬杀应用进程，再用同一数据库重启。当前实测：

- 硬杀方式：Linux/macOS `SIGKILL`；Windows `TerminateProcess`。
- 未完成意图：`1`。
- 轮次与运行：均恢复为 `ABORTED`，错误码 `E_PROCESS_INTERRUPTED`。
- 已落盘样本：`0`。
- 未落盘样本恢复声明：`false`。
- 恢复报告：`artifacts/force_kill_recovery_report.json`。

测试日志见 `artifacts/test_output.txt`。

## 数值契约摘要

- 阈值中的比例字段由 `numeric_contract.ratio_decimals` 显式声明小数位，并以整数定点存储；延迟和吞吐也使用声明小数位。
- 所有分位来自整数微秒样本，经 `Fraction` 精确计算后按声明舍入方式量化为定点整数。
- 最近秩：`rank = ceil(p * n)`，取排序后第 `ceil(p*n)-1` 个样本。
- 线性插值：位置 `(n-1)*p` 用精确有理数计算，不使用二进制浮点。
- P99 至少要求 `100` 个样本，样本不足返回 `E_INSUFFICIENT_SAMPLES`。
- 阈值比较使用整数定点值和 `ceil(threshold_fixed * ppm / 1000000)` 相对容差；不直接比较裸浮点。
- 错误率主裁决值采用“精确比例只舍入一次”的定点值；同时输出分批“错误率定点化 -> 样本权重定点化 -> 加权累计”的多级舍入账本和累计误差。
- 吞吐定义为可复现的闭环派生值：`success_count * concurrency * 1000000 / sum_latency_us`。实际墙钟耗时单独记录为诊断量，不参与裁决。
- `duration_seconds` 声明档位采样窗口并参与默认样本量推导；固定验收计划显式给出 `sample_size`，以便逐条复现样本序列。

更完整的设计、SQLite 表与状态迁移见 `docs/DESIGN.md`。

## 项目结构

```text
capacity_lab/              应用代码与 HTTP 服务
fixtures/                  固定计划、超时/错误计划、强制终止计划
scripts/                   验收执行器与强制终止演练
tests/                     数值、状态、并发、HTTP、恢复测试
artifacts/                 实际验收报告、测试日志与恢复报告
```

