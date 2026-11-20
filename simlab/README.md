# simlab — 故障隔离与容灾切换仿真台

面向多组件服务拓扑的本地仿真台：读取确定性拓扑样本与故障注入计划，在固定的
注入并发上限、每轮时间预算、故障域隔离配额与切换操作配额约束下，并行注入
真实故障（本地回环 HTTP 端点），推进隔离裁决与容灾切换，并通过 SQLite
持久化全部事实，支持进程强杀后的配额回滚与恢复报告。

纯 Python 3.11+，无 Docker / 外部数据库，全部流量仅访问 127.0.0.1。

## 目录结构

```
simlab/
  app/
    main.py          FastAPI 接口层（提交拓扑/计划、查询轮次/预算/跳过清单、恢复报告）
    engine.py        轮次引擎：确定性排序 -> 配额准入 -> 并行执行 -> 切换裁决
    executor.py      真实可中断注入执行（连接拒绝 / 超时关连接 / 延迟统计，NumPy）
    topology.py      拓扑与计划解析、归一化、成环检测、内容哈希幂等
    statemachine.py  服务状态机（HEALTHY/DEGRADED/ISOLATED/FAILED_OVER/FAILED）
    recovery.py      意图日志扫描、轮次中止、配额/切换/状态回滚
    db.py            SQLite 单写者序列化 + 写前意图日志 + 幂等键
    errors.py        业务错误码
  samples/
    topology.json            确定性拓扑样本（6 服务 / 3 故障域）
    injection_plan.json      故障注入计划样本（3 轮 / 8 注入项）
    test_endpoints_server.py 本地测试端点（/ok /delay?ms= /hang /flaky）
  tests/             pytest 套件（24 项）
```

## 快速开始（Windows 10/11，PowerShell / cmd 通用）

```bat
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt

rem 终端 1：本地测试端点（成功/挂起/延迟；连接拒绝用未监听端口 19099 体现）
python samples\test_endpoints_server.py 18081

rem 终端 2：仿真台 API
set SIMLAB_DB=simlab.db && python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Linux/macOS 下把 `set X=Y &&` 换成 `X=Y` 即可。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/topologies` | 提交拓扑；内容哈希幂等复用；成环/非法返回 `E_TOPOLOGY_INVALID` 且整批不加载 |
| POST | `/api/topologies/{id}/plans` | 提交注入计划（目标/故障类型/持续轮数/优先级/回环端点） |
| POST | `/api/plans/{id}/run` | 顺序推进全部轮次；`Idempotency-Key` 头幂等重放 |
| GET  | `/api/plans/{id}/rounds/{n}` | 轮次结论：服务状态、注入明细、切换记录 |
| GET  | `/api/plans/{id}/rounds/{n}/budget` | 预算明细：并发/队列、切换配额、时间余量、域配额占用 |
| GET  | `/api/plans/{id}/skips` | 被跳过/延后注入项及原因 |
| GET  | `/api/plans/{id}/services/{svc}/history` | 服务跨轮状态历史 |
| POST | `/api/recovery/report` | 扫描未决意图，中止未完成轮次并回滚配额，输出事实报告 |

## 关键设计

- **状态机**：`HEALTHY→{DEGRADED,ISOLATED,FAILED_OVER,FAILED}`；
  `ISOLATED→FAILED_OVER/FAILED/HEALTHY`；`FAILED→HEALTHY`（仅恢复）；
  非法迁移抛 `E_INVALID_TRANSITION` 且不写库。
- **调度次序**：优先级降序 + 故障域轮转（堆排序确定性输出），同服务同轮去重。
- **双重预算**：全局并发上限 + 有界等待队列（容量=并发+queue_capacity，溢出
  `E_INJECTION_QUEUE_FULL`）；每轮时间预算到期即停收结果，未启动/未完成项记
  `ROUND_TIME_BUDGET_EXHAUSTED`，整轮不越过预算。
- **真实注入**：`node_down` 打未监听端口（真实连接拒绝）；`link_timeout` 打
  `/hang` 并以硬 socket 超时关闭底层连接（`E_INJECTION_TIMEOUT`）；
  `replica_jitter` 多次探测用 NumPy 计算 p95 判定延迟抖动。
- **事务边界**：每轮仅两次写事务（准入、裁决）。配额占用与注入准入同事务；
  配额释放与注入结束同事务；服务状态、切换记录、隔离记录同一提交生效。
- **崩溃恢复**：写操作先落 `intents(pending)` 再提交；强杀后重启扫描未决意图，
  将轮次置 `ABORTED`，删除该轮的配额占用/切换记录/状态行，回滚到最后一个
  完整提交版本；恢复报告只陈述持久化事实，不承诺强杀进程有完整退出报告。

## 业务错误码

`E_TOPOLOGY_INVALID` `E_PLAN_INVALID` `E_FAULT_DOMAIN_FULL`（附占用明细）
`E_INJECTION_QUEUE_FULL` `E_INJECTION_TIMEOUT` `E_FAILOVER_DENIED`（附原因）
`E_INVALID_TRANSITION` `E_NOT_FOUND` `E_ROUND_ABORTED`

## 实测验证记录（本仓库交付时真实执行）

- `pytest`：**24 passed**（拓扑幂等/成环拒绝、状态机、配额满跳过、队列满跳过、
  时间预算跳过且整轮不超支、超时错误码、抖动降级、切换配额耗尽拒绝、多轮配额
  持有与原子释放、恢复回滚及幂等、API 全流程与幂等重放）。
- 样本计划实跑（uvicorn + 端点服务器）：
  - 第 1 轮：`order`/`payment` → `FAILED_OVER`（切换 fd-c 成功），`search` →
    `DEGRADED`（p95=261.7ms 超预算 80%），`inj-inventory-down`/`inj-notify-down`
    因 `FAULT_DOMAIN_QUOTA_EXHAUSTED` 跳过并附占用明细；用时 791ms / 1200ms。
  - 第 2 轮：`gateway` → `FAILED_OVER`，`order` 持续注入保持 `FAILED_OVER`，
    `payment` 注入结束恢复 `HEALTHY`。
  - 第 3 轮：`notify`（策略 none）→ `ISOLATED`，其余恢复 `HEALTHY`。
- 强杀恢复：手工写入 `RUNNING` 轮次 + 未释放配额 + 未决意图模拟崩溃，
  `POST /api/recovery/report` 后该轮 `ABORTED`、开放配额行清零、切换记录删除、
  状态回滚至最后完整版本，报告含 `state_diffs` 与免责声明。

## 运行测试

```bat
python -m pytest tests -q
```
