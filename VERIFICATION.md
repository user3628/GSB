# 故障注入执行工作台 — 技术说明与验证记录

## 结构

- `fiw/catalog.py` — 端点清单/注入计划解析与归一化；解析失败、未知端点引用、
  依赖环均返回 `E_PLAN_INVALID` 且整批不加载；版本单调（同版本幂等、高版本整体替换、
  低版本拒绝）。
- `fiw/faults.py` — 真实故障执行：`connection_refused` 用真实 `connect()` 判定；
  `connection_timeout` 对真实不响应套接字等待响应、超时后 `shutdown`+`close` 释放
  套接字并返回 `E_INJECTION_TIMEOUT`（无固定休眠冒充）；`corrupt_body` 以真实返回体
  JSON 解析失败判定。能力不匹配返回 `E_FAULT_UNSUPPORTED` 并附原因，不降级。
- `fiw/scheduler.py` — 轮次调度：全局并发上限 + 有界等待队列（满则该项
  `E_INJECTION_QUEUE_FULL`）、每轮时间预算（按优先级与端点轮转确定性准入，
  预算不足者 `SKIPPED/round_time_budget_exhausted`）、同端点同轮去重。
- `fiw/observation.py` — 每轮观测结论（NORMAL/UNSTABLE/UNREACHABLE）：注入结果 +
  健康权重基线（<0.5 记 UNSTABLE）+ 上游 UNREACHABLE 逐级降级并记录传播依据；
  仅基于已提交轮次轨迹计算。
- `fiw/statemachine.py` — PLANNED/RUNNING/PAUSED/COMPLETED/ABORTED 状态机，
  非法迁移返回 `E_INVALID_TRANSITION` 且不改动持久化状态。
- `fiw/store.py` — SQLite 持久化：计划、轮次（轨迹/预算/观测）、意图记录、恢复报告。
  所有写操作先单独提交意图记录，再在工作事务中完成写入并提交意图；状态迁移、轮次
  结论与预算占用在同一事务边界内生效。
- `fiw/engine.py` — 编排：加载、迁移、轮次执行（同轮次重复触发只执行一次，其余复用
  已提交结果）、恢复扫描（未完成意图 → 轮次回滚到最后一致版本、计划标记 ABORTED，
  输出仅基于已持久化事实的恢复报告）。
- `fiw/app.py` — 本地 HTTP 接口（FastAPI）：`POST /plans/load`、`POST /plans/{id}/transitions`、
  `POST /plans/{id}/rounds/{n}/run`、`GET /plans/{id}/rounds/{n}`、`POST /recover`、
  `GET /recovery/startup` 等。
- `test_endpoints/servers.py` — 本地测试端点：健康 HTTP 端点（支持 `X-Fault:
  corrupt-body` 故障注入头）、真实不响应的黑洞套接字、未监听端口。
- `samples/` — 确定性样本（端点清单 + 注入计划）；`scripts/demo.py` — 四类演练；
  `tests/` — 45 个 pytest 用例。

## 运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests -q      # 测试
.venv/bin/python scripts/demo.py         # 四类演练（端口可用 FIW_DEMO_PORT 覆盖）
FIW_DB=data/fiw.db FIW_PORT=18090 .venv/bin/python -m fiw.app   # 单独启动 API
```

## 实际验证结果（2026-11-22，本机 Linux，Python 3.14.4）

- `pytest tests -q`：**45 passed**（解析/状态机/调度/真实套接字故障/观测传播/
  幂等轮次/崩溃恢复/HTTP 接口）。
- `scripts/demo.py` 四类演练全部通过，关键输出：
  - 正常轮次：4 项注入在并发上限 2 下执行，预算 limit=2000ms used≈303ms
    remaining≈1697ms quota=4；round 1 重复触发返回 `reused=True`。
  - 故障不支持：`it-4`（对监听中端点注入连接拒绝）→ `FAULT_UNSUPPORTED
    [E_FAULT_UNSUPPORTED]`，原因 "port is listening; connection_refused cannot
    take effect"。
  - 真实超时：`it-2` 对黑洞套接字 → `TIMEOUT [E_INJECTION_TIMEOUT]`，
    elapsed≈301.6ms（≥300ms 真实等待），evidence 含 `socket_closed: True`。
  - 强制终止恢复：以 `FIW_CRASH_AFTER_INTENT=plan-demo:2` 重启服务并触发 round 2，
    进程以退出码 2 硬终止；重启后恢复扫描发现 1 条 PENDING 意图，执行
    `round_rolled_back`（interrupted_round=2, last_committed_round=1）与
    `plan_aborted`；round 1 的 4 条轨迹保持已提交状态，round 2 不存在。
- 观测传播：round 1 中 `ep-api-1`（权重 0.4 → UNSTABLE 基线）因上游 `ep-db-1`
  UNREACHABLE 被降级为 UNREACHABLE，传播依据持久化于轮次观测中。
