# 实际验证记录

验证时间：2026-11-22（Asia/Shanghai）。实际数据库时间戳采用 UTC。

## 1. Python 与依赖

创建了独立虚拟环境 `.venv` 并安装 `requirements.txt`。导入结果：

```text
Python 3.14.4
fastapi 0.165.0
uvicorn 0.54.0
numpy 2.5.5
pytest 9.2.0
sqlite3 3.46.1
httpx 0.28.1
hashlib ok 0badac3c6df445ad
```

## 2. 自动化测试

命令：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
..................                                                       [100%]
18 passed in under 3 seconds (repeat runs observed 1.5–2.1s)
```

覆盖点：

- 重复服务、非法镜像摘要、缺失依赖均返回 `E_SPEC_INVALID`，空库仍为 0 版本。
- 全量 v1 重放幂等；同 ID 异内容返回 `E_CONFLICTING_COMMIT`；新 ID 使用旧版本返回 `E_STALE_BASE`；v2 整体替换成功。
- 两个不同提交 ID 使用同一基础版本并发增量，一个成功到 v3，一个得到 `E_STALE_BASE`。
- 两个线程重复提交同一 ID 只执行一次，另一个复用结果。
- 4 个并发读线程在写者增量提交期间共读取 100 次绑定 v2，全部看到相同完整产物哈希。
- 镜像变化使 `inventory → orders → gateway` 下游计划内容变化，并触发三个环境评分重建。
- 配置键变化与依赖列表变化分别触发计划、拓扑、评分和配置键引用级联。
- 诊断接口执行前后部署计划、拓扑与产物哈希完全一致；滞后标记只针对 `staging`（落后 1、容忍 0）。
- 依赖环返回 `E_DEPENDENCY_CYCLE`，包含环、未污染三层和受阻单元；当前版本与计划不变。
- 历史 v1/v2/v3 均可按版本读取，未提交版本返回 `E_VERSION_RETIRED`。
- 子进程在 intent 落盘后、事务提交前 `os._exit(137)`，重启恢复到 v2，pending intent 被标记 aborted。

## 3. HTTP 端到端演示

命令：

```bash
python scripts/demo.py --port 0 --db data/demo.db
```

关键实际输出：

```text
EVENT local_http_ready http://127.0.0.1:<随机回环端口> db=.../data/demo.db

v1: snapshot_version=1, artifacts_rebuilt=30, references_written=252
    scores: dev 94.29, prod 100.0, staging 88.57
v2: snapshot_version=2, 三环境 environment_version=2

增量: snapshot_version=3
  changed_services:    ["inventory"]
  dependent_services:  ["gateway", "orders"]
  affected_services:   ["gateway", "inventory", "orders"]
  invalidated: prod plan(inventory,gateway,orders), prod topology,
               prod expected_state, dev/prod/staging consistency_score

CASCADE TRACE
  #01 prod unit:inventory -> deployment_plan:service:inventory [direct_change:image_digest]
  #02 prod unit:gateway -> deployment_plan:service:gateway [downstream_dependency]
  #03 prod unit:orders -> deployment_plan:service:orders [downstream_dependency]
  #04 prod environment:prod -> topology:environment [dependency_or_unit_change]
  #05 prod environment:prod -> expected_state:environment [unit_state_change]
  #06 dev environment:dev -> consistency_score:environment [cross_environment_dependency]
  #07 prod environment:prod -> consistency_score:environment [cross_environment_dependency]
  #08 staging environment:staging -> consistency_score:environment [cross_environment_dependency]

DIAGNOSTICS
  counts: critical=4, high=1, medium=2, low=0
  ENVIRONMENT_LAG: staging v2 vs baseline prod v3, behind_by=1, tolerance=0
  critical  dev     inventory  IMAGE_DIGEST_MISMATCH
  critical  dev     ledger     IMAGE_DIGEST_MISMATCH
  critical  staging inventory  IMAGE_DIGEST_MISMATCH
  critical  staging payments   IMAGE_DIGEST_MISMATCH
  high      staging <env>      ENVIRONMENT_LAG
  medium    staging gateway    CONFIG_KEY_MISMATCH
  medium    staging inventory  CONFIG_KEY_MISMATCH

CYCLE INJECTION
  HTTP 409 E_DEPENDENCY_CYCLE
  cycle=["auth","identity-db"]
  partial_layers=[["ledger"],["inventory","payments"],["orders"]]
  blocked_units=["auth","gateway","identity-db"]

FINAL
  snapshot_version=3
  dev v2 score 88.57, prod v3 score 100.0, staging v2 score 85.71 (lag_exceeded=true)
  prod layers=[["identity-db","ledger"],["auth","inventory","payments"],["orders"],["gateway"]]
  recovery pending_intents=0, exit_report_guaranteed=false

DEMO OK
```

## 4. 强制终止恢复演练

命令：

```bash
python scripts/recovery_drill.py --db data/recovery.db --fault mid_rebuild
```

关键实际输出：

```text
EVENT writer_exit_code 137
EVENT persisted_facts_before_recovery
  current_version=2
  pending_intent_count=1
  commit_id=demo-inc-prod-inventory-image
  target_version=3

EVENT restart_scan
  scanned_pending=1
  last_complete_version=2
  action=intent_aborted_no_uncommitted_rows_visible
  exit_report_available=false

EVENT facts_after_recovery
  current_version=2
  pending_intents=0
  complete_revisions=2
  committed_v2_prod_plan_services=[
    "identity-db","ledger","auth","inventory","payments","orders","gateway"
  ]
  current_prod_environment_version=2
  recovery_report.basis=sqlite_committed_rows_only
  recovery_report.exit_report_guaranteed=false

RECOVERY DRILL OK
```

该结果证明：进程在重建中途被强制终止后，未提交的 v3 行不可见；重启扫描只能依据已提交的 v2 快照事实恢复，并明确不承诺退出报告。

## 5. 复现命令汇总

```bash
# 测试
.venv/bin/python -m pytest -q

# HTTP 演示
python scripts/demo.py --port 0 --db data/demo.db

# 恢复演练
python scripts/recovery_drill.py --db data/recovery.db --fault mid_rebuild

# 启动工作台后访问 http://127.0.0.1:8765
python -m app --host 127.0.0.1 --port 8765 --db data/workbench.db
```

## 6. 交付入口冒烟验证

使用 `.venv/bin/python -m app --host 127.0.0.1 --port 8877 --db data/smoke-20261122.db` 实际启动成功。检查结果：

```text
GET /api/health  -> 200 {"ok":true,...,"snapshot_version":0}
GET /                -> 200 可视化首页
GET /static/app.js   -> 200 本地脚本
OpenAPI paths        -> 18
STATIC_SMOKE_OK
```
