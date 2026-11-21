# 实际验证记录

记录时间：2026-11-22（Asia/Shanghai）。

验证环境为工作区内的 Python 3.14.4 运行时；代码与依赖约束同时面向目标环境 Python 3.11/3.12，不使用 3.12+ 专属语法，依赖范围均兼容 3.11。

## 1. 静态检查与测试

执行：

```bash
python3 -m compileall -q app scripts tests
python3 -m pyflakes app scripts tests
python3 -m pytest -q
```

结果：

- `compileall`：通过。
- `pyflakes`：无告警。
- `pytest`：`26 passed`，另有 1 条来自当前 FastAPI TestClient/httpx 组合的上游弃用告警，不影响结果。

测试覆盖：

- 所有 `k=1..6, m=1..2` 的任选 `k` 行子矩阵可逆；
- 布局原子校验、摘要格式、分片索引乱序归一化；
- 时间戳非单调整批拒绝、位图越界拒绝；
- 直接读、副本补齐、EC 重建、不可恢复、预算降级、延迟保护；
- 有界队列满；
- 重建摘要不一致及双侧摘要；
- GF(256) 矩阵声明范围越界返回 `E_MATRIX_INVALID`；
- 跨数据库实例逐位一致裁决摘要；
- 重复请求复用与冲突检测；
- 已持久化分片结果复用；
- 提交后强制终止的任务/预算恢复；
- 恢复后重提轨迹仅重建剩余分片，已提交结果直接复用；
- HTTP 业务码统一、并发只读查询。

## 2. 确定性业务演示

执行：

```bash
python3 scripts/demo.py --output artifacts/demo_evidence.json
```

结果：`DEMO PASSED`。

主轨迹关键输出：

```text
request                        strategy      outcome  latency_us  error
req-gamma-unrecoverable        DIRECT_FAIL   FAILED         1920  E_UNRECOVERABLE
req-alpha-ec                   EC_REBUILD    SUCCESS        3126  -
req-beta-replica               REPLICA_FILL  SUCCESS        2270  -
req-delta-direct               DIRECT_READ   SUCCESS        1560  -
req-epsilon-ec                 EC_REBUILD    SUCCESS        4332  -
req-iota-digest-repair         EC_REBUILD    SUCCESS        4332  -
req-zeta-shed                  DIRECT_FAIL   FAILED         3126  E_REBUILD_SHED
req-eta-shed                   DIRECT_FAIL   FAILED         3126  E_REBUILD_SHED
req-theta-latency              DIRECT_FAIL   FAILED         3126  E_LATENCY_GUARD
bytes=2048/2048 makespan_us=2412/20000 tasks=4 shed=2
```

重放：

```text
decision digest identical:
sha256:48c06165e7a8fb067cf5ac50afc492dbfd23ca2c25d395bc3e6aa47c808badeb
reused request IDs: 9
第二轮字节消耗：0
```

有界队列：

```text
queue-zeta-01                  EC_REBUILD    SUCCESS        3126  -
queue-eta-01                   DIRECT_FAIL   FAILED         3126  E_REBUILD_QUEUE_FULL
queue-theta-01                 DIRECT_FAIL   FAILED         3126  E_REBUILD_QUEUE_FULL
bytes=512/4096 tasks=1 shed=2
```

重建摘要不一致：

```text
mismatch-kappa-01  EC_REBUILD  FAILED  E_RECONSTRUCT_MISMATCH
bytes=384/4096 tasks=1
expected_digest=sha256:0000000000000000000000000000000000000000000000000000000000000000
actual_digest=sha256:3f56de66eda7d7abca2687a7479034496e63f5c9611e826b4701cde3f2d1d3a5
```

## 3. 真实强制终止与恢复

执行：

```bash
python3 scripts/crash_and_recover.py
```

脚本启动真实 Uvicorn 子进程，在第一个分片结果原子提交后执行 `os._exit(42)`，再以新进程打开同一 SQLite 数据库。

结果：

```text
forced process exit observed: code=42
clean_shutdown_report=false
recovery_required=true
pending_intents=0
aborted_rounds=1
tasks_returned_to_ready=1
reusable_committed_shards=1
committed_bytes=512
released_reserved_bytes_for_uncommitted_tasks=512
remaining_bytes_recomputed=3584
RECOVERY PASSED
```

报告明确标注 `scope=startup_reconciliation`，并且不声称生成完整退出报告。证据写入 `artifacts/forced_kill_recovery.json`。

## 4. HTTP 服务

实际启动：

```bash
REPAIR_DB_PATH=/tmp/read-repair-final-20261122.db \
python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8878
```

验证结果：

```text
GET  /                         200  7694 bytes
GET  /static/app.js            200 13094 bytes
GET  /static/styles.css        200 14806 bytes
GET  /api/v1/health            200
GET  /api/v1/demo/fixtures     200 60929 bytes
POST /api/v1/layouts           201
POST /api/v1/traces            201
GET  rounds/{id}/decisions     200  9 decisions
GET  rounds/{id}/budget        200  committed_bytes=2048, reserved_bytes=0
GET  /api/v1/recovery          200
GET  /api/v1/rounds            200
```

同一轮次并发发起 12 次只读查询，全部返回 `200`。

最终 HTTP 冒烟脚本还校验了：主轨迹裁决摘要等于
`sha256:48c06165e7a8fb067cf5ac50afc492dbfd23ca2c25d395bc3e6aa47c808badeb`，
预算账本的 `committed_bytes=2048`、`reserved_bytes=0`，且轮次列表中的轮次状态为 `COMMITTED`。
