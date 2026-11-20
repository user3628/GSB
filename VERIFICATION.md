# 实际验证记录

验证日期：2026-11-21  
运行环境：Python 3.14.4（实现兼容 3.11+）；FastAPI 0.165.0；uvicorn 0.54.0；NumPy 2.5.5；pytest 9.2.0；httpx 0.28.1。  
所有验证仅使用工作区内的确定性样本，服务只监听 `127.0.0.1`，未访问外部服务。

## 自动化回归测试

命令：

```text
pytest -q
```

实际结果：

```text
..............................                                           [100%]
32 passed, 1 warning in 0.84s
```

唯一警告来自当前安装的 FastAPI/Starlette TestClient 对其 `httpx` 兼容层的弃用提示，不影响应用运行或测试结论。

覆盖项：

- 版本段数值比较、原始字符串保留与 `E_VERSION_MALFORMED`
- 阈值/可容忍延迟整数校验与 `E_POLICY_INVALID`
- binary v2 与 JSONL v1 逐字段往返
- binary v1 显式字段默认值补齐与秒级时间截断
- CRC32 损坏检测和高版本未知必填字段拒绝
- JSONL 多分片往返
- 导入覆盖/跳过/拒绝、整批回滚和失败批次幂等复用
- 同批次并发提交只执行一次
- 同采样时刻按最大位点差选择且与到达顺序无关
- 跨版本 `BELOW/EQUAL/ABOVE` 与 `E_VERSION_AHEAD`
- `HEALTHY -> DELAYED/DEGRADED -> RESTORED -> HEALTHY`
- 非法迁移返回 `E_INVALID_TRANSITION` 且状态与更新时间不变
- 裁决编辑、持久化回放与回放确定性
- 强制终止后的批次日中止、数据集保持与恢复报告事实
- 策略表与最后完整快照不一致时，重启恢复回滚到完整快照

## 真实 HTTP 样本加载

启动本地 uvicorn 后执行：

```text
python scripts/load_samples.py --base-url http://127.0.0.1:8899 --observe
```

实际结果：

```text
策略导入            HTTP 200, APPLIED, datasetVersion=2
观测导入            HTTP 200, observationCount=10, slaveCount=5
状态重算            HTTP 200
观测原始/去重数量    10 / 9
状态计数            HEALTHY=1, DELAYED=2, DEGRADED=1, RESTORED=0
人工复核            manualReviewCount=1
业务码集合          E_POLICY_MISSING, E_VERSION_AHEAD
顶层业务码          E_VERSION_AHEAD
回放                deterministicMatch=true
```

具体裁决：

```text
replica-east-01  HEALTHY -> DELAYED   EQUAL  THROTTLE_WRITES  selected binlogGap=80
replica-west-02  HEALTHY -> DELAYED   ABOVE  MONITOR          forwardCompatible=true
replica-north-03 HEALTHY -> DEGRADED  BELOW  FAILOVER         连续超过 tolerableDelay
replica-south-04 HEALTHY -> HEALTHY   ABOVE  MANUAL_REVIEW    E_VERSION_AHEAD
replica-ghost-99 无策略               E_POLICY_MISSING
```

## 高版本样本拒绝

导入 `samples/policies_future_v2.jsonl`：

```text
HTTP 409
code=E_FORMAT_INCOMPATIBLE
unknown_required_fields=["tenantRoutingKey"]
negotiation.decision=REJECT
```

自动化测试另行确认：拒绝前后 `datasetVersion` 均为 1，策略集合仍为空；相同 `batchId` 重提复用同一失败结果。

## 冲突与原子回滚

使用现有 `replica-east-01` 构造：

1. 更低修订号：报告 `SKIP`，数据集版本不变；
2. 更高修订号：报告 `OVERWRITE`，字段生效；
3. 修订号、优先级、写入时间完全并列但复制组不同：报告 `REJECT`。

第三类批次还包含一条全新策略。实际结果为整个批次返回 `E_IMPORT_CONFLICT`，新策略未写入，数据集版本保持不变，错误详情包含完整 `importReport`：

```text
counts = {"ADDED": 1, "OVERWRITE": 0, "SKIP": 0, "REJECT": 1}
atomicRollback = true
```

## 非法状态迁移

从 `replica-north-03=DEGRADED` 尝试迁移到 `HEALTHY`：

```text
HTTP 409
code=E_INVALID_TRANSITION
currentState=DEGRADED
allowedTargets=["RESTORED"]
persistedStateUnchanged=true
```

迁移失败前后持久化状态均为 `DEGRADED`，且 `updatedAtMs` 未变化。

## 强制终止与恢复

命令：

```text
python scripts/kill_recovery_demo.py
```

脚本以子进程启动 uvicorn，先导入 4 条基线策略，再在第二次导入的 SQLite 事务提交前执行 `os._exit(92)`。实际结果：

```text
基线导入 HTTP 200
崩溃点 before_commit 后服务退出码：92
恢复后数据集版本 2
恢复后策略数 4
abortedBatches 包含 crash-during-import
unfinishedIntents 中对应意图状态 ABORTED
integrity.currentDatasetVersion=2
integrity.lastCompleteDatasetVersion=2
integrity.restoredFromSnapshot=false
scope=PERSISTED_FACTS_ONLY
forcedTerminationExitReportAvailable=false
同批次重提 HTTP 409, code=E_BATCH_ABORTED, deduplicated=true
```

这验证了进程未提交事务没有改变已提交数据集，未完成意图在重启扫描后被标记中止，并且重复提交复用已持久化的中止结论，而不是再次执行。

