# 实际验证记录

验证日期：2026-11-23  
环境：Linux/Ubuntu，Python 3.14.4，Flask、NumPy、pytest 已安装，无 Docker、无外部服务。代码目标下限为 Python 3.11。

## 1. 自动化测试

命令：

```bash
pytest -q
```

实际结果：

```text
......................................                                   [100%]
38 passed in 3.61s
```

覆盖项：

| 类别 | 已验证行为 |
|---|---|
| 量纲与规格 | `40Gi`、`512Mi`、`250m`、`1s`；负数、未知单位、非整数、NaN、64 位溢出；未知配额组字段定位 |
| 配额准入 | 显存超额、算力超额、错误明细中的节点占用/预留/请求量、失败无部分预留 |
| 状态机 | 非法 `PENDING -> BATCHED` 原子失败；迁移前后状态、原因和 `op_seq` 持久化 |
| 并发幂等 | 8 路并发相同请求仅 1 次创建；6 路并发 auto-admit 仅 1 次预留 |
| 批组织 | 相邻同模型同节点合批、模型样本上限、延迟上限阻止合批、单样本超限返回 `E_BATCH_TOO_LARGE` |
| 拆分 | 模型上限拆为 3 片、延迟上限拆为 5 片；样本和显存总量严格保持不变并输出原因 |
| 策略 | 自动比较提交贪心和显存降序，选择更少批的 `memory_descending` |
| 调度 | 并发上限、时间预算、有界队列、`E_EXEC_QUEUE_FULL` 整轮回滚、同轮重复调用幂等、跳过原因 |
| 抢占 | 高优先级准入抢占低优先级请求；批、节点分配、配额预留同步释放；资源不足时完整回滚 |
| 恢复 | 强杀批组织事务、强杀准入事务、启动时中止遗留 RUNNING 批、恢复报告与不变量 |
| 读一致性 | 写事务暂停在提交前时，读者只能看到旧的 ADMITTED 状态和零批；提交后看到完整 BATCHED 状态 |
| HTTP | 提交、准入、批组织、调度、完成和错误响应结构 |

## 2. 确定性演示场景

命令：

```bash
python -m quota_scheduler --db /tmp/quota_demo.db demo
```

实际关键结果：

```text
ok: True
submitted: 6
admissions:
  req-vision-a       ADMITTED  preemptions=0
  req-vision-b       ADMITTED  preemptions=0
  req-vision-c       ADMITTED  preemptions=0
  req-llm-a          ADMITTED  preemptions=0
  req-llm-b          ADMITTED  preemptions=0
  req-vision-high    ADMITTED  preemptions=1

batching: 5 batches, strategy=greedy_submission, errors=0
  batch-000001  req-vision-a     samples=8  latency=320ms
  batch-000002  req-vision-b     samples=6  latency=240ms
  batch-000003  req-llm-a        samples=4  latency=1000ms
  batch-000004  req-llm-b        samples=6  latency=1500ms
  batch-000005  req-vision-high  samples=4  latency=160ms

round 1: selected 2, queued 3, time=1160ms
round 2: selected 2, queued 1, time=1820ms
round 3: selected 1, queued 0, time=240ms

final:
  req-vision-a       COMPLETED  reservation_active=False
  req-vision-b       COMPLETED  reservation_active=False
  req-vision-c       PREEMPTED  reservation_active=False
  req-llm-a          COMPLETED  reservation_active=False
  req-llm-b          COMPLETED  reservation_active=False
  req-vision-high    COMPLETED  reservation_active=False

invariants: {"ok": true, "issues": []}
recovery prepared intents: 0
```

## 3. 强杀恢复验证

测试 `tests/test_concurrency_recovery.py` 启动真实子进程，并设置：

```text
QUOTA_SCHEDULER_CRASH_AT=ORGANIZE_BATCHES:before_commit
QUOTA_SCHEDULER_CRASH_AT=ADMIT_REQUEST:before_commit
```

子进程在业务事务提交前调用 `os._exit(137)`。重启后实际观察：

- 数据库中存在 1 条 `PREPARED` 意图；
- `<db>.intents.jsonl` 中可看到对应的 `PREPARED` 事件；恢复后追加 `ABORTED` 事件；
- 批组织恢复前：请求仍为 `ADMITTED`，批数 0，节点分配 0，预留仍有效；
- 准入恢复前：请求仍为 `PENDING`，预留为 `NULL`，配额预留量不变；
- `recover()` 将未完成意图标记为 `ABORTED`，输出基于持久化事实的报告；
- 恢复后 `PREPARED` 意图数为 0，所有一致性不变量通过。

## 4. 只读快照验证

测试在 `ORGANIZE_BATCHES` 业务事务完成所有内存变更后、提交前暂停写线程。另一个连接读取时只看到：

```text
request.status = ADMITTED
reservation.active = true
request.batches = []
batch_count = 0
```

释放写线程后读取到：

```text
request.status = BATCHED
reservation.active = true
request.batches length = 1
invariants.ok = true
```

这验证了查询不会观察到“请求已经 BATCHED，但预留/节点事实仍是旧值”的中间状态。

## 5. 约束声明

- 所有资源均为本地逻辑节点，不访问外部目标。
- 未使用 Docker 或其他容器。
- 数据只来自 `sample_data` 与测试临时样本。
- 强杀故障注入默认关闭，仅在显式设置环境变量时生效。

