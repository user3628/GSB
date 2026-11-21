# 架构、并发与恢复语义

## 1. 分层

```text
HTTP API (FastAPI)
  |-- codecs / normalization / validation
  |-- simulation / diagnostics (pure deterministic functions)
  `-- SnapshotStore (single writer + immutable readers)
         `-- SQLite WAL: snapshots, snapshot_policies, intents, simulation_batches
```

仿真和诊断本身不访问数据库；`SnapshotStore` 先取得一个完整 `PolicySnapshot` 对象，再执行纯函数。因此同一快照版本与同一规范化样本必然得到逐包一致结果。

## 2. 快照切换

`PolicySnapshot` 是不可变值对象，包含 `snapshot_seq` 对应的格式版本、业务快照序号、生成时间和策略元组。

写路径：

1. 在写者锁内校验单调序号和内容。
2. 以独立事务写入 `intents(state=started)`。
3. `BEGIN IMMEDIATE`，插入 `snapshots` 和该 `snapshot_seq` 的全部 `snapshot_policies`。
4. 同一事务把意图置为 `committed` 并提交。
5. 提交成功后，在状态锁内一次性替换内存当前快照指针。

读者只调用 `current_snapshot()` 获取不可变对象，不会跨版本拼接策略。旧版本仍可按 `snapshot_seq` 从 SQLite 读取，供审计或恢复；内存只保留当前版本，切换后的旧对象由 Python 引用计数释放。

序号规则：

- 新序号更高：正常提交。
- 序号相同且规范化内容校验和相同：幂等成功，不重复写。
- 序号相同但内容不同：`E_SNAPSHOT_INVALID`。
- 序号更低：`E_SNAPSHOT_INVALID`。

## 3. 单写者与多读者

- 进程内 `RLock` 覆盖导入、快照切换和仿真批次，确保提交串行。
- SQLite 使用 `journal_mode=WAL`、`synchronous=FULL`、`BEGIN IMMEDIATE` 和 busy timeout。
- 多个 HTTP 诊断读者可在写提交期间继续读取旧完整快照；切换完成后读到新完整快照。
- 部署约束是单个 Uvicorn worker。若未来扩展多进程，需要额外的跨进程协调层。

## 4. 意图与恢复

意图状态机：

```text
started --业务事务提交--> committed
started --启动扫描/异常--> aborted
```

关键点：

- 意图记录先于业务事务独立提交，因此强杀后一定能观察到“曾经开始写”的事实。
- 业务行与 `state=committed` 在同一 SQLite 事务内，不存在“数据已提交但意图仍 started”的正常路径。
- 强杀时未提交的 SQLite 事务自动回滚，不会留下半个快照或半条批次结果。
- 启动扫描 `started` 意图，将其改为 `aborted`。
- 若它是仿真批次，同时插入 `status=aborted` 的批次行；`result_json` 和 `diagnostics_json` 保持 `NULL`。
- 恢复报告只包含持久化事实，并明确不承诺强杀后得到完整退出报告或仿真结果。

## 5. 仿真模型

### 5.1 双桶

每个队列维护：

- 承诺桶：容量 `CBS × 8` bit，补充速率 `CIR`。
- 峰值桶：容量 `PBS × 8` bit，补充速率 `PIR`。

以 `1e-6 bit` 为整数定点单位：

```text
refill_subunits = rate_bps × elapsed_us
capacity_subunits = burst_bytes × 8 × 1_000_000
packet_cost_subunits = packet_bytes × 8 × 1_000_000
```

第一次见到某队列时两桶均满。补充后先封顶，再执行判决。

### 5.2 判决

| 到达颜色 | 条件 | 结果 |
|---|---|---|
| green | 承诺桶和峰值桶都足够 | green，发送，双桶扣减 |
| green | 承诺桶不足、峰值桶足够 | yellow，发送，仅峰值桶扣减 |
| yellow | 峰值桶足够 | yellow，发送，仅峰值桶扣减 |
| 任意 | 峰值桶不足 | red，丢弃 |
| red | 任意 | red，丢弃，不消耗令牌 |

`marked_bytes` 包含黄色发送字节与红色丢弃字节。结果同时输出每包前后两桶余量、动作原因和逐队列累计统计。

## 6. 数值精度

- 令牌与速率补充：纯整数，无浮点误差。
- 实际发送速率：精确有理数 `sent_bits / measurement_window`，只在展示时舍入。
- 测量窗口为全样本最小时间戳到最大时间戳；单包或零跨度样本使用 1 µs 防止除零。
- 相对容差比较：Decimal。
- 比例输出：固定 6 位小数，`ROUND_HALF_UP`。
- 报告输出累计速率舍入误差。
- 无流量队列分类为 `no_traffic`，不参与超发/欠发清单。
- 所有清单按 `queue_id` 字典序稳定排序。
