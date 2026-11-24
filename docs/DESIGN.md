# 设计说明

## 目标与边界

本组件用于单机技术验证，不执行真实 GPU 计算或真实训练循环。它把训练编排器关心的
检查点分级事实、预算裁决和故障恢复边界落到 SQLite 与本地 HTTP：

- 分片内容由固定种子生成器确定性产生。
- 写盘是持久化事实层面的模拟任务；调度字节数和任务耗时由策略明确声明。
- 所有状态只保存在本地 SQLite，不使用容器、外部服务或网络。

## 版本与事实模型

### 策略版本

表 `policy_versions` 保存不可变快照。版本来源有两种：

1. `source=snapshot`：客户端提交完整策略，版本必须严格大于当前最大值。
2. `source=delta`：客户端给出 `base_version`，服务端要求其等于当前策略版本，并把
   受影响分片放置替换到旧快照，按增量字段推进损失缩放，生成 `base_version+1`。

策略校验和覆盖版本、生成时间、VRAM 上限、写预算、时间预算、队列容量、任务耗时、
损失缩放、增长因子、溢出阈值和排序后的分片放置。校验和只覆盖事实字段，不包含
自身。

### 检查点版本

表 `checkpoints` 及其子表保存不可变提交：

- `checkpoint_copies`：每个副本的轮次、精度、字节数、摘要、池、状态、访问时刻、
  写入状态、主参数摘要、动量摘要及内部定点样本。
- `checkpoint_derived`：主参数派生摘要、一致性结果、显存估算、恢复基线和依赖引用。
- `checkpoint_pools`：VRAM/PERSISTENT 的副本数、总字节、上限和剩余。
- `checkpoint_precision`：逐分片精度统计及逐元素明细。

查询接口只读取这些已提交表。策略版本和检查点版本分离，使“策略被接收”和“策略被
物化为完整检查点”不会混淆。

## 分片与双池

每个逻辑分片都有一个 FP32 主参数副本：

- `copy_id = master:<shard_id>`
- `precision = FP32`
- `pool = PERSISTENT`
- `status = PERSISTED`

策略放置为 FP16 时再生成：

- `copy_id = fp16:<shard_id>`
- `master_copy_id = master:<shard_id>`
- 内容摘要由量化后的 binary16 位串与损失缩放计算。

FP16 副本可驻留 VRAM；被驱逐或在策略中指定持久池时回落到 PERSISTENT。FP32 主参数
不得进入 VRAM。

## 驻留与驱逐

检查点规划先复制上一完整版本中仍被策略选中的 VRAM 驻留状态，再按以下顺序处理：

1. 策略要求降级到持久池的副本生成 `POLICY_DEMOTE` 迁移。
2. 新需要的 FP16 副本按优先级降序、最近访问降序、副本 ID 排序后尝试驻留。
3. 空间不足时，从当前 VRAM 候选中按最近访问升序、精度标记、优先级升序、副本 ID
   排序，选择并真实迁移到持久池，原因记为 `LRU_VRAM_PRESSURE`。
4. 驱逐所有候选后仍放不下，返回 `E_VRAM_POOL_FULL`，上下文包含上限、当前占用、
   所需字节和已模拟驱逐数；规划失败不落意图、不提交版本。

最终池汇总从迁移后的副本重算，并再次断言 VRAM 总量不超过上限。

## 写盘与时间预算

检查点内所有主参数和 FP16 副本都形成写入任务。排序键固定为：

1. 优先级降序；
2. 轮次升序；
3. 分片 ID；
4. 副本 ID。

逐任务同时检查剩余写盘字节与剩余时间（`task_time_ms` 是策略声明的确定性成本）。
一旦某个高优先任务无法放入，后续低优先任务全部跳过，防止小任务越过优先级插队。
跳过原因包括 `WRITE_BUDGET`、`TIME_BUDGET` 和 `WRITE_AND_TIME_BUDGET`。

跳过列表进入容量为 `write_queue_capacity` 的有界等待队列。跳过数量大于容量时返回
`E_WRITE_QUEUE_FULL`，整轮不提交。成功报告精确给出预算、已用、剩余、时间与清单。

## 混合精度数值契约

所有参数以 `int64` 定点微单位存储，`FixedScale=1_000_000`。

FP16 编码过程：

1. 计算 `scaled = master_fixed / 1e6 * loss_scale`。
2. 把 `scaled` 转为 IEEE 754 binary16，采用 round-to-nearest, ties-to-even。
3. 还原时解码 binary16，再除以损失缩放，得到定点值时使用 half-away-from-zero。
4. 每个元素保存 FP32 定点值、FP16 位串、还原值和偏差。

FP16 内容摘要与还原摘要使用不同域分隔字符串，避免把压缩内容与还原值混淆。检查点
同时保存主参数摘要和动量摘要；恢复基线引用精度、主参数摘要、动量摘要和损失缩放。

## 派生结构与原子重建

每个副本的派生结构包含：

- `master_derived_digest`：由副本 ID、精度、主参数摘要、动量摘要和损失缩放计算的
  域分隔 SHA-256；
- `consistency_ok`；
- `vram_estimate_bytes`；
- `recovery_baseline`；
- `dependency_refs`。

FP16 恢复基线同时引用 `restored_digest`，与 `master_digest`、精度、动量摘要和损失
缩放共同证明声明的还原关系，而不是只保留一个一致性布尔值。

检查点最终事务依次写入副本、派生结构、池汇总、精度报告和检查点元数据。任一写入
失败都会使事务回滚，因此不可能出现“池汇总已更新而恢复基线仍旧”的可见中间状态。

## 写意图、单写者与恢复

所有写路径先提交 `operations` 意图。检查点意图另写：

- `operation_shards`：每个目标副本一行，初始 `PENDING`；
- `operation_evictions`：每个计划驱逐一行，初始 `PENDING`。

最终事务同时写入不可变检查点事实，并把意图/暂存行/驱逐行标为 `COMMITTED`。如果
进程在意图之后、最终事务之前被强杀：

- SQLite 自动回滚未完成事务；
- 重启扫描 `status=RUNNING`；
- 操作改为 `ABORTED`，阶段改为 `RECOVERED`；
- 暂存分片改为 `PENDING`；
- 驱逐改为 `ABORTED`；
- 写入 `recovery_log`，记录基础检查点版本和恢复证据。

恢复报告只读取最后已提交检查点的副本、派生结构和池汇总；中止意图只作为审计信息
列出，不计入池容量或恢复基线。

## 并发

- 进程内 `writeMu` 串行化策略、增量、检查点与恢复扫描。
- SQLite 设置 WAL、`synchronous=FULL`、busy timeout 和多个连接。
- 每个只读 HTTP 请求在独立只读事务中读取单一不可变检查点版本，多读者不会看到
  未提交状态，也不会阻塞 WAL 写者。

## 主要数据表

| 表 | 作用 |
| --- | --- |
| `policy_versions` | 不可变策略版本 |
| `checkpoints` | 不可变检查点元数据和写预算结果 |
| `checkpoint_copies` | 副本事实与内部定点样本 |
| `checkpoint_derived` | 原子重建的派生结构和恢复基线 |
| `checkpoint_pools` | 双池汇总 |
| `checkpoint_precision` | 精度统计与逐元素比较 |
| `operations` | 写意图与最终状态 |
| `operation_shards` | 未提交分片暂存与回滚证据 |
| `operation_evictions` | 驱逐意图与中止证据 |
| `recovery_log` | 启动恢复审计记录 |
