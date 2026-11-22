# 技术说明：架构、确定性与一致性设计

## 1. 总体架构

工作台以「工作区目录」为唯一持久化边界，无任何外部服务依赖：

```
config.yaml                改写配置（同义词/桥接版本、路线上限、预算、裁决权重）
inputs/
  synonym_table.json       固定种子同义词表（含 entry_version）
  glyph_index.json         字形近似桶（sha256 近似码）
  bridge_dictionary.json   双语桥接词典（含 intentionally_missing 触发 UNK）
  manifest.json            分片清单（行数 + 语料指纹 + 跨分片重复声明）
  shards/*.jsonl           查询分片（query_id/raw_query/lang/locale/channel/click_history）
checkpoints/<qid>.jsonl    每查询追加式四阶段检查点
locks/                     flock 锁文件（全局检查点锁、预算锁、停止锁）
state/                     阶段产物、预算账本、失效事件、运行报告
```

阶段划分严格对应需求：`dispatch → candidate → orchestration → validation`。
`Pipeline.run` 是唯一编排入口，`QueryEngine` 负责单查询阶段计算与检查点发布，
`CheckpointStore` 负责持久化与并发保护，`BudgetLedger` 负责预算原子记账。

## 2. 确定性保障

- **规范化 JSON**：所有落盘与指纹均经 `canonical_dumps`（sort_keys、固定分隔符、
  ensure_ascii=False、allow_nan=False），消除 dict 序与浮点 NaN 的不确定性。
- **固定随机源**：数据生成使用 `random.Random(显式种子)`；字形码使用
  `sha256("glyph:"+seed+token)`；延迟模拟为每查询派生独立 RNG
  （`sha256("latency:"+seed+query_id)`），因此耗时与线程交错无关。
- **NumPy 仅用于纯函数数值**：`log` 合成裁决分，输入相同则输出相同；不使用随机数。
- **确定性排序**：候选按 (rewritten_query, basis 指纹) 排序；裁决同分按
  (-score, 路线固定优先序 synonym<bridge<glyph, 候选指纹) 仲裁；全局归约按 query_id
  与候选路径排序。

## 3. 并行归约为何与串行逐字节一致

- 接纳顺序固定为 query_id 升序，预算预占在 `BudgetLedger` 的文件锁内原子完成，
  与线程数无关；波次划分仅由 channel 集合与并发上限决定，是纯函数。
- 并行（ThreadPoolExecutor.map）与串行（for 循环）消费**同一个有序 wave 列表**，
  每个工作单元的输出只依赖其输入，不依赖其他线程的中间状态。
- 归约阶段统一从检查点/内存按 query_id 排序重建，因此执行载体不同不影响字节结果。
- 验收用 `cmp` 逐字节比较 `plans/candidates/orchestration/validation/summary` 五个产物，
  并比较整轮 `run_fingerprint`（对四个排序后产物数组再做一次 sha256）。

## 4. 预算与延迟模型

- **评估成本预估取上界**：单查询成本 = 各启用路线 `max_candidates` 之和（至少 1），
  实际候选数恒不超过预估，因此「先预占后执行」保证任意时刻全局累计不超预算。
- **三重约束**在同一次锁内判定：全局 `global_used+cost<=global_limit`、
  通道 `channel_used+cost<=per_channel_quota`；并发通道上限通过波次划分约束。
- 接纳失败立即归还预占并落结构化错误（`E_BUDGET_EXHAUSTED`/`E_CHANNEL_QUOTA`）。
- **延迟预算**采用确定性模拟耗时（可经 config.simulation 调整 work/jitter/seed），
  超预算返回 `E_LATENCY_BUDGET`（含 query_id、elapsed_ms、budget_ms）并归还预占，
  不阻塞线程池；线程池不使用「线程超时杀死」语义，工作线程异常统一包装为
  `E_WORKER_EXCEPTION` 且已完成阶段产物保留。

## 5. 检查点、序号与失效传播

- **原子写**：`mkstemp 临时文件 → write → fsync(file) → os.replace → fsync(dir)`，
  任意位置崩溃都不会留下半写记录；检查点为每行一条完整 JSON 的追加日志。
- **序号模型**：seq 在查询内从 1 单调连续递增；同阶段重复发布 → `E_STALE_CHECKPOINT`，
  阶段固定序回退 → `E_INVALID_STAGE`，未知阶段名同样拒绝。
- **版本指纹束**（`VersionBundle`）按阶段最小依赖拆分：
  - dispatch 依赖 `input_fingerprint` + `normalization_fingerprint`；
  - candidate 额外依赖三路词典指纹与 `routes_fingerprint`；
  - orchestration 依赖 `budget_fingerprint`；
  - validation 依赖 `adjudication_fingerprint`。
  因此修改裁决权重只会令 validation 失效，修改预算只影响 orchestration，
  修改同义词表只影响 candidate 及下游，未受影响上游检查点继续复用。
- `first_invalid_stage` 定位最早不匹配阶段，`invalidate_from` 在全局文件锁内截断
  该查询日志并把失效原因写入 `state/invalidations.jsonl`，禁止旧指纹冒充新结果。

## 6. 优雅停止与恢复语义

- 停止信号持久化为 `state/stop.flag`（文件锁保护），流水线在每个不可拆分查询单元
  边界检查；停止后未处理查询显式登记 `STOP_REQUESTED` 原因，不产生半截产物。
- 恢复时清除停止标记，逐查询先做指纹校验（复用或失效重建），预算账本清空瞬时预占后
  按相同确定序重新接纳；已提交编排的查询从检查点恢复且不重复占用预算。
- 因全部决策为确定性纯函数，恢复运行的最终产物与从零完整运行逐字节一致。

## 7. 并发发布保护

- `CheckpointStore.publish` 与 `BudgetLedger` 的所有读改写均在 `fcntl.flock`
  排他锁内完成，跨进程互斥；同进程内通过「持锁期间完成索引更新」保证线程可见性。
- `scripts/mp_lock_demo.py` 用 fork 双进程竞争发布：重复 dispatch 被拒
  （`E_STALE_CHECKPOINT`），新 candidate 正常推进，最终序号严格等于 2。

## 8. 错误处理可观测性

所有业务错误均为 `BusinessError(code, message, query_id, stage, context)`，
经规范化 JSON 落盘；CLI 顶层异常与 HTTP 5xx 返回结构化错误体，不暴露堆栈给业务流。
分片清单在装载时强校验行数与语料指纹，篡改即抛错阻止运行。
