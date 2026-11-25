# 设计说明

## 1. 模块边界

| 包 | 职责 |
|---|---|
| `internal/model` | 分片、策略、业务错误码和输入校验 |
| `internal/format` | 二进制/JSONL 编解码、CRC 和兼容矩阵 |
| `internal/numeric` | binary16 转换、定点还原、偏差报告 |
| `internal/store` | SQLite 连接、WAL/FULL 同步和迁移 |
| `internal/engine` | 单写者事务、驻留/驱逐、写队列、版本与恢复 |
| `internal/api` | 回环 HTTP 合约 |
| `internal/sample` | 固定种子样本和格式样本 |
| `internal/verify` | 子进程式端到端验证 |

## 2. 状态与事务

SQLite 使用 `journal_mode=WAL`、`synchronous=FULL`、`foreign_keys=ON`。每个变更请求先在 `commits` 表写入 `RUNNING` 意图，然后执行一个业务事务；分片、数据版本、派生状态、精度报告、导入/兼容裁决和 `commits.status=COMPLETED` 在同一事务中提交。若进程在业务事务前终止，重启只会看到意图；若在事务提交后终止，业务状态和完成状态已经一起持久化。

`Engine` 用一个 `RWMutex` 串行化所有写操作；查询持读锁，并通过版本化 `shard_versions`、`precision_reports`、`derived_state` 读取单一完整数据版本。SQLite WAL 允许多个只读连接并发。

启动恢复扫描 `commits.status='RUNNING'`：

1. 将仍标记 `write_intent=<commit_id>` 的未提交分片重置为 `PENDING`。
2. 将仍标记同一意图的未提交驱逐恢复为原池与原状态。
3. 将意图标记为 `ROLLED_BACK` 并写入 `recovery_events`。
4. 从 `shards.status='PENDING'` 重建内存中有界写队列。

检查点、导入和增量按 `shard_id` upsert。后续清单未出现的分片不会被隐式删除；需要清理时必须提交明确的领域操作。驱逐会把原驻留分片保留在持久池，而不是丢弃。

## 3. 分片校验

写入或导入任何状态前校验全部记录：

- `precision ∈ {FP16, FP32}`。
- `size_bytes > 0`。
- `digest`/`source_digest`/`momentum_digest` 为 64 位十六进制。
- FP16 必须有来源 FP32 摘要；FP32 不得携带来源摘要。
- FP32 只能位于 `PERSISTED`。
- 圆次为正，摘要和分片标识不重复。

任一条失败返回 `E_SPEC_INVALID`，业务状态不变。

## 4. 紧凑二进制格式

固定头 28 字节。标记为小端或大端后，头的多字节整数、记录长度、位图和 CRC 存储都使用该字节序。

| 偏移 | 长度 | 字段 |
|---:|---:|---|
| 0 | 4 | 魔数 ASCII `MPCK` |
| 4 | 1 | 格式版本：`1` 旧版、`2` 当前版 |
| 5 | 1 | 大小端：`1` little-endian、`2` big-endian |
| 6 | 2 | 解码必需能力位图 |
| 8 | 4 | 记录数 |
| 12 | 8 | 损失缩放定点值，刻度 `1e6` |
| 20 | 1 | 损失缩放精度：`1` float32、`2` fixed64 |
| 21 | 3 | 保留，必须为零 |
| 24 | 4 | 头 CRC32，覆盖字节 `[0,24)` |

每条记录：

```text
u32 record_length                     # 后续 presence+payload+crc 的字节数
u16 presence_bitmap
payload
u32 crc32                             # 覆盖 presence_bitmap+payload
```

必填 payload 顺序：

```text
u16 id_length, id UTF-8
u8  precision (1=FP16, 2=FP32)
u64 size_bytes
[32]byte digest
```

可选字段位图及顺序：

| 位 | 字段 | 编码 |
|---:|---|---|
| 0 | `pool` | `u8`，1=VRAM，2=PERSISTED |
| 1 | `source_digest` | 32 字节 |
| 2 | `round` | `u64` |
| 3 | `last_access` | `u64` |
| 4 | `momentum_digest` | 32 字节 |
| 5 | `priority` | `int32` |

未知 presence 位或字段后尾随字节返回 `E_RECORD_CORRUPT`，上下文含 1 起始的 `record_index`。未知占位版本、未知必需能力位返回 `E_FORMAT_UNSUPPORTED`。头损坏使用 `record_index=0`。

## 5. JSONL 格式

每条非空行都是一个同构分片记录。格式版本、必需能力位、记录数、损失缩放和精度重复在每一行中，因此外部工具可以按行流式处理，不需要旁路元数据。为兼容早期草案，解码器仍接受“首行 `kind=meta`，后续行记录”的旧式输入。记录 CRC 的材料是“位图大端两字节 + `payload` 对象原始 JSON 字节”，因此外部工具可以对自身序列化出的 `payload` 原样计算 CRC。

```json
{"kind":"record","format_version":2,"required_mask":0,"record_count":6,"loss_scale_fixed":8000000,"loss_scale_precision":"fixed64","presence":63,"payload":{"id":"r1-embed-master","round":1,"precision":"FP32","size_bytes":128,"digest":"...","pool":"PERSISTED","last_access":100,"momentum_digest":"...","priority":10},"crc32":"deadbeef"}
```

记录位图必须与实际出现的可选字段一致；未知位、CRC 不符、各行元数据不一致、记录数不符都返回 `E_RECORD_CORRUPT` 并给出记录序号。旧式 metadata 行的 CRC 覆盖除 `kind`/`crc32` 外元数据对象经 Go `encoding/json` 的规范紧凑编码。整批先解码校验，再进入数据库事务，因此失败不会留下部分导入。

## 6. 兼容矩阵

默认规则：

```json
{
  "rules": {
    "binary": {"1":"DEGRADE_ACCEPT","2":"ACCEPT","3":"REJECT"},
    "jsonl":  {"1":"DEGRADE_ACCEPT","2":"ACCEPT","3":"REJECT"}
  }
}
```

可用 `-compat` 指向覆盖文件。裁决为 `REJECT` 或版本未声明时返回 `E_FORMAT_UNSUPPORTED`。版本大于编解码器已知版本、或必需能力位包含未知位时，即使矩阵尝试接受也会拒绝，避免未知必填字段被静默忽略。

旧版缺少 `pool` 时补齐 `PERSISTED`；`float32` 损失缩放精度升级为 `fixed64`。每条补齐/升级均进入报告，写入 `compatibility_decisions` 与 `import_reports`，可由 `/v1/imports` 与 `/v1/compatibility` 复核。拒绝的输入也会持久化兼容裁决，但没有数据版本。

## 7. 驻留与驱逐

驻留只统计 `status=RESIDENT` 的分片字节。若加入新驻留分片后超限，候选按以下顺序驱逐：

1. FP16 先于其他精度。
2. `last_access` 更早者优先。
3. 标识字典序稳定排序。

本轮恢复基线 `baseline` 中的分片及本次显式请求驻留的分片不能被驱逐。驱逐会把分片真实迁移到 `PERSISTED`，状态置为 `PERSISTED`，并在响应中报告释放字节。候选全部释放仍不足时返回 `E_POOL_FULL`，上下文含 `required_bytes`、`reclaimable_bytes`、容量和当前占用。

## 8. 精度契约

本地参数由摘要和固定种子确定地导出为微单位整数（`1e6` 刻度）。FP16 还原：

1. `scaled = FP32(master) / loss_scale`。
2. 按 IEEE 754 binary16 最近偶数舍入得到 FP16。
3. `restored = FP16(scaled) * loss_scale`。
4. 按“恰好一半远离零”转回固定微单位。

报告声明 `rounding_mode=ieee754_binary16_nearest_even`、`fixed_rounding=half_away_from_zero`、`fixed_scale=1000000`，列出每个分片每个元素的 master/restored 定点值、绝对/相对偏差、分片最大偏差与累计舍入误差。绝对或相对阈值超限返回 `E_PRECISION_DRIFT` 并指明首个违规分片。输入和算法无时间/随机因素，重复生成逐字节一致。

## 9. 版本、增量与派生

- 策略版本严格递增；不大于当前值返回 `E_STALE_SNAPSHOT`。
- 增量携带 `base_version`，必须等于当前策略版本，否则返回 `E_STALE_BASE` 且业务状态不变。
- 成功增量生成下一策略版本和一个新完整数据版本。
- 分片精度、主摘要、来源摘要、动量摘要或损失缩放变化时，在同一事务中为受影响分片重建派生主摘要、一致性结果和恢复基线摘要；恢复基线摘要覆盖基线分片的精度、尺寸、主/来源/动量摘要和当前损失缩放，事件原因写入 `derivation_events`。

参数一致性、恢复基线等派生结构不跨版本修改；历史版本保持只读完整快照。

## 10. 写预算与队列

每个写入任务按“优先级降序、轮次升序、标识升序”排序。任务预计耗时固定为 `1ms + size_bytes/4096`，必须同时满足：

```text
used_write_bytes + task.size_bytes <= strategy.write_budget_bytes
used_time_ms + task.duration_ms <= strategy.time_budget_ms
```

未选中的任务进入有界队列，超过 `-queue-cap` 返回 `E_WRITE_QUEUE_FULL`，事务和队列都不改变。选中任务按目标池落为 `RESIDENT` 或 `PERSISTED`，排队任务保持 `PENDING`。队列语义状态由分片表恢复，重启不会丢失等待分片。

## 11. 幂等与冲突

`commits.request_hash` 由规范化请求 JSON（导入还包含数据 SHA-256）计算。相同标识、相同请求哈希：

- `COMPLETED`：复用原响应，不再执行业务写。
- `FAILED`：复用以 JSON 持久化的原业务错误。
- `ROLLED_BACK`：允许同一请求重新执行。

相同标识但请求哈希不同返回 `E_CONFLICTING_REQUEST`。写操作全程由提交单写者串行化。
