# 本地多租户任务调度平台——政策迁移组件

仅依赖 Python 3.10+ 标准库（`sqlite3`、`http.server`、`zlib`、`struct`、`json`、`threading`），
不使用容器、外部数据库或网络框架。所有样本与回环 HTTP 验证均为本地确定性数据。

## 目录结构

```
policy_migration/
├── polmig/
│   ├── errors.py         # 业务错误码（E_*）与 HTTP 状态映射
│   ├── model.py          # 旧版扁平/新版分层模型、字段校验、规范化摘要
│   ├── codecs.py         # 紧凑二进制 + JSONL 两种可互换持久化格式
│   ├── compatibility.py  # 格式探测与显式版本兼容矩阵
│   ├── migration.py      # 语义映射、降级补齐、重叠/父链检测、裁决
│   ├── storage.py        # SQLite 表结构与全部持久化访问
│   ├── service.py        # 单写者事务、幂等、乐观并发、崩溃恢复
│   └── api.py            # http.server 回环 HTTP 接口
├── samples/              # 固定本地样本
├── tests/                # unittest 测试（34 个）
├── verify.py             # 端到端验收脚本（26 个场景）
└── artifacts/            # 验收报告输出目录
```

## 领域模型与校验

- 旧版扁平字段：`tenant_id`、`concurrency_limit`、`priority_min/max`、
  `preempt_enabled`、`deprecated_fields`。
- 新版分层字段：`tenant_id`、`parent_ref`（null 表示根租户继承平台配额）、
  `concurrency_limit`、权重化 `preempt_tier`（1–100）、
  `lifetime.start_ms/end_ms`，可选 `priority_min/max` 与 `extensions`。
- 声明纪元 `EPOCH_MS = 1700000000000`；生命周期必须晚于纪元且起止有序。
- 校验租户标识唯一、并发上限为正整数、优先级区间有序；
  非法记录抛出 `E_POLICY_INVALID`，`details.field_paths` 给出字段路径与记录序号。
- 规范化摘要：对排序键、紧凑分隔符的规范 JSON 计算 SHA-256；
  两种格式往返后语义等价的记录摘要完全一致。

## 紧凑二进制格式

```
魔数 8B "PMIGBIN1" | 格式版本 1B | 记录数 4B
重复记录帧：
  载荷长度 4B（大端长度前缀）
  载荷：存在位图 1B | 租户文本 | 父引用标记/文本 | 并发上限 4B
        | 抢占档位 1B | 生命周期起止 8B*2 | [优先级 4B*2] | [扩展 JSON]
  CRC32 4B（仅对载荷计算，大端）
```

存在位图位：`0x01` 父引用、`0x02` 优先级区间、`0x04` 扩展字段。
魔数损坏、帧截断、CRC32 不匹配、未知位图位、尾字节均返回
`E_RECORD_CORRUPT` 并在 `details.record_index` 指明记录序号（魔数级错误为 -1）。
JSONL 每行一条新版记录，坏行同样按行号返回 `E_RECORD_CORRUPT`。

## 兼容矩阵（显式声明于 `compatibility.py`）

| 格式 | 版本 | 结论 | 说明 |
| --- | --- | --- | --- |
| binary | 1 | ACCEPT | 当前紧凑格式 |
| jsonl | 1 | ACCEPT | 当前分片 JSON 行 |
| legacy-json | 1 | DEGRADED | 补齐默认父引用、权重档位、显式生命周期 |
| binary/jsonl | 2 | REJECT | 高版本可能含未知必填字段，`E_FORMAT_UNSUPPORTED` |

旧版降级补齐规则：`parent_ref -> null`；`preempt_enabled true -> 60 / false -> 10`；
生命周期锚定纪元并赋予 365 天默认 TTL。所有补齐字段与映射依据写入裁决明细。

## 语义迁移裁决

- 可映射废弃字段：`legacy_queue_hint`、`legacy_retry_class` 迁入 `extensions`；
  不可映射：`legacy_magic_token`、`legacy_node_affinity`（及未知废弃字段），
  记录拒绝原因并以 `E_MIGRATION_REJECTED` 整批拒绝。
- 父引用必须存在于同批或当前已持久化版本；缺失即拒绝。
- 父链迭代检测成环（`E_PARENT_CYCLE` 附环上租户）与最大深度
  （默认 4 跳，超出 `E_PARENT_DEPTH_EXCEEDED`）。
- 同租户生命周期采用半开区间 `[start, end)` 比较，批内与已生效策略均检查；
  重叠返回 `E_LIFETIME_OVERLAP` 并附双方起止毫秒。
- 所有检测在单事务提交前完成，任何拒绝均回滚，旧策略集合保持不变。

## 事务、版本与并发

- 批次先写 `intents` 意图记录并提交，再在单连接单事务内完成裁决与写入；
  任一条拒绝或异常触发 `with conn` 回滚，批次落为 `REJECTED/ABORTED`。
- 乐观快照：请求携带 `base_version`，与当前版本不符立即 `E_STALE_BASE`，
  不改动状态；提交成功产出单调递增版本（`versions` 表）。
- 单写者：进程内写锁串行化迁移；只读查询直接读取某个完整提交版本，
  WAL 模式允许读写并发，读侧不会看到半迁移中间态。
- 幂等：相同 `batch_id` + 相同内容哈希只执行一次，等待方复用结果；
  同批次号内容不一致返回 `E_CONFLICTING_BATCH`。

## 崩溃恢复

启动时扫描状态仍为 `INTENDED` 的意图，仅依据已持久化事实处理：

- 批次已 `COMMITTED`：确认提交（`CONFIRMED_COMMIT`），版本保持；
- 批次未提交：SQLite 已回滚未完成事务，将意图/批次标记 `ABORTED`
  （`ROLLED_BACK_TO_LAST_COMPLETE_VERSION`），策略集合停在最后完整版本。

恢复事件落库 `recovery_events`，可通过 `GET /api/recovery` 查询。
`service.MigrationService(..., crash_hook=...)` 提供三个确定性注入点
（`after_intent`、`after_commit_inside`、`after_commit_outside`），
以 `os._exit(137)` 模拟强制终止。

## HTTP 接口（回环）

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/migration/batches` | 提交迁移批次（JSON 信封或 `application/octet-stream` + `X-Batch-Id`/`X-Base-Version` 头） |
| GET | `/api/migration/batches/{id}` | 批次状态 |
| GET | `/api/migration/batches/{id}?detail=decisions` | 裁决明细 |
| GET | `/api/migration/batches/{id}/diff` | 执行计划差分 |
| GET | `/api/parent-chain/{tenant}?version=N` | 父链视图 |
| GET | `/api/recovery` | 恢复报告 |
| GET | `/api/versions` | 版本列表 |
| GET | `/api/health` | 健康检查 |

失败响应统一为 `{"error": {"code": "E_...", "message": ..., "details": {...}}}`。

启动服务：

```
python -m polmig.api --db policy_migration.db --host 127.0.0.1 --port 8080
```

## 固定样本

- `samples/legacy_flat.json`：可降级接受的旧版扁平策略（含可映射废弃字段）。
- `samples/boundary_deprecated_overlap.json`：含不可映射废弃字段的拒绝样本。
- `samples/boundary_invalid_records.json`：非法并发/区间/重复租户样本。
- `samples/parent_cycle.jsonl`：三租户父链成环样本。
- `samples/new_happy_path.jsonl`、`lifetime_overlap.jsonl`、
  `parent_missing.jsonl`、`parent_depth.jsonl`、`high_version.jsonl`：
  新版导入、重叠、缺失引用、超深与高版本协商样本。

## 实际验证结果

```
python -m unittest discover -s tests   # Ran 34 tests ... OK
python verify.py                       # 26 passed / 0 failed / 26 total
```

`verify.py` 产出 `artifacts/verification_report.json`，覆盖格式协商、
两种格式往返与损坏检测、迁移裁决、区间重叠、父链成环/超深/缺失引用、
批次原子回滚、乐观版本与并发单赢家、批次幂等/冲突、强制终止恢复与全部 HTTP 接口。
