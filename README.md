# 单机混合精度分级检查点引擎

这是一个面向单机训练编排器的确定性检查点存储与显存预算调度服务。进程内使用单写者、多读者模型；状态、意图日志、池视图、写盘记录和派生结构全部持久化到本地 SQLite。服务不依赖 GPU、外部数据集或容器。

## 交付内容

- `cmd/checkpoint-engine`：本地 HTTP 服务。
- `internal/engine`：提交、精度校验、驻留/驱逐、预算调度、派生重建和恢复。
- `internal/store`：SQLite 模式（WAL、`synchronous=FULL`）与迁移。
- `internal/httpapi`：HTTP 路由、统一业务错误码。
- `samples/`：确定性策略快照、轮次增量片段和漂移样本。
- `scripts/`：Windows PowerShell 与 Bash 启动、测试、冒烟脚本。
- `internal/engine/*_test.go`、`internal/httpapi/api_test.go`：主线、错误码、并发、恢复和真实子进程强制终止测试。

## 环境与快速开始

要求 Go 1.22 或更高版本。SQLite 驱动固定为 `modernc.org/sqlite v1.34.5`，不需要 CGO。

Windows PowerShell：

```powershell
go version
go mod download
go test ./... -count=1
.\scripts\test.ps1
.\scripts\run.ps1
```

另开一个 PowerShell 窗口执行冒烟测试：

```powershell
.\scripts\smoke.ps1 -BaseUrl http://127.0.0.1:18080
```

Linux/macOS：

```bash
go mod download
go test ./... -count=1
go run ./cmd/checkpoint-engine -addr 127.0.0.1:18080 -db data/checkpoint-engine.db -data-dir data
# 另一个终端：
./scripts/smoke.sh http://127.0.0.1:18080
```

默认数据库为 `data/checkpoint-engine.db`，持久化写入清单位于 `data/blobs/`。两者都在本地工作目录。

重新生成样本：

```bash
go run ./cmd/gen-samples samples
```

## 核心设计

### 单写者与事务可见性

- 检查点提交、快照提交、驱逐、精度校验和派生重建由一个进程内写锁串行化。
- 查询在 SQLite 读事务中完成；WAL 允许查询与写入并发，并且单个响应中的状态、分片和池视图来自同一提交版本。
- 每次状态写入分为两个阶段：先独立提交意图记录，再在下一个事务中提交分片/池/写盘记录/派生结构，并在同一事务内把意图标记为 `APPLIED`。读者不会看到“分片已写完成但写盘记录未提交”或“已驱逐但池占用未释放”的中间态。
- 同一 `submission_id` 的并发提交会等待写锁并在完成后复用已存结果；请求规范摘要不同则返回 `E_CONFLICTING_SUBMIT`。

### 池模型与驱逐

每个分片记录以下事实：

- 轮次、分片标识、参数标识、`FP16`/`FP32` 精度标记、非负字节数。
- 内容摘要、动量摘要、可选的 `restored_from_digest`。
- 当前池 `VRAM`/`PERSISTED`、期望池、写状态 `PENDING`/`WRITTEN`。
- 最近访问时刻、优先级、写盘时间估算、固定标记和显存占用估算。

VRAM 占用按分片逻辑字节精确求和，不使用压缩或估算。新分片驻留时，本批输入分片受到保护，不会被同批立即驱逐。候选顺序固定为：

1. `FP16` 先于 `FP32`；
2. 最近访问时刻更早者优先；
3. 优先级更低者优先；
4. 轮次、分片标识字典序作为最终稳定键。

驱逐在同一事务中把分片迁移到 `PERSISTED`；池占用表随后从分片事实重新计算。无法腾出空间时返回：

```json
{"error":{"code":"E_POOL_EXHAUSTED","details":{"required_bytes":1024,"reclaimable_bytes":0}}}
```

### 混合精度数值契约

数值契约不使用二进制浮点累加：

- `FP16` 分片使用 `fp16_bits` 数组，元素为 IEEE-754 binary16 的 16 位无符号整数位型；舍入声明固定为 `IEEE754_BINARY16_RNE`。
- `FP32` 主参数使用 `fp32_nanos` 数组，元素为整数纳单位（`1e-9`）。
- 损失缩放系数是正十进制数，解析为精确有理数。
- 还原过程：解码 half 位型为精确有理数，除以损失缩放，再用显式“就近舍入、并列取偶”舍入到整数纳单位。逐元素输出还原值、带符号偏差、绝对偏差和相对 ppm。
- 每个分片输出 `max_abs_deviation_nanos`、`max_relative_ppm`、`sum_abs_deviation_nanos`、`cumulative_rounding_nanos`；报告累计绝对偏差和累计舍入误差。
- 偏差超过 `precision_abs_limit_nanos` 或 `precision_rel_limit_ppm` 时返回 `E_PRECISION_DRIFT`，并附带完整逐元素报告。结果摘要只依赖整数结果和摘要，同一输入重复校验逐位一致。

每个 `FP32` 主参数必须通过 `parameter_id` 关联同轮次、同参数的 `FP16` 分片，并让 `restored_from_digest` 等于该 `FP16` 分片的内容摘要。若只有一个候选，服务可自动补全该引用。

若提供确定性数值载荷，内容摘要可省略并由服务计算；摘要算法为：

- `payload_hex`：原始字节的 SHA-256。
- `fp16_bits`：`SHA-256("FP16_BITS_V1" || NUL || 十进制位型 CSV || NUL)`。
- `fp32_nanos`：`SHA-256("FP32_NANOS_V1" || NUL || 十进制纳单位 CSV || NUL)`。

若同时提供摘要和载荷但摘要不符，返回 `E_SHARD_INVALID`。没有数值载荷时必须提供摘要，此时只能做摘要关系校验，报告中的 `check_mode` 为 `DIGEST_ONLY`。

### 写盘预算与等待队列

- 字节预算和时间预算按轮次累计；时间成本使用显式 `write_ms`，缺省为 `1 + bytes/(1 MiB)` 毫秒。
- 调度顺序先按策略声明的 `FP32_FIRST`/`FP16_FIRST` 精度类，再按优先级降序、轮次升序、分片标识升序。响应中的 `write_order_basis` 记录该依据。
- 预算内提交的写任务在事务中同时更新 `WRITTEN` 和写盘记录；未提交的任务保持 `PENDING`，进入有界等待队列，并携带原因 `BYTE_BUDGET_EXHAUSTED`、`TIME_BUDGET_EXHAUSTED` 或 `BYTE_AND_TIME_BUDGET_EXHAUSTED`。
- 等待数超过 `write_queue_capacity` 时整批返回 `E_WRITE_QUEUE_FULL`，不落库。
- `POST /api/v1/schedule` 可在不提交新分片的情况下尝试继续排空队列。

### 版本与幂等

- 策略快照版本必须严格大于当前已提交策略版本，否则返回 `E_STALE_SNAPSHOT`。
- `engine_state.revision` 是当前完整提交版本。每次策略快照、检查点提交或实际执行写盘调度都会推进它。
- 增量提交的 `base_version` 必须等于当前 `engine_state.revision`，否则返回 `E_STALE_BASE` 且不改变状态。两个并发增量即使使用同一基础版本，也只有一个成功。
- 同一 `submission_id` 和相同规范内容只执行一次；重放返回原结果并标记 `idempotent_replay=true`。内容不同返回 `E_CONFLICTING_SUBMIT`。

### 派生结构与恢复

每个分片维护主参数摘要引用、参数一致性结果、显存占用估算和恢复基线；每个轮次维护带来源指纹的一致性结论。精度标记、主参数摘要、动量摘要、损失缩放或校验上界变化时，在提交事务内按当前事实重建精度结果、分片派生结构和轮次结论，因此不会出现“新精度标记仍引用旧摘要”的可见中间态。

所有写操作先落意图，再提交状态。持久写入先原子写入 `data/blobs/` 下的清单并 `fsync`。恢复流程：

1. 扫描 `PENDING` 意图；写盘意图回滚为 `PENDING`，驱逐意图恢复驻留位置。
2. 校验每条写盘记录对应的清单是否存在且摘要/轮次/分片匹配；缺失或损坏的记录被撤销，分片回退为 `PENDING`。
3. 删除没有写盘记录引用的孤儿清单。
4. 从分片事实重建池占用、分片派生结构和轮次一致性。
5. 只基于 SQLite 已提交事务和已验证清单生成恢复报告，明确列出中断意图；报告始终带有 `in_flight_writes_reported_complete=false`，不对在途写入声称完成。

可通过环境变量进行真实进程终止验证：

```powershell
$env:CHECKPOINT_ENGINE_FAULT = "after_intents"
go run ./cmd/checkpoint-engine
```

当引擎执行到该故障点时，进程以退出码 86 强制终止，数据库保留未完成意图。删除该环境变量并重启后，启动恢复会自动回滚并生成报告。

## HTTP API

所有错误统一为：

```json
{"error":{"code":"E_SHARD_INVALID","message":"...","details":{}}}
```

主要业务码：

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_SHARD_INVALID` | 422 | 分片字段、精度、唯一性、关系或载荷摘要无效 |
| `E_SHARD_MISMATCH` | 409 | 同一轮次同一分片标识提交了不同内容摘要 |
| `E_POOL_EXHAUSTED` | 507 | 驱逐后仍无法满足 VRAM 上限 |
| `E_PRECISION_DRIFT` | 422 | 绝对/相对偏差超过策略上界 |
| `E_STALE_SNAPSHOT` | 409 | 策略版本不大于当前版本 |
| `E_STALE_BASE` | 409 | 增量基础版本不是当前完整提交版本 |
| `E_WRITE_QUEUE_FULL` | 507 | 有界写等待队列已满 |
| `E_CONFLICTING_SUBMIT` | 409 | 同一提交标识复用了不同内容 |
| `E_SNAPSHOT_INVALID` | 400 | 快照字段或校验摘要无效 |
| `E_NO_POLICY` | 412 | 尚未提交策略快照 |
| `E_NOT_FOUND` | 404 | 资源不存在 |

### 路由

| 方法与路径 | 用途 |
|---|---|
| `GET /healthz` | 健康检查 |
| `POST /api/v1/snapshots` | 提交带版本策略快照 |
| `GET /api/v1/snapshots` | 列出策略版本 |
| `GET /api/v1/policy` | 当前策略与完整提交版本 |
| `POST /api/v1/checkpoints`、`POST /api/v1/increments` | 提交增量检查点 |
| `GET /api/v1/checkpoints/{id}`、`GET /api/v1/submissions/{id}` | 查询提交结果 |
| `GET /api/v1/state` | 状态、当前策略和池占用 |
| `GET /api/v1/shards?round=&pool=` | 分片清单 |
| `GET /api/v1/pools` | 池占用 |
| `GET /api/v1/rounds?round=` | 轮次一致性结论 |
| `GET /api/v1/derivatives?round=` | 分片派生结构 |
| `POST /api/v1/precision/verify` | 重新执行精度校验并持久化结果 |
| `GET /api/v1/precision?round=` | 查询精度结果 |
| `POST /api/v1/schedule` | 在现有预算内尝试继续写队列 |
| `GET /api/v1/write-queue` | 查询 `PENDING` 等待项 |
| `GET /api/v1/write-records?submission_id=` | 查询写盘记录 |
| `GET /api/v1/intents?pending=1` | 查询意图日志 |
| `GET /api/v1/recovery-report` | 最近一次启动恢复报告 |
| `POST /api/v1/recovery/scan` | 立即再执行一次恢复扫描 |

### 策略快照

```json
{
  "version": 1,
  "generated_at": "2026-11-24T00:00:00Z",
  "vram_limit_bytes": 2600,
  "write_budget_bytes_per_round": 3200,
  "time_budget_ms_per_round": 10,
  "write_queue_capacity": 8,
  "loss_scale_initial": "1024",
  "loss_scale_growth": "2",
  "precision_abs_limit_nanos": 2000000,
  "precision_rel_limit_ppm": 50000,
  "write_preference": "FP32_FIRST",
  "residency": {"vram": [], "persisted": []},
  "checksum": "可选：服务端规范化 JSON 的 SHA-256"
}
```

`residency.vram`、`residency.persisted`（也可使用顶层 `vram_residency`、`persisted_residency`）用于声明指定分片的驻留池。若提供 `checksum`，必须与服务端规范化摘要一致；省略时服务端计算并保存。

### 增量检查点

```json
{
  "submission_id": "sub-round-1",
  "base_version": 1,
  "round": 1,
  "loss_scale": "1024",
  "loss_scale_growth": "2",
  "shards": [
    {
      "shard_id": "r1.attn.fp16",
      "parameter_id": "attn.weight",
      "precision": "FP16",
      "bytes": 1024,
      "digest": "可由 payload 自动计算",
      "last_access_ms": 1000,
      "priority": 10,
      "write_ms": 1,
      "quantization": "IEEE754_BINARY16_RNE",
      "fp16_bits": [24576, 25856]
    },
    {
      "shard_id": "r1.attn.fp32",
      "parameter_id": "attn.weight",
      "precision": "FP32",
      "bytes": 2048,
      "restored_from_digest": "<对应 FP16 digest>",
      "momentum_digest": "<动量摘要>",
      "fp32_nanos": [500000000, 1250000000],
      "priority": 100,
      "write_ms": 2
    }
  ]
}
```

`fragments` 是 `shards` 的别名。每项可携带自己的 `round`；未携带时使用外层 `round`。`target_pool` 可取 `VRAM`/`PERSISTED`；未声明时由当前策略驻留清单或精度默认值决定（FP16 默认 VRAM，FP32 默认 PERSISTED）。

## 确定性样本

`samples/manifest.json` 记录了样本摘要。主流程（新数据库、按顺序提交）预期如下：

| 步骤 | 结果 |
|---|---|
| `policy_v1.json` | 完整提交版本 1 |
| `round1_increment.json` | 版本 2；写 2 个，延后 2 个；VRAM 1792 字节 |
| `round2_increment.json` | 版本 3；真实驱逐 `r1.attn.fp16`、`r1.mlp.fp16`；写 2 个，延后 4 个；VRAM 2000、PERSISTED 9376 |
| `round3_drift_increment.json` | HTTP 422 / `E_PRECISION_DRIFT`；最大绝对偏差 10,000,000 纳单位超过上限 2,000,000；版本仍为 3，轮次 3 不落库 |
| `round4_complete_increment.json` | 版本 4；两个分片均写入，轮次 4 一致性为 `PASS` |
| 重放 `round1_increment.json` | HTTP 200，`idempotent_replay=true` |
| `policy_v2.json` | 版本 5；VRAM 上限降为 1800，继续驱逐后 VRAM 为 1200 |

## 实际验证记录

本仓库交付前的实际验证命令与结果：

```text
$ go test ./... -count=1
ok  checkpointengine/internal/engine
ok  checkpointengine/internal/httpapi

$ go test -race ./internal/engine ./internal/httpapi -count=1
ok  checkpointengine/internal/engine
ok  checkpointengine/internal/httpapi

$ go vet ./...
（无输出）
```

测试覆盖：

- 样本主线：分级写盘、LRU/精度优先驱逐、池占用精确对账、完整轮次 `PASS`。
- 业务码：`E_SHARD_INVALID`、`E_SHARD_MISMATCH`、`E_POOL_EXHAUSTED`、`E_PRECISION_DRIFT`、`E_STALE_SNAPSHOT`、`E_STALE_BASE`、`E_WRITE_QUEUE_FULL`、`E_CONFLICTING_SUBMIT`。
- 精度：逐元素整数偏差、累计舍入误差、重复校验逐位一致。
- 并发：同基础版本两个增量只成功一个；同提交标识并发只执行一次；读者在写者运行期间读到完整池视图。
- 恢复：无 blob 的写记录被撤销；在途写回滚 `PENDING`；部分驱逐恢复 VRAM；孤儿 blob 清理；池与轮次结论重建。
- 真实强制终止：子进程在 `after_intents` 处 `os.Exit(86)`，重启后轮次 2 完全不可见，VRAM 恢复到 1792 字节。

## 设计边界

- `bytes` 是预算和显存核算的逻辑字节数；测试载荷是对应分片的确定性整数/位型清单，不分配等量物理显存或生成数 GB 文件。
- 单写者由单进程写锁保证；同一数据库文件不应同时被多个引擎进程写入。
- 等待队列容量是当前所有轮次 `PENDING` 写项的总容量。
- 恢复报告来自最近一次启动扫描；后续运行可通过 `POST /api/v1/recovery/scan` 再次生成。
