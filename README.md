# 检查点分级存储与显存预算调度组件

这是一个不依赖 GPU、容器或外部服务的 Go 参考实现。它把混合精度训练检查点拆成
FP32 主参数副本与 FP16 副本，使用 VRAM/PERSISTENT 双池、策略版本、不可变检查点
版本、SQLite 单写者事务和写盘/时间双重预算来提供可验证的本地组件。

## 交付内容

| 路径 | 说明 |
| --- | --- |
| `cmd/server` | 本地 HTTP 服务，监听地址、数据库和样本目录均由启动参数指定 |
| `cmd/verify` | 跨平台端到端验证器，真实启动服务、跑完场景、强杀并重启 |
| `internal/domain` | 分片、策略、增量、错误码、校验和及恢复报告类型 |
| `internal/mixed` | 定点整数、IEEE binary16 舍入、还原摘要和精度偏差计算 |
| `internal/sample` | 固定种子确定性样本与策略/增量文件生成器 |
| `internal/engine` | 驻留/驱逐、双重预算、SQLite 事务、查询与恢复 |
| `internal/httpapi` | HTTP 路由和统一错误响应 |
| `docs/DESIGN.md` | 数据模型、事务边界、调度和恢复设计 |
| `docs/VALIDATION.md` | 本次实际执行命令与结果 |
| `scripts/verify.sh` / `scripts/verify.ps1` | Linux/macOS 与 Windows 验证入口 |

## 环境与启动

要求 Go 1.22 或以上。唯一直接依赖是固定版本的 `modernc.org/sqlite v1.34.5`。
运行时不访问网络，不使用 CGO 或 GPU。

```bash
go version
go test ./... -count=1
go run ./cmd/server -addr 127.0.0.1:8080 -db ./data/checkpoints.db -sample-dir ./data/sample
```

Windows PowerShell：

```powershell
go test ./... -count=1
go build -o .\ampcheckpoint-server.exe .\cmd\server
.\ampcheckpoint-server.exe -addr 127.0.0.1:8080 -db .\data\checkpoints.db -sample-dir .\data\sample
```

启动成功会输出一行 JSON，例如：

```json
{"addr":"127.0.0.1:8080","checkpoint_version":0,"db":"...","event":"ready","policy_version":0,"recovered_intents":0,"sample_dir":"...","seed":20261125}
```

若省略 `-sample-dir`，程序会在系统临时目录创建固定种子样本。固定种子默认为
`20261125`，也可用 `-seed` 覆盖。

## 快速验收

```bash
# 自动化单元/集成测试
go test ./... -count=1

# 启动子进程、完成提交/驱逐/预算/错误码场景、强杀并重启
go run ./cmd/verify

# 或使用包装脚本
./scripts/verify.sh
```

PowerShell：

```powershell
.\scripts\verify.ps1
```

验证器退出码为 0 时打印完整 JSON 摘要；失败时返回非 0 并附带服务端日志尾部。
`-keep` 可保留临时数据库、样本、服务二进制和日志以便排查。

## 两套版本号

组件刻意区分两类不可变版本：

- `policy_version`：由精度策略快照或增量片段推进。快照版本必须严格大于当前版本；
  增量携带的 `base_version` 必须等于当前版本。分别对应 `E_STALE_SNAPSHOT` 和
  `E_STALE_BASE`。
- `checkpoint_version`：每次成功检查点提交生成一个不可变完整版本，包含分片清单、
  双池汇总、精度偏差、派生结构和恢复基线。所有 `GET /v1/versions/{version}/...`
  查询使用该版本号；`latest` 表示最近完整提交。

策略提交本身不会生成可见检查点事实；提交检查点时把当前策略物化为新的检查点版本。

## 固定样本

样本生成器生成 `shard-00` 到 `shard-09` 共 10 个逻辑分片。每个逻辑分片始终有一个
FP32 主参数副本；策略选择 FP16 时再生成对应 FP16 副本。

- 初始策略 v1：前 8 个分片 FP16/VRAM，后 2 个分片 FP32/PERSISTENT。
- 初始 FP16 驻留总量：`19968` 字节；VRAM 上限：`20480` 字节。
- 增量片段 base v1：把 `shard-08`、`shard-09` 改为 FP16/VRAM，并把损失缩放从
  `128.0` 增长到 `256.0`。
- 紧张写预算策略 v20：写预算 `9000` 字节。
- 队列上限策略 v21：写预算 `0`、队列容量 `2`，用于覆盖 `E_WRITE_QUEUE_FULL`。
- VRAM 不足策略 v22：上限 `1024` 字节而单分片已超过上限，用于覆盖
  `E_VRAM_POOL_FULL`。

启动后 `GET /v1/sample` 返回完整样本清单，磁盘上同时生成
`policy_snapshot.json`、`delta_fragment.json`、多个测试策略、`partitions.json`
和各分片样本文件。清单包含文件 SHA-256，重新加载时会校验。

## HTTP 接口

| 方法与路径 | 用途 |
| --- | --- |
| `POST /v1/policies` | 提交完整精度策略快照；别名 `/v1/policy-snapshots` |
| `POST /v1/deltas` | 提交只含受影响分片/损失缩放字段的增量；别名 `/v1/policy-deltas` |
| `POST /v1/partitions/validate` | 原子校验一个分片批次，不写库 |
| `POST /v1/checkpoints` | 提交一轮检查点；别名 `/v1/checkpoints/commit` |
| `GET /v1/policies/current` | 当前策略 |
| `GET /v1/policies/{version}` | 指定策略版本 |
| `GET /v1/status` | 当前策略/检查点版本、恢复计数和样本位置 |
| `GET /v1/sample` | 固定样本清单 |
| `GET /v1/versions/{version}/shards` | 不可变分片清单 |
| `GET /v1/versions/{version}/pools` | 双池占用与上限 |
| `GET /v1/versions/{version}/precision` | 逐分片及逐元素偏差报告 |
| `GET /v1/versions/{version}/derived` | 派生摘要、一致性、显存估算和恢复基线 |
| `GET /v1/versions/{version}/recovery` | 只基于已提交事实的恢复报告 |
| `GET /v1/versions/{version}/writes` | 写盘、跳过、等待队列和驱逐结果 |
| `GET /v1/versions/{version}` / `.../meta` | 检查点版本元数据 |

`/v1/checkpoints/{version}/...` 是 `/v1/versions/{version}/...` 的等价别名。
`{version}` 可写 `latest`。

### 策略快照示例

```json
{
  "version": 1,
  "generated_at": "2026-01-15T00:00:01Z",
  "vram_pool_bytes": 20480,
  "write_budget_bytes": 80000,
  "round_time_budget_ms": 500,
  "write_queue_capacity": 32,
  "task_time_ms": 2,
  "loss_scale_fixed": 128000000,
  "growth_factor_fixed": 2000000,
  "overflow_threshold_fixed": 65536000000,
  "placements": [
    {"shard_id": "shard-00", "precision": "FP16", "pool": "VRAM", "last_access_ns": 1760000000000000000}
  ],
  "checksum": ""
}
```

所有定点数使用 `1e-6` 比例。`checksum` 可省略或置空，服务端会计算并返回；若提供
则必须与规范事实一致，否则返回 `E_CHECKSUM_MISMATCH`。样本文件中的快照均携带
正确校验和。

### 增量片段示例

```json
{
  "base_version": 1,
  "generated_at": "2026-01-15T00:00:02Z",
  "placements": [
    {"shard_id": "shard-08", "precision": "FP16", "pool": "VRAM", "last_access_ns": 1760000000200000000}
  ],
  "loss_scale": {"action": "grow"},
  "checksum": ""
}
```

`loss_scale.action` 支持 `grow`、`rollback`、`set`。`set` 必须同时给出
`value_fixed`；可用 `growth_factor_fixed`、`overflow_threshold_fixed` 更新后续
推进规则。越界或乘法溢出返回 `E_LOSS_SCALE_OVERFLOW` 且不改变已提交状态。

### 检查点示例

```json
{"round": 1}
```

成功响应包含 `checkpoint_version`、`policy_version`、`evictions`、`pools` 和
`writes`。`writes` 明确给出预算、已用、剩余、跳过、排队、写入和跳过原因。

验证器使用的故障注入字段为：

```json
{"round": 4, "fault_after_intent": "evict"}
```

这是用于验证强杀恢复的本地测试钩子。服务先提交 `RUNNING/EVICTING` 意图和暂存
行，再返回 HTTP 202；随后验证器以 `Process.Kill()` 强杀进程。该字段不用于正常
训练提交。

## 统一错误响应

所有业务失败都返回：

```json
{
  "error": {
    "code": "E_VRAM_POOL_FULL",
    "message": "VRAM pool cannot accommodate the requested shard after eviction",
    "context": {"limit_bytes": 1024, "current_bytes": 0, "required_bytes": 2048}
  }
}
```

覆盖的业务码：

| 业务码 | HTTP | 触发条件 |
| --- | --- | --- |
| `E_PARTITION_INVALID` | 422 | 精度标记非法、字节数非正、批次总量溢出、FP16 缺少同批主参数等 |
| `E_VRAM_POOL_FULL` | 507 | 驱逐候选耗尽后仍无法容纳新驻留分片 |
| `E_LOSS_SCALE_OVERFLOW` | 422 | 损失缩放增长超过阈值或定点乘法溢出 |
| `E_STALE_SNAPSHOT` | 409 | 快照版本不大于当前策略版本 |
| `E_STALE_BASE` | 409 | 增量基础版本不是当前策略版本 |
| `E_WRITE_QUEUE_FULL` | 429 | 写预算跳过任务数超过有界等待队列容量 |
| `E_CHECKSUM_MISMATCH` | 422 | 客户端校验和或持久化事实校验失败 |
| `E_INVALID_REQUEST` | 400 | JSON、版本号或参数不合法 |
| `E_NOT_FOUND` | 404 | 策略或检查点版本不存在 |

## 数值与调度契约

- 主参数使用 `int64` 定点微单位，避免二进制浮点累计误差。
- FP16 编码显式采用 IEEE 754 binary16、最近偶数舍入。编码前把主参数乘以损失缩放；
  还原时解码 FP16，再除以损失缩放并使用 half-away-from-zero 定点化。
- 每个 FP16 分片输出 `max_abs_deviation_fixed`、
  `max_relative_deviation_ppm`、`cumulative_abs_deviation_fixed` 和逐元素
  `master_fixed/fp16_bits/restored_fixed/deviation_fixed`。
- VRAM 内所有副本总字节不得超过策略上限；候选按最近访问时间升序、精度标记、
  优先级和 `copy_id` 确定性排序。驱逐真实反映到新版本：池变为 `PERSISTENT`，
  状态变为 `PERSISTED`。
- 写任务按优先级降序、轮次升序、分片 ID 和副本 ID 排序。下一高优先任务无法同时
  满足写盘与时间预算后，后续低优先任务全部跳过，不允许小任务插队。
- 跳过任务进入容量有限队列；队列溢出时整轮检查点不提交。

## SQLite 与恢复

- SQLite 使用 WAL、`synchronous=FULL` 和 busy timeout；写操作由进程内单写者互斥锁
  串行化，每个只读接口在独立读事务中读取一个完整不可变检查点版本。
- 所有写操作先提交意图记录。检查点意图还包含逐分片 `PENDING` 暂存行和逐驱逐
  `PENDING` 行；最终分片、派生结构、池汇总、精度报告和恢复基线在同一事务中提交，
  同时把意图、暂存行和驱逐行改为 `COMMITTED`。
- 启动时扫描仍为 `RUNNING` 的意图：操作改为 `ABORTED/RECOVERED`，未提交分片改回
  `PENDING`，驱逐改为 `ABORTED`，并写入恢复日志。恢复报告不把中止意图计入池占用
  或恢复基线。

更细的事务顺序和表结构见 `docs/DESIGN.md`。

## 验证结果

本次实际验证覆盖：

- 初始 8 个 FP16 副本全量驻留，VRAM `19968/20480` 字节。
- 增量调整触发 3 次 LRU 驱逐，最终 VRAM `19712/20480` 字节，3 个副本均为
  `PERSISTENT/PERSISTED`。
- 写预算 `9000` 字节下写入 `8320` 字节、剩余 `680` 字节、跳过并排队 17 项。
- 六类指定业务错误均通过真实 HTTP 响应核对。
- 9 次损失缩放成功增长后，第 10 次以 `E_LOSS_SCALE_OVERFLOW` 拒绝且未污染已提交版本。
- 强杀后重启恢复 1 个 `EVICTING` 意图：20 个暂存分片回到 `PENDING`、3 个驱逐中止；
  最后完整检查点仍是 v3，VRAM/PERSISTENT 总计仍为 `19584/59136` 字节。

完整结果及原始输出见 `docs/VALIDATION.md` 和
`artifacts/verification-output.json`。
