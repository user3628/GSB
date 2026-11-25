# quantops：本地低精度量化算子、四阶段检查点与误差验证

这是一个纯 Go、无容器、无外部服务的本地量化工具链。它从固定种子生成权重/激活分片与算子清单，执行 4/8 位、对称/非对称、逐张量/逐通道的量化线性算子链，并用 SQLite 事务保存“标定 → 前向 → 反量化 → 验证”四阶段检查点。强制终止后，重启仅依据已提交阶段继续，最终摘要与一次连续运行逐字节一致。

## 快速开始

要求 Go 1.22 或更高版本；运行时不访问网络，只使用本地 JSON 样本和 SQLite 文件。

```bash
go build -o quantops ./cmd/quantops
./quantops generate --dir data
./quantops run --data data --db quantops.db --summary summary.json --workers 2
```

成功时退出码为 `0`，`summary.json` 包含逐分片输出摘要、阶段摘要、误差诊断、钳制记录、预算调度结果和总摘要。

Windows PowerShell：

```powershell
go build -o quantops.exe .\cmd\quantops
.\quantops.exe generate --dir data
.\quantops.exe run --data data --db quantops.db --summary summary.json --workers 2
```

### 重复运行与逐字节比对

同一配置下，对已经完成的数据库重跑不会重新发布阶段；在全新数据库上重跑也会得到相同摘要字节。

```bash
./quantops run --data data --db quantops.db --summary summary-rerun.json --workers 2
./quantops compare --left summary.json --right summary-rerun.json
```

`compare` 输出两个文件的 SHA-256、大小和 `byte_identical`，不相同时退出码为 `1`。注意：`--workers`、预算等调度参数会写入摘要，比较时应使用相同参数。

### 强制终止与恢复

用 `--crash-after-commits N` 可以在 N 个阶段事务提交后对本进程发送 `SIGKILL`（Windows 上为强制退出），用于本地验收；生产进程也可以直接被外部终止。

```bash
set +e
./quantops run --data data --db crash.db --summary crash-summary.json --workers 1 --crash-after-commits 9
echo $?                         # 137；不保证生成摘要
ls crash-summary.json           # 不存在
set -e
./quantops recover --data data --db crash.db --summary crash-summary.json --workers 1
```

将 `crash-summary.json` 与使用相同参数的一次连续运行摘要比较：

```bash
./quantops compare --left continuous-summary.json --right crash-summary.json
```

## 数据覆盖

`generate` 使用固定种子 `20261125`（可覆盖）生成 6 个分片：

| 分片 | 覆盖点 |
|---|---|
| `s01-sym8-perchannel-outlier` | 8 位、对称、逐通道、激活/权重离群值 |
| `s02-asym8-pertensor-zerochannel` | 8 位、非对称、逐张量、全零激活行与全零权重通道 |
| `s03-sym4-perchannel-outlier` | 4 位、对称、逐通道、强离群值与显式零 |
| `s04-asym4-pertensor-nan` | 4 位、非对称、逐张量、JSON 侧带 NaN 位置 |
| `s05-chain-mixed-8-4` | 两层混合链：8 位对称 → 4 位非对称，逐通道 |
| `s06-chain-asym4-perchannel` | 两层 4 位非对称逐通道链、零权重通道 |

JSON 本身不支持 NaN，因此 `tensors/*.json` 使用 `nan_positions` 记录异常位置，值字段放零占位。加载后恢复为 NaN；算子核心采用显式策略：NaN/Inf 确定性替换为零，并在标定产物和诊断报告中保留位置与数量。

## 核心接口

```go
cfg := quant.Config{
    Bits: 8, Mode: quant.Symmetric, Granularity: quant.PerChannel,
}
q, err := quant.Quantize(tensor, cfg)          // 自动计算 scale/zero-point
deq, err := quant.Dequantize(q)

// 使用外部给定 scale/zero-point 时，越界值会被钳制并逐位置记录。
q2, err := quant.QuantizeWithParams(tensor, cfg, []quant.Params{{Scale: 0.005, ZeroPoint: 128}})

// 整数域 matmul 会做 zero-point 修正，反量化使用完整的 scale 链。
linear, err := quant.Linear(activation, weight, cfgA, cfgW)
out, err := quant.DequantizeLinear(linear)
```

执行器：

```go
ds, _ := dataset.Load("data/manifest.json")
st, _ := store.Open("quantops.db")
r, _ := engine.New(ds, st)
res, err := r.Run(ctx, engine.RunOptions{
    Limits: scheduler.DefaultLimits(),
})
```

`Run` 对致命错误直接返回；`E_NUMERIC_MISMATCH` 与 `E_BUDGET_EXCEEDED` 作为业务错误聚合返回，同时仍返回完整 `Result` 和摘要。调用方可先保存 `Result.SummaryBytes`，再按错误码决定退出行为。

`engine.ClassifyCheckpoint` 会对单个分片返回 `normal`、`replacement` 或 `rollback` 分类；`replacement` 会从最早受影响阶段删除并重建下游，`rollback` 返回 `E_STALE_CHECKPOINT`。

## CLI 命令

| 命令 | 作用 |
|---|---|
| `generate` | 固定种子生成 `manifest.json` 与 tensor JSON |
| `run` / `recover` | 分批调度并执行；`recover` 与 `run` 使用同一恢复路径 |
| `schedule` | 只输出确定性接纳顺序、工作进程队列、元素/内存预估和拒绝原因 |
| `status` | 查询分片进度、阶段序号、摘要、失效历史 |
| `fingerprint` | 查询 manifest 内容指纹、阶段配置版本和版本指纹 |
| `checkpoint` | 区分正常推进、整体替换与回退，并传播失效 |
| `diagnose` | 从已提交验证阶段输出逐层误差报告 |
| `compare` | 对两个摘要文件做逐字节一致性与 SHA-256 比对 |
| `quantize` | 对单个 tensor JSON 调用量化/反量化核心，可强制固定 scale 以产生钳制事件 |

预算示例（刻意让所有分片超预算）：

```bash
./quantops schedule --data data --total-elements 100 --per-worker-elements 100 --workers 2
./quantops run --data data --db limited.db --summary limited.json \
  --total-elements 100 --per-worker-elements 100 --workers 2
echo $?   # 5，E_BUDGET_EXCEEDED
```

在 `limited.json` 的 `untreated` 数组中，每个未处理分片都带有 `estimated_elements`、`working_set_bytes`、`budget_remaining`、`memory_remaining` 和原因。

固定 scale 钳制示例：

```bash
./quantops quantize --file data/tensors/s01-sym8-perchannel-outlier.activation.json \
  --bits 8 --mode symmetric --granularity per_tensor --scale 0.05
```

输出包含 `clamp_count` 与每个钳制位置、原值、未钳制整数、上下界。

## 状态机与持久化

阶段固定为：

1. `calibration`：计算激活首层与所有权重的 quant params；
2. `forward`：量化域 matmul，保存整数累加器与 scale 链；
3. `dequantization`：按 scale 链还原逐层浮点输出；
4. `verification`：按显式抽样位置与补偿求和参考实现对账。

每个阶段提交时，在同一个 `BEGIN IMMEDIATE` 事务中：

- 追加一条 `stage_states`（阶段名、分片 ID、分片序号、阶段序号、全局单调序号、输入内容指纹、配置版本、阶段摘要、payload）；
- 用条件更新推进 `shard_progress.next_stage_seq` 与版本指纹；
- 对 `(shard_id, stage)` 和 `(shard_id, stage_seq)` 建唯一约束；
- 由 `sequence_counter` 分配严格递增的全局序号。

发布和协调同时持有 `<db>.lock` 的 OS 文件锁。Linux/macOS 使用 `flock`，Windows 使用 `LockFileEx`，进程被强杀时锁由内核释放。数据库使用 WAL、`synchronous=FULL` 和 10 秒 busy timeout。

数据库旁还有 `<db>.guard` 原子高水位文件，保存随机数据库标识、全局序号和逐分片最后提交序号。它可以识别“数据库文件被旧快照整体替换”和“进度表与状态表一起回退”这类仅靠库内表无法发现的情况；强杀发生在 SQLite 提交后、guard 更新前时，下次启动会把领先的数据库视为已提交事实并修复 guard。若确实要丢弃检查点，应同时删除 `.db` 与 `.guard`。

稳定业务码：

| 码 | 含义 |
|---|---|
| `E_INVALID_INPUT` | 样本/配置/形状错误 |
| `E_INVALID_TRANSITION` | 非法阶段跳转 |
| `E_MISSING_ARTIFACT` | 缺少前置阶段产物 |
| `E_STALE_CHECKPOINT` | 落后、重复、跨分片错位、序号回退或摘要不匹配 |
| `E_BUDGET_EXCEEDED` | 分片被确定性预算调度拒绝 |
| `E_NUMERIC_MISMATCH` | 采样误差超过容差，保留完整诊断上下文 |

## 版本指纹与失效传播

内容指纹不包含路径和 JSON 空白，只覆盖张量形状/数值/非有限位置、量化配置、算子链、容差与抽样点。阶段配置版本分层计算：

- 张量内容、位宽、模式、粒度或权重配置变化 → 最早从 `calibration` 失效；
- 算子链名称/结构或输出形状变化 → 最早从 `forward` 失效；
- 反量化实现版本变化 → 从 `dequantization` 失效；
- 容差或抽样位置变化 → 仅从 `verification` 失效。

`reconcile` 还会校验阶段 payload 摘要、输入指纹链、分片内严格递增的全局序号、进度版本和全局序号计数器。整体替换/篡改会从最早异常阶段删除并重建；序号或全局面退时拒绝并返回 `E_STALE_CHECKPOINT`，不会静默覆盖。

## 误差诊断

验证阶段使用 Neumaier 补偿求和的高精度 float64 参考链，只比较清单中显式声明的 `(layer,row,col)` 位置，并输出：

- 每层最大绝对/相对误差、最坏抽样位置；
- 每层误差贡献与基于参考权重 L1 范数的累计传播界估计；
- 全报告相对误差上界估计；
- 距容差最近的层及其余量；
- NaN/Inf 清洗位置和数量。

未通过的阶段仍会提交（保存诊断证据），随后 `run` 以退出码 `3` 返回 `E_NUMERIC_MISMATCH`；错误对象和摘要都包含分片、层名和完整报告。

## 确定性预算调度

调度在任何执行前完成，只依赖清单形状和限制：

- 先按分片序号排序；
- 对每个分片按形状估算元素量（标定、参考、整数 matmul、输出）和峰值工作集；
- 在总元素预算、单 worker 元素配额和总峰值内存约束下，选择当前累计负载最小的 worker（平局取较小索引）；
- 拒绝项记录预估、剩余预算和原因；决策与 goroutine/进程完成时序无关。

元素预算是保守的“标定 + 量化前向 + 参考 + 反量化”工作量；工作集按每层 activations、weights、accumulator 和输出的四份 float64 缓冲区估算。

## 验证结果

在当前工作区执行：

```bash
go test ./... -count=1
GOOS=windows GOARCH=amd64 go build ./...
```

实际结果：`go test ./... -count=1`、`go test -race ./pkg/store ./pkg/engine -count=1` 和 Windows 交叉编译均通过。连续运行、同库重跑、SIGKILL 后恢复三份摘要都是 24049 字节，SHA-256 同为：

```text
sha256:1efe635be54a30444813eaedc4f64c216f9ed1047946abe13a86ceb1ddde7cb7
```

完整证据见 `comparison-report.json` 与 `artifacts/verification-report.json`。测试包括：

- 量化/反量化、零点修正、逐通道全零通道、离群值、显式零、NaN 清洗和固定 scale 钳制审计；
- SQLite 阶段顺序、重复/越序/跨分片错误、并发发布单赢家、摘要篡改重建、序号回退拒绝；
- 多层链 + 容差/张量/算子链变更的精确阶段失效；
- 分片预算/worker 配额/峰值内存拒绝；
- 真实子进程 `SIGKILL` 后恢复摘要与连续运行逐字节一致；
- 两个独立进程对同一数据库发布不相交分片后，结果与连续运行逐字节一致。

本工作区根目录下：

- `data/`：固定种子生成的实际样例；
- `quantops.db`：实际检查点数据库（可删除后重建）；
- `summary-continuous.json`、`summary-rerun.json`、`summary-recovered.json`：连续、重跑、强杀恢复摘要；
- `comparison-report.json`：摘要 SHA-256、强杀退出码与逐字节一致结论；
- `artifacts/verification-report.json`：完整验证证据，包括预算拒绝、钳制事件和 `E_NUMERIC_MISMATCH` 诊断。

## 目录

```text
cmd/quantops/        CLI
pkg/quant/           量化、反量化、量化域线性算子与参考实现
pkg/dataset/         数据模型、固定种子生成器与加载校验
pkg/scheduler/       确定性预算调度
pkg/store/           SQLite 检查点、文件锁、协调与失效
pkg/engine/          四阶段执行、诊断、摘要、恢复与检查点分类
pkg/qerr/            稳定业务码
pkg/canon/           确定性 JSON/哈希/原子写
internal/filelock/   POSIX/Windows 真实文件锁
internal/force/      强杀测试辅助
```
