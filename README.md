# FlashAttention 风格在线 Softmax 分块注意力

本仓库是一个无 GPU、无外部服务、无容器依赖的确定性可运行版本。首次运行会用固定 PCG 种子在 `data/` 生成本地 Q/K/V、形状/分块配置和定点量化参考真值；SQLite 状态库位于 `data/flashattn.db`，JSON 审计与演示报告位于 `reports/`。

## 环境

- Go 1.22+（已用 `go version go1.27.4 linux/amd64` 验证；`go.mod` 指令为 `go 1.22`）。
- 可选：SQLite CLI 3.46+ 查看 `data/flashattn.db`。
- 驱动：纯 Go `modernc.org/sqlite v1.34.4`，不需要 CGO、SQLite 动态库或 Docker。
- 文件锁：`golang.org/x/sys v0.22.0`，Unix 使用 `flock`，Windows 使用 `LockFileEx`。

## 快速验收

```bash
go test ./...
go run ./cmd/flashattn demo
```

`demo` 会覆盖：

- `short`：B=2/H=2/S=16/D=8；
- `long`：B=1/H=2/S=512/D=32；
- `odd_dim`：S=37、D=13、行块 8、列块 7 的非整除场景；
- `causal`：因果掩码并使用 log-sum-exp 策略；
- `extreme`：极端 logit、float 指数下溢钳制和 8 位指数和位宽诊断 `E_NUMERIC`；
- 前缀纠错、K/V 追加、中断恢复、内存收缩和 SQLite 并发序号测试。

机器可读总报告：`reports/demo.json`。每个样本另有 `reports/<shard>-run.json`，增量报告为 `reports/odd_dim-incremental.json` 和 `reports/append_demo-incremental.json`。

## CLI

```bash
# 生成全部或单个样本
go run ./cmd/flashattn generate --sample all --force

# 执行并审计；默认从 SQLite 恢复
go run ./cmd/flashattn run --shard long --budget-mib 64

# 确定性模拟在 4 个事务检查点后优雅停止，然后恢复
go run ./cmd/flashattn run --shard recovery_long --stop-after 4
go run ./cmd/flashattn recover --shard recovery_long

# 修正受影响键区间 [10,14)，只重算对应列块
go run ./cmd/flashattn incremental --shard odd_dim \
  --mode correct --start 10 --end 14 --seed 777

# 追加 K/V 前缀
go run ./cmd/flashattn incremental --shard append_demo \
  --mode append --start 12 --end 16 --seed 888

# 审计、状态和版本/失效范围
go run ./cmd/flashattn audit --shard extreme
go run ./cmd/flashattn status --shard odd_dim
go run ./cmd/flashattn versions --shard odd_dim

# 导入外部分片 JSON
go run ./cmd/flashattn import --file /path/to/shard.json --id my_shard
```

真实终端中可发送 `SIGINT`/`SIGTERM`；内核会在当前检查点事务落盘后优雅停止，不会把线程超时描述成失控终止。

## 库接口

根包 `flashattn` 暴露 `Engine`、`RunOptions`、`UpdateRequest`、报告类型和稳定错误码：

```go
engine, _ := flashattn.NewEngine("data")
defer engine.Close()
report, err := engine.Run(ctx, "short", flashattn.RunOptions{})
inc, err := engine.Incremental(ctx, flashattn.UpdateRequest{
    ShardID: "odd_dim", Mode: "correct", Start: 10, End: 14,
}, 64<<20)
```

也可直接调用 `flashattn.Attention(ctx, q, k, v, cfg)` 使用分块内核。

## 实现要点

- `internal/attn`：按 `BH → 行块 → 列块` 计算；只实例化 `行块×列块` logits 和 `行块×D` 的 PV 缓冲，不物化 `S×S` 矩阵。每个行维护运行最大值 `M`、指数和 `Sum/LSE` 与缩放域累加器。
- `internal/reference` + `internal/quant`：float64 朴素参考仅用于生成真值，随后对称定点量化并保存 codes/step；审计从 codes 反量化，不依赖运行时 float 参考。
- `internal/audit`：逐行块全量核查并保留 32 个显式抽样位置；报告最大绝对/相对偏差、峰值相对误差、观测/峰值/保证有效位数和误差界公式。
- `internal/service/incremental.go`：每个列块保存局部 `M/Sum/PV`；更新时只重算受影响列块，其余从 SQLite 读取并按原顺序合并。输出、逐行 `M` 和归一化因子按 float 位逐位比较。
- `internal/store`：纯 Go SQLite；检查点含分片、BH、行块、覆盖范围、单调序号、M/Sum/LSE、累加 blob、输入/版本指纹和状态摘要。部分唯一索引保证同一分片/BH/行块/键范围只有一条 active 记录。
- 发布保护：OS 文件锁 + 事务 + `row_heads` 条件更新；只允许 `current_seq+1`。回退、重复、缺口、未登记指纹变化和摘要不匹配均返回 `E_STALE_CHECKPOINT`，字段带 `current_seq` 与 `kind`。
- 恢复：扫描并忽略不完整 blob、摘要不符和重复序号记录；失效区间之前的检查点继续复用。
- `internal/versioning`：Q/K/V、配置、量化位宽和参考摘要合成版本指纹；K/V 前缀变化映射到列块区间，因果模式下最早受影响行块为 `key_start/row_block`。
- `internal/scheduler`：估算 logits、PV、状态、快照和输出缓冲；超预算时先拆 batch，再收缩行块/列块，最小工作集仍超预算则返回带明细的 `E_MEMORY_BUDGET`。

## 稳定错误码

- `E_NUMERIC`：非有限值、极端值、指数下溢钳制或指数和超过声明位宽；事件含行/列位置。
- `E_ERROR_BOUND`：审计超容差，字段含 `shard`、`row_block`、`max_abs_err`。
- `E_INCREMENTAL_MISMATCH`：增量与从零全量计算不逐位一致，字段含首个差异位置。
- `E_STALE_CHECKPOINT`：序号回退/重复/缺口、指纹或摘要冲突，字段含当前生效序号。
- `E_MEMORY_BUDGET`：最小分块工作集超过预算，字段含逐项字节明细。

## SQLite 查看

```bash
sqlite3 data/flashattn.db '.tables'
sqlite3 data/flashattn.db \
  "select shard_id,bh,row_block,seq,col_end,active from checkpoints limit 20;"
sqlite3 data/flashattn.db \
  "select shard_id,trigger,old_fp,new_fp,first_row_block,first_col_block from invalidations;"
```

详细实测结果见 `VALIDATION.md`。

---

# 原始需求

内部要求先交付一个 FlashAttention 风格在线 Softmax 分块注意力的可运行版本，目录里除这份需求外没有现存代码。程序内置固定种子生成器在本地确定性产出 Q/K/V 张量分片（形状 [批量, 头数, 序列长度, 头维度]），覆盖短序列、长序列、非整除头维度、因果掩码与含极值样本；每份样本附形状声明、分块配置（行块与列块大小、在线累加策略）与以定点量化保存的高精度参考输出作为唯一真值来源。算子须在不物化完整注意力矩阵的前提下按行块流式累加运行最大值、指数和与加权输出，并逐块给出可复核的误差界与有效位数，使分块顺序、前缀增量更新与并发发布下的结果保持一致。

任务环境与准备：
安装 Go 1.22 或更高版本（Windows amd64 官方安装包），确认 go version 可执行。
安装 SQLite 命令行工具以便查看本地库文件；Go 侧使用纯 Go 的 SQLite 驱动或本地 SQLite 动态库，按所选驱动在 go.mod 中锁定版本。
无需 GPU、外部服务或网络素材；张量样本、分块配置与参考输出均由程序内置固定种子生成器在本地首次运行时产出到工作目录。
在空工作区初始化 Go module（go mod init），创建 cmd、internal 与 data 目录；运行说明写入项目内的运行入口，不依赖任何容器环境。

业务要求：
1. 实现分块前向核心：按行块与列块组织计算，沿键维度以在线 softmax 流式维护行运行最大值、指数和与加权输出，支持因果掩码、任意合法非整除头维度与批量维度，并逐行块输出中间量摘要；同一输入与配置必须产生逐字节一致的最终输出与中间量。
2. 实现在线 Softmax 的数值稳定性控制：指数归一化必须先减去行块运行最大值，加权累加在缩放域或对数域进行，禁止直接连乘概率；当某行出现极端极值、指数和为有限但超出声明位宽，或累加出现非有限值时，须在该行块记录钳制次数与位置并给出 E_NUMERIC 诊断。
3. 实现与高精度参考的误差界逐块审计：对每个行块按显式抽样位置与定点参考输出比对，给出最大绝对偏差、相对偏差与最终有效位数，误差界须由缩放因子、量化步长与累加顺序显式推导，推导依据随结果保留；偏差超容差须返回带分片标识、行块序号与最大偏差数值的 E_ERROR_BOUND。
4. 实现前缀增量更新协议：当分片的部分 K/V 前缀被追加或修正时，接受以分片标识与受影响键区间描述的更新请求，仅重算受影响列块并复用其余列块的运行最大值、指数和与部分输出；拼接后的最终输出、逐行最大值与归一化因子须与从零全量计算逐字节一致，不一致时返回带分片标识与首个差异位置的 E_INCREMENTAL_MISMATCH。
5. 实现分块中间状态的版本化事务持久化：每完成一组列块的每个行块状态即以 SQLite 事务写入含分片标识、行块序号、已覆盖键范围、运行最大值、指数和、部分输出摘要、输入指纹与单调序号的记录，同一分片同一行块同一键范围只允许一条有效记录；恢复时须忽略损坏、不完整或序号重复的记录。
6. 实现版本指纹与块级失效传播：当前缀内容、分块参数、因果掩码开关、在线累加策略或量化位宽任一变化时，由各阶段内容摘要构成版本指纹并识别受影响的最早行块与列块区间，使该区间状态失效重建，未受影响行块状态继续复用；失效范围须可查询且带触发字段与新旧指纹。
7. 实现并发状态发布保护与整体替换识别：多工作进程对同一库发布行块状态时使用 SQLite 条件更新与真实文件锁并比较行块序号，仅允许序号连续推进，落后、重复或跨行块错位发布返回 E_STALE_CHECKPOINT；检测到记录被整体替换（序号回退、指纹变化或摘要不匹配）时须区分回退与正常推进，回退拒绝并附当前生效序号。
8. 实现峰值内存约束下的执行调度并提供库接口与确定性演示：在可配置峰值内存上限内规划行块工作区、K/V 分块切片与中间状态缓冲的占用，超预算时按行块或批次拆分执行并记录拆分决策，或返回带预算明细的 E_MEMORY_BUDGET，不得以线程超时描述为已终止失控代码；库接口与命令行须支持导入分片、执行、增量更新、误差报告、状态与版本查询、优雅停止与恢复，演示覆盖短序列、长序列、非整除头维度与因果掩码。

执行约束：不使用 Docker 或其他容器环境；使用本地确定性样本验证，不访问未授权目标。按上述任务意图交付相应代码、测试或技术说明，记录实际验证结果。

验收：
用本地样本验证分块注意力输出、误差界审计、前缀增量一致性与中断恢复结果。
