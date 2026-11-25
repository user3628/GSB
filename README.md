# ropechunk

`ropechunk` 是一套以 Go 库形式集成的 RoPE 分块位置编码算子。它按行块和
列块流式计算固定点旋转与查询键打分，不物化完整 `sequence × head_dim`
角度矩阵；执行过程受并发、时间、内存预算约束，并通过 SQLite 事务保存可恢复
的在线 Softmax 中间状态。

## 交付内容

- `ropechunk` 根包：稳定的公开库接口。
- `internal/rope`：固定点旋转、线性/NTK 分段缩放、256 位高精度参考、
  在线 Softmax 与逐块摘要。
- `internal/engine`：内存规划、三重预算调度、版本失效、停止/恢复。
- `internal/store`：SQLite WAL、事务条件更新、真实 `flock` 发布锁。
- `internal/samples`：固定种子四样本生成器。
- `internal/httpapi`：本地 HTTP 控制面。
- `cmd/ropechunk`：CLI 与确定性端到端演示。
- 单元/集成/并发测试与实际验收记录。

## 环境与快速开始

要求 Linux（WSL Ubuntu 可用）、Go 1.22+、SQLite 3 CLI。SQLite 驱动为纯 Go
的 `modernc.org/sqlite v1.29.10`，不依赖 CGO；`sqlite3` 仅用于检查数据库。

```bash
go test ./...
go test -race ./...

go run ./cmd/ropechunk demo \
  --db artifacts/ropechunk-demo.db \
  --bundle artifacts/demo-bundle.json \
  --report artifacts/demo-report.json \
  --clean
```

`demo --clean` 会先删除旧数据库、`-wal`、`-shm` 和检查点锁文件，再执行：

1. 用固定种子生成 4 个分片及 256 位高精度参考旋转张量。
2. 导入 v1 配置并完成旋转、打分两阶段。
3. 逐定点值校验参考真值。
4. 切到 v2（只改变因果掩码），验证旋转状态复用、仅打分状态失效。
5. 回退 v1，验证 `rollback` 分类及块复用。

## 库集成

```go
package main

import (
	"context"
	"fmt"

	"ropechunk"
)

func main() {
	st, err := ropechunk.OpenStore("rope.db")
	if err != nil {
		panic(err)
	}
	defer st.Close()

	eng := ropechunk.NewEngine(st)
	bundle, err := ropechunk.GenerateSamples()
	if err != nil {
		panic(err)
	}
	if _, err := eng.Import(context.Background(), bundle); err != nil {
		panic(err)
	}
	report, err := eng.Compute(context.Background(), ropechunk.ComputeOptions{Stage: "all"})
	if err != nil {
		panic(err)
	}
	verification, err := eng.VerifyReferences(context.Background(), nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(report.Processed, verification.AllMatch)
}
```

核心纯函数也可单独使用：`RotateTensor`、`RotateBlock`、
`ReferenceRotate`、`ConsumeKeyRange`、`FinalizeAttentionRow`、`PlanMemory`。
HTTP 处理器由 `ropechunk.NewHTTPHandler(engine)` 返回。

## 输入契约

导入单位是一个 JSON `Bundle`。形状统一为 `[batch, heads, sequence, head_dim]`。
Q/K 和参考旋转张量均为定点 `int64` 数组，`scale` 表示每个整数代表的实数量。

```json
{
  "format_version": 1,
  "config": {
    "snapshot_version": 1,
    "base_frequency": 10000,
    "scaling_mode": "none",
    "scale_factor": 1,
    "angle_block_size": 4,
    "causal_mask": true,
    "row_block_size": 2,
    "key_block_size": 4,
    "max_concurrent_blocks": 4,
    "worker_block_quota": 8,
    "round_time_budget_ms": 20000,
    "peak_memory_bytes": 134217728
  },
  "shape_declarations": [
    {"shard_id": "s-short", "shape": [1, 1, 1, 2], "has_extremes": false}
  ],
  "shards": [
    {
      "id": "s-short",
      "shape": [1, 1, 1, 2],
      "q": {"shape": [1, 1, 1, 2], "scale": 1000000, "data": [1, -2]},
      "k": {"shape": [1, 1, 1, 2], "scale": 1000000, "data": [3, -4]},
      "reference_rotated_q": {"shape": [1, 1, 1, 2], "scale": 1000000, "data": [5, -6]},
      "reference_rotated_k": {"shape": [1, 1, 1, 2], "scale": 1000000, "data": [7, -8]}
    }
  ]
}
```

内置生成器固定种子为 `0x9e3779b97f4a7c15`，每次以分片 ID 和 Q/K 角色做
splitmix64 派生，生成：

- `s-short`：短序列；
- `s-long`：96 token 长序列；
- `s-odd`：head_dim=9 的奇数维、batch=2；
- `s-extreme`：batch=2，含 `int32` 界附近及更大定点极值。

参考张量由 256 位精度 `big.Float` 与泰勒三角函数生成，是旋转正确性的唯一
真值来源。

## 旋转语义

第 `i` 个维度对使用 `(2i, 2i+1)`，固定计算：

```text
exponent = 2i / head_dim
none:          base^(-exponent)
linear:        base^(-exponent) / scale_factor
ntk_piecewise: 前 head_dim/8 个维度对（至少 1 对）保留原 base；其余使用
               adjusted_base = base * scale_factor^(D/(D-2))
               的频率 adjusted_base^(-exponent)
```

`angle_block_size` 是头维度上的旋转块长度，必须为正偶数。块内只对成对维度
旋转；奇数 `head_dim` 的最后一维保持不变，并在 `unrotated_dims` 中显式记录。
实现逐行、逐维度对计算 `sin/cos`，不构造完整角度矩阵，只输出每块角度摘要
（首末角度、pair 数、SHA-256 角度摘要）。

输出量化使用 `math.Round` 回同一固定点 scale。相同输入和配置产生逐字节一致
的数据、块摘要和版本指纹。

## 打分语义

打分使用已旋转并落盘的 Q/K 定点值。对每个 query 行、每个 key 列块执行：

1. 计算 `dot(q, k) / sqrt(head_dim)`；
2. 检查非有限值；
3. 用运行最大值更新指数和与加权输出；
4. 每个列块提交一次行块中间状态；
5. 最终计算每行 `log_sum_exp`、归一化因子、权重摘要和输出摘要。

因果掩码开启时只允许 `key <= query`。出现 NaN、Inf、零/非有限归一化因子时
返回 `E_NUMERIC`，包含 `shard_id`、`batch`、`head`、`row_block`、`col_block`。

## 调度与预算

- 调度单元是 `(batch, head, row_block)`。
- 每轮并发数不超过 `max_concurrent_blocks`。
- 每个工作进程每轮最多处理 `worker_block_quota` 个块。
- 接纳前按形状和该行已覆盖键范围计算确定性微秒估算；估算超过本轮余量时不
  接纳，记录 `unprocessed`、原因和当时预算余量。
- 未达单轮容量的任务进入下一轮；无任何进展时保留未处理事实，不静默丢弃。
- 检查点已完整且指纹/分区兼容的行块记为 `reused`。
- 停止请求只在块边界生效；当前不可拆分块完成后退出。

内存规划分别估算旋转后张量、并发行块工作区、角度表缓冲、注意力工作区和固定
开销。空间不足时优先按 batch 拆分并记录每步估算；单 batch 仍不足时返回
`E_MEMORY_BUDGET`，附带完整/单 batch 明细。

## SQLite 一致性

数据库启用 WAL 与 `BEGIN IMMEDIATE`。检查点表对
`(shard_id, stage, batch, head, row_block)` 建唯一主键，只保留一条有效记录。
每次列块推进都以事务写入：

- 已覆盖键范围、行范围；
- 运行最大值、指数和、LSE；
- 实际可续跑的行状态和部分输出摘要；
- 输入指纹、版本指纹、单调 `sequence`；
- 配置版本、run ID、完成标记和结果摘要。

发布时同时持有真实文件锁
`<db>.checkpoint.lock`（`flock(LOCK_EX)`）和 SQLite 事务，先比较：

- 发布者被分配的分片/行块是否匹配；
- `expected_sequence == current_sequence + 1`；
- 版本指纹与输入指纹是否一致；
- 记录是否已完整。

条件 `UPDATE ... WHERE sequence=? AND fingerprint=?` 防止并发覆盖。落后、重复、
跳跃或跨分片发布返回 `E_STALE_CHECKPOINT`。失效删除也在同一文件锁下执行。

## 版本与失效

- `rotation fingerprint`：输入 Q/K 规范字节摘要 + 基础频率 + 缩放方式 +
  缩放系数 + angle block size。
- `score fingerprint`：rotation fingerprint + 因果掩码。
- key block、row block 仅属于执行分区，不进入内容指纹；分区兼容性由行范围
  条件单独校验。

配置导入时区分 `initial`、`advance`、`idempotent`、`replacement`、`rollback`，
写入 `version_events`。指纹或行分区不兼容的检查点按“最早行块 + 列块区间”
记录到 `invalidations` 后再删除；兼容行块继续复用。

## HTTP 与 CLI

启动本地 HTTP：

```bash
go run ./cmd/ropechunk serve --db artifacts/ropechunk-demo.db --addr 127.0.0.1:8080
```

主要接口：

| Method | Path | 用途 |
|---|---|---|
| POST | `/api/import` | 导入 Bundle |
| POST | `/api/demo` | 生成并导入内置四样本 |
| POST | `/api/compute` | 触发 `all/rotation/score` |
| GET | `/api/status` | 阶段完成/未完成/过期行数 |
| GET | `/api/fingerprints` | 版本与阶段指纹 |
| GET | `/api/blocks` | 角度/打分摘要及检查点 |
| GET | `/api/budget` | 逐任务预算裁决 |
| GET | `/api/memory-plans` | 拆分决策与每步内存估算 |
| GET | `/api/invalidations` | 版本失效区间与原因 |
| GET | `/api/version-events` | 推进/替换/回退分类 |
| POST | `/api/verify` | 校验固定点参考真值 |
| POST | `/api/stop` | 块边界优雅停止 |
| POST | `/api/resume` | 清除停止标志并可选续跑 |

CLI 命令：

```text
demo import compute resume status verify blocks budget invalidations versions serve clean
```

`compute`/`resume` 捕获 Ctrl-C 与 SIGTERM，通过 `Engine.Stop()` 在块边界退出。
强制终止后可直接重新执行 `compute` 或 `resume`；引擎只读取已提交检查点。

## 验证

已执行：

```text
go test ./...          PASS
go test -race ./...    PASS
go run ./cmd/ropechunk demo ...  PASS
```

四样本 v1 共完成 388 个行块任务；参考校验覆盖 10,108 个 Q/K 定点元素，
mismatch=0。仅切换因果掩码后产生 4 条失效事件，全部属于 `score`；rotation
阶段 0 失效。v2 复用 194 个旋转块、重建 194 个打分块。回退 v1 被分类为
`rollback`，再次复用 194 个旋转块。

更细的测试与实测输出见 `artifacts/VERIFICATION.md`。
