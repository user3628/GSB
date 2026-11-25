# SpMM 稀疏算子优化与诊断应用

本项目在本地实现一个可集成的 Go CSR×稠密矩阵 SpMM 算子库，并提供命令行与 HTTP
交互式诊断应用。默认无 CGO、无容器、无外部服务；SQLite 使用纯 Go 驱动
`modernc.org/sqlite`，跨平台发布锁使用 `github.com/gofrs/flock`。

## 快速开始

要求 Go 1.22 或更高版本。

```bash
go mod download
go test ./...
go build -o bin/spmmctl ./cmd/spmmctl

./bin/spmmctl generate --out artifacts/samples
./bin/spmmctl verify --samples artifacts/samples --db artifacts/verify.db
./bin/spmmctl demo   --samples artifacts/samples --out artifacts/demo
./bin/spmmctl serve  --samples artifacts/samples --db artifacts/spmm.db --addr 127.0.0.1:8787
```

Windows PowerShell 使用相同参数，只需把 `./bin/spmmctl` 换成
`.\bin\spmmctl.exe`。已验证以下零 CGO 交叉构建：

```bash
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/spmmctl.exe ./cmd/spmmctl
```

## 目录结构

```text
spmm/       CSR 数据、指纹、自动分块、SpMM 内核、误差估计、内存调度
samples/    固定种子样本生成器、清单加载与可重建性校验
store/      SQLite 版本链、行块事务、文件锁、完整性检查与缓存失效
engine/     交互查询与结算双路径、延迟预算、优雅停止/恢复、路径裁决
cmd/spmmctl CLI、交互式 HTTP 诊断应用、验收演示编排
artifacts/  已生成样本、校验报告与演示产物
```

## 样本语料

`generate` 使用固定种子生成 10 份本地样本：3 档稀疏率（约 1%、10%、40%）×
3 种形状（方阵、高瘦、矮胖），外加一份结构退化样本。

| 形状 | A 形状 | B 形状 |
| --- | ---: | ---: |
| square | 96×96 | 96×48 |
| tall | 240×48 | 48×40 |
| wide | 48×240 | 240×40 |
| degenerate | 48×64 | 64×24 |

每份样本包含：

- `.csr.json`：`rows`、`cols`、`indptr`、`indices`、`data`、形状与稀疏率声明；
- `.dense.json`：稠密右乘矩阵；
- `manifest.json`：样本名、形状、声明稀疏率、统计量、形状/结构/稠密指纹。

所有主样本及退化样本均含空行；退化样本还包含连续空行块、重复列索引、乱序列、
显式零值和全零值行。清单文件与矩阵文件可逐字节重建，测试
`TestGeneratorReproducibleAndCoversStructure` 会校验。

生成结果已写入 `artifacts/samples/`，可随时覆盖重建。

## CLI

### 发布方案

```bash
./bin/spmmctl plan \
  --samples artifacts/samples --sample square_p10 --db artifacts/spmm.db \
  --sum compensated --order block_order --skip-zeros=true \
  --cache-bytes 262144 --peak-bytes 16777216
```

输出包含方案键、版本序号、摘要、完整候选打分、淘汰方案、选择理由、内存拆分决策
与每一步内存估算。

### 交互查询与结算

```bash
./bin/spmmctl query \
  --samples artifacts/samples --sample square_p10 --db artifacts/spmm.db \
  --plan-key <PLAN_KEY> --start 5 --end 55 --latency-ms 2

./bin/spmmctl run \
  --samples artifacts/samples --sample square_p10 --db artifacts/spmm.db \
  --plan-key <PLAN_KEY> --persist --result-file artifacts/run.json
```

`query` 按请求行区间独立重算交互结果；`run` 执行结算路径并提交已完成行块。同一
快照下，两个路径对同一行区间的值、逐块摘要和显式误差报告必须一致，否则返回
`E_PATH_MISMATCH` 与首个差异行块。

### 停止、强杀与恢复

```bash
# 完成当前行块后优雅停止
./bin/spmmctl run ... --graceful-after 1

# 在提交第 1 个行块前模拟进程被强杀
./bin/spmmctl run ... --hard-exit-before-commit 1

# 重新运行同一 plan-key 即从 SQLite 已提交事实恢复
./bin/spmmctl run ... --plan-key <PLAN_KEY>
```

优雅停止会写入 `operator_state.state=stopped` 并返回部分结果；强杀不生成退出报告，
重启只信任 `block_results` 中已提交的行块。

### 全量验证与演示

```bash
./bin/spmmctl verify --samples artifacts/samples --db artifacts/verify.db
./bin/spmmctl demo   --samples artifacts/samples --out artifacts/demo
```

验证报告写入 `artifacts/verification.json`，完整验收报告写入
`artifacts/demo/report.json`。

## 交互式诊断应用

```bash
./bin/spmmctl serve --samples artifacts/samples --db artifacts/spmm.db
# 浏览器打开 http://127.0.0.1:8787
```

页面可发布方案、执行查询、结算与双路径比较，并直接显示结构化 JSON 诊断信息。

主要端点：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查 |
| GET | `/api/samples` | 样本及其结构统计 |
| GET | `/api/status` | 数据库方案概况 |
| POST | `/api/plan` | 自动/手动选块并发布新版本 |
| GET | `/api/history?plan_key=...` | 不可变方案历史 |
| GET | `/api/state?plan_key=...` | 算子状态与失效审计 |
| POST | `/api/query` | 交互路径查询 |
| POST | `/api/settle` | 结算路径执行 |
| POST | `/api/compare` | 同快照双路径裁决 |
| POST | `/api/reconcile` | 回退/替换识别与下游失效 |

示例：

```bash
curl -s -X POST http://127.0.0.1:8787/api/plan \
  -H 'content-type: application/json' \
  -d '{"sample":"square_p01","sum":"compensated","order":"block_order","skip_zeros":true}'
```

## 设计说明

### 1. CSR×稠密分块内核

- `Plan.RowBlocks` 是 M 维行块；`Plan.ColBlocks` 是稀疏累加列（CSR 的 K 维）分块。
- 内存调度另外产生输出列（N 维）切片，控制 B 的工作集与每个执行步骤的峰值估算。
- 内核先扫描一次 CSR 行，构造显式累加轨迹；数值循环只访问非零项。
- `column_ascending` 对每行非零项按列索引稳定排序，重复列保持 CSR 出现次序；
  `block_order` 按 K 维列块顺序，再按块内 CSR 出现次序累加。
- 显式零值可跳过或保留，并在轨迹中分别累加 `explicit_zero_skipped` 与
  `explicit_zero_kept`；重复列次数单独计数。
- 同一输入、方案和停止/内存配置产生相同输出摘要、轨迹摘要和误差摘要。

### 2. 自动分块选择

候选行块/列块组合按以下显式输入打分：

- 非零元数、平均每行非零数、全局空行比例、块内空块比例；
- 稠密矩阵输出列宽与缓存预算；
- 预测访存量；
- 行块 NNZ 变异系数表示的负载均衡度；
- 依据可移植流量模型与固定调度开销预测的单块查询时延；
- 超缓存预算、过大块与空块惩罚。

结果记录在 `SelectionReport`：选中方案、每个候选的三项得分/总得分、预测流量、
负载不均衡度、平均行 NNZ、空行比例、拒绝原因和自然语言理由。小矩阵会限制明显过大
的行块候选，并惩罚只有一个行块或空块占比过高的组合。

### 3. 双路径一致性

- 交互路径 `Query` 忽略已持久化块，按请求行区间重新计算所需行块，并在预估或实际
  耗时超过预算时返回含 `query_id`、`elapsed_ms`、`budget_ms` 的
  `E_LATENCY_BUDGET`。
- 结算路径 `Settle` 复用已提交事实，对缺失行块执行完整累加并按顺序事务提交。
- 两条路径仍使用独立编排；`ComparePaths` 逐块比较输出摘要、轨迹摘要、误差摘要、
  误差上界与请求行区间内的浮点原始位，不一致时给出首个差异行块。
- 完整区间下，`Result.CanonicalDigest` 不含路径名、耗时或查询 ID，因此查询与结算
  规范摘要可直接相等。

### 4. 数值精度与误差报告

- 支持 `sequential` 与 `compensated`（Neumaier 补偿）两种 float64 累加策略。
- 每个执行行块使用确定性随机位置加最大幅值位置，与本地 256-bit `big.Float`
  参考值比较。
- 报告中保存采样种子、覆盖率和每个位置的显式高精度参考字符串、实际值、绝对偏差、
  相对偏差；`UpperBoundAbs/Rel` 是这些显式抽样位置的已观测上界，不从缺失位置外推。
- 采用标准混合容差：仅当同一样本同时满足 `abs_deviation > abs_tolerance` 且
  `rel_deviation > rel_tolerance` 时判定超限；绝对或相对任一维度在容差内即接受，
  避免近零参考值产生虚假失败。超限时返回 `E_NUMERIC_MISMATCH`，附行列区间、最大
  绝对偏差和相对偏差。正常执行时误差报告与输出块一起持久化。

### 5. 峰值内存调度

每个执行步骤估算：

```text
output_buffer = effective_rows × dense_cols × 8
workspace     = effective_rows × dense_cols × 8
dense_slice   = K_block × output_col_chunk × 8
```

当超出 `peak_bytes` 时先缩小行块，再缩小输出列切片；`MemoryPlan` 记录
`row_split`、`column_split`、拆分理由、全部执行行/列和每步估算。若一行、一列切片
仍无法容纳，返回带 `MemoryBreakdown` 的 `E_MEMORY_BUDGET`，不会依赖 OOM 崩溃。
输入矩阵在进入执行工作区前加载，预算针对上述执行缓冲与切片。

### 6. SQLite 版本化与恢复

主要表：

- `plan_versions`：不可变方案版本；主键 `(plan_key, version_seq)`，含前后摘要链。
- `plan_heads`：当前发布头，用条件更新比较版本序号与摘要。
- `block_results`：每个已完成执行行块一条；主键
  `(plan_key, version_seq, execution_row_index)`。
- `error_reports`：独立误差报告，和行块在同一事务中写入。
- `operator_state`：最后提交行块与 `planned/running/stopped/completed` 状态；发布
  新方案版本时在同一事务内重置为 `planned`。
- `plan_invalidations`：回退/替换检测审计。

每个行块的所有写入在一个 `BEGIN IMMEDIATE` 事务中完成；测试通过制造
`error_reports` 主键冲突证明块行会整体回滚。方案键由形状、结构、稠密矩阵、求和
策略、累加顺序、显式零策略、缓存/峰值预算和误差配置决定，不包含块尺寸；因此同一
逻辑方案改变块尺寸会严格递增版本序号。

### 7. 并发发布与整体替换

- 所有发布先在数据库旁的 `${db}.lock` 上获取真实操作系统文件锁，再进入 SQLite
  立即事务。
- 方案头使用 `WHERE version_seq=? AND plan_digest=?` 条件更新；行块要求
  `next_index = max_index+1`，落后、重复、跳跃或跨方案坐标均返回
  `E_STALE_PUBLISH`。
- `ReconcilePlan` 验证完整摘要链、头/历史一致性、序号回退、指纹变化和摘要不匹配，
  区分 `normal_advance`、`rollback`、`replacement`；回退或替换会删除受影响下游块与
  误差报告，并写入失效审计，之后可从行块 0 重建。

## 稳定业务错误码

| 错误码 | 含义 |
| --- | --- |
| `E_INPUT` | 输入矩阵、维度、参数或 JSON 非法 |
| `E_LATENCY_BUDGET` | 交互查询超出延迟预算 |
| `E_PATH_MISMATCH` | 双路径结果、误差或摘要不一致 |
| `E_NUMERIC_MISMATCH` | 抽样高精度参考超出容差 |
| `E_MEMORY_BUDGET` | 无法在峰值内存预算内执行 |
| `E_STALE_PUBLISH` | 方案/行块发布落后、重复、跳跃或错位 |
| `E_STATE` | 持久化状态缺失或损坏 |
| `E_INTERRUPTED` | 预留的主动中断业务码 |

所有结构化错误由 `spmm.Error` 编码，包含相关区间、偏差、计划键、版本、预算或首个
差异行块等上下文字段。

## 实际验证结果

本次环境：Go 1.27.4（模块声明兼容 Go 1.22），Linux amd64；另已零 CGO 交叉编译
Windows amd64 可执行文件。

```text
go test ./... -count=1          PASS
go test -race ./... -count=1    PASS
go vet ./...                    PASS
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./cmd/spmmctl  PASS
HTTP /api/health /api/samples /api/plan /api/compare              PASS
```

`artifacts/demo/report.json` 记录：

- 10/10 样本 `paths_match=true`，完整查询与结算规范摘要相等；
- 最大抽样绝对误差约 `2.35e-16`，最大相对误差约 `1.22e-14`，全部低于默认
  `1e-9` 容差；
- 强杀进程退出码 `137`，重启前仅 `[0]` 行块已提交，共 4 个执行单元；
- 恢复后行块索引连续为 `[0,1,2,3]`，结果与连续执行逐字节一致，查询/结算仍一致；
- 优雅停止在 1 个行块后返回 `stopped`，重启结果与连续执行逐字节一致；
- 两个真实工作进程同时发布，仅一个获版本 1，另一个收到 `E_STALE_PUBLISH`，最终
  历史长度 1、头版本 1。

## 复现与注意事项

```bash
go clean -testcache
go test ./... -count=1
./bin/spmmctl demo --samples artifacts/samples --out artifacts/demo
```

演示会重建 `artifacts/demo` 下的数据库与稳定结果文件；样本目录可安全覆盖重建。
数据库、WAL 文件和锁文件均为本地运行产物。服务默认只监听 `127.0.0.1`，不提供认证，
不要直接暴露到不受信任网络。
