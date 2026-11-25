# headprune — 确定性注意力头裁剪工作台

在本地、可复现、无外部服务 / GPU / 网络依赖的条件下，判断一个小型掩码语言模型
（MLM）中哪些注意力头可以在推理前被裁剪，并让每次裁剪决策都留下可回滚、可精确
复原的版本记录。全部逻辑用 Go 实现，持久化使用纯 Go 驱动 `modernc.org/sqlite`。

## 环境准备

要求 Go 1.22+（开发机验证版本 go1.27.4，`go.mod` 声明 `go 1.22`）。

```bash
go mod init headprune            # 已初始化
go get modernc.org/sqlite
go mod tidy
mkdir -p samples
HEADPRUNE_WORKSPACE=./samples go run ./cmd/headprune generate
go build ./...
go test ./...
```

如默认代理不可达，可切换国内代理：`go env -w GOPROXY=https://goproxy.cn,direct`。

## 目录结构

- `internal/model`：数据类型、稳定业务错误码、确定性 JSON/摘要、数值引擎
  （激活缩放、含配对协同项的集成 logits、softmax、KL 散度）。
- `internal/generate`：固定种子（`20261125`）生成器，产出 3 层 × 4 头、
  12 条标注句对与策略快照；内置一个强耦合头组合与一个量化误差显著头。
- `internal/pipeline`：四阶段流水线、定点量化与误差界、血缘识别、失效传播与复用。
- `internal/store`：SQLite 事务化版本库、条件更新、真实 OS 文件锁与缓存。
- `internal/store/filelock`：Unix `flock(2)` 与 Windows `LockFileEx` 双实现。
- `internal/appcli`：本地服务（HTTP + CLI 共用）、优雅停止/恢复。
- `cmd/headprune`：命令行与 HTTP 入口、确定性演示。

## 四阶段流水线

1. 探测句采样与逐头裁剪重放（`stage1.go`）：按策略种子用 `math/rand` 确定性
   洗牌选取探测句；逐头单独裁剪后用其余头重算掩码位置分布，聚合 KL/TV 误差，
   输出逐头误差表与探测覆盖。同输入同快照产生逐字节一致结果；探测为空或低于
   `min_probes` 返回 `E_INSUFFICIENT_PROBE`（带层号/头号）。
2. 组合重放与不可加性裁决（`stage2.go`）：按候选裁剪比例枚举组合并做整体重放，
   真实误差与逐头误差的简单求和 / 加权求和预测值比对，输出耦合放大系数、偏差与
   超阈组合清单。组合误差始终由整体重放得到，绝不用逐头误差推算；发散或分布全零
   返回 `E_NUMERIC` 并在 `context` 保留组合标识与各头贡献日志。
3. 精度决策与预算约束（`stage3.go`）：在容差与裁剪数量上限内选择最终集合，逐头、
   逐组合记录取舍理由与误差余量；无组合满足容差时输出显式空集合并指明最接近容差
   的组合，绝不静默放宽容差。决策摘要经规范 JSON 哈希，逐字节稳定。
4. 量化误差界（`quantize.go`）：按策略位宽做对称有符号定点量化，记录每头缩放因子、
   零位、理论/实测绝对误差界、相对误差界与有效位数（ENOB），并保留显式推导字符串。
   缩放因子为零、激活含 NaN/Inf、量化后概率和为零均返回带层号/头号的
   `E_QUANTIZATION`。

### 头间耦合为何不可加

`internal/model/engine.go` 的集成 logits 包含一个配对协同项：协同头的激活经协同
权重生成正门控，再与配对头的贡献相乘。该协同项只有在两个头同时存活时才存在，因此
单独裁剪任一头的误差之和无法预测同时裁剪的真实误差——演示数据中可观察到约 1.8x
的耦合放大（`deviation > coupling_deviation_threshold`）。

## 量化误差界推导

对称有符号定点：`scale = max_abs / (2^(bits-1) - 1)`，量化噪声满足
`|Q(x)-x| <= scale/2`；相对误差界 `= (scale/2)/max_abs = 1/(2*max_code)`；
ENOB 使用标准定点定义 `log2(rms_signal / (scale/sqrt(12)))`。每个头的
`derivation` 字段完整保留代入数值的推导依据。

## 版本提交与回滚（SQLite）

- 每次提交在单个事务内写入裁剪集合指纹、探测句摘要、量化参数指纹、决策摘要与
  决策 JSON，配合真实文件锁（`flock`/`LockFileEx`）与条件插入，只允许版本号
  连续推进；落后/并发/重复/摘要不符统一返回 `E_STALE_VERSION` 并附当前版本号。
- 回滚以“追加新版本”的方式恢复目标版本的裁剪集合与决策依据（版本号保持单调），
  中间版本标记 `rolled_back`、超出 `rollback_keep_generations` 的更老版本标记
  `archived`，两者均仍可查询；读路径只看到已提交数据，不存在半提交状态。

## 整体替换识别与失效传播

导入时按内容摘要 + 单调序号区分 `initial/append/replace/rollback/fork`
（`lineage.go`），关系写入 SQLite `lineage` 表。逐头误差缓存键由
`头分片摘要|句对摘要|策略摘要` 构成：整体替换使全部下游失效重建；正常追加只让
受影响头失效，未受影响头命中缓存并在结果中以 `reusable_from_version` 标注来源。

## 稳定业务码

| 代码 | 触发场景 |
| --- | --- |
| `E_INSUFFICIENT_PROBE` | 探测句为空或低于可配置下限（带层号/头号） |
| `E_NUMERIC` | 组合重放发散、分布全零、KL 非有限（保留贡献日志） |
| `E_QUANTIZATION` | 缩放因子为零、NaN/Inf、量化概率和为零（带层号/头号） |
| `E_STALE_VERSION` | 版本回退/重复/并发冲突/摘要不符/回滚到已归档版本 |
| `E_HEAD_INVALID` | 头维度、权重长度或数值非法（带层号/头号） |
| `E_SENTENCE_INVALID` | 句对缺真值候选、特征维度不符（带探测句标识） |
| `E_POLICY_INVALID` | 采样上限为零、容差为负、位宽/预算非法等 |
| `E_CONFLICT` | 优雅停止期间触发执行、未先运行即提交等 |

## HTTP 与命令行

HTTP（默认 `127.0.0.1:8080`，可用 `HEADPRUNE_ADDR` 或参数覆盖）：

```
GET  /health                 POST /import/heads       POST /import/sentences
POST /policy                 POST /generate           POST /pipeline/run
GET  /errors/heads           GET  /errors/coupling    GET  /quantization
POST /versions/commit        GET  /versions           GET  /versions/{v}
POST /rollback/{v}           GET  /lineage            POST /stop  POST /resume
```

CLI（工作区由 `HEADPRUNE_WORKSPACE` 指定）：

```bash
go run ./cmd/headprune generate
go run ./cmd/headprune run
go run ./cmd/headprune head-errors
go run ./cmd/headprune coupling
go run ./cmd/headprune quantization
go run ./cmd/headprune commit
go run ./cmd/headprune history
go run ./cmd/headprune version 1
go run ./cmd/headprune rollback 1
go run ./cmd/headprune lineage
go run ./cmd/headprune stop
go run ./cmd/headprune resume
go run ./cmd/headprune serve 127.0.0.1:8080
go run ./cmd/headprune demo ./artifacts/demo
```

## 确定性演示与实际验证结果

`demo` 用内置生成器在两个全新工作区各完整运行一次（运行→提交→再运行→再提交→
回滚→复原校验），并逐字节比较两次摘要。最近一次验证：

- 数据规模：3 层 × 4 头（12 头）、12 条句对、探测句 8 条、4 比特定点量化。
- 强耦合组合：3 个组合 `coupled=true`，最高耦合放大约 1.8x（真实重放 > 逐头求和）。
- 量化显著头：`L0H2` 的 ENOB 与位宽差距最大（gap≈0.693，次优头≈0.26）。
- 两次运行 `demo-summary.json` 与 `stage-result.json` 完全一致；回滚复原的裁剪
  集合指纹与原决策一致。
- `go test ./...` 全部通过，覆盖确定性、四类异常注入、并发提交互斥（8 并发仅
  1 个成功）、回滚精确复原、保留代数归档、缓存复用与血缘失效传播、HTTP 全链路。

复现：

```bash
go build ./... && go test ./...
go run ./cmd/headprune demo ./artifacts/demo
```
