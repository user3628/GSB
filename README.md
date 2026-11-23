# Windows 安全基线策略一致性验证工作台

这是一个完全本地运行的 Go 工作台，用于把不同来源的 Windows 安全基线策略包归一到整数定点口径，计算偏差得分与误差传播上界，在并发会话中隔离失败结果，并用 SQLite 保存可回放的连续版本快照。

项目不依赖外部服务、不使用容器，SQLite 驱动为纯 Go 的 `modernc.org/sqlite`。依赖已放入 `vendor/`，Linux 与 Windows/amd64 均可离线构建。

## 核心口径

- 每个策略包由 `baseline.json`（基线快照）和 `repairs.txt`（带长度前缀的偏差修复记录）组成。
- 修复记录按策略项 ID 覆盖基线项，合并后的有效视图与清单中的参考包有效视图逐项比较。
- 分值统一为 `score_1e-6` 整数；时长统一为秒；FILETIME 从 1601 纪元按 100ns 换算为 UTC 秒。
- 所有乘法、除法、加法和换算缩放均有 `int64` 溢出检查，数值路径不使用浮点。
- 截断误差写入会话账本，并按权重聚合为包级误差上界。两个来源的误差区间不重叠时为确定性偏差，重叠时为不确定性区间。
- 只有完整、无业务码且快照提交成功的包才进入终端摘要的 `OK` 状态。

## 快速开始

要求 Go 1.22 或更高版本。

```sh
go version
go test ./...

# 一键生成确定性样本、执行完整校验并输出报告
go run ./cmd/baselinebench demo --workdir work/demo

# 五类异常注入断言
go run ./cmd/baselinebench verify --workdir work/verify
```

Windows PowerShell 示例：

```powershell
go version
go test ./...
go run .\cmd\baselinebench demo --workdir .\work\demo
go run .\cmd\baselinebench verify --workdir .\work\verify
go build -o .\baselinebench.exe .\cmd\baselinebench
```

输出：

- `work/demo/report.json`：确定性 JSON 诊断报告。
- `work/demo/summary.txt`：终端摘要副本。
- `work/demo/state/snapshots.db`：SQLite 快照库。
- `work/verify/verification.json`：五类异常注入结果。

## 命令

### `generate`

```sh
go run ./cmd/baselinebench generate --out work/samples
```

生成完整的本地样本集。重复运行的目录内容字节级一致；`manifest.sha256` 是路径与文件内容的稳定摘要。

### `demo`

```sh
go run ./cmd/baselinebench demo \
  --workdir work/demo \
  --mode strict \
  --concurrency 2 \
  --item-budget 64 \
  --max-error-bound 100
```

参数：

- `--mode strict|tolerant`：严格或容错误差策略。
- `--concurrency`：最大并发会话数；超出的包排队等待，不会被拒绝。
- `--item-budget`：单会话最大策略项数；超限时返回 `E_ITEM_BUDGET`，保留部分得分与账本片段。
- `--max-error-bound`：严格模式允许的包级误差上界，单位为 `score_1e-6`。
- `--config`：JSON 配置文件；显式命令行参数优先。
- `--keep`：保留已有数据库和输出。

默认每次重建 `samples`、数据库和输出，确保相同输入下报告字节级一致。

### `verify`

```sh
go run ./cmd/baselinebench verify --workdir work/verify --inject all
go run ./cmd/baselinebench verify --workdir work/verify \
  --inject unknown-unit,mul-overflow,filetime-overflow,version-gap,error-bound
```

每个用例都从独立的 v1 快照开始，断言：

- 实际业务码等于预期业务码。
- 失败注入不改变账本终态；数值类和越界类用例为 `rolled_back` 且无残留条目。
- 目标包快照版本保持 `v1 -> v1`，版本链活动摘要不变。
- 版本跳号追加被拒绝，并可从最近连续版本回放诊断。

## 输入格式

### JSON 基线快照

```json
{
  "schema": "baseline.workbench.snapshot/v1",
  "package_id": "normal",
  "sample_marker": "SAMPLE-FIXED-2024-01-01T00:00:00Z",
  "items": [
    {"id": "account.lockout_threshold", "unit": "minute", "value": "15", "weight": "1.0"},
    {"id": "log.level.score", "unit": "score", "value": "3.141592", "weight": "1.25"}
  ]
}
```

`value` 与 `weight` 推荐使用字符串以避免二进制浮点进入解析链；JSON 整数字面量也会按十进制文本解析。

### 长度前缀修复块

每行是“JSON 字节长度 + 冒号 + JSON + LF”：

```text
75:{"id":"account.lockout_threshold","unit":"minute","value":"15","weight":"1.0"}
```

声明长度不足以容纳 JSON 时返回 `E_POLICY_MALFORMED`，之前成功解析的块会保留。

### 合法单位

- 时长：`second/seconds/sec/s`、`minute/minutes/min/m`、`hour/hours/hr/h`、`day/days/d`。
- 时间戳：`filetime/filetime_100ns/filetime100ns`，值必须是 1601 纪元起的 100ns 整数。
- 分值：`score/points/point/microscore`，允许小数，落库截断到 `1e-6`。

时长与 FILETIME 必须是精确整数；分值最多保留六位小数，后续非零数字记为小于一个最低位的截断误差。未知单位返回 `E_UNIT_UNKNOWN`。

## 误差与评分

- `normalized`：时长/时间戳为整数秒，分值为整数微分值。
- `normalized_scaled`：统一比较口径；时长为 `秒 * 1e6`，分值为微分值。
- `weight_micros`：权重按 `1e-6` 定点化。
- 单项加权差：`(candidate.normalized_scaled - reference.normalized_scaled) * weight_micros / 1e6`。
- 包得分是各项加权差之和，单位为 `score_1e-6`。
- 包误差上界来自换算链截断、权重乘法和除法截断的逐项上界求和。
- 区间不重叠：`deterministic_deviation`；重叠：`uncertain_interval`，同时输出基线区间、候选区间和差值区间端点。

## 并发与预算

引擎使用固定大小 worker 池。作业先进入队列，再按 `MaxConcurrency` 消费；超过并发上限的包等待而非拒绝。每个包拥有独立的会话、账本和结果对象，汇总阶段按包路径稳定排序，因此部分失败不会污染其他会话。

`E_ITEM_BUDGET` 会话：

- 已处理的项保留在 `ledger`，终态为 `partial`。
- `deviation_score_micros`、误差上界和比较结果只覆盖已处理项。
- 不提交快照。

## 快照与恢复

SQLite 表 `snapshots` 以 `(package_path, version)` 为主键。每次提交在一个事务内完成：

- 期望版本必须是当前连续版本 `+1`。
- 保存父版本、父摘要、内容摘要、是否内容变化、是否整体替换、固定采样标记、完整归一化条目和诊断摘要。
- 内容摘要不同即 `content_changed=true`；若所有条目指纹均变化或前后项集合完全不相交，再标记 `whole_replacement=true`。

加载时逐条验证版本号、父版本、父摘要和载荷摘要。发现跳号或链损坏时：

- 返回/记录 `E_SNAPSHOT_VERSION`。
- 只保留最近连续前缀，活动快照回退到该前缀最后一项。
- 从快照载荷恢复该版本的得分、误差区间、比较分类、业务码和账本，报告中给出 `recovered_from_version` 与 `recovery_code`。

## 业务码

| 业务码 | 场景 |
| --- | --- |
| `E_POLICY_MALFORMED` | JSON/长度前缀/标识/权重/时长格式非法 |
| `E_UNIT_UNKNOWN` | 未登记单位 |
| `E_NUMERIC_OVERFLOW` | 定点乘法、加法、缩放或 FILETIME 整数溢出 |
| `E_ITEM_BUDGET` | 单会话策略项数超限 |
| `E_SNAPSHOT_VERSION` | 版本跳号、父链或摘要不一致 |
| `E_ERROR_BOUND` | 严格模式下包误差上界超过阈值 |

严格与容错的差异：严格模式下解析问题会终止评分并回滚账本；容错模式下会继续使用成功解析的项生成部分结果，但任何解析/数值业务码仍阻止快照提交。错误上界越界在严格模式返回 `E_ERROR_BOUND`，在容错模式接受并允许提交。数值溢出在所有模式下都是致命错误。

## 确定性报告

报告不含墙钟时间，只使用 `SAMPLE-FIXED-2024-01-01T00:00:00Z` 采样标记。包按相对路径、条目和比较按 ID、问题按来源与偏移、版本链按版本号稳定排序。相同代码、配置与输入下重复运行，样本和报告均字节级一致。

主要字段：

- `items[]`：归一化条目、原始单位/值、权重、换算链、来源偏移。
- `comparisons[]`：参考/候选区间、差值区间、加权偏差、加权误差上界、分类。
- `ledger`：逐次换算与加权截断记录、终态。
- `snapshot.chain`：版本、父版本、内容摘要、变化/整替标记。
- `summary.ranked_packages`：按偏差绝对值排序的终端摘要数据。

## 配置示例

```json
{
  "mode": "strict",
  "max_concurrency": 2,
  "max_items": 64,
  "max_error_bound_micros": 100
}
```

`verify` 的配置还支持 `inject` 数组，用于不传 `--inject` 时选择异常类别。

## 项目结构

```text
cmd/baselinebench/     CLI 入口
internal/app/          端到端编排、快照提交与恢复
internal/engine/       会话、评分、区间分类、预算控制
internal/fixed/        int64 定点数学与溢出检查
internal/units/        单位换算与换算链
internal/parse/        JSON 快照与长度前缀块解析
internal/policy/       内部模型、标识/权重校验、修复覆盖
internal/ledger/       会话误差账本
internal/snapshot/     SQLite 版本链
internal/report/       确定性 JSON 与终端摘要
internal/samples/      确定性样本生成器
internal/verify/       五类异常注入断言
vendor/                离线纯 Go SQLite 依赖
```

## 实测结果

本环境的完整验证记录见 `docs/VERIFICATION.md`。关键结果：

- `GOPROXY=off go test ./...` 全部通过。
- `GOPROXY=off go vet ./...` 无输出。
- Windows/amd64 交叉构建成功。
- 严格模式演示报告 SHA-256：`25ca84d784be48b64c9227c8224d026d95e2f53fc84373d18113e548a615402f`。
- 五类注入全部 `PASS`，验证报告 SHA-256：`a10c0cc81eeda0096f283ded3622e54cf058ce160d6ebd8b0218de6306891b89`。
- 连续两次运行演示与验证，报告摘要哈希完全一致。
