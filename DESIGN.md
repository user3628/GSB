# 规则集一致性裁决引擎 — 技术说明

## 概述

为多分支企业边界防火墙运维团队实现的规则集一致性裁决引擎。纯 Go 实现
（Go 1.22+，`database/sql` + 纯 Go SQLite 驱动 `modernc.org/sqlite`，无
CGO、无容器、无外部网络服务），全部验证通过 `go test` 与本地 CLI 完成。

## 代码结构

| 路径 | 职责 |
| --- | --- |
| `internal/codes` | 业务码与结构化错误（携快照标识/规则序号/来源偏移） |
| `internal/fixed` | 整数定点运算（scale=1e-6 的 int64）、十进制解析、溢出检查乘法、CIDR→128 位整数视图 |
| `internal/model` | 规范化规则与探测流量模型 |
| `internal/parse` | JSON 快照与长度前缀文本块两种来源的解析、校验与偏移定位 |
| `internal/ledger` | 追加式截断误差账本（1e-12 单位整数记账，可聚合有界上界） |
| `internal/engine` | 单快照裁决、多快照分叉定位、误差传播分类 |
| `internal/store` | SQLite 版本化快照/裁决/账本持久化，版本链校验，整体替换识别 |
| `internal/compare` | 并发会话管理：并发上限、规则预算、等待阈值、在途可观测 |
| `internal/report` | 确定性 JSON 报告（稳定排序、固定采样时间标记） |
| `internal/samples` | 确定性本地样本生成器 |
| `internal/app` | 场景编排与异常注入入口（仅通过参数/输入注入） |
| `cmd/fwadjudge` | CLI：`gen` 生成样本，`run` 执行场景并导出报告 |

## 业务码

`OK`、`E_RULE_INVALID`、`E_FIXED_OVERFLOW`、`E_RULE_DIVERGENCE`、
`E_SNAPSHOT_VERSION`、`E_RULE_BUDGET`、`E_COMPARE_CONTENTION`。

## 关键设计

### 1. 解析与规范化（需求 1）
- JSON 文件（`{"snapshot","rules":[...]}`）与文本块（8 位十进制长度前缀 +
  JSON 负载）统一为内部模型；解析时校验 CIDR、端口区间上下界
  （0≤lo≤hi≤65535）、协议/方向/动作枚举、规则序号唯一性。
- 非法条目产生 `E_RULE_INVALID` 并携带源字节偏移（JSON 为规则对象偏移，
  文本块为块起始偏移），合法规则不受污染继续入库；重复序号条目被剔除。

### 2. 整数定点换算（需求 2）
- CIDR → `[start,end]` 128 位无符号整数视图（IPv4 映射进 v6 空间，
  前缀按 96+bits 处理）；端口区间 → scale=1e-6 定点（精确无误差）。
- 流量权重/计数按十进制字符串解析为 int64 定点，第 7 位起的小数被截断，
  截断误差以 1e-12 单位逐次写入误差账本；乘法用 `math/bits` 做 128 位
  中间积，溢出返回 `E_FIXED_OVERFLOW`，余数（截断误差）同样记账。
- 误差按"先暂存、成功后一次性提交"入账：溢出或失败的路径不会留下部分
  写入；快照级上界 = 各次误差之和（每次 < 1 个 1e-6 单位，故有界）。

### 3. 裁决与分叉定位（需求 3）
- 单快照内按优先级从高到低匹配（方向/协议支持 any，地址段与端口区间按
  整数视图比较），记录命中规则序号；同优先级动作冲突 → 隐式分叉，取
  序号最小者保证确定性。
- 两快照裁决不一致时输出 `E_RULE_DIVERGENCE` 与两侧命中信息；分叉字段
  按固定顺序 `index→priority→action→direction→protocol→src→dst→
  port_range` 定位首个差异字段。

### 4. 误差传播分类（需求 4）
- 每快照累计命中规则的定点流量统计与账本误差上界 B，构成区间
  `[v-B, v+B]`；两快照区间不重叠 → `deterministic`（确定性分叉），
  重叠 → `uncertain`（不确定分叉）。判定不依赖墙钟与随机数，同输入
  重复裁决逐字节一致（引擎测试与报告字节比对双重验证）。

### 5. 版本化快照与整体替换（需求 5）
- 每轮解析、每轮裁决分别在单个 SQLite 事务内提交递增版本；旧版本整体
  保留可回放。加载时校验版本链连续性，跳号返回 `E_SNAPSHOT_VERSION`
  并回退到最近连续版本。
- 整体替换识别基于内容哈希集合比对：即使版本号递增，若上一版本无任何
  规则内容存活，则标记 `replaced=true` 并写入报告。

### 6. 并发资源控制（需求 6）
- 会话间规则状态与误差账本完全隔离（每会话独立 `Session`/`Ledger`）。
- 并发度、单会话规则预算、等待阈值均可配置；超并发任务进入等待队列，
  超等待阈值返回 `E_COMPARE_CONTENTION`；规则数超限以 `E_RULE_BUDGET`
  终止该快照并保留已解析前缀；`Manager.Inflight()` 提供在途会话观测；
  所有错误携带快照标识与规则序号。

### 7. 崩溃恢复（需求 7）
- 依赖 SQLite 已提交事务恢复：已提交快照/裁决/账本不回退，未提交变更
  整体丢弃，快照可重新比对提交。恢复后跳号仍以 `E_SNAPSHOT_VERSION`
  报告并进入最近连续版本。报告只在正常退出路径写出；强制终止（测试以
  子进程 `os.Exit(2)` 模拟 kill）不留下报告文件。

### 8. 异常注入与确定性报告（需求 8）
- 六类注入全部通过样本文件与 CLI 参数完成，不修改源码：
  CIDR 非法、端口区间倒置（→`E_RULE_INVALID`）、定点乘法溢出
  （→`E_FIXED_OVERFLOW`）、版本跳号（→`E_SNAPSHOT_VERSION`）、整体替换
  （→`replaced=true`）、同优先级动作冲突（→`E_RULE_DIVERGENCE` 隐式）。
- 报告含各快照规则摘要（序号/动作/优先级/哈希）、分叉定位与分类、误差
  上界区间、版本链与业务码；按快照标识、规则序号、业务码稳定排序；
  时间字段固定为 `SAMPLE@FIXED-0`。

## 使用

```sh
go build ./cmd/fwadjudge
./fwadjudge gen -dir samples
./fwadjudge run -db fw.db -samples samples -report report.json \
    -scenario all -max-concurrency 4 -rule-budget 1000 -wait-threshold 2s
```

`-scenario` 可取 `all|normal|diverge|conflict|bad-cidr|port-invert|overflow|version-gap|replace`。

## 实际验证结果（本机，Go 1.27.4 linux/amd64）

- `go test ./...`：全部通过（fixed/parse/engine/store/compare/app）。
  覆盖：定点截断记账与溢出、双来源解析与偏移、优先级裁决与隐式分叉、
  分叉字段定位、确定/不确定分类及重复一致性、版本链跳号回退、整体替换
  识别、kill 恢复（子进程模拟）、规则预算与并发争用、六类注入业务码、
  注入隔离性（溢出快照账本 0 污染、其余快照裁决不受影响）、报告字节级
  一致。
- CLI 双跑（`run1`/`run2` 独立数据库）：`report.json` 经 `cmp` 确认字节
  级一致。
- 各场景单独运行输出：
  `normal→OK`、`diverge→E_RULE_DIVERGENCE`、`conflict→E_RULE_DIVERGENCE`、
  `bad-cidr→E_RULE_INVALID`、`port-invert→E_RULE_INVALID`、
  `overflow→E_FIXED_OVERFLOW`、`version-gap→E_SNAPSHOT_VERSION`、
  `replace→OK`（报告内 `repl` v2 `replaced=true`）。
- 报告中可观测：`branch-b/hq` 探针 p1 为 `deterministic`（区间
  [11.999999,12.000001] 与 [9.999999,10.000001] 不重叠），p3 与
  `branch-c/hq` p1（字段 `action`）为 `uncertain`；`gap` 快照版本链
  `[1,3]`、`latest_continuous=1`、`E_SNAPSHOT_VERSION`。
