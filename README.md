# flowtrace — 敏感信息流追踪管线与结果验证工具

为内部数据平台合规团队构建：沿事件链追踪敏感标记（证件号 / 银行卡 / 密钥材料）
从源头字段的流向，判定每条传播路径是否越出授权边界，并对判定结果做可复核的
一致性验证。纯 Go 实现，SQLite 使用纯 Go 驱动 `modernc.org/sqlite v1.34.5`
（见 `go.mod`），不依赖容器、网络或外部数据。

## 构建与运行

要求 Go 1.22+（Windows PowerShell 与 Linux/macOS 命令相同）：

```powershell
go mod download
go build -o flowtrace ./cmd/flowtrace

# 生成本地确定性样本（程序内置生成器，不访问网络）
./flowtrace gen -dir artifacts/samples

# 基线运行：解析 -> 追踪 -> 裁决 -> 快照 -> 报告
./flowtrace run -dir artifacts/samples -db artifacts/trace.db -report artifacts/report.json

# 异常注入（仅通过参数注入，不修改源码）
./flowtrace run -dir artifacts/samples -db artifacts/inj.db -report artifacts/inj.json `
    -inject reorder,cycle,escape,snapshot-gap,contention

# 强制终止恢复：提交 2 个窗口后模拟 kill（退出码 2），再恢复
./flowtrace run -dir artifacts/samples -db artifacts/crash.db -report artifacts/c.json -crash-after 2
./flowtrace run -dir artifacts/samples -db artifacts/crash.db -report artifacts/resumed.json -resume

# 一致性核验
./flowtrace verify -report artifacts/report.json

# 测试
go test ./...
```

可调资源预算：`-max-concurrent`（并发窗口上限，超出进入等待队列）、
`-max-events`（单窗口事件预算，超限以 `E_EVENT_BUDGET` 终止并标记不完整）、
`-mem-budget`（单会话内存预算，超出的窗口排队等待）。

## 架构

| 包 | 职责 |
|---|---|
| `internal/codes` | 稳定业务码：`E_UNKNOWN_VERSION` / `E_TRUNCATED` / `E_EVENT_FIELD_MISSING` / `E_FLOW_ESCAPED` / `E_SNAPSHOT_VERSION` / `E_EVENT_BUDGET` |
| `internal/parse` | 二进制日志（魔数 `SFEV` + 版本头，v1/v2 两种字段布局）与 JSONL 摘要的统一归一化；错误事件不进入已归一化集合 |
| `internal/policy` | 带版本号的授权策略快照（敏感字段声明、授权边界、聚合脱敏标记） |
| `internal/trace` | 传播核心：按（事件序号， 逻辑时间）双序推进；跨表复制/跨服务导出全量传播，聚合写按策略脱敏；环路只增不减天然终止；越界产生 `E_FLOW_ESCAPED` 并保留最小越界路径 |
| `internal/adjudicate` | 交互侧（到达序）与结算侧（规范序）双轨裁决；修正仅向前推进版本并输出修正记录，两侧按（事件序号， 目标字段）逐条一致 |
| `internal/latency` | 单调逻辑时钟延迟采样（不用墙钟，固定采样标记），按窗口+结果码聚合 count/avg/P95/max，重复运行字节级一致 |
| `internal/snapshot` | SQLite 单事务提交递增版本快照；加载校验版本链连续性与内容哈希链，识别整体替换，跳号返回 `E_SNAPSHOT_VERSION` 并回退到最近连续版本 |
| `internal/scheduler` | 并发窗口调度：会话间标记状态隔离，超限窗口排队而非拒绝，预算终止的窗口不计入一致性比对 |
| `internal/samplegen` | 确定性样本生成器（策略快照 v1/v2、二进制 v1/v2 日志、JSONL、manifest） |
| `internal/pipeline` | 编排：加载 -> 注入 -> 调度 -> 提交 -> 崩溃/恢复 -> 报告 |
| `internal/report` | 确定性 JSON 验证报告（窗口/事件稳定排序，重复运行字节级一致） |

## 关键设计

- **双序推进**：结算侧始终按 `(Seq, LogicalTime)` 规范序重放；交互侧按到达序
  增量更新。乱序到达导致分叉时，结算侧发出 `Correction`（FromCode→ToCode，
  版本单调递增），交互侧展示对应同一判定版本，禁止静默改写。
- **环路安全**：每个字段的标记集合只增不减，`(字段, 标记)` 空间有限，传播必然
  终止；回边事件为无操作，不会无限扩散。
- **崩溃恢复**：窗口结算在单个 SQLite 事务内提交快照与进度行。模拟 kill 后重启，
  已提交窗口不回滚、未提交窗口整体丢弃并重追；恢复后报告与无崩溃运行字节级一致。
- **确定性**：延迟统计来自事件内容的固定采样标记（单调逻辑时钟），报告所有切片
  预排序、`json.MarshalIndent` 输出，同一输入重复运行字节级一致。

## 实际验证结果（本仓库 `artifacts/run/`）

- `go test -race ./...`：全部 9 个测试包通过（解析、传播、裁决、延迟、快照、
  调度、端到端管线）。
- 基线：3 窗口，5 条 `E_FLOW_ESCAPED`（含最小越界路径），`overall_consistent=true`。
- 注入 `reorder,cycle,escape,contention`：9 窗口（含 6 个竞争窗口排队完成）、
  12 条越界判定、产生前向修正记录，两侧逐条一致。
- 注入 `snapshot-gap`：链校验返回 `E_SNAPSHOT_VERSION` 并回退到最近连续版本，
  其他窗口链不受污染。
- `-crash-after 2` 后 `-resume`：恢复报告与干净运行 `cmp` 字节级一致
  （`RESUME-EQUALS-CLEAN`）；同输入重复运行报告字节级一致（`REPEAT-IDENTICAL`）。
