# VPN 隧道转发一致性裁决工作台

这是一个完全离线、可用 Windows amd64 直接构建的 Go 工作台。它从本地策略 JSON 与带长度前缀的转发请求块出发，完成隧道选择、整数定点负载分摊、交互侧/结算侧逐条一致性裁决、单调时钟延迟统计、SQLite 版本快照、崩溃恢复、并发会话控制和确定性 JSON 报告。

SQLite 驱动为纯 Go 的 `modernc.org/sqlite`，不依赖 CGO 或系统 SQLite 库。运行工作台时不访问网络。

## 快速开始

要求 Go 1.22 或更高版本。Windows PowerShell 示例：

```powershell
$env:CGO_ENABLED = "0"
go mod download
go build -trimpath -o .\vpnbench.exe .\cmd\vpnbench

.\vpnbench.exe generate --out .\samples
.\vpnbench.exe run `
  --config .\samples\configs\workbench.json `
  --db .\tmp\workbench.db `
  --report .\validation\workbench-report.json

.\vpnbench.exe verify `
  --samples .\samples `
  --work .\tmp\verify `
  --out .\validation\acceptance-report.json
```

Linux/macOS 等价命令见 `scripts/verify.sh`；Windows 可直接运行 `scripts/verify.ps1`。`generate`、`run`、`inject` 都只读取本地文件。

## CLI

```text
vpnbench generate  --out DIR
vpnbench run       --config FILE --db FILE --report FILE
                   --policy FILE --requests FILE --session ID --db FILE --report FILE
vpnbench inject    --params FILE --db FILE --out FILE
vpnbench report    --db FILE --out FILE
vpnbench recover   --db FILE [--out FILE]
vpnbench correction --db FILE --session ID --stream ID --seq N
vpnbench verify    --samples DIR [--work DIR] --out FILE
```

`run` 还支持 `--concurrency`、`--request-budget` 和 `--latency-clock fixed|real`。默认使用 `fixed` 单调时钟标记，保证相同输入的报告字节一致；`real` 使用真实单调时钟，适合诊断但不承诺跨运行字节一致。退出码约定：

- `0`：命令完成，且 `inject`/`verify` 的所有断言通过。
- `1`：报告已导出，但出现 `E_RESULT_MISMATCH` 或验收断言失败。
- `2`：参数、文件或解析错误；stderr 输出包含业务码与原始偏移的 JSON。

## 输入格式

### 策略表

支持顶层数组，或包含 `policies` 数组的对象：

```json
{
  "schema": "vpnbench.policy/v1",
  "policies": [
    {"tunnel_id":"tun-alpha","destination_cidr":"10.0.0.0/8","priority":100,"bandwidth":7,"forwarding_cost":2},
    {"tunnel_id":"tun-beta","destination_cidr":"10.0.0.0/8","priority":100,"bandwidth":11,"forwarding_cost":3}
  ]
}
```

加载时会规范化 CIDR、按隧道标识排序并计算 SHA-256 摘要。`tunnel_id` 全局唯一；`priority`、`bandwidth`、`forwarding_cost` 必须是 int64 范围内的非负整数。非法 CIDR、重复标识、负值、非整数、未知/重复字段等均返回 `E_POLICY_INVALID`，`offset` 指向原始输入中的相关字段或条目。

### 转发请求块

每块由一个十进制字节长度前缀、换行和恰好该字节数的文本正文组成：

```text
<body-byte-length>
stream_id=flow-a
request_seq=1
src=10.1.0.10
dst=10.2.0.9
packet_bytes=1200
```

正文也接受 `flow_id`、`seq`、`source`、`destination`、`packet_length` 别名与 CRLF。解析器保留到达顺序与原始偏移；结算阶段再按 `(stream_id, request_seq)` 稳定排序。长度不符、IP 非法、序号非正、包长为负、字段缺失/重复/未知都返回 `E_REQUEST_INVALID`。

### 运行配置

```json
{
  "schema": "vpnbench.run/v1",
  "policy_file": "../policies/base.json",
  "concurrency_limit": 2,
  "latency_clock": "fixed",
  "request_budget": 64,
  "sessions": [
    {"session_id":"session-a","requests_file":"../requests/base.txt","request_budget":64},
    {"session_id":"session-b","requests_file":"../requests/single.txt","request_budget":64}
  ]
}
```

相对路径以配置文件所在目录为基准。会话有独立的结果列表、修正轨迹和误差账本；超出并发上限的会话等待，不被直接拒绝。

## 决策与分摊口径

1. 所有目的网段包含请求目的地址的策略都是候选。
2. 取候选中的最高优先级；该优先级内的隧道构成活动集合。较低优先级仅作为回退策略，不参与本次权重。
3. 活动集合按带宽降序、隧道标识升序稳定排序。第一条是主出口。
4. 使用 `scale = 1_000_000` 的整数定点权重：`floor(bw_i * 1_000_000 / sum_bw)`。全程禁止浮点。
5. 初始截断后的整数余量按“小数余数最大优先、其次候选顺序”逐个补偿 1 个 micro-unit，最终权重和恒为 `1_000_000`。
6. 每个分配写一条账本记录（未截断时余数与误差均为 0）：精确分子、公共分母、初始权重、余数分子、补偿量和严格小于 1 micro-unit 的截断误差上界。按流聚合后 `error_upper_bound_micros` 是各截断上界之和，因此有界。
7. 无匹配返回 `E_NO_TUNNEL`；`bandwidth * scale` 或带宽求和溢出返回 `E_FIXED_OVERFLOW`，且不写快照或账本。

样本中 `tun-beta(11)` 与 `tun-alpha(7)` 的权重为 `611111` 与 `388889`，余数补偿只发生一次。

## 交互侧、结算侧与修正

请求到达时立即执行决策并保存为 `interactive_preliminary`；窗口关闭后重新计算整批请求并按 `(stream, seq)` 输出 `settlement_final`。报告同时保存 `interactive_reconciled`，即应用结算修正后的交互侧视图。

- 乱序或后到请求导致结算顺序调整时，写入 `C_ORDER_NORMALIZED` 修正轨迹。
- 结算重算改变决策时，写入 `C_DECISION_RECONCILED`，先记录修改前后决策，再以结算结果完成逐条对账。
- 对账基准为 `reconciled_interactive_vs_settlement_final`；任一条无法对账时产生 `E_RESULT_MISMATCH`，并对准具体会话、流标识与请求序号。
- 修正记录持久化到 `corrections` 表，可用 `vpnbench correction` 查询。
- 同一会话出现重复 `(stream, seq)` 返回带定位的 `E_RESULT_MISMATCH`，已处理的前序结果保留。

## 延迟统计

交互侧每次裁决都以接收前和返回后的时钟读数做差值，不读取墙钟时间戳。`--latency-clock fixed` 为每个会话建立独立虚拟单调时钟，每次裁决推进固定 `250us`，报告标记为 `T_MONO_FIXED_250US_V1`；`--latency-clock real` 使用 Go 的单调时钟读数，标记为 `T_MONO_REAL_V1`。验收默认使用 fixed，因此报告不存在运行时间戳或随机采样。

统计按 `(tunnel_id, result_code)` 聚合样本数、整数平均、nearest-rank P95（`ceil(0.95*n)`）和最大值。报告不含墙钟字段，重复运行字节一致。

## 快照、版本链与恢复

SQLite 表 `snapshots` 同时保存 policy 与 settlement 两类快照，每类有独立递增版本序列；旧版本整体保留。

- 每次策略加载在一个事务中写入 `snapshots` 与版本序列。
- 每个窗口结算在一个事务中写入窗口进度、逐条结果、修正、账本、结算快照与版本序列。
- 内容摘要基于完整快照内容。比较版本时同时检查内容摘要与身份集合：
  - `SAME_CONTENT`：内容完全一致。
  - `WHOLE_REPLACEMENT`：内容变化且新旧身份集合无交集。
  - `PARTIAL_REPLACEMENT`：身份集合部分重叠且内容变化。
  - `IN_PLACE_REPLACEMENT`：身份集合相同但内容变化。
- policy 身份为隧道标识；settlement 身份为 `session/stream/seq`。整体替换记录替换前后的摘要、版本和身份集合。
- 加载时从版本 1 检查连续链。发现跳号返回 `E_SNAPSHOT_VERSION`，并回退到最近连续版本；若不存在连续前缀则拒绝加载。
- 报告和 `recover` 均列出完整链、缺失版本和回退版本。

强制终止测试通过子进程在事务写入后、提交前执行 `os.Exit(86)`：

- 未提交的策略快照不会出现，重启后版本号可继续使用。
- 未提交的窗口、结果和结算快照整体丢弃，重启后可重新结算。
- 已提交窗口与快照保留，不回退。

崩溃点由环境变量控制，不在代码中写死测试分支：

```powershell
$env:VPNBENCH_CRASH_POINT = "policy_after_write"       # 或 settlement_after_write
.\vpnbench.exe run --config .\samples\configs\workbench.json --db .\tmp\crash.db --report .\tmp\crash.json
Remove-Item Env:\VPNBENCH_CRASH_POINT
.\vpnbench.exe recover --db .\tmp\crash.db
.\vpnbench.exe run --config .\samples\configs\workbench.json --db .\tmp\crash.db --report .\tmp\retry.json
```

## 并发与资源控制

- `concurrency_limit` 控制同时裁决的会话数；超限会话进入等待队列。
- 会话标识在活跃或等待期间唯一，重复并发进入返回 `E_SESSION_CONFLICT`，并记录会话标识与请求序号；先进入的会话结果不受影响。
- 每会话 `request_budget` 限制处理数量。超限时状态变为 `TERMINATED_REQUEST_BUDGET`，写入 `E_REQUEST_BUDGET` 并保留所有已处理请求的初判结果。
- 报告 `concurrency` 提供 limit、在途计数、峰值在途、峰值排队、累计准入与冲突列表。
- 每个会话使用自己的策略副本、结果集合、修正列表和误差账本，不共享可变决策状态。

## 业务码

| 业务码 | 含义 |
|---|---|
| `OK` | 正常裁决 |
| `E_POLICY_INVALID` | 策略非法，含原始偏移 |
| `E_REQUEST_INVALID` | 请求块非法，含原始偏移 |
| `E_NO_TUNNEL` | 目的地址无匹配隧道 |
| `E_FIXED_OVERFLOW` | 整数定点乘法或加和溢出 |
| `E_RESULT_MISMATCH` | 交互/结算无法逐条对账，含会话、流、序号 |
| `E_SNAPSHOT_VERSION` | 快照版本链跳号，报告回退版本 |
| `E_REQUEST_BUDGET` | 单会话请求预算耗尽 |
| `E_SESSION_CONFLICT` | 同会话并发冲突 |
| `E_POLICY_REPLACED` | 检出策略整体替换，属于非致命检测码 |
| `E_INTERNAL` | 本地 I/O、事务或运行时故障 |
| `C_ORDER_NORMALIZED` | 结算规范化乱序请求的修正码 |
| `C_DECISION_RECONCILED` | 结算重算改变初判的修正码 |

## 确定性报告

报告字段顺序由 Go 结构固定，数组按会话标识、流标识、请求序号、版本号或业务码稳定排序。报告不含绝对路径、墙钟时间和随机值。相同输入、相同构建与全新数据库重复运行会得到字节级一致的 JSON；`verify` 会实际双跑并比较两次完整报告字节。

`run` 输出本次调用的会话结果；`report --db` 会从数据库重建数据库内每个会话的最新已提交窗口。二者在全新数据库、单次运行且无后续提交时一致。

报告主要包含：

- `snapshots.policy` 与 `snapshots.settlement` 的完整版本链、缺失版本、回退版本与替换摘要。
- 每会话 `summary`、逐条 `interactive_preliminary` / `interactive_reconciled` / `settlement_final`、一致性对账。
- `error_ledger` 与 `error_by_stream`。
- `latency_mode`、`latency_marker`、样本和聚合统计。
- `corrections`、`business_codes` 与并发观测指标。

## 五类异常注入

`generate` 产出以下参数文件，`inject` 用外部 JSON 参数驱动，不修改源码：

| 样本 | 断言 |
|---|---|
| `no_match.json` | `E_NO_TUNNEL`；无匹配请求不影响同批其他匹配请求；快照链连续 |
| `fixed_overflow.json` | `E_FIXED_OVERFLOW`；安全请求仍为 `OK`；已提交策略快照未被污染 |
| `snapshot_gap.json` | 制造版本跳号后 `E_SNAPSHOT_VERSION`；回退到连续版本 1；回退内容摘要保持原值 |
| `whole_replacement.json` | 检出 `WHOLE_REPLACEMENT` 并产生 `E_POLICY_REPLACED`；记录替换前后策略集合 |
| `session_race.json` | `E_SESSION_CONFLICT` 与 `E_REQUEST_BUDGET`；其余会话仍完成且逐条一致 |

`vpnbench verify` 会重新生成样本、双跑 baseline、执行全部五类异常，并以子进程实际强制终止验证两类事务恢复。

## 测试与验收

```bash
go test ./...
go test -race ./...
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -o /tmp/vpnbench.exe ./cmd/vpnbench
```

本仓库的 `validation/` 保存了一次实际运行的 baseline 报告、五类异常验收报告与验证说明。详见 `docs/ACCEPTANCE.md`。

## 目录

```text
cmd/vpnbench/           CLI 入口
internal/policy/        策略解析、规范化、摘要
internal/requests/      长度前缀请求块解析
internal/decision/      候选选择、定点权重、误差账本
internal/engine/        交互/结算、修正、预算、一致性
internal/monotonic/     单调时钟与延迟聚合
internal/scheduler/     并发会话门与等待队列
internal/store/         SQLite 快照、链校验、事务与恢复
internal/report/        确定性报告模型
internal/samplegen/     内置确定性样本生成器
internal/anomaly/       五类异常注入与断言
internal/verify/        双跑、异常、崩溃恢复验收
internal/cli/           命令分发与 JSON 错误输出
```
