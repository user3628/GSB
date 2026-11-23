# 验收与实测记录

本文记录 `vpnbench` 在本地确定性样本上的实际验收结果。验收覆盖隧道选择、整数定点分摊、交互/结算逐条一致性、单调时钟延迟统计、SQLite 版本快照、强制终止恢复、并发会话控制、五类异常注入和报告字节一致性。

## 验收环境

| 项目 | 实测值 |
|---|---|
| 代码库根目录 | `/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-201` |
| Linux 构建环境 | WSL2，Ubuntu 用户空间 |
| Windows 环境 | Windows 11 原生进程，amd64 |
| 构建/测试 Go 版本 | `go1.27.4` |
| 最低目标版本 | Go 1.22，Windows amd64 |
| SQLite 驱动 | `modernc.org/sqlite v1.34.5`，纯 Go |
| CGO | `CGO_ENABLED=0` |
| 网络访问 | 构建和运行阶段均不需要；样本、参数与数据库均为本地文件 |
| 容器 | 未使用 Docker 或其他容器 |

Windows 验收路径包含空格（实测为 `C:\temp\vpnbench win final`），用于同时检查 PowerShell 参数传递和 SQLite 文件路径处理。

## 可复现命令

### Linux/WSL2

```bash
cd /home/zsl/feedback/uid37fecb20-target-mwnkf3ge-201

go test ./...
go test -race ./...
go vet ./...

CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
  go build -trimpath -o /tmp/vpnbench.exe ./cmd/vpnbench

./scripts/verify.sh
```

`scripts/verify.sh` 会构建纯 Go 二进制、重新生成样本、运行 baseline，并执行 `verify` 全量验收。也可以手工执行：

```bash
work="$(mktemp -d)"
CGO_ENABLED=0 go build -trimpath -o "$work/vpnbench" ./cmd/vpnbench
"$work/vpnbench" generate --out samples
"$work/vpnbench" run \
  --config samples/configs/workbench.json \
  --db "$work/workbench.db" \
  --report validation/workbench-report.json
"$work/vpnbench" verify \
  --samples samples \
  --work "$work/verify" \
  --out validation/acceptance-report.json
```

### Windows 11

在代码库根目录运行：

```powershell
$env:CGO_ENABLED = "0"
go env GOOS GOARCH CGO_ENABLED
go mod download
go test ./...
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

也可直接执行 `.\scripts\verify.ps1`。Windows 原生逐包测试已按以下顺序通过：

```text
decision policy requests monotonic scheduler engine store
app anomaly samplegen cli
```

## 实际验证结果

### 构建、测试与原生 Windows 运行

| 检查 | 结果 |
|---|---|
| `go test ./...` | PASS |
| `go test -race ./...` | PASS |
| `go vet ./...` | PASS |
| `CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build` | PASS |
| Windows 原生 `go test` 逐包执行 | 全部 PASS |
| Windows 原生 `generate` | 退出码 `0` |
| Windows 原生 `run` | 退出码 `0` |
| Windows 原生 `verify` | 退出码 `0` |
| 含空格 Windows 路径 | PASS |
| Linux 与 Windows 报告跨平台比较 | 字节完全一致 |

### 确定性报告哈希

以下哈希在 Linux 和原生 Windows 上分别实测得到，结果完全相同：

```text
a0574c1cee96394b093eb989b01fdbcbc7fd53d87cc38cde1f16fa9a6ff58d31  validation/workbench-report.json
911d685e3cc1f09bde11d82b86c391715987dff98b36cff4fdb489fcb7a3e784  validation/acceptance-report.json
86f07fe54bb033f158364c49fcd08865c95bbd750bc6dae928964d68a8332ce4  samples/manifest.json
```

baseline 在全新数据库上独立运行两次，`byte_identical=true`，包含 2 个会话、6 条请求。交互延迟使用固定单调时钟标记 `T_MONO_FIXED_250US_V1`，每次会话内按固定步长推进，不使用墙钟、随机数或真实调度耗时，因此相同输入可得到逐字节相同的报告。

### 五类异常注入

实测摘要来自 `validation/acceptance-report.json`。五类均通过，且每类都执行了对应快照保护和会话隔离断言。

| 样本 | 期望业务码 | 实测业务码 | 快照/隔离结果 |
|---|---|---|---|
| `fixed_overflow` | `E_FIXED_OVERFLOW` | `E_FIXED_OVERFLOW` | policy `[1]`，settlement `[1]`；安全请求仍为 `OK`；已提交策略快照未被污染 |
| `no_match` | `E_NO_TUNNEL` | `E_NO_TUNNEL` | policy `[1]`，settlement `[1]`；同批其他匹配请求不受影响 |
| `snapshot_gap` | `E_SNAPSHOT_VERSION` | `E_SNAPSHOT_VERSION` | policy 链 `[1,3]` 检出缺 2，回退到版本 `1`；settlement 链连续 |
| `whole_replacement` | `E_POLICY_REPLACED` | `E_POLICY_REPLACED` | policy `[1,2]`，`change_type=WHOLE_REPLACEMENT`；记录替换前后策略集合 |
| `session_race` | `E_REQUEST_BUDGET`、`E_SESSION_CONFLICT` | `E_REQUEST_BUDGET`、`E_SESSION_CONFLICT` | 竞争会话被隔离；其余两个会话均为 `FINALIZED` 且请求为 `OK` |

五类样本的共同断言结果：

- `expected_business_codes`: PASS
- `snapshot_guard`: PASS
- `other_sessions_unaffected`: PASS
- `interactive_settlement_consistency`: PASS

`snapshot_gap` 的回退内容摘要保持为已提交版本 1 的摘要；`whole_replacement` 的替换前身份集合为原隧道集合，替换后为新策略集合，报告保留了前后摘要。

### 强制终止与恢复

验收通过真实子进程在 SQLite 事务写入后、提交前调用 `os.Exit(86)`。重启后只依赖 SQLite 已提交事务恢复状态。

| 场景 | 终止退出码 | 终止后观测 | 重启后观测 |
|---|---|---|---|
| 策略加载提交前终止 | `86` | policy `[1]`，未提交版本不可见 | policy `[1,2]`；settlement `[1,2,3,4]` |
| 结算提交前终止 | `86` | policy `[1]`，settlement `[]`，windows `0` | policy `[1,2]`，settlement `[1,2]`，windows `2` |

结论：

- 已提交快照和窗口不回退。
- 未提交事务整体丢弃，不留下部分策略、部分请求结果或半个结算。
- 丢弃的窗口可在重启后重新结算。
- 恢复过程继续校验版本链；跳号仍返回 `E_SNAPSHOT_VERSION` 并回退到最近连续版本。

## 报告与验收入口

- `validation/workbench-report.json`：baseline 工作台报告，包含会话裁决、交互/结算逐条对账、误差账本、延迟统计、并发指标和快照版本链。
- `validation/acceptance-report.json`：全量验收报告，包含 baseline 双跑哈希、五类异常断言、异常会话工作台报告和强制终止恢复结果。
- `samples/manifest.json`：确定性样本清单及哈希。

验收判定以 `acceptance-report.json` 的 `passed=true` 为准。报告数组按会话标识、流标识、请求序号、版本号和业务码稳定排序，不包含绝对路径、随机值或墙钟时间。
