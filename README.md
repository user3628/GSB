# blocksnap

面向本地块存储的 COW 快照链增量核算与回滚裁决组件。组件以 SQLite 持久化卷、块版本、快照增量引用、指标投影、幂等请求与意图日志，并提供本地 HTTP API。数值路径只使用整数定点与 `math/big`，不经过浮点。

## 构建与运行

- Go 版本：`go 1.22` 及以上。
- SQLite 驱动：固定为纯 Go 的 `modernc.org/sqlite v1.35.0`，不需要 CGO。
- 无 Docker、无外部服务依赖。

```bash
go test ./...
go vet ./...
go build -o ./blocksnapd ./cmd/blocksnapd
./blocksnapd -addr 127.0.0.1:8080 -db ./data/blocksnap.db
```

启用本地故障注入入口时增加 `-fault-injection`。该入口仅用于演示/测试强制终止，默认关闭。

```bash
go run ./cmd/blocksnapd \
  -addr 127.0.0.1:8080 \
  -db ./data/blocksnap.db \
  -fault-injection
```

Unix 演示：

```bash
./scripts/demo.sh
```

Windows PowerShell 演示：

```powershell
./scripts/demo.ps1
```

## 目录

- `cmd/blocksnapd`：HTTP 监听入口；启动时先执行恢复扫描。
- `cmd/demo`：演示调用方；固定样本、全量+两次增量、合并、裁决、强制退出 86、重启恢复。
- `internal/fixed`：`10^-6` 定点整数、半远离零舍入、补偿累计、容差边界。
- `internal/metrics`：纯函数快照链解析与派生指标；稳定排序、精确累计、裁决。
- `internal/store`：SQLite 事务、版本历史、增量引用、幂等、意图和恢复。
- `internal/httpapi`：本地 HTTP 合约与统一 JSON 错误。
- `internal/sample`：固定 seed 的确定性块内容生成。

## 模型与不变量

### 卷、块和版本

卷创建时固定 `block_count`、`block_size` 和每块回写开销。每块的当前状态存在 `blocks` 表；不可变历史存在 `block_versions` 表：

- 初始块为版本 `0`、零内容摘要。
- 每次写入创建 `current_version + 1`，版本单调递增。
- 写入必须携带完整块内容、声明 SHA-256 与 `expected_version`。
- 内容摘要不一致返回 `E_BLOCK_CHECKSUM`，块状态不变。
- 期望版本落后返回 `E_BLOCK_CONFLICT`；同一块并发写入使用相同期望版本时只有一个成功。

### 快照

- 全量快照：保存全部块的当前版本引用，并记录 `consistency_seq`。
- 增量快照：只保存与其父快照有效版本不同的块，并保存 `parent_id`。
- 有效引用集合由根到叶依次叠加增量引用得到。
- 删除仍有未合并子快照的父快照返回 `E_SNAPSHOT_REFERENCED`。
- 合并 `child` 到其直接父快照时，在同一事务内重写父快照的增量引用、把孙快照重新挂到父快照、合并整数权重并删除已合并子节点。

### 回滚

回滚按目标快照的有效版本集合读取历史内容，为存在摘要差异的块创建新的更高版本。因此“回滚”本身也遵守版本单调递增。快照一致度按摘要比较，因此回滚完成后目标快照的内容一致度为 `1.000000`。

## 派生指标的精确定义

设 `B` 为块字节大小，`N` 为卷块数，`D` 为目标快照链的有效 `(block_no, version)` 去重集合：

- 已用容量：当前版本号大于 0 的块数乘 `B`，按块逐项补偿累计。
- 卷容量：`N * B`。
- 已用容量占比：`usedBytes / capacityBytes`。
- 链物理存量：`len(D) * B`，按去重版本稳定排序逐项补偿累计。
- 回写块：目标有效摘要与当前块摘要不同的块。
- 回写字节：`writebackBlocks * B`。
- 回滚余量：`max(chainStorageBytes - writebackBytes, 0) / chainStorageBytes`。
- 单快照一致度：摘要与当前卷一致的块数 / `N`。
- 跨快照加权一致度：`sum(snapshot.weight * snapshot_consistency) / sum(snapshot.weight)`。
- 回滚代价（字节等价）：`writebackBytes + writebackBlocks * perBlockOverheadBytes`。
- 裁决：余量低于下限或代价高于预算时拒绝；带相对容差时边界内接受。

这些定义全部写入响应，并附带精确分子、分母、定点结果、除法余数与误差上界。具体公式集中在 `internal/metrics/metrics.go`。

## 定点、舍入与累计

- 比例统一表示为 `10^-6` 的整数单位，JSON 同时返回 `units` 与固定 6 位小数 `decimal`。
- 舍入方式固定为 `half-away-from-zero`。
- 每次除法记录 `exact_numerator`、`exact_denominator`、`remainder` 与单步误差上界 `0.500000`。
- 多块字节数和多快照权重使用 `big.Int` 补偿累计器；整数域等价于精确 Kahan 补偿，表示误差上界为 0。
- 加权一致度使用“逐项商 + 余数携带”的等价补偿路径；最终结果与精确加权分子一次舍入一致，避免逐项舍入漂移。
- 诊断接口同时给出朴素逐项舍入路径。对固定样本 `weights=[1,1,1]`、`value_units=[1,1,1000000]`：
  - 精确/补偿结果：`333334` 单位；
  - 朴素逐项结果：`333333` 单位；
  - 朴素误差上界：`1.500000` 单位（3 项 × 0.5）；
  - 补偿最终舍入上界：`0.500000` 单位。
- 诊断容量样本 `9007199254740993 + 1 + 1 + 1` 覆盖大数小数混合；整数补偿和朴素和均为 `9007199254740996`，差值与误差上界均为 `0`。
- 代码库中不存在 `float32`/`float64` 指标运算路径。

## 容差裁决

设下限为 `floor`、预算为 `budget`、相对容差为 `tol`：

- 有效下限：`floor - ceil(|floor| * tol)`。
- 有效上限：`budget + ceil(budget * tol)`。
- 仅当 `margin < effectiveLower` 或 `cost > effectiveUpper` 时拒绝；边界值视为满足。
- 候选按 `snapshot_id` 字典序稳定排序；多个候选同时满足时选择字典序最小者，与请求枚举顺序无关。

## HTTP API

所有失败响应格式：

```json
{
  "error": {
    "code": "E_BLOCK_CONFLICT",
    "message": "block version changed",
    "context": {"block_no": "3", "current_version": "4"}
  }
}
```

核心业务码：

| 业务码 | 场景 |
|---|---|
| `E_BLOCK_CHECKSUM` | 声明摘要与实际内容不一致；块状态不变 |
| `E_BLOCK_CONFLICT` | `expected_version` 与当前版本不一致 |
| `E_CONFLICTING_REQUEST` | 相同 `request_id` 携带不同规范内容 |
| `E_SNAPSHOT_REFERENCED` | 父快照仍被未合并子快照引用 |
| `E_ROLLBACK_REJECTED` | 容差裁决拒绝回滚 |
| `E_OPERATION_ABORTED` | 请求在提交前被中断，恢复后重放此结果 |
| `E_METRIC_STALE` | 持久化指标投影与已提交状态不一致 |
| `E_SNAPSHOT_CORRUPT` | 快照引用指向缺失/不一致的历史版本 |
| `E_SNAPSHOT_PARENT` | 全量/增量快照父子关系不合法 |
| `E_VOLUME_EXISTS` / `E_SNAPSHOT_EXISTS` | 标识已存在 |
| `E_INVALID_REQUEST` | 参数、字段或 JSON 不合法 |
| `E_NOT_FOUND` / `E_VOLUME_NOT_FOUND` / `E_SNAPSHOT_NOT_FOUND` | 路由或对象不存在 |
| `E_METHOD_NOT_ALLOWED` | 路径存在但 HTTP 方法不匹配 |
| `E_INTERNAL` | 未分类内部错误 |

主要路由：

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/v1/volumes` | 创建卷 |
| `GET` | `/v1/volumes/{volume}` | 查询卷 |
| `PUT` | `/v1/volumes/{volume}/blocks/{block}` | 写块 |
| `GET` | `/v1/volumes/{volume}/blocks/{block}` | 读当前块与 base64 内容 |
| `POST` | `/v1/volumes/{volume}/snapshots` | 创建全量/增量快照 |
| `GET` | `/v1/volumes/{volume}/snapshots` | 列出快照 |
| `DELETE` | `/v1/volumes/{volume}/snapshots/{snapshot}` | 删除快照 |
| `POST` | `/v1/volumes/{volume}/snapshots/merge` | 合并直接父子快照 |
| `GET` | `/v1/volumes/{volume}/snapshots/{snapshot}/analysis` | 快照链、指标、舍入明细；可带裁决参数 |
| `GET` | `/v1/volumes/{volume}/versions/{snapshot}/analysis` | 同上的“版本”路径别名 |
| `POST` | `/v1/volumes/{volume}/rollback/evaluate` | 多候选容差裁决与稳定平局选择 |
| `POST` | `/v1/volumes/{volume}/rollback` | 裁决并执行回滚 |
| `GET` | `/v1/diagnostics/arithmetic` | 定点/补偿/误差说明 |
| `GET` | `/v1/recovery/reports/latest` | 最近一次恢复报告 |
| `POST` | `/v1/debug/crash` | 仅故障注入开启时可用；写意图后以整数码 86 退出 |

写入示例（内容必须是完整块，`sha256` 必须与解码后的内容一致）：

```bash
curl -X PUT http://127.0.0.1:8080/v1/volumes/vol-1/blocks/0 \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id":"write-001",
    "expected_version":0,
    "content_base64":"...",
    "sha256":"..."
  }'
```

分析查询可附加：

```text
?margin_floor=0.900000&cost_budget_bytes=1000000&relative_tolerance=0.000001
```

## 并发、幂等与一致性

- 所有变更操作由进程内单写者锁串行化；读事务可在 SQLite WAL 下并发读取完整提交版本。
- 同一 `request_id` 的并发重复请求等待首个完成，随后逐字节复用持久化响应。
- 同一 `request_id` 携带不同规范内容返回 `E_CONFLICTING_REQUEST`。
- 写块、快照创建、合并、回滚和删除均在同一 SQLite 事务中更新业务状态与全部受影响快照的指标投影。
- 每次变更完成后重算所有快照的指标；读取时再次从提交状态计算，并与持久化投影逐字节比较，不一致返回 `E_METRIC_STALE`。
- 快照创建与写块共享同一写锁，因此创建期间的新写入不会进入该快照。

## 意图、终止与恢复

每个变更的操作顺序：

1. 独立事务写入 `requests(status=pending)` 与 `intents(state=pending)` 并提交。
2. 开启业务事务，执行卷/快照/引用/指标更新。
3. 在同一业务事务内把请求结果和 `intents(state=committed)` 一并提交。

如果进程在步骤 2 被杀，SQLite 丢弃未提交事务；已提交意图仍为 `pending`。启动时 `Recover`：

- 扫描所有 `pending` 意图，标记为 `aborted_recovered`；
- 把对应请求标记为已完成的 `E_OPERATION_ABORTED`，供后续同请求 ID 重放；
- 按每个卷的已提交事实重算指标投影；
- 执行 `PRAGMA quick_check` 与 `PRAGMA foreign_key_check`；
- 写入恢复报告。报告只包含落盘事实，不推断未提交内容。

演示通过 `/v1/debug/crash` 在一个真实合并事务中修改引用后调用 `os.Exit(86)`。重启后报告 `pending_found=1`、`aborted_count=1`，快照链与指标保持崩溃前完整版本。

## 端到端验收

`./scripts/demo.sh` 使用固定样本：

- 卷：257 块 × 4096 字节 = 1,052,672 字节；
- 固定 seed `demo/v1`；
- 全量快照 `snap-full`，两次增量快照 `snap-inc1`、`snap-inc2`；
- 成功合并 `snap-inc1` 到 `snap-full`；
- 在 `snap-full` 与 `snap-inc2` 上做带容差裁决，二者均满足时按字典序选择 `snap-full`；
- 执行回滚并写回 14 块、57,344 字节；
- 在后续合并中强制退出 86，重启并验证恢复；
- 对重启前后的指标 JSON 与裁决 JSON 做逐字节比较。

最近一次实际运行的关键结果：

```text
used_capacity_ratio = 0.249027 (全部版本一致，64 个已写块)
snap-full  rollback_margin = 0.906615, cost = 110592
snap-inc1  rollback_margin = 0.948718, cost = 64512
snap-inc2  rollback_margin = 1.000000, cost = 0
forked exit code = 86
recovery status = complete, pending_found = 1, aborted_count = 1
metrics_byte_identical_across_restart = true
decision_byte_identical_across_restart = true
```

测试覆盖：

- 定点解析、舍入、容差边界、朴素与补偿加权误差；
- 指标在随机 map 枚举顺序下逐位一致；
- 校验和失败不改块、版本冲突、并发同块单赢家；
- 同请求并发重放、冲突请求；
- 增量快照 delta、父快照引用删除保护、合并重写与权重累计；
- 指标投影写入与读取校验；
- 意图中断后的恢复、恢复前后指标逐字节一致；
- HTTP JSON 错误、幂等响应、分析裁决与恢复报告。

## 最近一次实际验证环境

- 日期：2026-11-24（Asia/Shanghai）。
- 工具链：`go version go1.27.4 linux/amd64`；模块语言版本固定为 `go 1.22.0`。
- 兼容性复核：`GOTOOLCHAIN=go1.22.12 go test ./...` 全部通过。
- SQLite：`modernc.org/sqlite v1.35.0`（纯 Go，固定于 `go.mod`）。
- `go test ./...`：全部通过。
- `go test -race ./...`：全部通过。
- `go vet ./...`：无输出。
- `GOOS=windows GOARCH=amd64 go build ./cmd/blocksnapd ./cmd/demo`：通过。
- `./scripts/demo.sh`：退出码 `0`，输出包含上节全部验收事实。
