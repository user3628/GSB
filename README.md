# 复制位点快照与延迟观测归并引擎

这是一个面向主从复制延迟治理的本地应用。核心对象是某个从库在一个采样时刻上报的**复制位点快照**，而不是策略字段。引擎将同一从库的多条快照按 GTID 区间集合归一化、排序和归并，形成可审计的延迟演进序列，并据此推导停滞、回退、跨越、延迟等级、状态迁移和建议动作。

实现只使用 Go 和纯 Go SQLite 驱动 `modernc.org/sqlite`，`CGO_ENABLED=0`，不需要 Docker、外部数据库、账号或真实复制集群。HTTP 服务只面向本机回环地址使用。

## 快速开始（Windows PowerShell）

要求 Go 1.22 或更高版本。先在 PowerShell 中确认：

```powershell
go version
# 例如：go version go1.22.12 windows/amd64
```

在仓库根目录执行：

```powershell
go mod download
go run .\cmd\samplegen -out .\samples
go test ./...
go build -trimpath -o .\lagengine.exe .\cmd\lagengine

New-Item -ItemType Directory -Force .\data | Out-Null
.\lagengine.exe -addr 127.0.0.1:8080 -db .\data\lagengine.db -compat .\samples\compat.json
```

另开一个 PowerShell 导入样本并查询：

```powershell
Invoke-RestMethod -Method Post `
  -Uri 'http://127.0.0.1:8080/v1/import?batch_id=sample-1&format=jsonl' `
  -ContentType 'application/x-ndjson' -InFile .\samples\observations.jsonl

Invoke-RestMethod 'http://127.0.0.1:8080/v1/replicas/replica-a/sequence'
Invoke-RestMethod 'http://127.0.0.1:8080/v1/replicas/replica-a/state'
Invoke-WebRequest 'http://127.0.0.1:8080/v1/export?format=binary' -OutFile .\observations.bin
```

命令行也提供同一事务路径的导入/导出，便于脚本化验收：

```powershell
.\lagengine.exe -db .\data\verify.db -compat .\samples\compat.json `
  -import .\samples\observations.jsonl -format jsonl -batch sample-cli-1
.\lagengine.exe -db .\data\verify.db -export .\roundtrip.bin -export-format binary
.\lagengine.exe -db .\data\verify.db -recover-only
```

## 观测模型与归一化

一条快照包含：

- `replica_id`：`[A-Za-z0-9][A-Za-z0-9._-]{0,127}`
- `sampled_at_ms`：正 Unix 毫秒时间
- `gtid` / `gtid_segments`：一个或多个 `UUID:起止列表`
- `position_name`：规范 GTID 集合或 `file:offset` 位点名
- `lag_ms`：非负整数；HTTP 编辑时字段必须显式存在
- `reported_version`：至少一个数字版本段，例如 `8.0.36`

GTID 先按源 UUID 分组，再解析闭区间、排序并合并重叠及相邻区间。例如：

```text
3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3,4-5
=> 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5
```

源内多个区间使用冒号，源之间使用逗号。所有字段错误返回 `E_OBSERVATION_INVALID`，上下文固定包含 `replica_id`、`sampled_at_ms` 和 `field`；批量中任一条非法，整批不提交。

### 归并顺序与位点判定

同一从库的观测按 `sampled_at_ms` 升序归并；同一时刻再以规范 GTID 文本形成确定性顺序。同一个 `(replica_id, sampled_at_ms)` 若内容不同，返回 `E_OBSERVATION_CONFLICT`。内容指纹不包含导入元数据。

相邻快照仅根据规范化区间集合判定：

- `INIT`：首条观测。
- `ADVANCE`：前一位点是当前位点的子集，并附 `added_intervals`。
- `STALL`：集合完全相同，累加 `stall_count`。
- `REGRESSED`：当前集合缺少前一位点中的事务，并附 `revoked_intervals`；部分交集时也同时附新增区间。
- `crossed=true`：前进时跳过某现有源中至少一个事务号。

每次归并都重新排序和重算，不使用上报顺序作为语义依据。

## 持久化格式

### JSON Lines v1

首行是元数据，其后每个非空行是一条观测：

```json
{"format":"replication-observation-jsonl","format_version":1,"required_fields":["replica_id","sampled_at_ms","gtid_segments","position_name","lag_ms","reported_version"]}
{"replica_id":"replica-a","sampled_at_ms":1700000000000,"gtid_segments":["3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"],"position_name":"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5","lag_ms":120,"reported_version":"8.0.36","last_write_ms":1700200000000,"format_version":1}
```

`last_write_ms` 可省略，导入时采用该次导入时间；`format_version` 可省略并使用流版本。历史 v0 可启用秒级 `sampled_at`、波浪号区间、整数简写、缺版本默认值。JSON 语法错误返回 `E_FORMAT_CORRUPT` 并带 `line` 与 `offset`。

### 紧凑二进制 v1

所有整数使用大端序，CRC 使用 IEEE CRC32：

```text
Header (9 bytes)
  magic         4 bytes  "RLAG"
  version       u8
  record_count  u32

Record frame
  payload_len   u32
  payload       payload_len bytes
  crc32         u32 over payload

Payload
  bitmap        u8   bit0=position_name, bit1=reported_version, bit2=last_write_ms
  replica_len   u16
  replica       replica_len bytes UTF-8
  sampled_at    i64
  lag_ms        i64
  segment_count u16
  segments      repeated { u32_len + UTF-8 string }
  position_name optional { u32_len + UTF-8 string }
  version       optional { u32_len + UTF-8 string }
  last_write_ms optional i64
```

未知必填 bitmap 位或未知必填字段返回 `E_FORMAT_UNSUPPORTED`。长度、截断、尾部字节、CRC 或记录数错误返回 `E_FORMAT_CORRUPT`，并指出记录号与零基偏移。字段位图允许旧版本缺少上报版本等可选字段。

## 兼容矩阵

矩阵从 JSON 文件读取，按 `format/version` 显式裁决 `ACCEPT`、`DEGRADE` 或 `REJECT`。未命中使用 `default`。降级项会写入导入报告。

```json
{
  "rules": {
    "jsonl/1": {"decision": "ACCEPT", "time_precision": "ms"},
    "binary/1": {"decision": "ACCEPT", "time_precision": "ms"},
    "jsonl/0": {
      "decision": "DEGRADE",
      "time_precision": "s",
      "default_version": "5.7.0",
      "allow_tilde_ranges": true,
      "allow_integer_shorthand": true,
      "default_gtid_source": "3e11fa47-71ca-11e1-9e33-c80aa9429562",
      "allow_missing_version": true
    }
  },
  "default": {"decision": "REJECT"},
  "known_required_fields": ["replica_id", "sampled_at_ms", "gtid_segments", "position_name", "lag_ms", "reported_version"]
}
```

时间精度支持 `ms`、`s`、`us`、`ns`。降级展开后的整数区间与显式 `UUID:区间` 写法进入同一归一化流程，结果完全一致。

## 导入冲突与批次原子性

导入观测与现有观测键相同时，按以下顺序打破平局：

1. 较高格式版本。
2. 较大 GTID 基数。
3. 较新的 `last_write_ms`。

内容指纹相同为 `SKIP`；新键或按规则胜出为 `OVERWRITE`；全部平局但内容不同为 `REJECT`。每个结局写入导入报告。任一条 `REJECT` 返回 `E_OBSERVATION_CONFLICT`，整批回滚，现有观测与演进序列保持导入前版本。

相同 `batch_id` 串行执行一次并复用已完成或失败结果；复用成功响应带 `X-Idempotent-Replay: true`。同批次标识但请求内容不同返回 `E_CONFLICTING_BATCH`。带 `base_version` 的编辑若不是当前版本，返回 `E_STALE_BASE` 且不写入。

## 延迟等级与状态机

默认策略：警告阈值 1000 ms，严重阈值 5000 ms，连续停滞阈值 3，恢复观测数 2。延迟阈值使用严格大于；连续停滞次数严格超过配置值才进入 `STALLED`。

```text
HEALTHY      -> LAG_WARN | LAG_CRITICAL | STALLED
LAG_WARN     -> HEALTHY | LAG_CRITICAL | STALLED | RECOVERING
LAG_CRITICAL -> LAG_WARN | STALLED | RECOVERING
STALLED      -> RECOVERING
RECOVERING   -> HEALTHY | LAG_WARN | LAG_CRITICAL | STALLED
```

自循环合法。位点回退立即进入 `STALLED`；停滞期间延迟升高仍保持 `STALLED`。恢复要求连续 `recovery_observations` 条观测同时满足位点 `ADVANCE` 与延迟 `NORMAL`。非法迁移返回 `E_INVALID_TRANSITION`，可通过 `POST /v1/state/validate` 做无副作用校验。每个演进 step 都保存 `transition_basis`、`lag_level`、`state`、`stall_count`、`recovery_streak` 和 `suggested_action`。

## 一致性与崩溃恢复

写路径如下：

1. 单写锁内检查批次、基础版本和幂等哈希。
2. 独立事务写入 `PENDING` 意图和批次占位。
3. 在单个主事务中整体替换观测、区间、演进序列、状态、版本快照、提交记录和批次结果，并把意图标记完成。
4. 业务失败则在独立事务标记意图与批次中止；没有部分观测生效。

SQLite 使用 WAL。多个读者在各自只读事务中读取同一完整提交版本；响应头 `X-Engine-Version` 标识该版本。每次提交保存完整版本快照。启动时扫描未完成意图：若当前版本仍等于意图基础版本，将批次标记中止，观测保留在最后完整版本；若检测到无完成标记的后续版本，则从持久化快照回滚。恢复报告仅使用数据库中已落盘的事实。

验证钩子：`-crash-after-intent` 或环境变量 `LAGENGINE_CRASH_AFTER_INTENT=1` 会在意图落盘后执行 `os.Exit(137)`；默认关闭。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| GET | `/v1/version` | 当前提交版本与策略 |
| GET | `/v1/observations` | 完整观测集合 |
| POST | `/v1/observations` | 新增/替换一条观测，可带批次与基础版本 |
| PUT | `/v1/observations/{replica}/{sampledAt}` | 按路径键编辑 |
| DELETE | `/v1/observations/{replica}/{sampledAt}` | 删除并重算 |
| POST | `/v1/import` | 导入 JSONL/二进制；参数 `batch_id`、`base_version`、`format` |
| POST | `/v1/merge` | 从已提交观测重算全部演进序列 |
| GET | `/v1/sequences` | 全部延迟演进序列 |
| GET | `/v1/replicas/{id}/sequence` | 单从库演进序列 |
| GET | `/v1/replicas/{id}/state` | 当前状态和建议动作 |
| GET | `/v1/export?format=jsonl\|binary` | 导出完整观测集合 |
| GET | `/v1/recovery/report` | 最近一次启动恢复报告 |
| POST | `/v1/state/validate` | 无副作用校验 `{from,to}` |
| GET/PUT | `/v1/policy` | 读取/更新进程内阈值；更新后执行 merge 落盘派生状态 |

观测编辑请求可直接发送观测对象，也可使用包装：

```json
{
  "batch_id": "edit-1",
  "base_version": 3,
  "observation": {
    "replica_id": "replica-a",
    "sampled_at_ms": 1700000001000,
    "gtid_segments": ["3e11fa47-71ca-11e1-9e33-c80aa9429562:1-8"],
    "position_name": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-8",
    "lag_ms": 1500,
    "reported_version": "8.0.36"
  }
}
```

导入也可用 JSON 包装并传 `data` 或 `data_base64`，或直接在正文发送原始 JSONL/二进制。所有失败响应统一为：

```json
{"code":"E_OBSERVATION_INVALID","message":"observation field is invalid","context":{"replica_id":"replica-a","sampled_at_ms":1700000001000,"field":"lag_ms","detail":"must be non-negative"}}
```

业务码：`E_OBSERVATION_INVALID`、`E_OBSERVATION_CONFLICT`、`E_FORMAT_CORRUPT`、`E_FORMAT_UNSUPPORTED`、`E_INVALID_TRANSITION`、`E_CONFLICTING_BATCH`、`E_STALE_BASE`、`E_BATCH_ABORTED`、`E_NOT_FOUND`、`E_BAD_REQUEST`、`E_INTERNAL`。

## 样本与项目结构

`go run ./cmd/samplegen -out ./samples` 会生成：

- `observations.json`：乱序、可读的 13 条双源观测。
- `observations.jsonl`：与上面同内容、乱序的 JSONL v1。
- `observations.bin`：同内容紧凑二进制 v1。
- `legacy.jsonl`：v0 秒级时间、整数简写、波浪号位点、缺版本。
- `legacy.bin`：v0 二进制、秒级时间、缺版本。
- `compat.json`：启用上述降级规则的显式矩阵。

```text
cmd/lagengine        HTTP 服务和命令行导入/导出入口
cmd/samplegen        确定性样本生成器
internal/domain      GTID、观测、演进、状态机
internal/compat      兼容矩阵与时间精度裁决
internal/format      JSONL 与二进制编解码
internal/store       SQLite、意图、快照、恢复、读写快照
internal/engine      导入冲突、编辑、归并、批次编排
internal/httpapi     回环 HTTP API
internal/integration 强制终止恢复测试
```

## 实际验证结果

本次交付环境为 Linux amd64、Go 1.27.4（模块语言版本 `go 1.22`）。Windows 目标以 `GOOS=windows GOARCH=amd64 CGO_ENABLED=0` 构建通过。实测摘要：

- `go vet ./...`：通过。
- `go test -race ./... -count=1`：全部包通过。
- 样本 JSONL 导入：版本 1，13 条观测，`replica-a`/`replica-b` 最终均为 `HEALTHY`。
- 当前 JSONL 与二进制换库重导入：逐记录观测、GTID 区间与位置一致。
- v0 历史样本：`DEGRADE`，秒转毫秒、整数简写展开、缺版本补 `5.7.0`，降级明细逐条记录。
- 强制终止：子进程在意图落盘后以 137 退出；重启后 `pending_intents_found=1`、`action=ABORT_PENDING_INTENT`、最后完整版本保持 1、批次为 `ABORTED`。
- 回环 HTTP：导入、序列、状态、恢复报告和二进制导出均返回 200，导出响应带 `X-Engine-Version: 1`。

详细可复核输出见 `artifacts/verification.log` 与 `artifacts/verification-extra.log`。

可在仓库根目录重新执行：

```bash
bash scripts/verify.sh                 # Linux / WSL
```

```powershell
.\scripts\verify.ps1                   # Windows PowerShell
```

这两个脚本会重新生成样本、运行测试、执行 Windows 构建（Linux 脚本）、执行导入/历史降级、真实强制终止恢复和回环 HTTP 烟测。

