# 本地依赖供应链完整性审计工具

`supplyaudit` 是一个纯 Go、无 CGO 的本地命令行工具，用确定性样本验证三种依赖清单格式的解析、归一化、多来源归并、摘要冲突/降级裁决、SQLite 版本化快照与故障注入隔离。实现不访问网络，不调用容器，所有时间字段使用固定样本标记。

## 环境与构建

- Windows 10 或以上，Go 1.22 或以上。
- SQLite 驱动：`modernc.org/sqlite v1.34.5`，纯 Go，无 CGO。
- 仅需要本地文件系统与 SQLite 文件写权限。

```powershell
go version
go test ./...
go build -o supplyaudit.exe ./cmd/supplyaudit
```

Linux/macOS 下将输出文件名改为 `supplyaudit` 即可。模块固定为 `go 1.22`。

## 快速运行

以下示例只使用临时工作目录中的确定性样本。

```powershell
$W = Join-Path $env:TEMP "supplyaudit-demo"

.\supplyaudit.exe sample --workdir $W --clean

.\supplyaudit.exe audit `
  --input "$W\input" `
  --db "$W\state\audit.db" `
  --report "$W\audit-report.json" `
  --format-version v2 `
  --priority binary,jsonl,textblock `
  --max-concurrency 3 `
  --max-components 1000

.\supplyaudit.exe verify --workdir $W --report "$W\verification-report.json"

.\supplyaudit.exe trace --db "$W\state\audit.db" --component "@demo/shared-lib"
.\supplyaudit.exe replay --db "$W\state\audit.db" --snapshot 1 --report "$W\replay.json"
.\supplyaudit.exe config --format-version v1 --priority jsonl,binary,textblock
```

`sample --clean` 只删除工作目录下由生成器拥有的 `input/` 与 `faults/`。`verify` 会重建自己的 `verification-state/` 子目录，不会删除调用方放在 `state/audit.db` 的规范快照。为比较重复运行的字节一致性，应使用两个干净工作目录或先清理同一工作目录及状态库。

## 格式契约

### 1. 二进制采集器清单

所有整数按小端序。魔数为 8 字节 `SADBIN01`。

公共头：

| 字段 | 宽度 |
|---|---:|
| magic | 8 字节 |
| format version | uint16，`1` 或 `2` |
| record count | uint32 |

v1 在公共头后增加全局来源仓库：`uint16 length + bytes`。每条记录为三个 `uint16` 长度加 `name`、`version`、`digest`。

v2 不含全局仓库；每条记录为五个 `uint16` 长度加 `name`、`version`、`digest`、`source repository`、`signature digest`。签名摘要仅保留溯源，业务完整性判断使用 `digest`。

未知魔数、未知版本头，或当前目标版本不支持该版本时返回 `E_FORMAT_VERSION`。任何头部或记录字段越界返回 `E_TRUNCATED`，诊断中带偏移与实际已读字节数；越界前已经完整解析的记录会保留。

### 2. JSON Lines 清单

每行一个 JSON 对象，支持以下字段及兼容别名：

- `name` 或 `component`
- `version`
- `digest` 或 `integrity`
- `source` 或 `repository`
- 可选 `signature`

空行忽略。单行 JSON、字段或摘要错误不会阻止后续行解析；错误记录进入源级问题列表，后续有效记录继续归并。

### 3. 旧版文本块清单

魔数为 8 字节 `SADTXT01`。之后重复：

```text
uint32 big-endian payload length
payload = name|version|digest|repository
```

长度前缀大于剩余字节数时返回 `E_TRUNCATED`，报告块起始偏移和实际已读字节数；此前完整块保留。字段数不为四项返回 `E_RECORD_INVALID`。

## 归一化规则

### 组件键

组件名使用 Unicode 小写并去除首尾空白，保留 `/`、`@`、`:`、`.`、`-`、`_` 等命名空间分隔符。例如 `@Demo/Shared-Lib` 归一为 `@demo/shared-lib`。

### 版本视图

- 接受三段整数 SemVer 与四段整数版本；第四段为 0 时与三段写法等价。
- `v1.2.3` 与 `1.2.3` 归一为 `1.2.3`。
- 带构建元数据的 `1.2.3+build.7` 保留原始写法和 `build` 字段，但构建元数据不参与 SemVer 优先级，也不进入归并键，因此 `1.2.3+build.7` 与 `1.2.3+build.9` 归入同一版本组。
- 预发布标识按 SemVer 规则比较：数值标识按数值比较，数值标识低于非数值标识，前缀相同时标识更少者更低。
- 版本视图包含 `raw`、`canonical`、四段 `core`、`prerelease` 与 `build`。

### 摘要视图

- 带算法标签：`sha1:<payload>`、`sha256:<payload>`，payload 可为十六进制或 base64，允许从 1 字节到算法标称宽度的截断前缀。截断优先使用显式标签，避免歧义。
- 无标签完整宽度：40 位十六进制识别为 SHA-1；44 字符 base64 识别为 SHA-256。
- 无标签截断宽度：十六进制不超过 40 字符识别为 SHA-1 前缀，41–64 字符识别为 SHA-256 前缀；base64 不超过 28 字符识别为 SHA-1 前缀，29–44 字符识别为 SHA-256 前缀。
- 算法名未知、编码非法、payload 为空，或标签算法宽度不足以容纳实际字节数时返回 `E_DIGEST_INVALID`。
- 视图保留原始字符串，并提供 `algorithm`、规范化十六进制、规范化 base64、字节数、标称位数、有效位数与是否截断。

## 归并裁决

归并键为 `组件键 + NUL + 规范化版本`，因此不同版本不会被覆盖。优先级通过 `--priority` 配置，默认：

```text
binary > jsonl > textblock
```

同一归并键内：

- 最高优先级来源为权威声明；同优先级按稳定来源路径、原始偏移、声明 ID 裁决。
- 权威摘要与其他来源摘要（算法或字节）不一致时产生 `E_DIGEST_CONFLICT`，保留双方来源、路径、偏移、算法与摘要。
- 同一组件低于该组件最高归一版本的所有版本组产生 `E_VERSION_DOWNGRADE`。裁决与采集完成顺序无关。
- 某声明的有效摘要位数低于该组件已归并声明的最大有效位数时产生 `E_WEAK_DIGEST`。有效位数为 `min(算法标称位数, 实际字节数*8)`，因此 SHA-1 和截断前缀都会显示为弱摘要。
- 同一组件可以同时保留 `E_DIGEST_CONFLICT`、`E_VERSION_DOWNGRADE`、`E_WEAK_DIGEST`。
- 每个源声明只进入一个版本组；报告包含逐源覆盖计数，并断言源声明总数等于归并后声明总数。

## 并发与预算

每个来源在独立解析会话中处理，结果对象互不共享可写状态。调度器最多保持 `--max-concurrency` 个在途会话，其余来源进入等待队列；等待来源不会被拒绝。报告中的调度器字段包括：

- `peak_in_flight`
- `peak_queue_length`
- `sources_started`
- `sources_completed`
- `final_in_flight`
- `final_queue_length`

归并阶段单线程、按组件键顺序执行，保证裁决确定性。单源保留声明数超过 `--max-components` 时返回 `E_COMPONENT_BUDGET`，保留前缀条目并停止该源的归并贡献。

## 快照与兼容加载

SQLite 表 `snapshots` 每次只插入一行，插入和链校验处于同一事务。快照包含：

- 连续递增的 `version` 与 `parent_version`；
- 报告 schema 版本；
- 所需格式版本集合，例如 `binary/v2`、`json/v1`；
- 来源优先级配置；
- 审计数据载荷及其 SHA-256 `content_hash`；
- 前一快照哈希 `previous_hash` 与当前链哈希 `snapshot_hash`。

加载时重新计算配置指纹、载荷哈希和链哈希，并校验从 v1 到目标版本的连续性。以下情况整体拒绝并返回 `E_SNAPSHOT_VERSION`：

- 版本跳号、父版本不连续或前序哈希不匹配；
- 需要而当前不接受的目标格式版本；
- 来源优先级顺序不同；
- 快照整行/载荷被整体替换，即使版本号未变；
- 快照 schema 与当前不兼容。

兼容策略：`--format-version v2` 接受 `binary/v1`、`binary/v2`、`json/v1`、`textblock/v1`，因此能加载旧 v1 采集器产生的快照；`--format-version v1` 不再接受需要 `binary/v2` 的快照。

## 业务码

| 业务码 | 触发条件 |
|---|---|
| `E_FORMAT_VERSION` | 未知魔数、未知/不支持的格式版本 |
| `E_TRUNCATED` | 头、记录或长度前缀越界，并报告已读字节 |
| `E_DIGEST_INVALID` | 摘要编码/算法未知或宽度与算法不符 |
| `E_DIGEST_CONFLICT` | 同一归并键的权威与非权威摘要不同 |
| `E_VERSION_DOWNGRADE` | 版本低于同组件最高归一版本 |
| `E_WEAK_DIGEST` | 有效摘要位数低于同组件最大声明 |
| `E_COMPONENT_BUDGET` | 单源保留组件数超过预算 |
| `E_SNAPSHOT_VERSION` | 快照链、格式、优先级、schema 或内容哈希不兼容 |
| `E_RECORD_INVALID` | 单条记录字段无效或不可归一 |
| `E_SOURCE_IO` | 来源文件无法读取 |
| `E_INVALID_CONFIG` | 优先级、并发度等参数非法 |

结构解析错误（格式、截断、摘要非法、记录非法、源 I/O、预算）默认阻止快照推进。`E_DIGEST_CONFLICT`、`E_VERSION_DOWNGRADE`、`E_WEAK_DIGEST` 是审计结论，正常 `audit` 会将其写入新快照；可用 `--fail-on-code` 强制拒绝提交。

## 子命令

### `sample`

生成四个正常来源和六类故障输入：

```text
faults/unknown-version-header
faults/text-length-overrun
faults/digest-algorithm-mismatch
faults/digest-conflict
faults/version-downgrade
faults/snapshot-version-gap
```

### `audit`

完整解析、归并、报告与可选快照提交。支持 `--input`、重复 `--source`、`--config`、`--no-commit` 与重复/逗号分隔的 `--fail-on-code`。配置 JSON 示例见 `examples/audit-config.json`。

### `trace`

加载经过兼容校验的快照，按键查询组件的全部版本组、权威来源、每条来源声明、偏移与相关业务码。

### `replay`

加载指定快照并重新输出当时保存的审计数据。优先级或格式版本不兼容时返回 `E_SNAPSHOT_VERSION`。

### `config`

输出标准化后的目标格式版本、允许格式集合、优先级和调度参数，用于切换前检查配置。

### `verify`

在干净工作目录中建立基线快照 v1，然后逐个执行六类故障输入。每类断言：

- 观察到预期业务码；
- 业务码归属目标组件，或快照故障命中目标版本；
- 同场景控制组件未被污染；
- 归并计数自洽；
- 已提交的规范快照版本未推进。

`verify` 还额外验证优先级不兼容、所需格式不兼容、快照载荷整体替换和版本跳号。故障探针不向规范链提交，因此控制链始终保持基线 v1。

## 报告与确定性

审计报告按以下稳定顺序输出：

- 来源按相对路径、ID；
- 组件按归一组件键；
- 版本组按版本降序；
- 声明按来源优先级、路径、偏移、ID；
- 业务发现按组件键、业务码、来源路径、偏移、版本、消息；
- 快照链按版本升序。

报告只包含相对来源路径和固定样本时间 `2000-01-01T00:00:00Z`，不记录运行时钟、随机 ID 或绝对临时目录。相同输入、相同配置和干净状态数据库产生字节级一致报告。增量重复审计会自然增加快照版本，因此“重复运行字节一致”的验证场景应使用干净状态或独立工作目录回放。

## 实际验证结果

本环境执行：

```text
go test ./...                 PASS
go vet ./...                  PASS
supplyaudit verify            PASS（6/6 故障场景，规范快照保持 v1）
```

正常样本审计结果：

```text
source files:         4
source declarations:  16
merged declarations:  16
components:           8
merge groups:         11
self consistent:      true
E_DIGEST_CONFLICT:    2
E_VERSION_DOWNGRADE:  3
E_WEAK_DIGEST:        2
```

在两个不同临时目录完成 `sample --clean`、`audit`、`verify` 后：

```text
audit-report.json SHA-256
9dee13886bede8e28af696f1519a4c917b8f48c5ffbe8c167bc605f080122426

verification-report.json SHA-256
bfe3d23af8a85fb1e5f65db6538428ebf27e9905f1c2c9640af191802d778784
```

两次产物分别字节相同（`cmp` 返回 0）。

额外黑盒检查：

```text
trace "@DEMO/Shared-Lib"        找到 4 条来源声明（三个 2.1.0，另有一个 1.9.0）
replay --snapshot 1            成功
切换 priority 后回放 v1         E_SNAPSHOT_VERSION
切换 format-version v1 后回放   E_SNAPSHOT_VERSION（快照需要 binary/v2）
同一数据库再次 audit           提交 snapshot v2，parent_version=1
并行 trace/trace/replay        均成功
```

## 目录

```text
cmd/supplyaudit        CLI 子命令
internal/formats       三格式解析与来源发现
internal/normalize     组件键、版本、摘要归一
internal/merge         多来源归并和业务码裁决
internal/pipeline      并发调度、预算、归并入口
internal/snapshot      SQLite 快照、哈希链与兼容加载
internal/report        确定性 JSON 报告
internal/sample        正常/故障确定性样本生成器
internal/verify        六类异常注入验证链路
examples/             配置示例
```
