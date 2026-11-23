# Windows 服务控制与提权面裁决工作台

这是一个从零实现的、只在本地运行的 Go 工作台。它把同一批 Windows 服务的三类政策声明归并成确定性的提权面结论，并把结论以带版本链的 SQLite 快照提交。

实现不依赖 CGO、Docker、外部账号或远端服务。`go.mod` 的 Go 版本为 1.22，SQLite 使用纯 Go 驱动 `modernc.org/sqlite v1.34.5`。

## 目录与职责

- `internal/domain`：统一声明、路径画像、权限画像、报告和稳定业务码。
- `internal/source`：二进制 v1/v2、JSONL、长度前缀文本块解析器及并发解析调度。
- `internal/winpath`：Windows 路径归一、未加引号候选前缀、可写目录判定。
- `internal/privilege`：整数权限等级和有效权限画像。
- `internal/merge`：按规范化服务名分区的确定性多源归并。
- `internal/snapshot`：SQLite 单事务快照、配置指纹、哈希链与恢复校验。
- `internal/samplegen`：确定性样本和六类故障注入。
- `internal/workbench`：执行管线、回放、单服务溯源、演示与故障自检。
- `main.go`：`run`、`demo`、`replay`、`trace` 子命令。

## 构建与验证

```powershell
go version
go build ./...
go test ./...
go run . demo
```

Windows amd64 无 CGO 构建：

```powershell
$env:CGO_ENABLED = "0"
$env:GOOS = "windows"
$env:GOARCH = "amd64"
go build -o svcworkbench.exe .
```

默认 `run` 使用临时目录，生成样本、执行裁决、输出 JSON 后清理临时状态，因此相同的无状态参数会产生字节级一致报告。需要保留快照链时显式传入 `--state-dir`。

### 常用命令

```powershell
go run . run --target-format v2
go run . run --target-format v1 --compact
go run . run --state-dir .\state --compact
go run . run --state-dir .\state --priorities json_manifest,binary_policy,legacy_text
go run . run --max-concurrency 2 --service-budget 64
go run . replay --state-dir .\state --version 2
go run . trace --state-dir .\state --service "Writable Agent"
go run . demo --target-format v2
```

`replay` 输出包含 `snapshot` 元数据、`chain` 校验信息和 `report` 原始裁决负载；`trace` 输出单服务在指定快照中的完整声明、字段来源和冲突链。

`--fault` 可取：

`none`、`unknown-version`、`text-prefix-oob`、`unterminated-quote`、`unknown-account`、`declaration-conflict`、`snapshot-gap`。

命令在成功产出 JSON 时返回 0，即使报告包含需要拦截提交的业务码。基础设施错误返回非 0。

## 输入格式

### 二进制策略包

所有整数使用大端序。文件头固定为：

```text
magic[8] = "GSBSPOL\0"
version  = uint16
count    = uint32
```

v1 单条记录：

```text
name_len u16, name, display_len u16, display, start u8, path_len u16, path
```

v2 单条记录：

```text
name_len u16, name, display_len u16, display, start u8,
account_type u8, account_len u16, account, logon_mask u16,
path_flags u8, path_len u16, path
```

`start`：`1=boot`、`2=system`、`3=automatic`、`4=manual`、`5=disabled`。

`account_type`：`0=unknown`、`1=LocalSystem`、`2=NetworkService`、`3=LocalService`、`4=custom`。

`logon_mask`：`1=interactive`、`2=network`、`4=service`、`8=batch`、`16=denied`。

`path_flags` 的 bit 0 表示来源声明映像目录可写。v1 不含账户和登录能力字段，统一模型将其标记为隐式 LocalSystem/service，并允许归并时从下一优先级来源补全字段。

非法魔数、未知版本、截断记录和未知文本块长度都会产生 `E_FORMAT_VERSION`。解析结果同时记录 `skippable_ranges`，不会让结构错误污染其他记录或其他来源。

报告和问题中的字节范围均为半开区间 `[offset,end)`。

### JSONL 清单

每个非空行是一个独立 JSON 对象：

```json
{"service_name":"Spooler","display_name":"Print Spooler","start_type":"auto","image_path":"C:\\Windows\\System32\\spoolsv.exe","account_type":"LocalSystem","account":"LocalSystem","logon_capabilities":["interactive","service"],"path_writable":false}
```

`account_type` 可省略，此时按 `account` 原始值分类。`logon_capabilities` 也接受逗号分隔字符串。未知扩展字段被忽略，非法行只跳过当前行并记录行起止偏移。

### 长度前缀文本块

每条记录是 `uint32` 大端序长度加 UTF-8 负载。负载字段以 `|` 分隔：

```text
service|display|start|image_path|account|logon_csv|writable
```

`writable` 为 `1/true` 或 `0/false`，可省略。前缀声明长度超过剩余字节时返回 `E_FORMAT_VERSION`，此前已经解析成功的记录保留在部分结果中。

## 路径与权限模型

映像路径统一转换为小写、反斜杠、折叠重复分隔符的 Windows 绝对路径，例如：

```text
C:/Program Files/Vendor//Updater.EXE -> c:\program files\vendor\updater.exe
```

报告同时保留 `original`、`normalized`、`arguments` 和 `image_path_key`，因此路径比较和风险判定都可复核。双引号必须成对；未终止引号、非绝对路径、控制字符和保留字符返回 `E_PATH_MALFORMED`。

对含空格且未加引号的命令，工作台会给出可执行候选前缀。例如 `C:\Program Files\Vendor\updater.exe -k run` 会给出 `c:\program.exe` 和完整 `updater.exe` 前缀，最终映像路径与参数分别保留。

静态可写目录模型覆盖用户目录、ProgramData、Windows Temp、系统 Temp、回收站和 Public；来源给出的 `path_writable` 也会合入，并在 `writable_reasons` 中注明 `path:*` 或 `source:declared-writable`。

账户等级使用可传递的整数：

```text
Unknown 0 < Custom 1 < LocalService 2 < NetworkService 3 < LocalSystem 4
```

有效分数为：

```text
account_level*100 + startup_level*10 + logon_level
```

启动权重为 `boot/system=3`、`automatic=2`、`manual=1`、`disabled=0`；登录权重为 `interactive=4`、`network=3`、`service=2`、`batch=1`、`denied=0`。全程不使用浮点。

高权限账户阈值为 NetworkService 及以上。高权限、服务有效运行且映像位于可写目录时输出 `E_PRIVILEGE_EXPOSURE`。未知账户输出 `E_ACCOUNT_UNKNOWN`，并使该条声明不参与权威选择。

## 多源归并

归并键为小写、去首尾空白的服务名。来源优先级默认：

```text
binary_policy > json_manifest > legacy_text
```

可通过 `--priorities` 改写。权威来源确定启动类型和映像路径；若 v1 二进制包未携带账户或登录能力，则按优先级从下一来源补全，并记录 `field_sources` 中的 `implicit` 标记。

权威声明与其他来源在启动类型、归一映像路径或服务账户上不同时，产生 `E_DECLARATION_CONFLICT`。冲突对象同时保存双方原始值、归一值和 `source_id/offset/end`。仅大小写、正反斜杠、重复分隔符或引号差异不会误报冲突。

服务按规范化名称排序；服务内声明、字段来源、冲突、风险和业务码均固定排序。全局问题按服务名、业务码、来源和偏移稳定排序。

## 快照与版本链

SQLite 表包括：

- `snapshots`：版本、链 ID、父哈希、内容哈希、配置哈希、格式版本集合、优先级、报告负载。
- `snapshot_meta`：已提交链尾版本和哈希、快照 schema。
- `source_progress`：仅在快照事务提交时更新，记录来源指纹、格式版本、已接受条数和关联快照版本。

每次成功裁决在一个 SQLite 事务中同时写入快照、来源进度和链尾元数据。内容哈希覆盖版本、父哈希、快照 schema、固定标记、目标格式、配置哈希和报告负载。配置哈希覆盖格式版本集合、来源优先级、目标格式和固定标记。

加载时执行以下整体校验：

1. 版本必须从 1 开始连续。
2. 所有链 ID 必须一致。
3. 父哈希必须等于前一条内容哈希。
4. 配置字段必须能重新计算出一致的配置哈希。
5. 负载必须能重新计算出一致的内容哈希。
6. 链尾元数据必须与最后一条快照完全一致。

任一项失败返回 `E_SNAPSHOT_VERSION`。跳号或负载被替换时，报告暴露最近连续兼容版本 `fallback_version`，但不会推进新快照。相同版本但不同内容、父哈希或链 ID 会被识别为整体替换，而不是仅比较版本号。

旧版裁决器留下的异构表结构或无法按当前 schema 读取的元数据同样收敛为 `E_SNAPSHOT_VERSION`，不会作为进程崩溃处理。

目标格式或来源优先级与已有链不兼容时整体拒绝。例如先用 v1 提交，再用 v2 运行会得到 `E_SNAPSHOT_VERSION`，原快照保持可回放。

## 并发、预算与恢复

每个来源文件在独立解析会话中读取，会话内部状态不共享。工作池大小由 `--max-concurrency` 控制；超过并发上限的来源进入等待队列，指标中可见 `queued_sources`、`completed_sources`、`observed_max_in_flight`。首批会话使用同步起跑屏障，使该指标真实且可复现。

单来源记录数超过 `--service-budget` 时返回 `E_SERVICE_BUDGET`，停止继续读取该来源，但保留此前已解析声明。该业务码会阻止快照提交。

解析完成后按服务名分区，归并阶段串行处理分区，保证优先级和冲突顺序稳定。所有可追溯到记录的并发错误都包含具体来源和字节偏移。

强制终止验证可直接使用 CLI：

```powershell
go run . run --state-dir .\state
go run . run --state-dir .\state --crash-point commit --crash-exit-code 86
go run . run --state-dir .\state
go run . replay --state-dir .\state --version 2
```

`commit` 崩点位于 SQLite 事务提交之前，`merge` 崩点位于解析完成、归并开始之前。进程以指定退出码退出；重启后 SQLite 丢弃未提交事务，来源进度保持在上一个已提交快照，来源可重新裁决，下一版本继续使用连续编号。

## 六类异常注入

| 参数 | 预期业务码 | 快照行为 |
| --- | --- | --- |
| `--fault unknown-version` | `E_FORMAT_VERSION` | 不提交，版本不推进 |
| `--fault text-prefix-oob` | `E_FORMAT_VERSION` | 不提交，文本块已解析部分保留 |
| `--fault unterminated-quote` | `E_PATH_MALFORMED` | 不提交，其他来源/服务仍产出结论 |
| `--fault unknown-account` | `E_ACCOUNT_UNKNOWN` | 不提交，其他服务结论不受污染 |
| `--fault declaration-conflict` | `E_DECLARATION_CONFLICT` | 不提交，并保留冲突双端偏移 |
| `--fault snapshot-gap` | `E_SNAPSHOT_VERSION` | 显示回退版本，拒绝跳号链 |

`snapshot-gap` 在空状态下会先建立连续基线快照，再注入跳号；在已有状态中会直接破坏链尾用于测试。该参数只用于本地确定性验证。

## 确定性 JSON

- 报告只含固定标记 `DETERMINISTIC_SAMPLE_V1`，不含墙钟时间。
- 服务、业务码、问题、声明、冲突、来源摘要和风险标记全部稳定排序。
- map 仅用于存储，不用于决定顺序；Go JSON 编码器对 map 键排序输出。
- 临时目录路径、SQLite 路径和主机名不进入报告。
- 同一空状态、同一输入和参数重复运行，报告字节级一致。
- 显式持久状态会在每次成功运行时按设计递增版本，因此应使用相同版本回放或新建空状态比较字节一致性。

## 实测结果

本环境实际执行结果：

```text
go build ./...                          PASS
go test ./... -count=1                   PASS（6 个含测试包）
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
                                        PASS
```

重复无状态运行（均使用 `--compact`）：

```text
run  output sha256 = 110485587e0d291d542a9d66eb2df819e956cb91156396543f3ea4aaed76d467
demo output sha256 = 3d84429bb4217860a623f588bcca9c4a78bd2f9f9d38c1caad5e6a5cff34c5bc
两次 run  cmp      = 0
两次 demo cmp      = 0
```

`demo` 顶层就是最终裁决报告字段，并额外并列 `snapshot_versions`、`replay`、`trace`、`fault_checks` 和 `demo_schema_version`。演示快照链为 `[1,2,3]`，`replay --version 2` 恢复版本 2，`trace --service "Writable Agent"` 返回三来源声明；该服务同时呈现 `HIGH_PRIVILEGE_ACCOUNT`、`UNQUOTED_IMAGE_PATH`、`WRITABLE_IMAGE_DIRECTORY` 和 `E_PRIVILEGE_EXPOSURE`。六类故障自检均为 `passed=true`，`snapshot-gap` 的前后已提交版本均为 1。

CLI 强杀验证：基线版本 1，`--crash-point commit` 退出码 86，重启后版本 2 提交，回放版本 2 成功；未出现半提交快照或来源进度。

## 边界说明

- 可写目录是稳定的本地策略模型，不读取真实 Windows ACL；扫描器声明会被合入并可复核。
- 账户权限等级是内置有序模型，不是完整 Windows 令牌模拟。
- SQLite 链尾元数据是本地可信锚点；同时协调篡改数据库和链尾元数据的攻击需要外部可信存储才能识别。
- 文本块格式没有独立魔数，解析器将其标识为固定 `legacy-text-v1` 来源格式。
