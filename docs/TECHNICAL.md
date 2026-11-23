# 实现与设计说明

## 组成

| 路径 | 职责 |
| --- | --- |
| `internal/workbench/parser_binary.go` | `DNSL` v1/v2 头部、资源记录和截断/未知版本处理 |
| `internal/workbench/parser_json.go` | JSONL 行级解析、十六进制/十进制事务号、字段别名 |
| `internal/workbench/parser_text.go` | 长度前缀块、字段和记录列表解析 |
| `internal/workbench/session.go` | 源发现、格式分类、并发工作队列和会话隔离 |
| `internal/workbench/correlate.go` | 跨源冲突、事务配对、四类可疑迹象 |
| `internal/workbench/snapshot.go` | SQLite 快照事务、内容哈希、父链哈希、兼容加载 |
| `internal/workbench/engine.go` | 一轮窗口的编排和稳定报告构造 |
| `internal/sample/generate.go` | 干净样本与五类注入样本生成 |
| `internal/verify/verify.go` | 注入后业务码、旧快照、其余裁决断言 |
| `cmd/dnswatch/main.go` | 专业子命令入口 |

## 解析会话与资源控制

`DiscoverSources` 按扩展名识别 `.dnsbin/.bin`、`.jsonl/.json`、`.dnslog/.txt`，并把源按相对路径排序。`ParseWindow` 将源放入有界任务队列，只启动 `MaxConcurrency` 个 worker。超过并发上限的源在队列中等待，不会因繁忙被直接拒绝。

每个源调用独立的解析函数；解析状态、版本集合和错误列表均位于该次 `SessionResult` 内，没有跨会话可变全局状态。报告回收到主流程后才进行跨源关联。

`MaxEvents` 是每源预算。解析器在下一事件开始前检查预算，超限时保留全部已成功事件、增加 `Skipped`，并在该事件的真实偏移处记录 `E_EVENT_BUDGET`。解析错误同样带有 `Source` 与 `Offset`，文件打开错误使用偏移 0 和 `E_SOURCE_OPEN`。

## 归一化

事务号最终都是 `uint32`。`0x` 前缀按十六进制解析，其余按十进制；JSON 数字/字符串同样归一。任何负数或超过 `0xffffffff` 的输入被拒绝。

问题名规范化步骤：

1. 去除首尾空白和一个尾点；
2. 按点分标签，拒绝空标签、超出 63 字节的标签和超出 255 字节的线格式；
3. 标签转小写；
4. 生成 `[长度][正文]...0` 的规范线格式；
5. 同时生成标签长度序列键，例如 `3:www|7:example|4:test|`。

因此 `Cache.Example.Test.` 与 `cache.example.test` 在 JSON、文本块、v1/v2 二进制中都可配对。

## 事务冲突与裁决规则

### `E_NAME_CONFLICT`

收集同一 32 位事务号的全部事件。只要两个不同来源中的规范化问题名不同，就产生一条冲突；报告包含直接出现位置 `source/offset` 和另一侧 `related_source/related_offset`。同源同名重复不会误报。

### `E_UNSOLICITED`

以 `(txid, name_wire)` 为事务分组。组内存在 response 但不存在 query 时，以时间、源路径、偏移排序后的首个 response 作为偏离位置。

### `E_ADDITIONAL_INJECT`

对每个 response 检查附加区。若存在 `A`/`AAAA` 地址记录，且其规范化名称与问题名不同，记录该记录自身的源与偏移。每份应答报告首个无关地址。

### `E_TTL_MISMATCH`

按规范问题名收集 response 与 cache 事件。对每个 cache 事件，在 `WindowTicks` 内选择时间最近的上一条 response，按地址比对应答 TTL 与缓存 TTL。首个不一致地址产生 `E_TTL_MISMATCH`，位置是缓存记录偏移。

### `E_FLIP_FLOP`

按规范问题名收集 response/cache 中同名的首个地址观测，按 tick、源、偏移稳定排序。相邻观测地址改变且 tick 差不超过 `FlipWindowTicks` 时，在第二个观测的地址记录处报告一次。`0` 表示不设置上限。

### 排序与时间

报告源按相对路径排序；事务按事务号再按问题名排序；所有发现按源路径、事务号、业务码、偏移、关联源排序。时间只使用 `S%06d` 虚拟采样标记，不使用墙钟、随机数或主机本地时区。

## 快照链

### 表结构

`snapshots` 一行一个版本：

- `version`、`parent_version`；
- `parent_chain_hash`、`content_hash`、`chain_hash`；
- `required_parser_version`、`format_versions`；
- `target_parser_version`、`sample_clock`；
- `report_json`。

### 单事务提交

`CommitReport` 在一个 SQLite 事务中：

1. 读取并校验现有完整链；
2. 计算下一版本、父版本和父链哈希；
3. 若 `versionJump > 0`，在插入前整体回滚并返回 `E_SNAPSHOT_VERSION`；
4. 计算内容哈希和链哈希；
5. 插入版本并回填含快照元数据的报告；
6. 一次 `COMMIT`。

任何失败都由同一个事务回滚，之前提交的快照内容不变。

### 内容与链哈希

`content_hash = SHA256(canonical_report_without_snapshot_metadata)`。报告使用标准 JSON 序列化，切片已排序，避免映射迭代顺序影响字节。

```text
chain_hash_i = SHA256(
  parent_chain_hash_i NUL version_i NUL content_hash_i NUL
  required_parser_version_i NUL sorted(format_versions_i, ",") NUL
  target_parser_version_i NUL sample_clock
)
```

加载指定版本时重新读取 1..N 全部行，校验连续版本、父版本、父链哈希、内容哈希和链哈希。即使只替换某版本的 `report_json` 而保持版本号不变，内容哈希或父链也会失配，返回 `E_SNAPSHOT_VERSION`。因此比较不只依赖版本号。

### 解析器兼容矩阵

| 当前版本 | 可加载的 required parser |
| --- | --- |
| v1 | v1 |
| v2 | v1, v2 |

`required_parser_version` 由观察到的最高格式版本决定：出现任一 v2 输入即为 v2。加载不兼容旧快照时在反序列化业务报告前整体拒绝。`switch-version` 展示该结论；`replay`/`query` 对不兼容快照返回同一业务码。

`target_parser_version` 是生成该快照时的目标环境。把同一输入按 v1/v2 扫描时，源摘要、事务配对和四类发现保持一致；差异集中在兼容结论、业务码、目标版本和因此变化的快照哈希元数据上。

## 注入验收

`internal/sample` 生成：

1. `truncated-binary`：在干净二进制源尾部追加声明长度超过剩余字节的记录；
2. `unknown-version`：追加合法长度但版本 9 的完整记录；
3. `text-overflow`：追加长度前缀 `999999` 和少量短载荷；
4. `name-conflict`：增加独立 JSON 源，同一事务号映射到不同问题名；
5. `snapshot-jump`：输入保持干净，扫描使用 `SnapshotJump=2`。

`internal/verify` 对每例先提交干净基线，再解析注入或尝试跳号，断言：

- 对应业务码出现；
- 四类可疑裁决仍全部存在；
- 失败前已提交基线的内容哈希不变且可加载；
- 干净源事件数保持不变；
- 跳号失败后快照总数和末版本都不变。

注入完全通过生成器参数、输入文件与命令行完成，测试不修改源码。

## 确定性

样本时间只来自显式 tick；报告没有时间戳、时区、随机 ID、临时绝对路径或数据库行号。源相对路径、事务、发现和快照链均稳定排序。`demo` 在两个独立临时 SQLite 数据库上扫描同一输入，直接比较两次报告字节。

SQLite 文件本身不是报告产物；仅 `report_json` 的规范化内容参与内容哈希，快照元数据单独参与链哈希。
