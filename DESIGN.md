# 注入检测与上下文编码裁决引擎 — 技术说明

## 1. 总览

引擎完全本地运行，不依赖任何外部数据库服务或网络账号。SQLite 通过纯 Go
驱动 `modernc.org/sqlite v1.29.2` 引入（`CGO_ENABLED=0` 静态编译验证通过），全部样本由内置
确定性生成器写到本地临时目录。

模块划分（`internal/`）：

| 包 | 职责 |
|---|---|
| `model` | 落点/编码枚举、业务码、统一内部模型、裁决结构 |
| `parse` | JSON 上下文声明 + 长度前缀文本块解析，容错与来源偏移 |
| `codec` | 上下文编码模拟、边界标记扫描、归一化前后字节偏移映射 |
| `session` | 交互侧即时裁决、结算侧闭批复核、乱序修正与推理版本 |
| `metrics` | 单调时钟延迟采样、按落点+裁决码聚合 P95/平均/最大 |
| `store` | SQLite 持久化、版本化快照、版本链与整体替换校验、恢复 |
| `scheduler` | 并发会话调度、等待队列、预算/字节上限、FIFO 提交闸门 |
| `samplegen` | 确定性样本与上下文声明生成器 |
| `report` | 稳定排序的确定性 JSON 报告 |
| `app` | 流水线装配、五类异常注入自检、崩溃恢复入口 |

CLI 入口：`cmd/injectctl`，子命令 `run / selfcheck / recover / crash`。

## 2. 输入格式与解析（要求 1）

- 上下文声明为 JSON：顶层 `context_version` 与 `declarations[]`，每条含
  `id / surface / landing / encodings`。解析器使用流式 `json.Decoder` 记录
  每条声明在字节流中的起始偏移（`SourceOffset`）。
- 载荷集为长度前缀文本块：每块 `uint32` 大端长度 + UTF-8 文本，文本前两行
  为 `id=...` 与 `surface=...`，其余字节为原始载荷（可含任意字节）。
- 校验项：落点受支持、编码集合非空且每个编码受支持、标识唯一、载荷与声明
  表面一致。非法项产出 `E_CONTEXT_INVALID`（表面越界为 `E_CONTEXT_OOB`）并
  记录来源偏移，单条非法不会中断整批，错误与合法样本一并返回。

## 3. 编码模拟与越界判定（要求 2）

判定只依据“编码后的实际字节位置”和“上下文语法边界标记”，不使用任何
关键词黑名单。五类编码：

| 编码 | 规范化行为 |
|---|---|
| `single_quote_escape` | `'` 翻倍为 `''` |
| `identifier_quote` | `"` 翻倍为 `""` |
| `param_placeholder` | 整体绑定为占位符 `?`，边界标记被结构性消除 |
| `path_normalize` | `\` 归一为 `/` |
| `html_entity` | `& < > " '` 实体化（先处理 `&` 避免二次转义） |

每个编码器维护 `归一化偏移 -> 原始偏移` 映射，扩展型编码（实体、翻倍引号）
的每个输出字节都能回溯到源字节。边界扫描规则：

- 字符串字面量：首个未配对的 `'`（配对 `''` 视为转义，留在字面量内）。
- 标识符：首个未配对的 `"`。
- 命令参数位：shell 语法边界字符表（`; | & $ ( ) < > 反引号 空白 通配符`
  等结构性标记），落在参数位即越界。
- 路径段：归一化后首个 `/`。
- 模板插值位：`}}` 插值闭合或 `<` 标签开启，取最早者。

越界返回 `E_CTX_ESCAPE`，带规则标识 `rule_id`、原始载荷偏移 `raw_offset`、
落地偏移 `landed_offset` 与边界标记 `marker`。空载荷为 `E_EMPTY_PAYLOAD`，
编码后字节超过单样本上限为 `E_ENCODE_LIMIT`。

## 4. 双侧一致性与修正（要求 3）

- 交互侧 `AdmitInteractive` 在收样时即时裁决；结算侧 `Close` 对同批样本
  独立重放同一确定性 codec，按样本标识与规则标识逐条比对。
- 同标识样本后到（乱序/重复）时显式写入 `Correction`：记录 `from/to`
  裁决、修正来源序号并递增会话推理版本，绝不静默覆盖。
- 终态报告中的推理版本统一引用确定性的快照版本号（见第 6 节），使输出不
  依赖 worker 调度顺序。

## 5. 单调时钟延迟统计（要求 4）

`metrics.Recorder.Measure` 用 `time.Now()` 的单调读数做差
（`t2.Sub(t1)` 自动使用单调时钟，不受墙钟回拨影响），按 `(landing, code)`
聚合样本数、平均、最近秩 P95、最大值。报告中的时间字段只输出固定标记
`MONOTONIC_SAMPLE`；真实纳秒值仅保留在进程内供断言，从不序列化，因此重复
运行的统计摘要字节级一致。测试通过注入确定性时钟验证 P95/平均/最大数学。

## 6. 版本化快照与整体替换识别（要求 5）

每轮结算后：

1. 结算（批次状态 + 全部裁决 + 修正）在单个 SQLite 事务提交；
2. 快照在另一个独立事务内，于同一事务中读取当前链、计算运行哈希、分配
   `MAX(version)+1` 并插入，消除版本号 TOCTOU 竞争。

快照内容哈希 `content_hash` 覆盖裁决摘要、规则集合版本与上下文声明版本；
`summary_hash` 是对整条链的折叠哈希。加载时同时校验：

- 版本号从 1 连续，跳号 → `E_SNAPSHOT_VERSION`，回退到最近连续版本；
- 链式哈希连续，即使版本号被保留、内容被整体替换也能识别为
  `E_SNAPSHOT_VERSION`。

## 7. 并发调度与资源控制（要求 6）

- 并发度 `MaxConcurrent`、队列深度 `MaxQueueDepth`、单批样本数
  `MaxSamplesPerBatch`、单样本编码字节 `MaxEncodedBytes` 均可配置。
- 超并发上限的批次进入 FIFO 等待队列而非拒绝；队列满才同步拒绝。
- 单批样本超限以 `E_SAMPLE_BUDGET` 仅终止该批次（持久化
  `budget_terminated`），其他批次结论不受影响。
- 各批次在独立 `Session` 中裁决，会话状态与推理版本隔离。
- 裁决并发执行，但持久化提交经过 FIFO 提交闸门（条件变量）按提交顺序串行
  化，保证快照版本/哈希在任意并发度下跨运行确定。`Inflight()` 与 `Queued()`
  提供在途/排队可观测计数。

## 8. 强制终止恢复（要求 7）

- 所有多写操作都是 SQLite 事务。进程在 `COMMIT` 前被强杀，重启时未提交
  事务整体回滚；已提交批次与快照不回退。
- 重启走恢复模式（单 worker、严格 FIFO）：已提交批次直接复用持久化裁决，
  不重新裁决；若批次已结算但快照事务丢失，则幂等补一张快照
  （`AppendSnapshotForBatch` 先查重再插入），版本链保持连续，重复恢复幂等。
- 恢复后若检测到版本跳号/哈希断裂，按 `E_SNAPSHOT_VERSION` 拒绝加载并报告
  最近连续版本。

## 9. 异常注入与确定性报告（要求 8）

`injectctl selfcheck` 构造并断言五类异常：

| 场景 | 期望业务码 | 隔离断言 |
|---|---|---|
| 空载荷 | `E_EMPTY_PAYLOAD` | 偏移均为 -1 |
| 上下文越界声明 | `E_CONTEXT_OOB` | 该样本不进入合法集，其余不受影响 |
| 快照版本跳号 | `E_SNAPSHOT_VERSION` | 回退到最近连续版本 |
| 并发批次竞争 | `OK` | 全部批次成功、快照链连续 |
| 编码字节超限 | `E_ENCODE_LIMIT` | 受影响批次标记，干净批次仍 `OK` |

另有 `E_SAMPLE_BUDGET` 独立预算检查。报告为 JSON，按批次标识与样本序号
稳定排序，无任何墙钟/随机字段，重复运行输入不变时字节级一致。

## 10. 业务码汇总

| 业务码 | 含义 |
|---|---|
| `OK` | 载荷被编码约束在上下文边界内 |
| `E_CONTEXT_INVALID` | 上下文声明非法（落点/编码/标识），带来源偏移 |
| `E_CONTEXT_OOB` | 载荷表面越出声明上下文 |
| `E_CTX_ESCAPE` | 编码后越过上下文语法边界，带规则与双侧偏移 |
| `E_EMPTY_PAYLOAD` | 空载荷 |
| `E_ENCODE_LIMIT` | 编码后字节超过单样本上限 |
| `E_SAMPLE_BUDGET` | 单批样本数超预算，仅终止该批次 |
| `E_SNAPSHOT_VERSION` | 快照版本跳号或整体替换，回退最近连续版本 |

## 11. 本地验证命令

```bash
go build ./...
go vet ./...
go test -race ./...
go run ./cmd/injectctl run -workdir <dir>            # 生成报告 report.json
go run ./cmd/injectctl selfcheck -workdir <dir>     # 五类异常自检
go run ./cmd/injectctl crash   -workdir <dir>       # 首快照提交后 exit 9
go run ./cmd/injectctl recover -workdir <dir>       # 重启恢复，幂等可重复
```

Windows 命令行（PowerShell/cmd）下同样可用：`go run .\cmd\injectctl ...`，
路径与参数语义一致；纯 Go SQLite 驱动无需 gcc 或外部服务。
