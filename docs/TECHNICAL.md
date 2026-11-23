# HTTP 请求走私规范化裁决组件 — 技术说明

## 组件结构

```
smuggle/            可集成 Go 包（宿主代理服务导入 "smuggle/smuggle"）
  codes.go          稳定业务码定义
  parse.go          报文归一化与请求边界计算核心
  adjudicate.go     走私面裁决引擎 + 双算法路径
  snapshot.go       带版本号的裁决快照（SQLite / modernc.org/sqlite）
  batch.go          并发连接批处理与资源控制
  report.go         确定性排序 JSON 报告
  samples.go        本地确定性样本集生成与加载
  inject.go         异常注入验证链路
cmd/smuggle/        命令行入口（gensamples / run / inject / verify / all）
```

## 关键设计

### 1. 归一化与边界计算（parse.go）
- 起始行、头部字段、消息体长度分别解析；头部名称统一小写比较。
- obs-fold 续行归一化为单空格拼接；字段值首尾 SP/HT 空白裁剪。
- Content-Length：多值（重复头或逗号列表）需数值一致，冲突返回 `E_CL_CONFLICT`；
  非法值返回 `E_CL_MALFORMED`。
- Transfer-Encoding：多值合并、大小写归一；chunked 必须为最终编码（否则 `E_TE_ORDER`）；
  未知编码返回 `E_TE_UNKNOWN`；CL 与 TE 并存记录 `E_CL_TE_CONFLICT` 并按 TE 优先继续定界。
- 分块长度允许前导零与大小写十六进制；obs-fold 嵌入分块边界返回 `E_CHUNK_OBSFOLD`。
- 任何无法确定边界的报文返回稳定业务码，绝不静默截断。

### 2. 裁决引擎（adjudicate.go）
四类结论：`CONSISTENT` / `COUNT_MISMATCH` / `BOUNDARY_MISMATCH` / `ENCODING_AMBIGUITY`。
优先级：视角解析业务码（编码歧义）> 条数不一致 > 边界错位 > 一致。
偏离结论携带首个偏离字节偏移、涉及头部字段名与裁决码 `E_SMUGGLE_AMBIGUITY`。
比较仅基于边界区间，头部顺序差异不会误报。

### 3. 双算法路径（parse.go / adjudicate.go）
- 路径选择：`SelectPath` 以两视角头部终止符数量的较大者估计请求数，
  低于阈值走 `precise`（逐请求精算），否则走 `stream`（流式扫描）。
  选择完全由输入字节与阈值决定，无随机或墙钟因素；阈值与路径在报告中可观测。
- 一致性保证：两条路径共用 `scanNext`/`stepView` 原语与同一个增量 `comparer`，
  测试 `TestDualPathEquivalence` 对含 300 条流水线请求的语料逐字段断言结论一致。

### 4. 版本化快照（snapshot.go）
- 每连接裁决完成后在事务内提交递增版本号快照（主键 `(conn_id, version)`，
  已提交快照不可部分覆盖）。
- `IsWholesaleReplacement` 通过内容 SHA-256 识别整体替换式版本变化，而非仅比对版本号。
- `LoadChain` 校验版本链连续性，发现跳号返回 `E_SNAPSHOT_VERSION` 并回退到最近连续版本前缀。
- 并发提交由互斥锁 + 单连接池串行化，避免 SQLite 写竞争。

### 5. 批处理资源控制（batch.go）
- 多连接在独立会话裁决，会话间无共享解析状态。
- 并发度（`MaxConcurrency`）、等待队列（`MaxQueue`）、单连接请求数上限
  （`MaxRequestsPerConn`）均可配置。
- 准入按提交序号确定性判定：超出 `MaxConcurrency+MaxQueue` 的连接返回
  `E_BATCH_CONTENTION` 并记录连接标识；单连接超限以 `E_REQUEST_BUDGET`
  终止并保留已裁决部分。

### 6. 异常注入（inject.go）
五类畸形样本仅通过参数（`-anomalies` 或 `RunInjection` 入参）生效，不修改源码：
`cl-te`、`cl-conflict`、`chunk-leading-zero`、`obs-fold-chunk`、`unknown-te`。
链路断言：对应业务码、金丝雀连接不受污染、快照版本递增（v1 -> v2）。

### 7. 确定性报告（report.go）
- 报告按连接相对路径稳定排序，请求按序号、偏离偏移有序。
- 时间字段仅使用固定采样标记 `SAMPLE-0001` / `SNAPSHOT-SAMPLE-0001`。
- 输入不变且并发度变化时报告字节级一致（`TestReportDeterministicAcrossRunsAndConcurrency`）。

## 使用

```sh
# 一键运行：生成样本 -> 批裁决 -> 异常注入验证（纯本地文件）
go run ./cmd/smuggle all -workdir out

# 分步运行
go run ./cmd/smuggle gensamples -dir samples
go run ./cmd/smuggle run -samples samples -db out/adjudicate.db -report out/report.json \
    -workers 4 -threshold 64 -budget 1024 -concurrency 4 -queue 16
go run ./cmd/smuggle inject -anomalies all -db out/inject.db -report out/inject-report.json
go run ./cmd/smuggle verify -db out/adjudicate.db -conn normal/consistent
```

宿主集成：

```go
st, _ := smuggle.OpenStore(dbPath)
eng := smuggle.NewEngine(smuggle.DefaultConfig(), st)
reports := eng.Run([]smuggle.Connection{{ID: "conn/1", Proxy: proxyRaw, Backend: backendRaw}})
```

## 环境说明
- Go 1.22+（实际验证 go1.27.4）；`GOOS=windows go build` 交叉编译通过，
  代码仅使用 `os`/`path/filepath`/`database/sql`，临时目录经 `os.MkdirTemp` 可读写。
- SQLite 驱动 `modernc.org/sqlite v1.63.0`（纯 Go，无外部数据库服务）；
  依赖已写入本地模块缓存，`GOPROXY=off go build ./...` 验证离线可构建。
- 未使用 Docker；样本由 `WriteSamples` 程序内生成，不访问网络与外部目标。

## 实际验证结果（2026-11-24）
- `go vet ./...` 通过；`go test ./...` 全部通过（归一化 11 例、裁决 6 例、
  双路径一致性语料、快照链 3 例、批处理 3 例、报告确定性 2 例、注入链路 1 例）。
- `go run ./cmd/smuggle all -workdir out`：9 条样本连接裁决结果符合预期
  （一致/条数不一致/边界错位/编码歧义 + 五类畸形样本业务码），注入链路 5/5 PASS。
- 重复运行 `report.json`、`inject-report.json` 与样本目录字节级一致；
  `-workers 1` 与 `-workers 16` 报告字节级一致。
- 200 条流水线请求连接在 `-threshold 100` 下选择 `stream` 路径，结论与精算路径一致。
- `verify` 子命令对正常连接返回 `code=OK contiguous_versions=1`；
  跳号场景由 `TestSnapshotVersionGap` 覆盖（返回 `E_SNAPSHOT_VERSION` 并回退）。
