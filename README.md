# OCRLab：确定性 OCR 检测与识别诊断工作台

OCRLab 是一个纯 Go 本地工作台，用固定种子生成三档 RGB PNG 文字分片，并依次执行 **文本检测 → 文本识别 → 策略取舍 → 诊断汇总**。系统同时提供 SQLite 阶段检查点、增量失效、查询缓存、延迟预算、CLI 和仅回环监听 HTTP API。

项目不依赖 Docker、外部样本、外部 OCR 服务或运行时网络访问。样本、真值清单和 PNG 均在首次运行时由程序生成。

## 快速开始

环境：Go 1.22+。Linux/macOS/WSL：

```bash
go build -o bin/ocrlab ./cmd/ocrlab
./bin/ocrlab init --workspace ./ws
./bin/ocrlab run --workspace ./ws
./bin/ocrlab export --workspace ./ws
./bin/ocrlab verify --workspace ./ws
```

Windows PowerShell：

```powershell
go build -o ocrlab.exe .\cmd\ocrlab
.\ocrlab.exe init --workspace .\ws
.\ocrlab.exe run --workspace .\ws
.\ocrlab.exe export --workspace .\ws
.\ocrlab.exe verify --workspace .\ws
```

启动本地 HTTP 接口（仅允许回环地址）：

```bash
./bin/ocrlab serve --workspace ./ws --addr 127.0.0.1:18787
```

执行全部测试：

```bash
go test ./...
go vet ./...
```

## 内置样本

程序内置固定种子 `20261125`、5×7 连通字形和确定性 PNG 编码。三档观测固定为：

| 档位 | 分片数 | 总字符数 | 真值文本框数 |
|---|---:|---:|---:|
| small | 4 | 43 | 12 |
| medium | 16 | 257 | 64 |
| large | 40 | 806 | 200 |
| 合计 | 60 | 1106 | 276 |

每个分片包含多行不同字号（2/3/4 倍）与倾斜角（-8°～8°）的彩色文字块。清单逐分片声明 `image_id`、组别、宽高、语言标签及归一化真值框和文本。真值只读取清单，不从检测结果或缺失字段推断。

## 工作区结构

```text
ws/
  config.json                        配置与版本来源
  data/manifest.json                 确定性任务清单
  data/images/{group}/{image}.png    RGB PNG 分片
  state/state.db                     SQLite 检查点、分片序号、查询缓存
  state/publish.lock                 真实文件锁
  state/artifacts/{group}/{image}/   四阶段原子发布产物
  reports/report.json                确定性诊断报告
  reports/report.json.sha256         报告摘要
```

`init` 会生成样本、默认配置和 SQLite 库并导入清单。后续 `run`、`query`、`serve`、`export` 均会校验清单、内容指纹和已发布检查点。

## CLI

```bash
# 仅执行某一档
./bin/ocrlab run --workspace ./ws --groups small

# 清空阶段产物后重建
./bin/ocrlab run --workspace ./ws --rebuild

# 在完成 N 个新发布单元后优雅停止；用于中断/恢复验收
./bin/ocrlab run --workspace ./ws --stop-after-units 7

# 查询候选框、识别明细、单分片聚合或分组诊断
./bin/ocrlab query --workspace ./ws --kind boxes --image small-001
./bin/ocrlab query --workspace ./ws --kind recognition --image small-001
./bin/ocrlab query --workspace ./ws --kind image --image small-001
./bin/ocrlab query --workspace ./ws --kind diagnostics --group small

# 指定 5 ms 预算并绕过缓存
./bin/ocrlab query --workspace ./ws --kind boxes --image small-001 \
  --budget-ms 5 --force-recompute

# 导出并做增量结果与从零全量构建的逐字节比较
./bin/ocrlab export --workspace ./ws --out ./ws/reports/report.json
./bin/ocrlab verify --workspace ./ws
```

## HTTP API

监听地址必须为回环地址；`--addr 127.0.0.1:0` 会随机选择可用端口并在 stderr 打印实际地址。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/healthz` | 清单/配置版本健康检查 |
| GET | `/v1/queries/boxes?image_id=...` | 候选框摘要 |
| GET | `/v1/queries/recognition?image_id=...` | 识别明细 |
| GET | `/v1/queries/image?image_id=...` | 单分片四阶段聚合 |
| GET | `/v1/queries/diagnostics?group=small` | 分组诊断；可改用 `image_id` 或查全局 |
| POST | `/v1/import` | 重新导入清单 |
| POST | `/v1/run` | 执行；JSON：`{"groups":["small"],"rebuild":false}` |
| POST | `/v1/export` | 写报告；JSON：`{"path":"..."}` |
| GET | `/v1/export` | 直接下载报告 |
| POST | `/v1/verify` | 增量/全量逐字节比较 |

查询参数：

- `budget_ms=<int>`：覆盖默认延迟预算，`0` 可用于稳定触发预算错误。
- `force_recompute=true`：绕过查询缓存并重新读取/组装阶段产物。

成功响应示例字段：

```json
{
  "query_id": "q-...",
  "kind": "boxes",
  "cache_hit": true,
  "budget_ms": 250,
  "elapsed_ms": 0.123,
  "cache_key": "...",
  "cache_key_parts": {
    "kind": "boxes",
    "image_id": "small-001",
    "content_sha256": "...",
    "config_version": "cfg-...",
    "stage_version": "...",
    "argument_digest": "..."
  },
  "result": {},
  "result_sha256": "..."
}
```

超时不会等待计算完成或截断结果，而是返回 HTTP 504：

```json
{
  "query_id": "q-e42f6b486628f6c6c2eea0ec",
  "elapsed_ms": 0.058,
  "budget_ms": 0,
  "error": {
    "code": "E_LATENCY_BUDGET",
    "message": "query exceeded the configured latency budget before completing",
    "query_id": "q-e42f6b486628f6c6c2eea0ec",
    "elapsed_ms": 0.058
  }
}
```

## 错误码

| 错误码 | 触发条件 | 结构化字段 |
|---|---|---|
| `E_BOX_INVALID` | 候选框越界或宽/高为零 | `image_id`、`box_index` |
| `E_TEMPLATE_MISS` | 模板库为空、候选过小或无完整字形 | `image_id`、`candidate_index` |
| `E_LATENCY_BUDGET` | 查询超出预算 | `query_id`、`elapsed_ms` |
| `E_INCREMENTAL_MISMATCH` | 增量与从零全量结果不一致 | 首个差异项 `item`、期望/实际摘要 |
| `E_STALE_CHECKPOINT` | 落后、重复、跨分片、指纹变化、摘要不匹配 | `stage`、`reason`、`image_id` |
| `E_NOT_FOUND` | 阶段产物或镜像不存在 | `image_id`/`stage` |
| `E_INVALID_ARGUMENT` | 参数、清单或配置无效 | 上下文信息 |

同一分片的 `E_BOX_INVALID` 与 `E_TEMPLATE_MISS` 还会分别保留在检测/识别阶段产物及诊断报告的 `issues` 中，不会被静默丢弃。

## 配置与诊断探针

`config.json` 控制二值化阈值、连通域最小面积、行投影合并规则、识别归一化尺寸、策略成本、计算预算、查询延迟预算与缓存开关。配置版本为规范化 JSON 的 SHA-256 前 16 位，例如 `cfg-4803c8347c438142`。

为可重复验证错误码，配置中提供两个默认关闭的探针：

```json
{
  "diagnostics": {
    "probe_invalid_box": true,
    "probe_empty_templates": true
  }
}
```

- `probe_invalid_box`：每分片追加一个越界诊断框并记录 `E_BOX_INVALID`。
- `probe_empty_templates`：识别阶段使用空模板库，每个候选均记录 `E_TEMPLATE_MISS`，不跳过候选。

## 一致性设计摘要

- 全部产物、缓存值和报告使用规范化 JSON；报告与阶段产物不含时间戳、绝对路径、随机 ID 或实际耗时。
- 缓存键同时包含 `content_sha256`、全局 `config_version`、阶段产物 `stage_version` 和查询参数摘要；命中与重算返回同一结果字节及摘要。
- 阶段失效按相关输入摘要定位最早受影响阶段：像素/二值化/合并规则 → 检测；真值模板/识别参数 → 识别；预算/策略配置 → 策略；真值/全部上游摘要 → 诊断。
- SQLite 中每个 `(image_id, stage)` 只有一条有效检查点。发布会校验分片序号票据、上游阶段、期望前序序号，并执行条件更新。
- 产物先原子写临时文件并 `fsync`、`rename`，再提交 SQLite 事务；中断最多留下未被检查点引用的文件，不会出现半写记录。
- Linux/macOS 使用 `flock`，Windows 使用 `LockFileEx` 作为真实文件锁。
- 优雅停止只在阶段单元边界检查信号，当前阶段完成后退出；恢复时复用有效上游检查点。

更细的数据结构、版本指纹和并发发布规则见 [docs/DESIGN.md](docs/DESIGN.md)。

## 实际验证结果

验证日期：2026-11-25；环境：Linux amd64，Go 1.27.4（模块声明 `go 1.22`）；`GOOS=windows GOARCH=amd64 go build ./cmd/ocrlab` 通过。

### 三档结果

最终全量执行：60 个分片、240 个新发布阶段单元，`issues` 为空。

| 档位 | 分片/字符 | 真值框/候选框 | 检测召回 | 识别准确率 | 平均距离 | 检测策略命中 | 识别策略命中 |
|---|---:|---:|---:|---:|---:|---|---|
| small | 4 / 43 | 12 / 12 | 1.000 | 1.000 | 0.000 | d-balanced×4 | r-hash-balanced×4 |
| medium | 16 / 257 | 64 / 64 | 1.000 | 1.000 | 0.000 | d-heavy×16 | r-hash-dense×16 |
| large | 40 / 806 | 200 / 200 | 1.000 | 1.000 | 0.000 | d-heavy×40 | r-hash-dense×40 |
| 合计 | 60 / 1106 | 276 / 276 | 1.000 | 1.000 | 0.000 | — | — |

small 全体 95% Wilson 召回/准确率区间为 `[0.757499243, 1]`；总计 276 个样本的区间为 `[0.986272234, 1]`。预算单元使用量为 14052 / 250000。

报告摘要：

```text
report_sha256 = 69c60ba29387f56e7f00f92ae824ff3d0dbaa12e7b2fe3993488cce4bf28af51
```

### 延迟预算与缓存一致性

- 冷查询、缓存命中、`force_recompute` 三次候选框查询的 `result_sha256` 完全一致，缓存键一致。
- 缓存键实测包含：内容摘要 `e57744bd...`、配置版本 `cfg-4803c8347c438142`、阶段版本 `d00ad51b...`、参数摘要 `4e1967e7...`。
- `boxes`、`recognition` 与 `image` 三条查询路径返回的嵌套检测/识别字节完全一致。
- `budget_ms=0&force_recompute=true` 返回 `E_LATENCY_BUDGET`，CLI 退出码 1；实测 `elapsed_ms=0.058`，包含 `query_id`，未阻塞等待。

### 增量失效

将 `small-001` 第一条真值 `UGQ` 改为同长度 `AGQ` 后执行增量：

```text
detect:     executed=0  reused=60
recognize:  executed=1  reused=59
strategy:   executed=1  reused=59
diagnostic: executed=1  reused=59
```

随后 `verify` 从零全量重建并逐字节比较：

```text
equal = true
report_digest      = 8501eb66fd442b1fedc025e36c4ae290f217609abfce6451e706ca82a1d9ae52
full_report_digest = 8501eb66fd442b1fedc025e36c4ae290f217609abfce6451e706ca82a1d9ae52
bundle_digest      = 141abdc286a6692ddeb06b9d86701fa89f3470baa7a39582ef28ba9fba3ec966
full_bundle_digest = 141abdc286a6692ddeb06b9d86701fa89f3470baa7a39582ef28ba9fba3ec966
```

### 中断恢复

`run --stop-after-units 7` 在完成 7 个阶段单元后以 `stop_reason=unit_limit` 退出；再次 `run` 完成剩余 233 个单元，并复用 7 个检查点。最终 `verify`：

```text
equal = true
report_digest      = 69c60ba29387f56e7f00f92ae824ff3d0dbaa12e7b2fe3993488cce4bf28af51
full_report_digest = 69c60ba29387f56e7f00f92ae824ff3d0dbaa12e7b2fe3993488cce4bf28af51
bundle_digest      = d421a392bc7e2eb81e1a5a3677c302fb62568d93b061fee40633413b6daec4e0
full_bundle_digest = d421a392bc7e2eb81e1a5a3677c302fb62568d93b061fee40633413b6daec4e0
```

### 自动化测试

`go test ./...`、`go test -race ./...` 与 `go vet ./...` 均通过。测试覆盖：样本/PNG 确定性、三档全量与报告、检测召回、识别准确率、距离与 N/A、策略淘汰记录、缓存命中/重算字节一致、延迟错误、HTTP 导入/执行/查询/导出、像素/真值/二值化/合并/模板/预算失效边界、检查点回退/重复/跨分片/摘要篡改、优雅停止恢复及增量与全量逐字节比较。

## 说明与边界

- 这是诊断工作台，不是通用商业 OCR。识别模板按任务要求由清单真值字符集在本地构造；唯一用途是验证本地检测、策略、缓存和一致性链路。
- 诊断报告中的“预算消耗”是确定性的策略成本单元，以便相同输入产生逐字节一致报告；HTTP 查询预算则约束真实 wall-clock 时延。
- PNG、清单、阶段产物均保存在本地工作区；HTTP 只绑定回环地址。
