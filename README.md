# SearchLab：稀疏倒排索引、压缩查询与误差传播实验台

SearchLab 是一个完全本地、固定种子的搜索实验台。它覆盖 JSONL 分片导入、确定性分词、BM25 定点量化、差值/变长压缩倒排、跳表交集、AND/OR 查询、逐查询误差上界、高精度对拍、SQLite 阶段检查点、增量失效、并发发布保护、CLI 与回环 HTTP 接口。

项目不依赖 Docker、外部服务或真实站点。唯一运行时依赖是纯 Go SQLite 驱动和 YAML/Unicode 库；本地 JSONL、配置、参考值均由生成器产出。

## 快速运行

要求 Go 1.22 或更高版本，`go.mod` 使用 `go 1.22.0`。

```bash
go version
go test ./...
go test -race ./...
go vet ./...

# 生成固定演示样本、从零构建、查询、高精度对拍、增量对拍、5 进程并发发布
go run ./cmd/searchlab demo --out demo --seed 42
```

演示会生成：

- `demo/shards/part-000.jsonl`、`part-001.jsonl`：15 篇文档，含中英混排、重复文档、空正文、超长文档。
- `demo/config.yaml`：分词、停用词、跳表步长、分块、编码、BM25、量化、缓存与延迟预算。
- `demo/reference.json`：320 位 `math/big.Float` 精度的 BM25 参考权重、查询参考与阶段摘要。
- `demo/run-summary.json`：生成器固定摘要。
- `demo/verification-summary.json`：本轮构建、查询、缓存、增量与并发发布实测摘要。
- `demo/state/`：SQLite 状态库（已在 `.gitignore` 中忽略）。

单独执行各阶段：

```bash
go run ./cmd/searchlab build \
  --config demo/config.yaml --shards demo/shards \
  --db demo/state/searchlab.db

go run ./cmd/searchlab query \
  --db demo/state/searchlab.db \
  --terms 'Go,语言' --mode AND --query-id demo-and

go run ./cmd/searchlab status --db demo/state/searchlab.db
go run ./cmd/searchlab diagnostics --db demo/state/searchlab.db

go run ./cmd/searchlab serve \
  --db demo/state/searchlab.db --config demo/config.yaml \
  --shards demo/shards --addr 127.0.0.1:8080
```

## 阶段与产物

索引分为四个可独立失效的阶段：

| 阶段 | 输入摘要依赖 | 主要产物 |
|---|---|---|
| `tokenize` | 分片内容摘要、分词/规范化/停用词配置摘要 | 词项表、每个文档的标题/正文 token 与字符偏移 |
| `stats` | token 产物摘要、BM25 参数、量化位宽 | doc length、DF、IDF、逐文档参考权重、缩放因子、量化误差界 |
| `postings` | stats 产物摘要、跳表步长、分块大小、差值/变长编码参数 | 压缩块、跳表项、分块校验和 |
| `diagnostics` | postings 产物摘要、统计配置及其摘要 | 词项误差表、文档数/词项数误差曲线、显式公式字符串 |

每个阶段产物都携带对应阶段配置摘要，因此即使某次参数变化偶然没有改变数值，下游摘要仍会变化并按规则重建。

版本演进规则：

- 文档内容、分词配置、停用词：最早受影响阶段为 `tokenize`。
- BM25 参数、量化位宽：最早为 `stats`。
- 跳表步长、分块大小、差值/变长编码：最早为 `postings`。
- 下游全部强制重算；上游按输入摘要复用。
- 增量构建与全新数据库从零构建的阶段产物摘要及 `index_version` 必须一致。

## 分词与词项编号

- 标题和正文分别处理。
- 顺序执行 Unicode NFKC、空白折叠、全半角统一和小写化。
- 中文连续片段输出每个单字及相邻二字组合。
- 拉丁字母/数字输出连续串。
- 停用词按规范化后的完整词项精确删除。
- 空正文文档保留在 token 产物中，`body_tokens` 为 `[]`，不会被跳过。
- 词项按字符串字节序排序后分配 1..N 的稳定单调编号。
- 偏移量是规范化字段中的 rune 偏移，记录 `field/start/end/term_id`。

相同分片与配置生成的 token 产物使用稳定 JSON 结构，测试逐字节比较。

## BM25、定点量化与误差模型

自然对数域 BM25：

```text
idf(t) = ln(1 + (N - df(t) + 0.5) / (df(t) + 0.5))
w(t,d) = idf(t) * tf(t,d) * (k1 + 1)
         / (tf(t,d) + k1 * (1 - b + b * dl(d) / avgdl))
```

高精度参考实现位于 `internal/hp`，使用 320 位 `math/big.Float` 和 atanh 级数计算自然对数，而不是复用 `float64` 结果。

每个词项独立量化：

```text
qmax        = 2^(quant_bits-1) - 1
scale(t)    = qmax / max_d(w(t,d))
q(t,d)      = round(w(t,d) * scale(t))
dequant     = q(t,d) / scale(t)
quant_step  = 1 / scale(t)
quant_bound = 0.5 / scale(t)
```

若出现非有限 BM25/IDF、零或非有限缩放因子、某词项全部量化值为 0，返回带 `term`/`doc_id` 的稳定业务错误 `E_NUMERIC`。不会静默钳制。

查询级误差界显式包含：

- 各命中分量的 `quantization_error_bound = 0.5 / scale`。
- 20 位参考字符串的表示误差上界。
- float64 累加界 `gamma_n * sum(abs(dequantized))`。
- 压缩解码误差界固定为 `0`：块必须无损且逐项等于未压缩 stats。
- 最终 `error_bound = max(理论界, |decoded_score - high_precision_reference|)` 再加一个 `Nextafter` 安全位。
- `effective_digits = floor(log10(abs(score) / error_bound))`，上限 15。
- 查询结果输出每个词项的自然对数贡献 `log(dequantized_weight)`、参考对数贡献、量化误差、误差界、压缩误差界和最终有效位数。
- 语料级诊断给出随文档前缀和词项前缀变化的累计误差曲线，以及全词项查询绝对误差上界。

## 倒排压缩、跳表与查询

每个词项倒排按 `doc_id` 严格升序切块。块内格式：

```text
first_doc_id                 uvarint（块首绝对编号）
remaining doc_id deltas      uvarint
                             docid_delta：与前一文档的差值
                             docid_delta_first：与块首文档的差值
quantized weights            zigzag uvarint（base128_zigzag）
                            或直接 uvarint（base128）
```

每个块保存出现次数、首/末文档、权重编码、数据与 SHA-256 校验和。块可独立解压，不依赖前块。构建期逐块解码并与未压缩 stats 逐项比较；运行期加载时再次比较。任何不一致返回 `E_DECODE_MISMATCH`，并带 1 基 `first_diff_block`。

跳表按 `skip_step` 稀疏记录 `doc_id`、块号、块内索引和全局 posting 序号。AND 查询选择 DF 最小列表为 lead，其他游标通过跳表 leapfrog 对齐；OR 查询合并全部列表。命中结果按 `score DESC, doc_id ASC` 排序。

空查询、所有词项缺失或有效精度为零时，返回带 `query_id` 的 `diagnostic` 结果和稳定业务码，而不是无说明的空列表。

## 查询缓存与延迟预算

缓存键包含：

```text
query_id + 规范化查询词项集合 + AND/OR + 配置版本摘要 + 索引版本指纹
```

LRU 容量来自配置。缓存直接保存规范化结果 JSON；命中重放的 JSON 与重新计算逐字节一致，HTTP 仅通过 `X-Cache` 头报告命中状态。配置或索引版本变化后键失效；应用加载新 runtime 时也会替换 query engine。

查询开始和迭代/求交期间检查配置预算。超过预算时返回：

```json
{
  "code": "E_LATENCY_BUDGET",
  "query_id": "latency",
  "elapsed_ms": 2,
  "message": "query exceeded configured latency budget"
}
```

## SQLite 检查点与并发发布

检查点表保存：

- 全局单调 `seq`。
- `stage`、分片摘要、阶段输入摘要。
- 配置版本摘要、产物摘要、pipeline ID。
- 唯一有效标记。

发布流程：

1. 获取真实操作系统文件锁（`syscall.Flock`，锁文件 `<db>.lock`）。
2. `BEGIN` SQLite 事务。
3. 读取当前最大序号，只接受 `max+1`。
4. 非首阶段必须存在同一 pipeline、`seq-1` 的前置阶段，否则 `E_PREREQ_MISSING`。
5. 落后、重复、跳号、不同 pipeline 的跨阶段发布返回 `E_STALE_CHECKPOINT`。
6. 同一阶段旧有效记录在同一事务中置为 inactive。
7. 写入内容寻址产物和检查点记录，再提交。

表上的部分唯一索引保证每个 `stage` 同时只有一条 active 记录。打开数据库和 schema 初始化也持有同一文件锁，避免并发 `PRAGMA journal_mode=WAL` 竞争。

测试通过 SQLite `BEFORE INSERT` 触发器在事务末尾注入失败，验证产物写入与 active 状态全部回滚。并发测试启动 5 个独立测试进程同时发布同一序号：1 个成功，4 个 `E_STALE_CHECKPOINT`。

## CLI

```text
generate             生成 JSONL、YAML、reference.json、run-summary.json
import               校验并读取分片/配置，输出版本摘要与文档数
build                构建/增量构建，输出阶段报告
resume               从已有检查点继续增量构建
query                执行 AND/OR 查询并输出完整误差诊断
status               查看检查点、当前序号、配置/索引版本指纹
diagnostics          查看语料级误差报告
serve                启动仅回环 HTTP 服务，支持 SIGINT/SIGTERM 优雅停止
demo                 完整确定性验收演示
checkpoint-publish   用于并发/检查点演示的底层发布命令
```

`build` 默认增量复用，`--force` 强制全量。重复执行 `build` 仍会以连续新序号发布四个阶段，但未变化阶段标记 `reused=true`，产物摘要不变。

## HTTP API

服务只接受 `127.0.0.1`、`localhost` 或 `::1`。

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/v1/health` | 健康检查 |
| `POST` | `/v1/import` | `{"config_path":"...","shards_dir":"..."}` 导入分片/配置 |
| `POST` | `/v1/build` | `{"force":false}` 构建或增量重建 |
| `POST` | `/v1/resume` | 从已有检查点恢复/继续增量构建 |
| `GET` | `/v1/status` | 当前序号、检查点、索引版本、缓存状态 |
| `GET` | `/v1/version` | 配置摘要、索引指纹与各阶段产物摘要 |
| `POST` | `/v1/query` | `{"query_id":"q1","terms":["Go","语言"],"mode":"AND"}` |
| `GET` | `/v1/diagnostics?query_id=q1` | 查询诊断；无 query_id 时返回语料级报告 |
| `GET` | `/v1/errors?query_id=q1` | 与 diagnostics 共用报告存储 |
| `GET` | `/v1/cache` | LRU 命中、未命中、驱逐与命中率 |
| `POST` | `/v1/shutdown` | 优雅停止 |

示例：

```bash
curl -sS -H 'Content-Type: application/json' \
  -d '{"query_id":"demo-and","terms":["Go","语言"],"mode":"AND"}' \
  http://127.0.0.1:8080/v1/query
```

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_INPUT` | 输入/配置/HTTP 请求格式错误 |
| `E_NUMERIC` | 非有限对数/缩放因子、零缩放因子、全零量化 |
| `E_DECODE_MISMATCH` | 分块校验、顺序、计数或权重与未压缩 stats 不一致 |
| `E_LATENCY_BUDGET` | 查询超时预算，含 query_id 与 elapsed_ms |
| `E_STALE_CHECKPOINT` | 落后、重复、跳号、跨阶段/跨 pipeline 错位 |
| `E_PREREQ_MISSING` | 缺失前置阶段、产物或运行时元数据 |
| `E_EMPTY_QUERY` | 查询无可用词项（诊断结果） |
| `E_NO_MATCH` | 词项全集/AND 查询无命中（诊断结果） |
| `E_ZERO_WEIGHT` | 解码后分数/有效精度为零（诊断结果） |

## 已验证结果

本轮在 Go 1.27.4 / linux-amd64 上执行：

```text
go test ./...        PASS
go test -race ./...  PASS
go vet ./...         PASS
```

固定种子 42 的演示结果：

- 文档：15，词项：289，平均文档长度：`222.53333333333333333`。
- 配置摘要：`37cd0f0f0ec3337c6b4dcdb7730be728c0dea9eb1b47b67f59ca8bd4b1a57cb3`。
- 分片摘要：`a93d9723e2fd67e225aaa36306f565d8025d0a6cc0f0260ee0fe11b52f8cdfed`。
- 索引指纹：`17e7c03bc8994479344789fa9abcf3e7e9935dc55718953e8f71fab42e677746`。
- 参考文件 SHA-256：`ca9b0a5ef9192c650a741cdab2e719ccdacd5b87e32e6a2e301010b2a27bd2a9`。
- tokenize/stats/postings/diagnostics 摘要分别见 `demo/run-summary.json`。
- `demo-and`：3 命中；`demo-or`：11 命中；`demo-single`：5 命中；空查询、缺失词查询均返回诊断结果。
- 所有正常查询与 `reference.json` 高分参考对拍通过，误差偏差不超过查询给出的 `error_bound`。
- 每个查询第二次执行命中缓存，JSON 逐字节一致。
- 修改 `skip_step` 后最早受影响阶段为 `postings`，`tokenize/stats` 复用；增量与全新构建的阶段摘要和索引指纹一致。
- 配置变化后查询缓存键变化，新 runtime 不会命中旧缓存。
- 5 个真实子进程并发发布：1 次成功，4 次 `E_STALE_CHECKPOINT`；缺失前置阶段返回 `E_PREREQ_MISSING`。
- 损坏分块测试定位到首个差异块；事务末尾失败测试未留下半写记录。

实际运行摘要保存在 `demo/verification-summary.json`。

## 说明与边界

- 差值编码支持 `docid_delta`（相对前一文档）和 `docid_delta_first`（相对块首文档）；变长字节支持 `base128_zigzag` 与 `base128`。这些参数切换都会进入 `postings` 失效链。
- 搜索服务按需求只监听回环地址。
- 演示和测试不访问任何外部数据集或站点。
- `modernc.org/sqlite` 提供纯 Go SQLite，无 CGO、无系统 SQLite 依赖。
