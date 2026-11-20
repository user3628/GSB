# 中文稀疏倒排索引：构建、压缩与一致性查询

本项目从零实现一条本地、确定性、可恢复的中文检索管线。所有输入、配置、检查点和查询结果都来自本地文件；运行时不需要容器、外部服务或网络。

实现覆盖四个阶段：

1. `tokenize`：NFKC、空白折叠、全半角统一、小写化、中文单字+相邻二字、拉丁字母数字连续串、停用词过滤、稳定词项编号。
2. `statistics`：文档频次、文档长度、平均长度、BM25、定点量化与逐词项缩放因子。
3. `postings`：doc_id/权重双差值、LEB128、可配置分块、CRC32、稀疏跳表、逐块回读校验。
4. `query_summary`：惰性分块解压、跳表求交、AND/OR 查询、与独立顺序遍历参考实现逐字段比对。

## 快速开始

要求 Python 3.11。仓库中的 `requirements.txt` 同时给出 Windows 安装命令。

```powershell
py -3.11 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt

# 重新生成固定种子样本（可选，仓库已包含生成结果）
.venv\Scripts\python.exe scripts\generate_dataset.py

# 完整构建
.venv\Scripts\python.exe scripts\build_index.py --config data\index.yaml

# 状态、压缩块校验、参考查询、HTTP 服务
.venv\Scripts\python.exe scripts\build_index.py status --config data\index.yaml --pretty
.venv\Scripts\python.exe scripts\build_index.py verify --config data\index.yaml --pretty
.venv\Scripts\python.exe scripts\reference_query.py --config data\index.yaml --terms 检索 索引 --mode and
.venv\Scripts\python.exe scripts\serve.py --config data\index.yaml --host 127.0.0.1 --port 8765

# 固定演示与验收报告
.venv\Scripts\python.exe scripts\run_demo.py --config data\index.yaml
```

Linux/macOS 将 `py -3.11` 和 `.venv\Scripts\python.exe` 替换为 `python3.11` 和 `.venv/bin/python` 即可。

`scripts/run_demo.py` 会执行完整构建、参考一致性校验、缓存命中/淘汰、停止恢复、增量失效、本地 HTTP 查询和错误注入，并写出 `artifacts/demo_report.json`。

## HTTP 接口

```bash
curl http://127.0.0.1:8765/health
curl http://127.0.0.1:8765/status
curl http://127.0.0.1:8765/summary
curl -X POST http://127.0.0.1:8765/query \
  -H 'Content-Type: application/json' \
  -d '{"query_id":"q1","terms":["检索","索引"],"mode":"and","limit":20}'
```

业务错误统一返回：

```json
{"ok":false,"error":{"code":"E_QUERY_OUT_OF_BOUNDS","message":"...","details":{}}}
```

## 数据与配置

- `data/shards/*.jsonl`：固定种子 `20261120` 生成，共 34 篇文档、4 个分片，含中英混排、跨分片近似重复和 2 篇空正文文档。
- `data/index.yaml`：分词、停用词、BM25、量化、分块、跳表、编码、缓存和延迟预算配置。
- `data/queries.jsonl`：8 条 AND/OR 基准查询，其中包含未知词项。
- `data/error_cases/`：非法跳表步长、越界查询、零延迟预算样本；注入只改配置，不改源码。
- `artifacts/`：阶段产物、检查点、查询缓存和固定演示报告。

同一输入与配置产生的 `terms.json`、权重统计、postings 二进制和查询摘要均使用规范 JSON/稳定排序；产物中不写时间戳或绝对工作区路径。

## 查询语义与缓存

- `terms` 被视为词项集合；重复词项去重，但输出按规范化词项排序。
- `and` 要求所有已解析词项同时命中；任一未知词项使结果为空。
- `or` 返回至少命中一个已解析词项的文档。
- 分数为量化权重还原后的浮点累加；按 `score` 降序、同分按 `doc_id` 升序排列。
- 每个命中包含所属 `term_id`、词项、量化值、量化和缩放因子及还原权重。
- 查询缓存键包含规范化词项集合、词项编号、AND/OR 语义、limit、完整配置指纹和阶段指纹/产物摘要。
- 缓存只保存结果对象；命中与未命中的 `result` 逐字段一致。LRU 淘汰和持久化缓存不会改变结果。

### 延迟预算

查询开始后持续检查配置的 `query.latency_budget_ms`。超时返回：

```json
{"code":"E_LATENCY_BUDGET","details":{"query_id":"...","elapsed_ms":0.02,"latency_budget_ms":0}}
```

这是协作式预算检查，不做强制线程终止，也不会返回截断结果。

## 压缩格式

`postings.bin` 文件头为 `CSIDX1\0`。每个分块为：

| 字段 | 长度 | 说明 |
|---|---:|---|
| magic | 4 | `PBLK` |
| version | 1 | 当前为 1 |
| payload length | 4 | 大端 |
| CRC32 | 4 | 大端，覆盖 payload |
| payload | 变长 | posting 数量、doc_id 差值、权重差值 |

每个分块独立解码：首条 doc_id 和首条权重的 zigzag 值使用绝对编码，后续使用差值；doc_id 差值编码为无符号 LEB128，权重差值编码为 zigzag LEB128。分块边界通过长度、CRC、数量、首尾 doc_id 和“不得有尾随字节”共同校验。

跳表按 `postings.skip_step` 稀疏记录 `{ordinal, doc_id, block_index}`。AND 查询用跳表向前跳跃并在块内扫描；OR 查询按 doc_id 做堆归并。单个游标只保留当前解压块，不预解压整条倒排列表。

## 检查点、停止和恢复

每个阶段完成后，先原子写产物，再发布检查点。检查点位于 `artifacts/checkpoints/NNNNNNNN-stage.json`，包含：

- 阶段名和阶段号。
- 输入摘要、完整配置版本。
- 产物复合摘要、阶段链式指纹。
- 全局单调序号、上一条记录摘要和本记录自校验摘要。

写入过程使用同目录临时文件、`fsync` 和 `os.replace`。中断只会留下可忽略的 `.tmp` 文件，不会产生半写记录。恢复时扫描有效记录链，忽略损坏、缺字段、摘要不符、序号重复、阶段错位和断链记录，从最长一致前缀继续。

`E_STALE_CHECKPOINT` 和 `E_INVALID_STAGE` 由跨进程文件锁保护。全局序号按高水位严格 `+1` 推进；失效记录会移入 `checkpoints/archive/`，因此重试不会复用旧序号。

协作式停止只通过 `SIGINT`/`SIGTERM` 设置标志；构建在文档、词项或查询单元边界检查标志。未完成的当前阶段不会发布检查点。强制终止不承诺生成退出报告。

可复现的停止测试入口：

```bash
python scripts/build_index.py --config data/index.yaml --force --stop-after-units 3
python scripts/build_index.py --config data/index.yaml
```

## 增量失效

阶段指纹是内容链：

```text
stage1 = hash(索引版本 + 文档内容摘要 + 分词/停用词配置)
stage2 = hash(stage1 + BM25 + 量化配置)
stage3 = hash(stage2 + 跳表/分块/编码配置)
stage4 = hash(stage3 + 查询集摘要 + 查询/缓存/延迟配置)
```

| 变化 | 最早失效阶段 | 下游失效 |
|---|---|---|
| 文档内容、分词配置、停用词、索引版本 | tokenize | 全部 |
| BM25、量化参数 | statistics | postings、query_summary |
| 跳表步长、分块大小、编码参数 | postings | query_summary |
| 查询集、缓存容量、延迟预算、limit 配置 | query_summary | 缓存清空 |
| 阶段产物缺失或被篡改 | 对应阶段 | 全部下游 |

`status` 返回失效原因、受影响阶段、旧/新阶段指纹和失效历史。旧缓存键同时包含配置指纹与阶段产物摘要，且阶段 4 失效会清空缓存文件，因此不会命中旧版本结果。

## 错误码

| 错误码 | 触发条件 |
|---|---|
| `E_WEIGHT_NUMERIC` | 词项无 posting，或 BM25 权重为 NaN/Inf；含文档与词项上下文 |
| `E_INVALID_SKIP_STEP` | 跳表步长不是正整数 |
| `E_QUERY_OUT_OF_BOUNDS` | limit、词项数量、查询语义或游标越界 |
| `E_QUERY_MISMATCH` | 压缩查询与顺序遍历参考不一致；含查询词项编号 |
| `E_LATENCY_BUDGET` | 查询超过延迟预算；含 query_id 和 elapsed_ms |
| `E_STALE_CHECKPOINT` | 序号落后、重复或并发发布竞争失败 |
| `E_INVALID_STAGE` | 非法阶段跳转、缺少前置阶段或产物不可用 |
| `E_BLOCK_CORRUPTION` | 分块长度、CRC、数量、首尾 doc_id 或尾随字节异常 |

## 实际验证结果

本地 Python 3.11.18 运行全部 27 项测试通过；Python 3.14.4 + NumPy 2.5.5 + PyYAML 6.0.3 环境下重复验收也通过。

```text
Ran 27 tests in 1.374s
OK
```

固定演示结果（`artifacts/demo_report.json`）：

- 文档 34 篇，分片 4 个，空正文档 2 篇。
- 词项 314 个，压缩块 410 个，跳表项 485 个。
- 8 条基准查询全部与参考遍历一致，共 54 个命中；AND 5 条、OR 3 条。
- 查询 `检索 AND 索引` 命中 4 篇：`doc-03-0001`、`doc-03-0009`、`doc-01-0007`、`doc-02-0004`。
- 缓存首次未命中、第二次命中，命中/未命中结果逐字段相同；容量 3 下的淘汰未改变结果。
- 停止后恢复产物与完整构建产物逐字节相同：`87bbc73673ee8fec6e775d36cefc0b5d2fe9799f540be3c863e18fe1ff030d44`。
- 非法跳表步长、越界查询、零延迟预算、陈旧发布和非法阶段跳转均返回预期结构化错误码。
- 演示报告连续两次生成逐字节一致，`report_digest = e095f697422faae8ba3b0b373f5a33cfa1d5dceff7df7dedd96d431c24d0000f`。

关键摘要：

```text
config_fingerprint  = 1a7d42f42db290077927693dba90ab4525d6b81933e5fa03c4355bbe8700d78b
dataset_digest      = 7fa16c9bcf464a76b8b504df45fc1257abc54b67411813bf5ec5943468cf7fbe
query_result_digest = 627bc8813d8e9d9b737553e2cf11d5c4507f81ba79814b7800474584008c8e71
```

## 测试与目录

```bash
python -m unittest discover -s tests -v
```

```text
sparse_index/
  tokenizer.py       规范化与分词/词项编号
  stages.py          阶段 1-3 与压缩全程校验
  codec.py           LEB128、zigzag、分块 CRC
  query.py           惰性游标、跳表求交、OR 归并、缓存
  reference.py       未压缩顺序遍历参考实现
  checkpoints.py     原子检查点、序号与阶段保护
  pipeline.py        指纹链、失效传播、阶段 4
  http_api.py        本地 HTTP 服务
scripts/
  generate_dataset.py
  build_index.py
  reference_query.py
  serve.py
  run_demo.py
tests/
```

## 边界说明

- 延迟预算通过协作式检查实现；解释器内的单次系统调用或极端大块解码不可能被硬实时抢占。
- 当前编码器固定使用无符号 LEB128；将 `postings.varint_encoding` 改为其他值会返回 `E_CONFIG`。
- 文档 doc_id 按字符串升序组织 posting；生成样本使用固定宽度 doc_id，便于稳定复现。
