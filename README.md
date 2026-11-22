# mmbackfill：多模态样本回填与索引状态演进引擎

`mmbackfill` 是一个完全本地、确定性、可恢复的多模态检索索引回填引擎。它从固定种子生成的小尺寸 RGB PNG、图像条目 JSONL、文本条目 JSONL 和分片布局 JSON 出发，完成向量构建、分片规划、批次原子提交、停止恢复、版本失效和并发归属保护。系统不需要视觉语言模型、GPU、外部服务或 Docker。

## 快速开始

要求 Python 3.11+、NumPy、FastAPI、uvicorn。测试另需 pytest/httpx。

```bash
python3 -m pip install -e '.[test]'
python3 -m pytest -q
python3 -m mmbackfill demo --out .demo-run-20261123 --seed 42
```

`demo` 会在输出目录下生成 `dataset/`、多份相互独立的运行工作区，以及 `demo-report.json`。演示覆盖中英混排、一对多关联、晚到样本、备用路由、分片分裂、批次间停止/恢复、乱序收敛、版本失效和注入故障。

环境自检：

```bash
python3 -m mmbackfill --workspace .mmb-work doctor
```

`doctor` 实际执行一次 UTF-8 写入、临时文件 `fsync` + `os.replace` 原子替换和 OS 文件锁获取。POSIX 使用 `fcntl.flock`，Windows 使用 `msvcrt.locking`；路径和文件均按 UTF-8 处理。

## 输入契约

图像条目 JSONL：

```json
{"image_id":"img-red-blue","file":"img-red-blue.png","language":"zh"}
```

文本条目 JSONL：

```json
{"text_id":"txt-a","text":"红色与蓝色 checker 样本 / red-blue sample","image_id":"img-red-blue","target_shard":"s0"}
```

布局配置示例：

```json
{
  "schema_version": "layout-v1",
  "layout_id": "demo-main-v1",
  "vector": {
    "dimension": 280,
    "fusion": {"image_weight": 0.45, "text_weight": 0.55},
    "visual": {"grid_rows": 4, "grid_cols": 4, "edge_bins": 8},
    "text": {"dimension": 128}
  },
  "layers": [{"layer": 0, "max_vectors": 10, "max_shards": 8}],
  "shards": [
    {"shard_id": "s0", "layer": 0, "capacity": 1, "fallbacks": ["s1"]},
    {"shard_id": "s1", "layer": 0, "capacity": 2, "fallbacks": []},
    {"shard_id": "s2", "layer": 0, "capacity": 2, "fallbacks": []}
  ]
}
```

语义：

- `layers[].max_vectors` 是单层允许承载的向量总数，`max_shards` 是单层最多分片数。
- 每个分片的 `capacity` 必须为正数，且不得超过所属层的 `max_vectors`。
- `fallbacks` 必须是同层分片；目标分片满时先按配置顺序尝试仍有容量的备用分片。
- 目标与备用都不可用且层内无法继续分裂时返回 `E_SHARD_FULL`，绝不静默丢弃。
- 统一向量维度由 `grid_rows * grid_cols * 9 + edge_bins + text.dimension` 推导；示例为 `4*4*9+8+128=280`。声明维度不一致会在向量阶段返回 `E_DIM_MISMATCH`。

## 确定性向量

**图像侧**：用内置 PNG 解码器读取 8-bit RGB/灰度/RGBA 非交错 PNG；按固定网格分块，每块每通道计算颜色矩（均值、标准差、偏度），再拼接全局边缘方向 8 桶直方图。每个模态先单位归一化。

**文本侧**：依次执行 Unicode NFKC、空白折叠、小写化；ASCII 词与 CJK 字符分别切分；每个 token 以 SHA-256 固定投影到指定维度，并按词频加权。

**跨模态融合**：`concat(image_weight * visual, text_weight * text)`，随后单位归一化，并量化到 12 位小数。同一输入、布局和权重产生逐字节一致的缓存文件。

每个文本条目对应一个检索向量，因此同一 `image_id` 可被多个 `text_id` 一对多关联。PNG 缺失/不可读、规范化文本为空、文本哈希抵消为零或融合后为零时返回带 `text_id`、`image_id` 的 `E_EMPTY_INPUT`，错误同时写入持久化诊断目录。

## 回填规划与顺序收敛

规划器不依赖提交顺序。每次规划都从声明分片拓扑开始，按稳定的 `(ordinal, text_id)` 键重放当前已提交的全部样本：

1. 目标分片有空位则直接插入。
2. 目标已满则尝试同层备用分片，并记录 `from_shard`/`to_shard` 路由。
3. 目标与备用均无空位、但层内还有向量/分片余量时，确定性分裂目标分片。父分片保留前半条目，子分片继承父分片层级和容量；迁移条目保留原 `ordinal` 并记录来源、目标、向量摘要。
4. 所有可用空间耗尽时返回 `E_SHARD_FULL`。

提交前会检查所有待索引向量摘要；同一向量被分配给两个不同条目时返回 `E_DUPLICATE_ROUTE`，并附冲突条目及目标分片。错误发生在计划提交前，批次记录、索引和归属状态均不改变。

由于每次提交是全量稳定重放，以下内容在最终样本集合与配置相同时逐字节一致：

- 分片布局 `runtime/ownership/index.json`
- 分片分裂历史 `split_history`
- 迁移记录、备用路由和归属摘要
- 确定性向量缓存 `runtime/ownership/vectors.json`

## 批次记录、原子提交与恢复

批次在互斥锁内执行以下事务顺序：

1. 比较调用方 `expected_sequence` 与记录链头。
2. 构建/复用向量并完成全量规划；任何业务错误只写诊断，不写批次记录。
3. 以临时文件、`fsync`、`os.replace` 原子写入 `runtime/records/batch-000001.json`。
4. 原子写入向量缓存、规范索引和活动状态。

批次记录包含 `batch_seq`、`batch_id`、输入内容指纹、数据摘要、布局版本、三阶段版本指纹、归属摘要、失效事件及前序记录摘要。文件封装还包含整条记录的 SHA-256。恢复时只接受文件名序号、记录内序号、校验和及 `prev_record_digest` 链全部匹配的连续记录；损坏、不完整、序号不符或重复样式的文件被忽略并在恢复结果中报告。

记录是提交事实，索引/状态是可重建物化结果。进程在任意落盘位置中断后，`recover` 会从有效记录链重算索引；已经一致的已提交批次会被直接跳过。停止请求只阻止下一个原子批次，当前批次作为最小不可拆分提交单元完成。

## 版本指纹与失效

三阶段指纹依次为：

- `vector_build`：向量配置 + 每条样本的原始文本、规范化文本、图像字节摘要、语言标签、图像/文本关联和注入覆盖摘要。
- `backfill_planning`：布局层/分片容量与拓扑 + `vector_build` 指纹。
- `shard_ownership`：规划指纹 + 每条样本的目标分片与向量摘要。

布局或已提交样本变化时，系统识别最早受影响阶段，并计算旧/新分片归属摘要差异：

- 融合权重、描述子配置、图像字节或样本文本变化：从 `vector_build` 失效。
- 容量、层上限、分片拓扑或目标分片变化：从 `backfill_planning` 失效。
- 已提交样本移除等归属变化：从 `shard_ownership` 失效。

失效事件包含原因、旧/新版本指纹、失效分片、复用分片和受影响条目，通过 `invalidations` 查询接口可见。向量缓存按每条的 source key 复用；配置或内容变化会生成新 key，旧向量不会进入新索引。归属摘要未变化的分片列为复用分片。

## 并发提交

所有导入、布局安装、批次提交和恢复都覆盖同一把 OS 文件锁。锁覆盖“读取记录头 → 比较序号 → 全量规划 → 写记录 → 写归属状态”的完整临界区。两个进程同时携带相同 `expected_sequence` 时，只有一个能推进序号，另一个稳定返回 `E_STALE_CHECKPOINT`；锁不是通过时间间隔或重试次数推断。

## CLI

所有命令使用 `--workspace` 指定独立工作区，默认 `.mmbackfill-work`。

```bash
# 生成固定种子输入与故障夹具
python3 -m mmbackfill generate-demo --out ./demo-data --seed 42

# 导入布局、图像与文本
python3 -m mmbackfill --workspace ./ws import-inputs \
  --layout ./demo-data/layout.json \
  --images-jsonl ./demo-data/images.jsonl \
  --texts-jsonl ./demo-data/texts.jsonl \
  --images-dir ./demo-data/images

# 一个原子批次（两阶段流水线：vector_build -> backfill_planning）
python3 -m mmbackfill --workspace ./ws backfill \
  --batch-id b1 --text-ids txt-a,txt-b --expected-seq 0 --order id

# 分批回填；arrival 或 id 顺序最终收敛
python3 -m mmbackfill --workspace ./ws backfill-all \
  --text-ids txt-a,txt-b,txt-c,txt-d,txt-e \
  --batch-size 1 --expected-seq 0 --order arrival

python3 -m mmbackfill --workspace ./ws status
python3 -m mmbackfill --workspace ./ws index
python3 -m mmbackfill --workspace ./ws batches
python3 -m mmbackfill --workspace ./ws invalidations
python3 -m mmbackfill --workspace ./ws vectors
python3 -m mmbackfill --workspace ./ws diagnostics
python3 -m mmbackfill --workspace ./ws stop --reason "maintenance"
python3 -m mmbackfill --workspace ./ws recover
python3 -m mmbackfill --workspace ./ws serve --host 127.0.0.1 --port 8765
```

故障注入通过参数/文件完成，不修改源码：

```bash
python3 -m mmbackfill --workspace ./ws import-layout --layout ./demo-data/faults/layout_zero_capacity.json
python3 -m mmbackfill --workspace ./ws backfill \
  --batch-id bad-vector --text-ids txt-a --expected-seq 0 \
  --vector-overrides ./demo-data/faults/vectors_nan.json
```

## 本地 HTTP API

启动：`python3 -m mmbackfill --workspace ./ws serve`。关键端点：

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health`, `/status`, `/doctor` | 健康、序号/停止状态、环境自检 |
| `PUT/POST` | `/layout` | 直接提交布局 JSON，校验后安装/重建 |
| `POST` | `/layout/import` | 从 `{"path": "..."}` 导入布局 |
| `PUT` | `/images`, `/texts` | 导入条目数组并暂存 PNG |
| `POST` | `/images/import`, `/texts/import` | 从文件路径导入条目 |
| `POST` | `/backfill` | 原子提交一个批次 |
| `POST` | `/backfill/all` | 连续提交多个原子批次 |
| `GET` | `/index`（别名 `/shards`） | 分片布局、分裂历史、归属摘要 |
| `GET` | `/batches`（别名 `/checkpoints`） | 各批次序号与摘要 |
| `GET` | `/invalidations` | 失效分片、原因与版本指纹 |
| `GET` | `/vectors`, `/diagnostics`, `/catalog` | 向量缓存、诊断、输入目录 |
| `POST` | `/stop` | 请求批次边界优雅停止 |
| `POST` | `/recover`（别名 `/resume`） | 清除停止标记并恢复最近一致状态 |

业务错误返回 `{"ok":false,"error":{"code":...,"stage":...,"batch_seq":...,"entries":...}}`，HTTP 状态为 400/409/422。

## 错误码

| 码 | 场景 |
|---|---|
| `E_INVALID_LAYOUT` | 容量为零、层上限小于分片容量、未知层/备用分片、非法拓扑 |
| `E_DIM_MISMATCH` | 布局声明维度、融合向量或注入向量维度不一致 |
| `E_NUMERIC` | 融合权重或注入向量含 NaN/Inf |
| `E_EMPTY_INPUT` | PNG 缺失/不可读、空文本、零文本向量或零融合向量 |
| `E_UNKNOWN_REF` | 文本引用不存在的 `image_id` 或未知分片 |
| `E_SHARD_FULL` | 目标与备用满、层内无法继续分裂 |
| `E_DUPLICATE_ROUTE` | 相同检索向量被规划给两个条目 |
| `E_STALE_CHECKPOINT` | 期望序号落后、超前或 `batch_id` 重复 |
| `E_STOPPED` | 优雅停止标记存在，新批次未提交 |
| `E_INVALID_INPUT` | 请求、目录或批次结构非法 |

## 工作区布局

```text
workspace/
  config/layout.json
  inputs/images.jsonl
  inputs/texts.jsonl
  inputs/images/*.png
  runtime/locks/workspace.lock
  runtime/records/batch-000001.json ...
  runtime/ownership/index.json
  runtime/ownership/vectors.json
  runtime/ownership/state.json
  runtime/ownership/invalidations.json
  runtime/ownership/layout-events.json
  runtime/diagnostics/error-*.json
  runtime/stop.json
```

批次记录、归属状态和锁文件位于独立目录。所有 JSON 使用 UTF-8、排序键、紧凑分隔符；向量以固定小数位量化后再摘要。

## 实际验证结果

验证环境：Python 3.14.4、NumPy 2.2.1、FastAPI 0.115.6、uvicorn 0.34.0、Linux/posix（`fcntl.flock`）。

- `python3 -m pytest -q`：`13 passed`。
- `doctor`：原子替换 `true`，文件锁 `true`，锁实现 `fcntl.flock`，UTF-8 往返 `true`。
- 停止/恢复：批次 1 → 停止请求 → 新批次被 `E_STOPPED` 拒绝 → 恢复 → 批次 2/3；最终索引与从零单批次运行逐字节一致，SHA-256 均为 `8a9d3b50a497f34eb893c89cb027955fa6ba829a0381992a5d86379fed0d2026`。
- 顺序收敛：`arrival` 与 `id` 两种顺序各提交 5 个单条目批次；布局、向量缓存、分裂历史、归属摘要逐字节/结构一致，索引 SHA-256 同为 `8a9d3b50a497f34eb893c89cb027955fa6ba829a0381992a5d86379fed0d2026`。
- 分裂/路由：演示产生 2 次确定性分裂和 1 次备用路由，迁移条目均保留来源分片、目标分片和序号。
- 双进程并发：同一 `expected_sequence=0` 的两个真实子进程仅一个成功，另一个返回 `E_STALE_CHECKPOINT`；最终记录数 1、序号 1、无重复向量归属。
- 真实 uvicorn 冒烟：`/health` 成功，`/backfill` 返回 200/批次 1，停止后再次提交返回 HTTP 409/`E_STOPPED`，`/recover` 返回 200 并复用一致索引。
- 故障码：零容量与层上限错误为 `E_INVALID_LAYOUT`；声明维度错误/注入维度错误为 `E_DIM_MISMATCH`；NaN 融合权重/NaN 向量为 `E_NUMERIC`；缺图、空文本、零融合为 `E_EMPTY_INPUT`；缺失 image 引用为 `E_UNKNOWN_REF`；已满无备用为 `E_SHARD_FULL`。所有失败后已提交序号保持不变。
- 失效：融合权重从 `0.45/0.55` 改为 `0.60/0.40` 后，最早受影响阶段为 `vector_build`，旧/新向量指纹分别为 `6b5e243979a63072a3b6e694f6b1badfd49bd888f55a647c05b7d7a2309e5723` 和 `19fd4c9c764524a82c2ba780cd16dd914de8a92d2815e12d9ea70e1ecc9076fd`；失效分片 `s0, s1, s1~split-1, s1~split-2`，空分片 `s2` 复用。
- 完整机器可读结果：`artifacts/verification-report.json`。

这些结果由 `python3 -m mmbackfill demo --out <dir> --seed 42` 的自动编排产生，没有下载数据集或访问外部目标。
