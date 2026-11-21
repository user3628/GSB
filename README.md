# CMIR Alignment

一个从零实现、可本地运行、可供上层检索服务以库形式集成的跨模态对齐检索参考组件。它只依赖 Python 标准库、NumPy 和 Pillow，不使用真实视觉语言模型、GPU、容器、外部服务或网络数据。

组件覆盖：确定性图像/文本特征、64 维跨模态投影、4/8 位定点量化、双向倒排检索、误差传播诊断、分阶段版本指纹、单分片增量重建、原子检查点、真实文件锁、优雅停止/恢复、本地 HTTP 与 CLI、完整可重复 demo。

## 快速开始

```bash
# Python 3.10+；当前环境验证锁为 requirements.lock，
# Python 3.10/3.11 可使用 requirements-py310.lock。
python3 -m pip install -r requirements.lock

python3 -m cmir_alignment.cli generate \
  --output data/sample --seed 20261122 \
  --shards 3 --items-per-shard 6 --write-configs configs

python3 -m cmir_alignment.cli demo \
  --workspace runtime/demo-workspace \
  --data-dir data/sample \
  --report artifacts/demo-report.json

python3 -m cmir_alignment.cli http-demo \
  --workspace runtime/http-demo-workspace \
  --data-dir data/sample --port 0

python3 -m unittest discover -s tests -v
```

一键脚本：`scripts/demo.sh`、`scripts/http_demo.sh`、`scripts/validate.sh`。

## 库集成

```python
from cmir_alignment.configs import default_config
from cmir_alignment.engine import RetrievalEngine

engine = RetrievalEngine("runtime/workspace", config=default_config(8, "per_dim"))
engine.import_dataset("data/sample")
engine.build()

text_result = engine.search(
    mode="text2image",
    query="红色 red 方块 square 明亮 bright",
    k=5,
)
image_result = engine.search(
    mode="image2text",
    query_item_id="shard-00-item-000",
    k=5,
)

engine.set_config(patch={"bits": 4})
engine.build()
assert engine.verify_full_consistency()["consistent"]
```

核心类：

- `RetrievalEngine`：导入、构建、增量、双向查询、停止/恢复、导出和全量校验。
- `CheckpointStore`：真实 `fcntl.flock`、不可变检查点链、内容寻址产物和审计。
- `Dataset`：JSONL/PNG 加载、字段校验、内容指纹和缺失输入诊断。
- `descriptors`：视觉块描述子、文本规范化、词袋/字符 n-gram 与偏移记录。
- `alignment`：64 维矩阵、语义概念对齐、维度/NaN/Inf 校验。
- `quantization`：定点校准、码字、缩放/零点、误差界和有效位数。

## 数据契约

`dataset.json`：

```json
{"schema":"cmir.dataset.v1","shards":[{"shard_id":"shard-00","manifest":"shards/shard-00/data.jsonl"}]}
```

每行 JSONL 必须含：

```json
{"item_id":"shard-00-item-000","image_file":"shards/shard-00/images/shard-00-item-000.png","caption":"红色 red 方块 square，配 绿色 green 明亮 bright 视角","lang":"zh-en","source":"synthetic-seed-20261122"}
```

`image_file` 相对数据集根。固定种子生成器输出 64x48 RGB PNG，含几何色块、渐变、条纹和明暗视角。缺失图像、空 caption、缺失字段返回 `E_MISSING_INPUT`，带 `item_id`、`field`，并保留在 `diagnostics` 与 `diagnostics/last_error.json`，不会静默跳过。

## 特征、投影与量化

- 默认视觉网格 `4x4`；每块 RGB 均值、标准差、8 方向梯度直方图，共 224 维。
- caption 经 NFKC、全半角统一、小写和空白折叠；提取词袋与 2/3/4 字符 n-gram。
- 每个文本特征记录归一化偏移、权重和 512 桶哈希位置；向量 L2 归一化。
- 视觉/文本分别经可配置矩阵投到同一 64 维空间并单位归一化。
- 默认矩阵由 SHAKE-256 确定性生成，并叠加固定中英语义概念基；也可提供显式矩阵。
- 矩阵维度错误、NaN/Inf 返回 `E_PROJECTION_INVALID`；非法位宽返回 `E_INVALID_BITWIDTH`。
- 支持 4/8 位与 `per_dim`/`per_vector`；每维落盘 scale、zero point、code 和误差界。
- 量化 scale 为零返回 `E_QUANTIZATION_ZERO_SCALE`，不继续计算。

搜索结果输出视觉/文本两路的量化余弦、参考余弦、权重、对数贡献、误差界、有效位数、倒排命中率、来源分片和错误传播上下文。误差界同时包含步长保守界与高精度参考核验后的归一化扰动界；相似度非有限或误差界超过上限返回 `E_NUMERIC`。

## 检索

支持 `text2image`、`image2text`，以及显式 64 维 `query_vector`。排序公式：

```text
cosine_weight * (visual_weight * visual_cosine +
                 text_weight * text_cosine) +
inverted_hit_weight * hit_ratio
```

同分按 `item_id` 升序。`k<=0`、`k` 超过索引规模、空文本、未知模式或查询维度不符返回 `E_INVALID_QUERY`。相同输入、配置和状态产生逐字节一致响应。

## 增量与检查点

阶段失效规则：

| 变化 | 最早失效阶段 | 复用 |
| --- | --- | --- |
| 图像/caption/特征配置 | `feature` | 未变分片特征 |
| 投影矩阵/位宽/粒度 | `projection_quant` | 全部特征 |
| 排序/倒排权重/索引配置 | `index` | 特征与量化 |
| 召回/误差上限配置 | `recall_validation` | 特征、量化、索引 |

单分片入口：`cmir update-shard --shard shard-01 --workspace runtime/workspace`。构建结果与查询诊断含 `invalidations`（最早阶段、原因、重建/复用分片）。`verify` 在临时工作区全量重建，对特征摘要、量化向量、索引、召回摘要和固定探针 top-k 逐字节比较；不一致返回 `E_INCREMENTAL_MISMATCH` 并附首个 `difference.path`。

每个分片按 `feature → projection_quant → index → recall_validation` 写入不可变检查点，含全局序号、分片内序号、周期、输入摘要、配置版本、量化参数摘要、产物摘要和链式摘要。写入使用 `temp + fsync + os.replace + dir fsync`，不会留下半写记录。

发布由真实 `fcntl.flock` 保护，并严格校验下一单元。落后、重复、跨分片错位或产物摘要改动返回 `E_STALE_CHECKPOINT`；缺前置阶段返回 `E_INVALID_STAGE`。审计区分 `advance`、`rollback`、`tamper`。

`POST /stop` 只设置停止请求；构建完成当前原子阶段分片单元后退出。`resume` 清理停止请求并从连续有效检查点恢复，忽略损坏/不完整记录；最终摘要须与一次完整运行一致。强制终止不承诺生成退出报告。

## HTTP 与 CLI

```bash
python3 -m cmir_alignment.cli serve \
  --workspace runtime/workspace --host 127.0.0.1 --port 8765
```

端点：

- `POST /import`：`{"dataset_dir":"data/sample"}`
- `POST /config`：`{"bits":4,"granularity":"per_dim"}` 或投影/排序 patch
- `POST /build`：可选 `stop_after_units`
- `POST /search`：`{"mode":"text2image","query":"红色 red 方块","k":5}`
- `POST /update-shard`：`{"shard_id":"shard-01"}`
- `GET /status`、`POST /stop`、`POST /resume`
- `POST /verify`、`POST /export`
- `GET /checkpoints/audit`、`POST /checkpoints/recover`

错误响应：

```json
{"ok":false,"error":{"code":"E_INVALID_QUERY","message":"...","details":{"item_id":"...","stage":"..."}}}
```

HTTP 映射：查询错误 400；输入/投影/位宽/数值错误 422；陈旧检查点/阶段/增量不一致 409。

CLI：`generate | import | config | build | update-shard | search | status | stop | resume | verify | export | checkpoint-audit | recover | serve | demo | http-demo`。

## 错误码

- `E_MISSING_INPUT`：缺失文件/字段/空 caption。
- `E_INVALID_DATASET`：清单或数据集结构非法。
- `E_PROJECTION_INVALID`：矩阵维度、NaN/Inf 或查询投影维数错误。
- `E_INVALID_BITWIDTH`：不是 4 或 8 位。
- `E_QUANTIZATION_ZERO_SCALE`：量化 scale 为零。
- `E_NUMERIC`：NaN/Inf 或误差界超过上限。
- `E_INVALID_QUERY`：空查询、非法 k、未知模式或查询维度不符。
- `E_INCREMENTAL_MISMATCH`：增量与全量字节不一致。
- `E_STALE_CHECKPOINT`：陈旧、重复、错位或整体替换。
- `E_INVALID_STAGE`：缺前置产物或计划状态不允许。

## 交付结构

```text
cmir_alignment/      领域组件
tests/               unittest 验收测试
configs/             生成器写出的 8/4 位配置
data/sample/         固定种子样本（可重新生成）
docs/DESIGN.md       公式、阶段协议和一致性边界
docs/VALIDATION.md   实际环境与验证记录
scripts/             一键 demo、HTTP demo、验证脚本
artifacts/           demo 报告
requirements.lock    当前验证环境固定版本
requirements-py310.lock  Python 3.10/3.11 固定版本
```

摘要不含时间戳和绝对工作区路径。逐字节一致性依赖相同 Python/NumPy/Pillow 锁、相同本地数据和配置。检查点、产物和查询响应使用规范化 JSON；摘要输入对浮点使用精确十六进制表示。

详见 `docs/DESIGN.md` 与 `docs/VALIDATION.md`。
