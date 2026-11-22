# 本地确定性依存句法增量重建与恢复一致性引擎

这是一个从零实现、无模型、无 GPU、无网络依赖的本地领域组件。它把中文/中英混排句子分片解析为依存树，并以四个可检查点阶段完成增量重建、优雅停止、故障恢复、版本失效传播和并发发布保护。

实现特点：

- Arc-standard 转移式解码，固定 SHA-256 哈希特征与 YAML 权重打分。
- 四阶段流水线：`featureization -> decoding -> validation -> aggregation`。
- 每个检查点单文件原子提交，记录全局序号、分片内序号、输入指纹、配置指纹、产物摘要和记录摘要。
- 使用标准库 `fcntl.flock` 实现真实跨进程发布锁。
- 增量与从零全量构建的 `trees.json`、`diagnostics.json`、`summary.json` 逐字节比对。
- 固定随机种子演示语料：3 个分片、21 句、207 token，含中英混排、纯标点空句和非法依存距离样本。

## 快速开始

要求 Python 3.10+。在工作区根目录执行：

```bash
pip install -r requirements.txt

# 重新生成固定种子语料、清单和 YAML 配置
python scripts/generate_demo_data.py

# 全量构建
python -m dep_engine build

# 查询摘要、树、诊断、指纹和失效计划
python -m dep_engine query summary
python -m dep_engine query trees --shard 0
python -m dep_engine query fingerprints
python -m dep_engine query invalidations

# 与干净目录中的从零全量构建逐字节比对
python -m dep_engine verify

# 固定演示：构建 1 个分片后停止，再恢复并校验全量一致性
python -m dep_engine demo

# 测试
pytest -q
```

默认所有输入、状态和视图路径均为相对路径：

```text
data/shards/shard_001.txt ... shard_003.txt
data/manifest.json
config/analysis.yaml
runtime/state/checkpoints/
runtime/views/
```

## 目录

```text
dep_engine/                 核心库、CLI、流水线、检查点与演示
  parser.py                 哈希特征、动作打分、贪心转移解码
  validation.py             环、根唯一性、最大距离和数值诊断
  pipeline.py               四个阶段
  store.py                  原子检查点、恢复、flock 并发发布
  engine.py                 失效规划、增量执行、查询和全量比对
  cli.py                    命令行入口
scripts/generate_demo_data.py
scripts/demo.py
data/                       固定种子分片和显式分片清单
config/analysis.yaml        分析配置样例
expected/demo_report.json   固定演示输出
tests/                      28 项本地测试
```

## 分析配置

样例 `config/analysis.yaml`：

```yaml
schema_version: 1
tokenizer_version: zh-mixed-unicode-v1
score_weights:
  shift: 0.62
  left_reduce: 0.91
  right_reduce: 0.78
max_dependency_distance: 7
unique_root: true
smoothing_strength: 0.08
confidence_threshold: 0.02
```

阶段与配置字段的版本归属：

| 阶段 | 输入依赖 | 字段 |
| --- | --- | --- |
| `featureization` | 分片内容、清单条目、分词器 schema | `tokenizer_version` |
| `decoding` | 特征化产物、动作权重、平滑与置信阈值 | `score_weights`、`smoothing_strength`、`confidence_threshold` |
| `validation` | 解码产物、约束参数 | `max_dependency_distance`、`unique_root` |
| `aggregation` | 校验产物 | 无 |

因此：

- 改权重、平滑强度或置信阈值：最早失效阶段是 `decoding`，`featureization` 继续复用。
- 改最大距离或根唯一性：最早失效阶段是 `validation`，特征化与解码继续复用。
- 替换单个分片内容：只有该分片从 `featureization` 起失效，其他分片全部复用。
- 每个阶段输入指纹由 `阶段 schema + 上游产物摘要 + 阶段配置分量` 计算；失效计划和新旧指纹均可通过 `query invalidations` / `query fingerprints` 查询。

## 解码与诊断

分词把 Unicode 单词字符序列视为 token；空白、标点和下划线是分隔符，因此在中文样本中使用空格分隔词项，英文和数字保持为完整 token。纯标点或空句在解码阶段返回带句子标识的 `E_EMPTY_SENTENCE`。

动作合法时，按固定哈希特征计算：

```text
signed = sum(stable_hash(feature) in [-1, 1])
score(action) = weight[action] * (0.35 + signed / sqrt(n)) + smoothing * tanh(signed / (sqrt(n) + 1))
```

贪心选择最高分动作，固定平局顺序为 `SHIFT -> LEFT_REDUCE -> RIGHT_REDUCE`。每个 token 输出：

- `heads`：0 基中心词下标，根为 `-1`；
- `labels`：`ROOT`、`LEFT_DEP` 或 `RIGHT_DEP`；
- `distances`：与中心词的绝对距离。

约束校验检查环、根数量、中心词越界/自指和最大依存距离。违反时诊断码为 `E_TREE_INVALID`，并携带句子标识、token 下标及违规类型。动作得分或评分参数出现 `NaN/Inf` 时返回 `E_NUMERIC`，诊断中包含 `score_source`、动作、步数、缓冲区位置和栈大小，不会静默替换。

## 检查点、恢复与并发

每次阶段发布写入一个独立文件：

```text
runtime/state/checkpoints/000000000001.json
runtime/state/checkpoints/000000000002.json
...
```

记录包含：

- `sequence`：工作区全局单调序号；
- `shard_index` / `shard_sequence`：分片和分片内连续序号；
- `stage` / `stage_index`；
- `input_fingerprint` / `input_components`；
- `config_fingerprint` / `stage_config_fingerprint` / `stage_config_components`；
- `upstream_output_digests`；
- `output_digest` / `output`；
- `record_digest`。

写入使用同目录临时文件、`fsync`、原子 `os.replace` 和目录 `fsync`。中断只会留下不会被扫描的临时文件，不会产生半条检查点。恢复按全局序号扫描连续前缀，并验证文件名、记录摘要、产物摘要、阶段字段和分片内连续序号；损坏、截断、重复或错位记录被忽略。

发布锁为 `runtime/state/.publish.lock` 上的 `flock` 排他锁。锁内重新扫描状态并比较调用方观察到的 `expected_shard_sequence`：

- 两个进程竞争同一分片序号时，只有一个成功；
- 落后、重复、跨分片错位或基于旧上游的发布会返回 `E_STALE_CHECKPOINT`；
- 非法阶段、缺失前置产物、越界分片或越界记录访问返回 `E_INVALID_STAGE`，并附 `valid_stages` 与 `available_stages`。

`build` 只在分片边界检查停止标记。若停止请求在分片处理中出现，当前分片的四个阶段仍会全部提交，随后状态为 `stopped`。恢复时已完成阶段直接复用，不会重复计算。

## 增量接口

```python
from dep_engine import Engine

engine = Engine(".")

# 全量或恢复
engine.build()
engine.build(stop_after_shards=1)
engine.request_stop()

# 单阶段执行；会严格检查全部前置阶段存在且当前
engine.run_stage(0, "decoding")

# 查询
engine.get_trees()
engine.get_diagnostics(shard_index=0)
engine.get_summary()
engine.get_fingerprints()
engine.get_invalidations()

# 单分片替换和单配置变更
engine.replace_shard(1, "新的 分片 文本 。\n")
engine.update_config({"...": "完整配置"})

# 与干净目录从零全量构建做逐字节比对
report = engine.verify_against_full()
```

不匹配时 `verify_against_full()` 抛出：

```python
EngineError(
    code="E_INCREMENTAL_MISMATCH",
    details={
        "first_diff": {
            "artifact": "trees",
            "shard_index": 0,
            "sentence_id": "shard-0000:sentence-0001",
            "token_index": 0,
            "field": "head_index",
        }
    },
)
```

CLI 还提供：

```bash
python -m dep_engine run-stage --shard 0 --stage decoding
python -m dep_engine replace-shard --index 1 --file replacement.txt
python -m dep_engine update-config --file config/analysis.yaml
python -m dep_engine stop
python -m dep_engine resume
```

## 固定演示结果

`python -m dep_engine demo` 每次重建 `runtime/demo/`，执行“完成 1 个分片后优雅停止 -> 恢复 -> 与干净全量构建比对”，输出固定报告，并与 `expected/demo_report.json` 一致：

```json
{
  "shard_count": 3,
  "sentence_count": 21,
  "token_count": 207,
  "stats": {
    "tree_count": 20,
    "error_count": 3,
    "error_counts": {
      "E_EMPTY_SENTENCE": 1,
      "E_TREE_INVALID": 2
    },
    "constraint_violation_count": 5,
    "constraint_violation_counts": {
      "MAX_DEPENDENCY_DISTANCE": 5
    }
  },
  "digests": {
    "tree_digest": "f679bf05dcfdc226d8cdc28394796eb149c8e066bda112d362b42f138b2f4634",
    "diagnostic_digest": "362a55db0ca59ba2e8c012ed68e42a2625fa3ece8982c6e477ae65d7a5d0afd4",
    "summary_digest": "37f31cadcbfa6d5ab19b3daf374205b2d48a41cac88293f372256e847defbcc9"
  },
  "stop_resume": {
    "stopped_checkpoint_sequence": 4,
    "resume_computed_stage_count": 8,
    "resume_reused_stage_count": 4,
    "final_checkpoint_sequence": 12,
    "final_record_count": 12
  },
  "consistency": {
    "incremental_equals_full": true
  }
}
```

实际验证结果：

- `pytest -q`：`28 passed`。
- 3 个分片、21 句、207 token；固定种子 `20261123`。
- 无 GPU、无网络、无外部服务、无 Docker。
- 停止后 4 条连续检查点，恢复后共 12 条，无跳跃、回退或重复；恢复仅计算剩余 8 个阶段，复用已持久化的 4 个阶段。
- `trees.json`、`diagnostics.json`、`summary.json` 与从零全量构建逐字节一致。
