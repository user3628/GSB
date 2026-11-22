# Embedding Gather

一个可在 Windows/Linux、Python 3.10+、仅依赖 NumPy 的本地 Embedding 查找与 Gather 组件。项目包含统一分片索引模型、manifest v1/v2 双向编解码、定点量化 Gather 内核、四阶段原子检查点、增量失效传播、并发发布保护和完整确定性示例。

## 快速开始

```bash
python -m pip install -r requirements.txt
python scripts/generate_samples.py --out samples
pytest -q
python -m embedding_gather demo
```

Windows PowerShell 可将 `python` 换成 `py -3`。无需 Docker、GPU 或网络。

Windows 也可直接运行 `run_demo.bat`，或只跑测试使用 `run_tests.bat`。

`demo` 会生成/使用 `samples/`，在 `demo_state/` 中完成：

- v1 清单百分比行占比升级、错误维度纠正、pad 行回补；
- v1↔v2 往返语义一致性检查；
- 正常批次完整运行；
- 停止于第 2 个批次后恢复，与一次完整运行的 `bundle.npz` 做逐字节比较；
- 空批次、越界、非整数索引、全零聚合权重的错误码演示；
- 修改权重分片后的最早失效阶段与上游复用查询。

## 项目结构

```text
embedding_gather/
  model.py       统一索引工程模型与跨分片校验
  codec.py       manifest v1/v2 编解码、升级、降级回补
  samplegen.py   固定随机种子的 .npz 样本与清单生成器
  weights.py     权重分片加载、内容摘要、格式校验
  batches.py     索引批次加载与清单关联校验
  kernel.py      查找、Gather、加权定点聚合、误差界
  checkpoint.py  文件锁、原子阶段记录、序号锚点、篡改/回退识别
  engine.py      四阶段流水线、恢复、增量失效、确定性 bundle
  cli.py         本地命令行入口
samples/         main 与 ragged 两套已落盘样本
tests/           38 个 pytest 用例
docs/            格式、设计与实际验证记录
```

## 库集成

```python
from embedding_gather import GatherConfig, V1UpgradeConfig, open_engine

engine = open_engine(
    "samples/main/manifest_v2.json",
    "samples/main",
    "state/example",
    gather_config=GatherConfig(output_scale=0.0001),
    v1_config=V1UpgradeConfig(),
)

report = engine.run(["batch-ok", "batch-cross-shard"])
print(report.bundle_digest)

for status in engine.inspect(["batch-ok"]):
    print(status["earliest_invalid_stage"], status["reasons"])
```

单独使用内核：

```python
from embedding_gather.codec import read_manifest_file, decode_manifest
from embedding_gather.kernel import GatherConfig, gather, quantize, validate_batch
from embedding_gather.weights import load_manifest_facts, load_weight_table
from embedding_gather.batches import load_index_batch

manifest = read_manifest_file("samples/main/manifest_v2.json")
facts = load_manifest_facts(manifest, "samples/main")
project = decode_manifest(manifest, facts=facts)
table = load_weight_table(project, "samples/main")
batch = load_index_batch(project, project.batch_map()["batch-ok"], "samples/main")

config = GatherConfig(output_scale=0.0001, negative_index_policy="wrap")
validation = validate_batch(table, batch, config)
result = quantize(gather(table, batch, config, validation), config)
print(result.output_q, result.output_scale, result.error_bound)
```

## Gather 语义

- 索引数组保留原形状，每个 id 查到一个长度为 `embedding_dim` 的向量。
- `gather_axis` 指定对索引数组哪一个轴做加权求和；输出形状为去掉该轴后追加 embedding 分量维。
- `weights` 可选，形状必须与索引数组完全一致；缺省权重为 `1.0`。
- 重复索引按扁平 C 顺序逐次累加，命中明细保留 `occurrence`、分片、索引、行偏移、局部行与 pad 标记。
- 负索引策略为 `wrap` 时加 `total_rows` 一次；仍为负则 `E_INDEX_OOB`。策略为 `reject` 时直接 `E_INDEX_OOB`。
- pad 行返回零向量、计入 `pad_hit_count`，不参与数值累加。
- 输出固定点张量为 `int64`，配套 `output_scale`、逐元素 `error_bound`、`observed_error` 与命中 JSON。

## 检查点与恢复

每个批次固定经过 `validate → gather → quantize → verify` 四阶段。每个阶段完成时：

1. 确定性产物先原子写入（临时文件 + `fsync` + `os.replace`）；
2. 阶段记录再原子写入，包含批次/分片标识、阶段、输入指纹、清单结构版本与指纹、配置版本、产物摘要、前序记录摘要、单调序号和记录自摘要；
3. 批次锁内更新高水位锚点。

收到停止文件、`--stop-after-batches` 或 CLI 的 Ctrl-C/SIGTERM 时，只会在当前批次完整结束后退出。恢复时按阶段指纹复用有效记录；缺少产物返回 `E_INVALID_STAGE`，重复/落后/跨分片/版本回退/摘要不匹配返回 `E_STALE_CHECKPOINT` 并区分 `duplicate`、`rollback`、`cross_shard`、`tamper` 等原因。

## 命令行

```bash
# v1 升级与 v1/v2 往返
python -m embedding_gather codec-check \
  --manifest samples/main/manifest_v1.json --root samples/main

# 正常批次；默认从 sample_metadata.json 读取非错误批次
python -m embedding_gather run \
  --manifest samples/main/manifest_v2.json --root samples/main \
  --workspace state/run --reset

# 运行两个批次后停止
python -m embedding_gather run \
  --manifest samples/main/manifest_v2.json --root samples/main \
  --workspace state/resume --stop-after-batches 2

# 恢复并生成最终 bundle
python -m embedding_gather run \
  --manifest samples/main/manifest_v2.json --root samples/main \
  --workspace state/resume

# 错误注入
python -m embedding_gather errors \
  --manifest samples/main/manifest_v2.json --root samples/main \
  --workspace state/errors --reset

# 查询失效原因与各阶段新/旧指纹
python -m embedding_gather inspect \
  --manifest samples/main/manifest_v2.json --root samples/main \
  --workspace state/run --batches batch-ok
```

## 样本覆盖

`samples/main` 使用固定种子 `20261123`：96 行、8 维、3 个行分片，包含跨分片索引、重复索引、负索引、pad 行、空批次、越界、非整数索引与全零权重。`samples/ragged` 使用固定种子 `20261124`：101 行、6 维、分片行数为 `37/33/31`，覆盖不整除分块与 v1 百分比回补。

所有 `.npz`、v1/v2 清单和 `sample_metadata.json` 已随项目落盘；重新生成会得到逐字节相同文件。

## 格式与设计说明

- [manifest v1/v2 格式](docs/manifest-formats.md)
- [检查点、失效与并发设计](docs/design.md)
- [实际验证记录](docs/validation.md)

## 已知边界

- v1 本身不可表达精确行偏移与 pad 语义。导出 v1 时默认写 `_compat` 兼容扩展以保证无损回补；纯历史 v1 则优先从权重 `.npz` 元数据恢复，并在 `provenance` 中显式记录恢复来源或不可恢复事实，不会静默丢弃。
- 锚点可发现工作区内的整目录替换、回退、摘要篡改与版本倒退；若攻击者同时替换记录与锚点，需要工作区外的可信锚点才能提供更强证明。
- 确定性字节比较面向同一 Python/NumPy 大版本和相同平台浮点环境；数值参考使用 `longdouble` 高精度中间量，输出本身为整数定点域。
