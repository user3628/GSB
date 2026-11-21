# FlashBlock 分块注意力组件

本地可运行的 FlashAttention 风格分块注意力库，使用 Python 标准库 + NumPy 实现。支持稳定在线 softmax、因果掩码、非整除头维度、批量输入、行块检查点、优雅停止与恢复、K/V 前缀增量重算、依赖失效查询和峰值内存预算规划。

## 快速开始

要求 Python 3.11+ 和 NumPy 1.26+。无需 GPU、Docker 或外部服务。

Windows PowerShell：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
py scripts\generate_samples.py
py scripts\run_demo.py
```

也可直接双击或在 `cmd.exe` 执行：

```bat
run_demo.bat
```

Linux/macOS：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/generate_samples.py
python scripts/run_demo.py
```

演示会自动清理并重建 `artifacts/runtime/`、`artifacts/resume_runtime/`，完成：

1. 3 个固定种子分片的完整分块注意力；
2. 与逐行全量参考实现的容差比较；
3. 与从零分块计算的逐字节比较；
4. 写入检查点后重新加载并逐字节比较；
5. 处理 1 个行块后优雅停止，再由新进程对象恢复；
6. 修正指定 K/V 前缀区间并增量重算，验证复用行块/列块和字节一致性；
7. 输出固定 `aggregate_sha256` 到 `artifacts/demo_summary.json`。

当前实测入口输出：

```text
aggregate_sha256: c46130f941b857e01d17e83aee7f9da7c75949462607c84b5e6c2d611c1f108c
short_causal: reference_ok=True fresh_bytes=True
long_causal:  reference_ok=True fresh_bytes=True
odd_dim_causal: reference_ok=True fresh_bytes=True
resume: byte-identical=True
prefix update: reused_rows=[0], recomputed_rows=[1, 2, 3, 4], byte-identical=True
```

## 样本

`scripts/generate_samples.py` 以默认种子 `20260301` 生成：

| shard | Q/K/V shape | 覆盖点 |
| --- | --- | --- |
| `short_causal` | `[2, 2, 128, 32]` | 短序列、批量 > 1、因果 |
| `long_causal` | `[1, 2, 1024, 64]` | 长序列、因果 |
| `odd_dim_causal` | `[2, 1, 257, 40]` | 头维度 40 非整除、奇数序列、批量 > 1 |

另生成 `data/manifest.json` 形状清单和 `configs/operator.yaml` 算子配置。配置声明行/列块大小、因果开关、在线策略、数值钳制和容差。

## 库接口

```python
from flashblock import AttentionConfig, AttentionEngine, PrefixUpdateRequest

config = AttentionConfig.from_yaml("configs/operator.yaml")
engine = AttentionEngine.from_npz(
    workspace="artifacts/runtime",
    shard_id="short_causal",
    input_path="data/shards/short_causal.npz",
    config=config,
)

result = engine.run()
assert result.complete
print(result.digest())

# 可选：只推进一个行块，实现优雅停止
paused = engine.run(max_new_row_blocks=1)

# 从已有检查点恢复/读取缓存结果
resumed = AttentionEngine.from_npz(
    workspace="artifacts/runtime",
    shard_id="short_causal",
    input_path="data/shards/short_causal.npz",
    config=config,
).run()

# 前缀修正（数组形状 [B, H, key_end-key_start, D]）
updated = engine.apply_prefix_update(
    shard_id="short_causal",
    key_start=16,
    key_end=32,
    k_values=new_k_slice,
    v_values=new_v_slice,
    verify=True,
)
request = PrefixUpdateRequest("short_causal", 16, 32, new_k_slice, new_v_slice)
request_result = engine.apply_prefix_update_request(request)
print(updated.reused_row_blocks, updated.recomputed_row_blocks)
print(engine.query_invalidations())
print(engine.query_state())
print(engine.budget_report())
```

纯前向核心也可独立调用：

```python
from flashblock import AttentionConfig, full_attention, reference_attention

result = full_attention(q, k, v, AttentionConfig(causal=True))
reference = reference_attention(q, k, v, AttentionConfig(causal=True))
```

## 命令行

```bash
python -m flashblock generate --root .
python -m flashblock attention --input data/shards/short_causal.npz \
  --shard-id short_causal --config configs/operator.yaml \
  --workspace artifacts/runtime --max-new-row-blocks 1
python -m flashblock resume --input data/shards/short_causal.npz \
  --shard-id short_causal --config configs/operator.yaml \
  --workspace artifacts/runtime
python -m flashblock prefix-update --input data/shards/odd_dim_causal.npz \
  --shard-id odd_dim_causal --config configs/operator.yaml \
  --workspace artifacts/runtime --key-start 80 --key-end 112 --seed 7
python -m flashblock state --workspace artifacts/runtime --shard-id short_causal
python -m flashblock invalidations --workspace artifacts/runtime --shard-id odd_dim_causal
python -m flashblock budget --input data/shards/short_causal.npz \
  --shard-id short_causal --config configs/operator.yaml --workspace artifacts/runtime
python -m flashblock demo --root .
```

所有错误以 JSON 返回，包含 `code`、`message` 和业务上下文。

## 验证

```bash
python -m unittest discover -s tests -v
```

测试覆盖：

- 因果/非因果、非整除行块/列块/头维度与全量参考的容差一致；
- 重复运行字节一致；
- 前缀修正只复用未受影响行/列块，并与从零计算逐字节一致；
- 配置失效原因、复用清单与重建输出；
- 断点恢复、截断记录、重复序号、原子临时文件；
- 落后、重复、跨行块检查点发布拒绝；
- 非法续算阶段、`E_NUMERIC`、`E_MEMORY_BUDGET` 上下文；
- YAML/形状清单生成与 CLI 查询。

实际结果见 `VALIDATION.md` 和 `artifacts/demo_summary.json`。详细设计见 `docs/DESIGN.md`。

## 说明

- 内部计算使用 `float64`，输入会转换为 C 连续数组。
- “逐字节一致”比较的是同一实现、同一运行环境下的增量/恢复结果与从零分块结果；逐行参考实现因归约顺序不同，按配置容差检查。
- 默认 `verify_incremental: true` 会额外从零重算一次用于检测 `E_INCREMENTAL_MISMATCH`。生产性能路径可关闭验证；实际参与拼接的状态仍只重算受影响列块。
- 未实现 GPU、反向传播和分布式训练调度。

