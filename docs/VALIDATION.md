# Validation Record

This file is populated from the final validation run in the delivered environment.

## Environment

- OS: Linux x86_64 (WSL2 kernel 6.18)
- Python: 3.14.4
- NumPy: 2.5.5
- Pillow: 13.0.0
- No GPU, model download, external service or container runtime is used.

## Commands

```bash
python3 -m unittest discover -s tests -v
python3 -m cmir_alignment.cli demo \
  --workspace runtime/demo-workspace \
  --data-dir data/sample \
  --report artifacts/demo-report.json
python3 -m cmir_alignment.cli http-demo \
  --workspace runtime/http-demo-workspace \
  --data-dir data/sample \
  --port 0
```

## Results

### Automated tests

```text
Ran 16 tests in 17.950s
OK
```

覆盖项：

- 固定种子两次生成的 `dataset.json`、JSONL 与 PNG 逐字节一致；
- 视觉 224 维、文本 512 维、重复提取摘要一致；
- 文本特征偏移、权重和哈希桶合法；
- 缺失图像、空 caption → `E_MISSING_INPUT` 且诊断落盘；
- 投影维度/NaN → `E_PROJECTION_INVALID`，非法位宽 → `E_INVALID_BITWIDTH`；
- 零 scale → `E_QUANTIZATION_ZERO_SCALE`；
- 误差上限过小 → `E_NUMERIC`；
- 双向检索、重复请求逐字节一致、空文本/k 越界/63 维查询向量 → `E_INVALID_QUERY`；
- 8 位 → 4 位增量重建、特征复用、增量/全量摘要一致；
- 单分片更新、未变分片特征复用、全量一致；
- 排序权重变化从 `index` 阶段失效、上游特征/量化复用；
- 优雅停止/恢复与从零全量运行摘要一致；
- 人为改变状态摘要 → `E_INCREMENTAL_MISMATCH` 且带 `difference.path`；
- 4 进程并发发布仅 1 个成功，其余 `E_STALE_CHECKPOINT`；
- 重复/跨阶段乱序发布、缺前置阶段、回退/篡改/正常推进审计；
- 损坏检查点记录恢复后序号连续且最终全量一致；
- HTTP 导入、配置、构建、停止、恢复、检索、校验、导出完整流程。

### Demo, quant 8-bit

```text
dataset_digest       = 45d03b2e2d06be69617295859f6b8eb6f008d7f125ef71f1eb3ebfc3625e8061
run_digest           = 481eb7414d20874018963313ea12c0eb84f97458fed2e82fed069d0a5ac82184
top-k digest         = 8636786cfaec89469f13872af0b92f98e1f665859301d65d49a822aab69187db
text2image top-1     = shard-00-item-000
score                = 0.6501788427623092
effective digits     = 2
initial build        = 4 units then graceful stop; resume complete
checkpoint audit     = advance
```

### Demo, quant 4-bit incremental

```text
earliest_stage       = projection_quant
reason               = quantization_changed
reused feature shards= shard-00, shard-01, shard-02
run_digest           = 2f597b11a78698cbd3cf787761544a8af6154534bfdb8f29b33d244307b108c2
top-k digest         = 485b914c4268fa2452edf667676eb3bec9a43ba2187ad163bb7539de10cdd068
text2image top-1     = shard-00-item-000
score                = 0.6499661632354254
effective digits     = 1
full consistency     = true
full run digest      = 2f597b11a78698cbd3cf787761544a8af6154534bfdb8f29b33d244307b108c2
incremental digest   = 2f597b11a78698cbd3cf787761544a8af6154534bfdb8f29b33d244307b108c2
```

检查点共 24 条，全局序号 `1..24` 连续，无跳跃；最终审计状态 `advance`。两次从零运行 demo 得到相同 `demo_summary_digest`：

```text
82a455f3dbebec1faf1955eac600cb9b03ce4bc53c83dd05dc1d0c9005da7013
```

### HTTP demo

```text
health                 = ok
first build            = stopped after 4 units
stop request observed  = true
resume                 = complete
8-bit text2image top-1 = shard-01-item-000
image2text top-1       = shard-00-item-000
4-bit build            = complete
earliest stage         = projection_quant
checkpoint audit       = advance
full consistency       = true
http demo digest       = 2901a4d5cee78215db0b4f501ff9078276f997f4d79932e8f3df18e432a31a02
```

HTTP demo 使用 3 分片 x 4 条样本、随机空闲本机端口；其数据集摘要与 6 条/片 demo 不同，因此 run digest 也不同。

### 误差诊断观察

8 位 `per_dim` 精确训练集 caption 查询的 top-1 相似度误差界为约 `4.56e-3`，有效位数 2；4 位 `per_dim` 同一查询误差界约 `8.37e-2`，有效位数 1。结果中同时保留更保守的 step-only 界，便于审计“显式步长上界”和“高精度参考核验界”的差别。

### 异常码实测

测试进程中实际触发的领域错误码包括：

```text
E_MISSING_INPUT
E_PROJECTION_INVALID
E_INVALID_BITWIDTH
E_QUANTIZATION_ZERO_SCALE
E_NUMERIC
E_INVALID_QUERY
E_INCREMENTAL_MISMATCH
E_STALE_CHECKPOINT
E_INVALID_STAGE
```
