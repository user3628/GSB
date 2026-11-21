# ShardSeek

本地、确定性、可审计的视觉表征与双向检索系统。它只依赖 CPU、NumPy、Pillow 和 Python 标准库，不使用真实视觉模型、GPU、Docker、账号或外部服务。

系统从固定种子生成的 64×64 RGB PNG 分片与 JSONL 图文清单开始，完成：

- 分块颜色矩 + 梯度方向直方图视觉描述子；
- NFKC / 空白折叠 / 小写化后的哈希词袋与字符 n-gram 文本特征；
- 两路特征经显式投影矩阵进入同一 64 维单位空间；
- 8 位或 4 位、逐维或逐向量定点量化，并记录每维缩放因子、零点、步长和误差界；
- 稠密索引 + 倒排索引的双向检索、相似度分解和 Decimal 高精度误差诊断；
- 内容摘要驱动的阶段增量重建、全量/增量逐字节一致性校验；
- `fcntl.flock` 保护的原子检查点、优雅停止/恢复、快照回退识别；
- 本地 HTTP 交互应用、CLI、固定摘要 demo 和自动测试。

实际验收结果见 [VALIDATION.md](VALIDATION.md)，机器可读记录见 `artifacts/demo_report.json`。

## 环境

本项目已在以下环境实际安装并验证：

| 组件 | 版本 |
|---|---|
| Python | 3.11.18 |
| NumPy | 2.4.6 |
| Pillow | 13.0.0 |
| 进程锁 | Linux `fcntl.flock` |
| HTTP | Python 标准库 `ThreadingHTTPServer` |

`requirements.txt` 固定为：

```text
numpy==2.4.6
Pillow==13.0.0
```

工作区内的 `.venv` 使用独立 Python 3.11。重建环境：

```bash
./scripts/bootstrap.sh
```

该脚本只安装 Python 3.11、Pillow 和 NumPy。未安装 PyTorch、TensorFlow、CUDA 或其他深度学习/GPU 依赖。

## 快速开始

运行完整、可复现的演示：

```bash
./run.sh --root runtime demo
```

启动交互应用：

```bash
./run.sh --root runtime serve --port 8767
```

浏览器访问 `http://127.0.0.1:8767/`。应用支持重置/导入、切换配置、全量/增量构建、停止/恢复、双向检索、查看相似度分解/误差诊断、导出报告和触发增量一致性校验。

从零手动演示：

```bash
# 1. 清空工作区、导入 8 个样本、选择 8 位配置
./run.sh --root runtime reset

# 2. 在第 2 个分片后优雅停止
./run.sh --root runtime build --mode full --stop-after-shards 2

# 3. 从检查点恢复
./run.sh --root runtime resume

# 4. 双向检索
./run.sh --root runtime search \
  --direction text_to_image --text '红色方块 blue gradient' --top-k 5
./run.sh --root runtime search \
  --direction image_to_text --image-item item-01 --top-k 5

# 5. 切到 4 位配置并增量重建
./run.sh --root runtime config use compact_4bit
./run.sh --root runtime build --mode incremental

# 6. 增量结果与从零全量结果逐字节校验
./run.sh --root runtime verify-incremental

# 7. 查询阶段状态/版本指纹、语料级误差界
./run.sh --root runtime fingerprints
./run.sh --root runtime checkpoints
./run.sh --root runtime diagnostics
```

运行测试：

```bash
.venv/bin/python -m unittest discover -s tests -v
```

## 数据与配置

固定种子 `20261122` 生成 8 个 64×64 PNG 分片：

- `item-01` 至 `item-07` 为可索引样本；
- `item-06` 是跨分片、亮度不同的近似重复项；
- caption 覆盖中英混排；
- `item-08` 的 caption 为空，在清单中显式标记 `enabled=false`、`expected_error=E_EMPTY_CAPTION`。导入时它不会静默消失，而是进入 `rejected` 诊断并带 `item_id/stage/code`；
- 如果启用的条目 caption 为空，导入/特征阶段会直接返回 `E_EMPTY_CAPTION`。

文件：

- `samples/manifest.jsonl`：含隔离异常样本的完整清单；
- `samples/manifest_strict.jsonl`：仅有效样本；
- `samples/images/*.png`：确定性分片；
- `samples/generation_summary.json`：种子、尺寸、哈希摘要。

配置：

- `configs/baseline_8bit.json`：逐维 8 位量化；
- `configs/compact_4bit.json`：同一投影矩阵、逐维 4 位量化；
- `configs/projection_alt_8bit.json`：不同投影矩阵，用于投影变更测试。

投影矩阵由本地配对样本做确定性的正则化 CCA 线性标定，前 6 个潜在方向最大化配对视觉/文本相关性，其余方向使用固定的极小确定投影以保证 64 维非零可测。这是纯 NumPy 线性代数标定，不是神经网络或外部视觉模型。重新生成：

```bash
.venv/bin/python scripts/calibrate_configs.py
```

## 特征

### 视觉描述子

图像转为 RGB `float64`，按 `grid_rows × grid_cols` 切块。每块计算：

- RGB 均值：3 维；
- RGB 总体标准差：3 维；
- 8 向梯度方向加权直方图：8 维（方向直方图按梯度幅值归一化）。

默认 2×2 网格，维度为 `2×2×(3+3+8)=56`。网格、方向桶数均可配置。块边界、均值、标准差、直方图与整体特征摘要均写入确定性摘要。

### 文本特征

1. Unicode NFKC；
2. 连续空白折叠为单个空格并去首尾；
3. 小写化。

规范化后：

- 词袋：拉丁字母/数字词 + 单个 CJK 字符，经 BLAKE2b 哈希至 64 维；
- 字符 n-gram：去除空白后的 2/3-gram，经 BLAKE2b 哈希至 64 维。

总维度 128。无词表状态，新增 caption 不会改变全局维度；哈希碰撞是固定且可复现的。

## 投影、量化与误差传播

### 投影

视觉原始特征 `x_v` 与文本原始特征 `x_t` 分别通过显式矩阵 `P_v`、`P_t`：

```text
z_v = x_v P_v / ||x_v P_v||_2
z_t = x_t P_t / ||x_t P_t||_2
```

`z_v, z_t ∈ R^64`。投影形状、有限性和全零范数均在对应阶段校验。

### 定点量化

对称有符号量化范围：

```text
qmin = -2^(b-1)
qmax =  2^(b-1) - 1
```

`b=8` 或 `b=4`。每个向量维度保存：

- `scale`：缩放因子；
- `zero`：零点（本实现为 0）；
- `quant_step`：量化步长（等于 `scale`）；
- `error_bound`：半步长 `scale/2`。

逐维模式对所有语料向量在每个维度取 `max(abs(x))` 标定。逐向量模式对每个向量取全局绝对最大值生成一个尺度，并记录到该向量的 64 个维度记录中。量化使用 `rint`（就近偶数）并夹取到 `[qmin,qmax]`；反量化为 `q * scale`。

表征库目录还保存每向量的显式 L2 误差包络：

```text
alpha = sqrt(sum_i max(step_i/2, |x_i - dequant_i|)^2) + 1e-12
```

第二项覆盖查询向量越过语料标定区间时的裁剪误差。没有参考值或缩放信息时不会推断误差。

### 相似度误差

解析标度上界使用查询与目标的 `alpha`：

```text
Eq = alpha_query
Et = alpha_target
E  = Eq + Et + Eq*Et
analytic_bound = E*(1+|cos_ref|) / ((1-Eq)*(1-Et)) + 1e-15
```

检索报告同时给出：

- `cosine_error_bound_analytic`：仅由半步长/裁剪包络得到的保守解析界；
- `cosine_error_bound`：用 50 位 Decimal 对固定点反量化余弦与高精度参考余弦做核验后的紧上界，即 `|cos_quant - cos_ref| + 1e-12`。

两者都满足 `actual_error <= bound`。紧上界用于有效位数展示，解析上界保留用于审计“仅凭缩放信息”的保守误差传播口径。高精度参考向量以十进制字符串单独存放于 `runtime/diagnostics/reference_vectors.json`，只用于诊断，绝不参与排序。

有效位数：

```text
effective_digits = floor(log10(|cos_ref| / bound))
```

结果还包含视觉/文本两路的分数分量与 `log1p(abs(component))` 对数贡献。

## 索引与排序

每个样本保存视觉/文本两套 64 维定点向量。索引包含：

- 稠密：反量化后的逐项余弦；
- 文本倒排：规范化 caption token；
- 视觉倒排：视觉定点向量绝对值最大的若干维符号 token；
- 文本潜在倒排：文本定点向量绝对值最大的若干维符号 token。

排序分数：

```text
combined = dense_weight * dequantized_cosine
         + inverted_weight * normalized_idf_hit_score
```

`dense_weight`、`inverted_weight` 是配置的一部分。检索请求若传入与已发布索引不同的权重，会返回 `E_SNAPSHOT_MISMATCH`，必须重建索引阶段，避免评测口径与索引配置错配。

排序键固定为：

```text
(-combined_score, item_id ascending)
```

因此精确并列时 item_id 小者优先。文本→图像结果中的视觉分量来自稠密余弦，文本分量来自倒排；图像→文本反之。

## 增量阶段

每个阶段指纹独立计算：

| 阶段 | 输入指纹包含 |
|---|---|
| `feature_extract` | 图像 SHA-256、规范化 caption、source/lang、特征配置 |
| `project_quantize` | 特征摘要、投影矩阵、量化位宽/粒度/投影配置 |
| `index_build` | 表征摘要、排序权重、倒排配置 |
| `recall_verify` | 分片 posting 摘要、量化参数指纹 |

增量构建比较上一份已发布 `build_state.json` 与当前特征/配置：

- 图像、caption、source/lang 或特征配置变化 → 从 `feature_extract` 失效；
- 投影或量化变化 → 从 `project_quantize` 失效，特征缓存复用；
- 排序权重/倒排配置变化 → 从 `index_build` 失效，上游复用；
- 全部一致 → 全部复用，不重写快照。

`verify-incremental` 会在两个独立工作区分别执行“基线全量 → 变更增量”和“变更后从零全量”，逐字节比较：

- `representation_summary.json`；
- `index_summary.json`；
- 四条固定检索报告的规范 JSON 字节。

不一致时返回 `E_INCREMENTAL_MISMATCH`，附带阶段与首个差异 item_id/查询标签。

## 检查点与恢复

每个启用的分片按四阶段发布：

1. `feature_extract`
2. `project_quantize`
3. `index_build`
4. `recall_verify`

记录示例：

```json
{
  "seq": 12,
  "session_id": "…",
  "shard_id": "item-03",
  "stage": "index_build",
  "stage_index": 3,
  "input_fingerprint": "…",
  "config_version": "…",
  "quantization_fingerprint": "…",
  "artifact_digest": "…",
  "snapshot_sequence": 2
}
```

约束与实现：

- 每次记录写入都在 `checkpoints.lock` 上持有真实 `flock`；
- 整个 JSONL 先写同目录临时文件、`fsync`、再 `os.replace`，不会出现半写记录；
- `seq` 必须严格等于上一条 +1；
- 当前会话期望的 `(shard_id, stage_index)` 序列固定，重复/落后/跨分片错序返回 `E_STALE_CHECKPOINT`；
- 缺少前置阶段返回 `E_INVALID_STAGE`；
- 新构建会话会原子归档旧会话日志；
- 收到停止请求后，当前分片四个阶段完成后才退出；
- `resume` 复用相同会话，最终表征/索引/检索报告与一次全量运行逐字节一致。

## 快照保护

发布顺序为表征/参考向量/索引/摘要，最后写 `state/current_snapshot.json` 高水位。检索前验证：

- 表征/索引文件都存在；
- 表征与索引 `snapshot_sequence` 均不低于高水位；
- 两者属于同一快照；
- 摘要规范字节的 SHA-256 与发布指纹一致；
- 索引引用的表征指纹一致；
- 快照配置指纹与当前活动配置一致；
- `build_state` 不高于当前高水位。

旧产物、回退序号或摘要/配置不匹配会返回 `E_SNAPSHOT_REGRESSION` 或 `E_SNAPSHOT_MISMATCH`，不会把新表征与旧索引混用。

## HTTP API

默认只绑定 `127.0.0.1`。无账号、Cookie 或外部网络依赖。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 交互式 UI |
| GET | `/api/health` | 健康检查 |
| GET | `/api/status` | 配置、导入、快照、检查点状态 |
| GET | `/api/fingerprints` | 各阶段与发布版本指纹 |
| POST | `/api/reset` | 清空、导入、选配置 |
| POST | `/api/import` | 导入清单 |
| POST | `/api/config` | 选择投影/量化配置 |
| POST | `/api/build` | 全量/增量构建 |
| POST | `/api/stop` | 请求优雅停止 |
| POST | `/api/resume` | 恢复匹配会话 |
| POST | `/api/search` | 双向检索 + 分解/诊断 |
| GET | `/api/diagnostics` | 语料级误差界序列 |
| POST | `/api/verify-incremental` | 增量 vs 全量逐字节校验 |
| GET | `/api/image/{item_id}` | 本地 PNG |

示例：

```bash
curl -s http://127.0.0.1:8767/api/status
curl -s -X POST http://127.0.0.1:8767/api/search \
  -H 'Content-Type: application/json' \
  -d '{"direction":"text_to_image","query_text":"红色方块 blue gradient","top_k":3}'
```

## 业务错误码

| 代码 | 含义 |
|---|---|
| `E_EMPTY_IMAGE` | 图像缺失、空字节或无法解码为 RGB |
| `E_GRID_OUT_OF_BOUNDS` | 网格/方向桶非法或超出图像 |
| `E_EMPTY_CAPTION` | 规范化后 caption 为空 |
| `E_DIM_MISMATCH` | 投影/量化/参考向量维度不匹配 |
| `E_NUMERIC` | NaN、Inf、负/零缩放、非有限范数等 |
| `E_INVALID_MANIFEST` | 清单/字段/图像引用非法 |
| `E_INVALID_CONFIG` | 配置缺失、格式或枚举值非法 |
| `E_INVALID_STAGE` | 检查点缺少前置阶段或会话不存在 |
| `E_STALE_CHECKPOINT` | 序号、重复、落后或跨分片错序发布 |
| `E_INCREMENTAL_MISMATCH` | 增量与全量摘要/索引/报告不一致 |
| `E_SNAPSHOT_REGRESSION` | 检测到快照序号回退 |
| `E_SNAPSHOT_MISMATCH` | 配置、指纹、摘要或索引/表征组合不一致 |
| `E_INDEX_NOT_READY` | 索引尚未发布 |
| `E_QUERY_EMPTY` | 查询文本为空或无 token |
| `E_INVALID_K` | top-k ≤ 0 或超过索引规模 |
| `E_INVALID_DIRECTION` | 未知检索方向 |
| `E_BUILD_STOPPED` | 构建按请求在分片边界停止（非异常失败） |

错误 JSON 稳定包含 `code`、`message`、`stage`、`item_id`（如适用）与结构化 `details`。

## 目录

```text
shardseek/
  features.py       视觉/文本特征
  config.py         配置、CCA 标定、阶段指纹
  quantization.py   定点量化、Decimal 误差传播
  pipeline.py       构建、增量复用、发布
  checkpoint.py     flock + 原子检查点
  snapshot.py       快照校验/回退识别
  search.py         双向检索与诊断
  workspace.py      导入、配置、控制文件
  http_server.py    本地 HTTP/API
  web/              无外部依赖交互 UI
  verification.py   增量/停止恢复一致性校验
  demo.py           固定演示产物
tests/              29 项 unittest
configs/            显式投影与量化配置
samples/            固定种子分片和清单
artifacts/          demo 与机器可读验收记录
```

## 设计边界

- 系统没有真实视觉语义模型。跨模态稠密对齐来自本地配对样本的确定性线性 CCA；更换完全不同的语料时应重新标定投影矩阵，否则性能会下降。
- 词袋/字符 n-gram 使用固定维哈希，碰撞可复现但不保证无碰撞；这换取了增量新增 caption 时无需全局词表重建。
- 逐字节确定性验证绑定当前固定依赖版本、平台浮点实现和配置。跨 CPU 架构/数学库迁移时应重新生成基准摘要。
- 演示仅使用本地生成数据，不访问未授权目标。
