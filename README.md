# spmm-ops

本地可运行的确定性 CSR × 稠密矩阵 SpMM 组件。实现只依赖 Python 标准库与 NumPy；不调用 SciPy、MKL 稀疏接口或其他外部稀疏算子，也不需要 GPU、网络、账号或真实数据集。

核心能力：

- 行块 + 稀疏列块（K 维分块）SpMM，只遍历 CSR 已存非零元。
- 显式累加顺序：全局列索引稳定升序，或列块顺序内保持原非零元出现次序。
- 重复列索引按出现次序累加；显式存储的零值不跳过；顺序求和与 Neumaier 补偿和。
- 自动分块评分：工作集、预测访存、行/列负载不均衡、空块比例、空转比例。
- 显式 Decimal 高精度参考、可验证误差上界、随机位置抽样、朴素非零元自校验。
- 稳定业务错误码与精确 array/row/column/nnz 位置。
- 原子版本存储、摘要链、高水位锚点、Windows/Unix 文件锁、跨方案缓存保护。
- 峰值内存规划、行/列拆分执行、预算明细与拆分诊断。
- 库 API、CLI、固定随机种子样本、一键演示和自动化测试。

更详细的算法和协议见 [TECHNICAL.md](TECHNICAL.md)。

## 快速开始

### Windows（Python 3.10+）

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e . --no-build-isolation --no-deps
python scripts\generate_samples.py
python scripts\run_demo.py
python -m pytest -q
```

### Linux / macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e . --no-build-isolation --no-deps
python scripts/generate_samples.py
python scripts/run_demo.py
python -m pytest -q
```

项目使用 `src/` 布局；上述离线安装命令不访问包索引。安装后可直接导入 `spmm_ops`，也可使用 `spmm-ops` 命令。若虚拟环境没有 setuptools，也可零安装运行：脚本会自动加入 `src/`，CLI 使用 `PYTHONPATH=src python -m spmm_ops.cli ...`。

本次实际验收环境为 Linux（WSL）Python 3.14.4 + NumPy 2.5.5。代码语法面向 Python 3.10+；Windows 文件锁路径使用标准库 `msvcrt`，文件替换使用 `os.replace`，脚本与路径处理均使用 `pathlib`。Windows 专属分支未在当前 Linux 主机上执行，若在 Windows 验收请按上面的 PowerShell 命令运行测试。

## 固定样本

`samples/manifest.json` 清单包含形状、目标/实际稀疏率、空行、重复索引、显式零值、NaN/Inf 标记、指纹与逻辑数组摘要。样本固定为：

| 名称 | A 形状 | B 形状 | 约稀疏率 | 说明 |
|---|---:|---:|---:|---|
| `square_1pct`, `square_10pct`, `square_40pct` | 96×96 | 96×24 | 1% / 10% / 40% | 方阵 |
| `tall_1pct`, `tall_10pct`, `tall_40pct` | 160×64 | 64×20 | 1% / 10% / 40% | 高瘦 |
| `wide_1pct`, `wide_10pct`, `wide_40pct` | 64×160 | 160×32 | 1% / 10% / 40% | 矮胖 |
| `degenerate_struct` | 48×40 | 40×16 | 约 10.5% | 空行、全零行块、乱序、重复索引、显式 ±0、强消去行 |
| `anomaly_nan_inf` | 24×18 | 18×12 | 约 20% | A/B 含 NaN、+Inf、-Inf |

每个稀疏矩阵保存为 `<name>.npz` 的 `indptr`、`indices`、`data` 和 `shape`；右乘稠密矩阵保存为 `<name>.dense.npy`；高精度参考保存为 `<name>.ref.npz`，其 `values` 是 80 位 Decimal 上下文计算、40 位有效数字输出的 UTF-8 字符串（特殊值写为 `NAN`/`INF`/`-INF`）。

## 库接口

```python
from spmm_ops import RunOptions, spmm

result = spmm(
    "samples/square_40pct.npz",
    "samples/square_40pct.dense.npy",
    options=RunOptions(
        sum_strategy="compensated",
        order="ascending",
        cache_budget_bytes=64 * 1024,
        memory_limit_bytes=48 * 1024,
        version_store=".spmm_store",
        audit=True,
        sampling_seed=20261121,
        sampling_count=64,
    ),
)

print(result.output.shape)
print(result.output_digest)
print(result.scheme.to_dict())
print(result.audit_report["passed"])
print(result.cache, result.version)
```

也可以使用关键字参数直接调用：

```python
result = spmm(A_path, B_path, sum_strategy="sequential", audit=False)
```

主要对象：

- `CSRMatrix`：校验、特征、形状/结构/值指纹、重复索引容差检查。
- `select_block_scheme(...) -> BlockPlan`：自动候选评分和选择理由。
- `plan_execution_schedule(...) -> ExecutionSchedule`：峰值内存规划与拆分。
- `execute_spmm(...) -> KernelResult`：底层内核；`naive_spmm(...)` 是独立自校验参考。
- `audit_result(...)`：高精度误差界、随机抽样与朴素实现行块抽查。
- `VersionStore`：方案/缓存版本发布、状态、缓存管理。
- `run_spmm/spmm(...) -> SpMMResult`：端到端编排。

CSR 的 `data` 与稠密 B 必须同为 `float32` 或同为 `float64`，内核不会隐式提升精度域。

## CLI

```bash
# 生成样本
python -m spmm_ops.cli generate-samples --out samples

# 查看自动分块打分明细
python -m spmm_ops.cli select \
  --sparse samples/square_40pct.npz \
  --dense samples/square_40pct.dense.npy \
  --cache-budget 65536

# 执行 SpMM + 审计 + 版本发布 + 缓存
python -m spmm_ops.cli run \
  --sparse samples/square_40pct.npz \
  --dense samples/square_40pct.dense.npy \
  --strategy compensated --order ascending \
  --cache-budget 65536 --memory-limit 49152 \
  --store .spmm_store --report report.json --output result.npy

# 完整误差报告
python -m spmm_ops.cli audit \
  --sparse samples/degenerate_struct.npz \
  --dense samples/degenerate_struct.dense.npy \
  --order block --row-block 16 --col-block 24

# 版本与缓存
python -m spmm_ops.cli version-status --store .spmm_store
python -m spmm_ops.cli cache list --store .spmm_store
python -m spmm_ops.cli cache clear --store .spmm_store
python -m spmm_ops.cli cache prune --store .spmm_store
```

Windows 下可将续行符改为 PowerShell 反引号或写成单行。CLI 输出 JSON；业务错误同时带 `code`、`message`、`details`，并在可用时带 `location`。

## 错误码

| 业务码 | 含义 | CLI 退出码 |
|---|---|---:|
| `E_SHAPE_INVALID` | 矩阵维度、B 前导维度或 dense 输出宽度非法 | 10 |
| `E_BAD_CSR` | `indptr` 长度/首尾/单调性、`indices/data` 长度或列越界 | 11 |
| `E_DUP_INDEX` | 重复列索引超过 `duplicate_tolerance` | 12 |
| `E_NUMERIC_MISMATCH` | 抽样行块超容差、误差上界失败或非有限类别不符 | 13 |
| `E_MEMORY_BUDGET` | 缓存工作集或受管执行峰值无法满足预算 | 14 |
| `E_STALE_PUBLISH` | 过期/重复发布、序号回退、摘要或锚点不匹配、跨方案缓存发布 | 15 |
| `E_REFERENCE_MISSING` | 审计开启但显式高精度参考不存在 | 16 |
| `E_REFERENCE_INVALID` | 参考文件格式、形状或 token 非法 | 17 |
| `E_DTYPE_UNSUPPORTED` / `E_DTYPE_MISMATCH` | 不支持的类型或 CSR/B 精度域不一致 | 18 / 19 |
| `E_LOCK_TIMEOUT` | 版本存储文件锁超时 | 20 |

## 固定运行结果

`python scripts/run_demo.py` 会清除并重建 `artifacts/demo_runtime/`，生成 `artifacts/demo_summary.json`。本机实际结果：

```text
summary_digest=22aafc0e876d2b7c01e16191af8f2728a09e6504580563a9c6eab61cc160cdbf
manifest_digest=682e89f8929d94251ad336c48fcb3902ba59d6cabf8b1a0c63c49172c30bf788
samples=11 version_sequence=28
plan_change: A_initial_hit=False A_repeat_hit=True B_initial_hit=False A_return_hit=False stable=True A_ne_B=True
```

该摘要覆盖三档稀疏率、方阵/高瘦/矮胖、重复索引、显式零、NaN/Inf、拆分执行、误差界和 A→B→A 缓存失效重建。来源相同、运行目录干净时摘要可复现。

## 验证

```text
PYTHONPATH=src pytest -q
31 passed
```

测试覆盖：样本可复现、CSR/形状/列越界/重复索引业务码、升序与块序语义差异、重复索引与显式零位置、float32/float64、自动分块预算与空行惩罚、高精度误差界、随机抽样、朴素实现自校验、NaN/Inf 来源、内存拆分、版本回退/同序号替换/锚点检测、并发发布、跨方案缓存保护、缓存重建，以及一键示例固定摘要复现。

## 目录

```text
src/spmm_ops/       库实现
scripts/            样本生成与一键演示
samples/            固定样本、清单、高精度参考
tests/              pytest 验收测试
artifacts/          固定运行摘要（运行时缓存目录可重建）
TECHNICAL.md        算法、误差界、内存与版本协议
```

## 适用边界

- `memory_limit_bytes` 管理输出、B 临时切片、累加器/补偿器、排序工作区和元数据；调用方持有的完整 B 输入为常驻输入，不重复计入，但会在诊断中单独给出其形状/指纹。
- 结果必须使用完整输出数组；若输出本身加最小元数据已超预算，返回 `E_MEMORY_BUDGET`，不会等待运行时 OOM。
- 缓存键除业务要求字段外还包含稠密 B 指纹、稀疏 `data` 指纹、dtype、累加顺序、内存上限和有效拆分边界，避免相同结构但值或执行配置变化时错误复用。
- 文件锁与原子替换支持 Windows `msvcrt` 和 Unix `fcntl`；不使用 Docker/容器。
