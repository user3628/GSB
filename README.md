# GEMM/BMM 分块算子系统

这是一个可在本地 CPU 上完整运行的 GEMM（单矩阵乘）与 BMM（批量矩阵乘）算子实验室。项目从确定性本地 `.npy` 样本出发，实际执行分块内核、自动方案选择、误差自校验、版本化调优配置、跨进程结果缓存和内存预算调度，不依赖 GPU、Docker、外部服务或 BLAS 之外的第三方加速库。

当前工作区已包含生成好的样本和一次完整运行产物：

- `data/samples/`：11 组固定种子的 float64 矩阵样本。
- `data/manifest.json`、`data/shape_list.json`、`data/SHAPES.md`：形状、转置标志、种子、异常注入位置和文件摘要。
- `artifacts/demo_summary.json`：完整示例的确定性语义摘要。
- `artifacts/demo_measurements.json`：本机测量耗时（不参与确定性摘要）。
- `artifacts/plan_benchmark.json`：不同分块方案与求和策略的真实本机测量。
- `artifacts/VALIDATION.md`：本次实际验证记录。

## 快速开始

目标环境为 Windows、Python 3.11、NumPy 1.26。代码同时兼容 Linux/macOS；当前交付环境实际验证版本为 Python 3.14.4、NumPy 2.3.5。

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install pytest  # 或使用 requirements-dev.txt

python scripts/generate_samples.py --output data
python -m pytest
python examples/full_demo.py
python examples/benchmark_plans.py
```

包采用仓库根目录布局，不安装也可直接运行。若需要 `gemm-lab` 命令：

```powershell
python -m pip install -e .
gemm-lab version
```

运行测试也可直接安装开发依赖：

```powershell
python -m pip install -r requirements-dev.txt
```

Linux/macOS 将 `py -3.11` 与 `.venv\Scripts\Activate.ps1` 替换为 `python3.11` 与 `source .venv/bin/activate` 即可。脚本启动时设置 `OPENBLAS_NUM_THREADS=1`、`OMP_NUM_THREADS=1`、`MKL_NUM_THREADS=1`，保证逐次运行可比。

## 项目结构

```text
gemm_lab/
  blocking.py       三层分块方案、候选生成、预算内自动打分
  kernel.py         GEMM/BMM 统一分块内核与三种求和策略
  reference.py      二次幂 K 分解参考、math.fsum 高精度基准、误差界
  verify.py         行块抽样自校验、NaN/Inf 位置统计
  memory.py         输出/工作区/批量切片内存规划
  config_store.py   原子、单调、带外部高水位的版本化配置
  cache.py          版本感知结果缓存与全局发布序号
  storage.py        跨平台文件锁、原子写、摘要工具
  api.py            库 API、工作区与结果摘要
  cli.py            命令行入口
scripts/
  generate_samples.py
examples/
  full_demo.py      完整验收演示
  benchmark_plans.py
tests/              43 项自动化测试
data/               确定性样本与形状清单
docs/TECHNICAL.md   格式、算法与一致性说明
```

## 样本与真值

样本包含：

- 小、中、大三档：`64x64x64`、`512x256x384`、`1024x1024x1024`。
- 非方阵：`128x96x80` 等。
- 四种转置组合：`N/N`、`T/N`、`N/T`、`T/T`。
- 批量样本：`4x32x24x40`、`3x17x29x23` BMM。
- NaN/Inf 异常样本：单矩阵与批量各一组。

所有输入由 `numpy.random.default_rng(base_seed + i*1009).standard_normal(...)` 生成。`manifest.json` 记录每个 `.npy` 的 SHA-256。重新生成方法见 `data/README.md`。

误差真值有两条独立路径：

1. 递归二次幂 K 分解参考：按 K 区间做最大二次幂拆分，再逐层相加，不共享实现内核的累加器。
2. 高精度逐元素基准：对抽样元素逐项计算 float 乘积，再用 `math.fsum` 求和。

自校验对每个输出矩阵抽样最多 3 个行块（默认每块 8 行），将输出与二次幂参考逐元素比较；另抽样元素与高精度基准比较。报告 `|actual-reference|`、`|actual-reference|/|reference|`、最大偏差、理论误差界及超界数。失败时抛出 `E_NUMERIC_MISMATCH`，详情包含行/列区间和最大偏差，且不向调用方返回输出缓冲。

## 核心内核

每个矩阵按以下层次执行：

1. `MC x NC` 缓存块限定输出工作区。
2. `MR x NR` 寄存器累加块持有当前输出微块。
3. 沿 K 方向以 `KC` 为面板累加，每个寄存器微块使用独立的 `MR x NR` 累加器。

寄存器微块通过 NumPy 所绑定的本机 BLAS 执行 `MR x KC` 与 `KC x NR` 乘法。这样既保留显式三层分块、边界块处理和独立累加器语义，又能让 `1024^3` 在 CPU 上实际完成。小问题（`M*N*K <= 1,000,000` 且 `K <= 256`）默认进入逐 K 项 `scalar` 模式，三种求和树直接作用于每个 K 项；较大问题使用 `panel` 模式，求和策略作用于 K 面板部分和。结果报告会写明 `accumulation_mode`。

三种策略：

- `sequential`：顺序累加。
- `pairwise`：平衡二次幂树累加。
- `compensated`：Kahan/补偿求和；遇到非有限部分和时回退为保序 IEEE 加法，避免补偿更新制造伪 NaN。

BMM 的最外层循环是批次，每个批次调用同一个 GEMM 内核且新建累加器，不复用批次间状态。因此同一分块方案下，每个批次结果与单独调用 GEMM 逐元素完全相同。

## 自动分块

候选方案同时受寄存器工作区和 L2 缓存块工作区约束。选择器以 M/N/K、批量大小、dtype 和 L1/L2 预算预测：

- 操作数访存量与 `M*N*K` 的比值。
- 寄存器面板调用次数。
- 缓存块占用与 L2 预算比。
- 可并行缓存块数量与可用工作进程数。
- 边界填充浪费比例。

小规模提高调用次数/访存权重，避免过度分块；大规模提高访存权重，并硬过滤超出 L2 工作集预算的候选。若用户显式指定 `MR,NR,MC,KC,NC`，会校验正数、硬上限、`MC % MR == 0`、`NC % NR == 0` 以及寄存器块不大于缓存块；真实 M/N/K 不必整除分块，边界块由内核处理。每次结果均记录选择理由、原始指标、归一化分量和权重。

## 配置与缓存

`OperatorWorkspace(root)` 同时管理：

- `root/config_store/`：版本化分块配置。
- `root/cache/`：结果缓存。
- `root/locks/`：跨进程文件锁工作目录。

配置记录包含序号、前序摘要、形状指纹、分块方案、求和策略、L1/L2 预算、选择来源和记录摘要。写入顺序为记录文件、`HEAD.json`、外部高水位锚，均使用临时文件 + `os.replace` 原子替换。外部锚位于 `root/.config_store.anchor.json`，因此整个 `config_store/` 被旧快照替换时仍可检测。

读取会校验记录链、文件名序号、载荷摘要、记录摘要、HEAD 和外部高水位：

- 正常连续推进：可采纳。
- 序号回退或旧快照整体替换：`E_STALE_CONFIG`，`details.reason = "rollback"`。
- 摘要/链/文件被改动：`E_STALE_CONFIG`，`details.reason = "tampered_record"` 等。
- 发布序号冲突、重复发布或落后发布：`E_STALE_PUBLISH`。

结果缓存键由形状指纹、完整分块方案、求和策略、实际累加模式（scalar/panel）和输入数据 SHA-256 组成。缓存条目另存配置版本序号；键命中但配置版本不匹配时立即删除并重建，不能复用旧结果。缓存索引也以全局单调序号和文件锁保护。

## 内存预算

执行前先规划，不依赖运行时崩溃：

```text
峰值估算 = 完整输出缓冲
         + 缓存块工作区
         + 寄存器工作区
         + 固定开销
         + 已物化的 BMM 批次操作数切片
```

BMM 超预算时按批次切片，每个分片记录批次区间、估算峰值和预算余量。即使一个切片也无法容纳时返回 `E_MEMORY_BUDGET`，并携带输出、工作区、单批操作数、最低需求与缺口的明细。切片与不切片都按同一批次顺序调用同一内核，已验证输出逐元素完全相等。

## Python API

```python
import numpy as np
from gemm_lab import OperatorWorkspace, gemm, bmm, select_plan

workspace = OperatorWorkspace("data")
a = np.load("data/samples/small_64x64x64_a.npy")
b = np.load("data/samples/small_64x64x64_b.npy")

selection = select_plan(64, 64, 64, batch=1, l2_bytes=1024 * 1024)
result = gemm(
    a,
    b,
    sum_strategy="pairwise",
    workspace=workspace,
    use_cache=True,
    publish_config=True,
    verify=True,
)

print(result.plan_selection.to_dict())
print(result.kernel.to_dict())
print(result.error_report)
print(result.output)
print(result.summary()["deterministic_digest"])

batch_a = np.load("data/samples/batch_4x32x24x40_a.npy")
batch_b = np.load("data/samples/batch_4x32x24x40_b.npy")
batched = bmm(
    batch_a,
    batch_b,
    sum_strategy="compensated",
    memory_budget_bytes=158_720,
)
print(batched.memory.to_dict())
```

常见异常均通过 `GemmError.code` 返回，调用方也可读取 `to_dict()`：

| 业务码 | 含义 |
|---|---|
| `E_INVALID_SHAPE` | 形状、转置、dtype、分块参数或预算参数非法 |
| `E_BATCH_MISMATCH` | BMM 批次不一致，或 GEMM/BMM 调用形态用错 |
| `E_NUMERIC_MISMATCH` | 抽样行块超出容差，不返回部分输出 |
| `E_STALE_CONFIG` | 配置回退、指纹不匹配或摘要/链篡改 |
| `E_STALE_PUBLISH` | 配置/缓存发布序号落后、重复或并发冲突 |
| `E_MEMORY_BUDGET` | 无法容纳最小执行单位，携带预算明细 |

## 命令行

```powershell
# 自动分块查询及打分明细
python -m gemm_lab plan --sample large_1024x1024x1024 --manifest data/manifest.json

# 显式分块 MR,NR,MC,KC,NC
python -m gemm_lab plan --m 512 --n 256 --k 384 --plan 16,32,128,64,128

# 执行并输出完整摘要
python -m gemm_lab run --sample batch_4x32x24x40 --strategy compensated `
  --manifest data/manifest.json --workspace data --use-cache `
  --memory-budget 158720 --output artifacts/batch_run.json

# 仅查看误差与污染统计
python -m gemm_lab report --sample anomaly_nan_inf_48x40x36 --strategy sequential

# 配置版本与缓存状态；--workspace 可前置或放在子命令后
python -m gemm_lab config show --workspace data
python -m gemm_lab config verify --workspace data
python -m gemm_lab cache status --workspace data
python -m gemm_lab cache clear --workspace data --expected-seq 0
```

CLI 将业务错误打印为 JSON，并使用稳定退出码：参数/形状 2、批次 3、数值 4、陈旧配置 5、陈旧发布 6、内存预算 7。

## 已验证结果

`python examples/full_demo.py` 的实际语义摘要摘要值为：

```text
deterministic_digest = 8ca51e1c1b844787bb3776c6ca67e6ae38022a3a86b14fc4066d60f6d168b77f
```

关键结果：

- 11 个样本全部通过行块自校验；最大高精度绝对偏差 `4.26e-14`（1024³）。
- 小规模自动方案 `16x32/64x64x64`，仅 8 次寄存器面板调用、98,304 B 缓存工作区。
- 中/大规模自动方案 `16x32/256x64x256`，缓存工作区 786,432 B，低于 1,048,576 B L2 预算。
- `1024³` 完整 API 调用（含抽样自校验）在本机约 0.5-0.6 秒；纯内核中位测量约 0.30 秒。
- BMM 四个批次的输出与四次独立 GEMM 逐元素完全相等。
- 缓存首次遇到旧配置版本时 `config_version_mismatch` 失效重建，第二次为 `hit`。
- 配置回退返回 `E_STALE_CONFIG/rollback`，篡改返回 `E_STALE_CONFIG/tampered_record`，过期发布返回 `E_STALE_PUBLISH`。
- 内存预算不足返回 `E_MEMORY_BUDGET`；158,720 B 时批次切片大小为 2，切片结果与完整结果逐元素相等，恢复执行一致。

完整机器可读结果见 `artifacts/demo_summary.json`，本机测量见 `artifacts/demo_measurements.json` 和 `artifacts/plan_benchmark.json`。

## 测试

```powershell
python -m pytest -q
```

当前执行结果：`43 passed`。测试覆盖 GEMM 转置/非方阵、三种求和策略、BMM 隔离、NaN/Inf、自动/显式分块、错误码、畸形/篡改配置、缓存失效、内存切片、多进程配置与缓存发布竞争。完整设计和文件格式见 `docs/TECHNICAL.md`。

## 离线与平台约束

- 所有数据、配置、缓存和测量均在本地文件系统完成，不访问网络。
- 运行时不使用 GPU、Docker 或第三方数值库；仅使用 Python 标准库与 NumPy。
- Windows 文件锁使用 `msvcrt.locking`，POSIX 使用 `fcntl.flock`，同一套 API。
- `np.save/np.load` 使用 `allow_pickle=False`；清单中记录输入 SHA-256。
- 当前实测结果来自本机 CPU。更换 BLAS 实现、线程数或 NumPy 版本时，耗时和末位舍入可能变化；应重新运行 `full_demo.py` 并记录新摘要。
