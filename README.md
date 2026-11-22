# BlockGEMM：可检查点、可恢复的 GEMM/BMM 本地算子库

这是一个从零实现的 CPU 端 GEMM/BMM 算子库，使用 Python 3.10+ 与 NumPy，不依赖 GPU、Docker 或外部服务。核心特性：

- 三层分块：寄存器块 `MR×NR`、缓存块 `MC×KC` 与 `KC×NC`，沿 K 方向累加。
- 统一 GEMM/BMM 路径：支持非方阵、A/B 转置、批量维度、显式越界尾块裁剪与尾块标记。
- 基于 L1/L2 预算的确定性分块方案选择，记录候选、打分和选择理由。
- 每个 `MC×NC` 输出块作为不可拆分状态单元，原子发布分片记录与块产物。
- 外部停止请求只在下一次块边界生效；恢复后的输出、块摘要和汇总报告与完整运行逐字节一致。
- 输入内容局部变化时，通过分阶段块摘要定位最早受影响块，失效该块及其全部下游块；更早未受影响块继续复用。
- 文件锁、连续序号比较与稳定业务错误码；拒绝落后、重复和跨方案发布。
- 抽样块对 `longdouble` 二次幂分解参考实现做数值自校验；NaN/Inf 按 IEEE 语义传播并标记受影响块。

## 快速开始

要求：Python 3.10 及以上、NumPy。项目不访问网络，验证可完全在本机离线执行。

### Windows

```bat
py -3.10 -m blockgemm generate --out samples --force
py -3.10 -m blockgemm demo --manifest samples\manifest.json --out runs\demo
rem 可选离线安装（已准备 pip/setuptools 时）：
py -3.10 -m pip install --no-build-isolation --no-deps -e .
```

也可直接执行 `run_demo.bat`。文件锁在 Windows 上使用 `msvcrt.locking`，原子替换使用 `os.replace`。

### Linux / WSL / macOS

```bash
python3 -m blockgemm generate --out samples --force
python3 -m blockgemm demo --manifest samples/manifest.json --out runs/demo
# 或
./run_demo.sh
# 可选离线安装：
python3 -m pip install --no-build-isolation --no-deps -e .
```

运行测试：

```bash
python3 -m pytest -q
```

## 目录结构

```text
blockgemm/
  kernel.py       # 三层分块内核、形状/转置处理、块布局、尾块与 NaN/Inf 标记
  tiling.py       # L1/L2 预算、候选方案、确定性评分和方案指纹
  checkpoint.py   # 文件锁、原子写、检查点扫描、发布、恢复、查询
  engine.py       # 版本分析、失效传播、停止/恢复、装配与汇总报告
  reference.py    # longdouble 二次幂分解参考实现与抽样误差检查
  manifest.py     # 固定种子 .npy 样本生成与形状清单加载
  cli.py          # generate/plan/run/resume/status/report/diagnostics/stop/demo
  demo.py         # 全样本、停止恢复、失效重建的端到端演示
samples/
  manifest.json   # 完整样本清单、形状、摘要、异常位置
  shapes.json     # 独立形状清单
  matrices/*.npy  # 固定随机种子生成的三档、批量、转置与异常矩阵
tests/            # 26 项自动化测试
runs/             # 本地工作区、最终产物和验收摘要（运行后生成）
```

工作区约定：

```text
<workspace>/
  meta/lock                     # 跨进程文件锁
  meta/run-spec.json            # 当前运行规格、版本指纹和方案选择
  meta/invalidation.json        # 最近一次失效原因与受影响块区间
  meta/STOP.json                # 停止请求标记
  checkpoints/0000000000.json   # 原子发布的块记录
  blocks/0000000000.npy         # 原子发布的块产物
  output/result.npy             # 仅在完整连续前缀完成后生成
  output/result.json            # 输出摘要
  reports/summary.json          # 确定性最终汇总报告
```

## 本地样本

生成器使用 `numpy.random.default_rng(seed)` 和固定种子，值与 `.npy` 文件均确定性可复现。

| 样本 | 类型 | M×N×K | 批量 | 转置标志 | 覆盖点 |
|---|---|---:|---:|---|---|
| `small_gemm` | GEMM | 64×64×64 | 1 | 无 | 小规模 |
| `medium_gemm` | GEMM | 512×256×384 | 1 | 无 | 中规模、非方阵 |
| `large_gemm` | GEMM | 1024×1024×1024 | 1 | 无 | 大规模 |
| `transpose_gemm` | GEMM | 96×72×48 | 1 | A、B 均转置 | 双向转置、非方阵、尾块 |
| `bmm_batch3` | BMM | 96×64×80 | 3 | 无 | 批量维度 > 1、尾块 |
| `bmm_transpose_batch2` | BMM | 72×56×48 | 2 | A、B 均转置 | 批量 + 双转置 |
| `bmm_a_transpose_batch2` | BMM | 80×48×64 | 2 | A 转置 | 单侧 A 转置 |
| `bmm_b_transpose_batch2` | BMM | 64×72×40 | 2 | B 转置 | 单侧 B 转置 |
| `nan_gemm` | GEMM | 64×64×64 | 1 | 无 | A/B 含 NaN |
| `inf_bmm_batch2` | BMM | 48×40×32 | 2 | 无 | 仅 batch 1 含 ±Inf |

`samples/manifest.json` 中保存存储形状、有效形状、输出形状、转置标志、输入摘要、异常位置及形状指纹；`samples/shapes.json` 是可单独读取的形状清单。CLI 加载样本时会校验存储形状、有效形状、dtype 与转置标志，不一致返回 `E_INVALID_SHAPE`。

## 库接口

### 直接计算

```python
import numpy as np
from blockgemm import gemm, bmm

A = np.random.default_rng(7).standard_normal((37, 23))
B = np.random.default_rng(8).standard_normal((23, 29))
C = gemm(A, B)                         # 临时工作区，返回 ndarray

Ab = np.random.default_rng(9).standard_normal((3, 35, 23))
Bb = np.random.default_rng(10).standard_normal((3, 23, 19))
Cb = bmm(Ab, Bb)                       # 批量矩阵乘
```

转置标志表示“存储形式已经转置”：`a_transpose=True` 时传入形状应为 `(K, M)`（BMM 为 `(batch, K, M)`），`b_transpose=True` 时传入形状应为 `(N, K)`（BMM 为 `(batch, N, K)`）。

### 可恢复作业

```python
from blockgemm import run_gemm_job, query_status, query_report
from blockgemm import query_diagnostics, request_stop

result = run_gemm_job(
    "runs/my_job", A, B,
    l1_cache_kib=64,
    l2_cache_kib=1024,
    verify_blocks=3,
    accumulation_strategy="k-ascending",   # 或 "k-descending"
)
assert result.completed
print(result.output_digest)
print(result.summary["plan_fingerprint"])

status = query_status("runs/my_job")
report = query_report("runs/my_job")
errors = query_diagnostics("runs/my_job")

# 从另一个进程/线程请求优雅停止；执行器完成当前块后退出。
request_stop("runs/my_job", "scheduler_preemption")
```

`accumulation_strategy` 的两种取值会改变 K 方向缓存块的累加顺序，因此策略本身进入静态版本指纹；切换策略会使全部块失效。同一输入、方案、策略和预算的块结果逐字节一致。

### 分块方案与版本查询

```python
from blockgemm import query_plan, query_invalidation

plan = query_plan(m=512, n=256, k=384, batch=1, l1_cache_kib=64, l2_cache_kib=1024)
print(plan["selected"]["plan"])             # MR/NR/MC/KC/NC
print(plan["selected"]["details"])          # 访存量、缓存压力、并行块数、打分明细
print(plan["plan_fingerprint"])

print(query_invalidation("runs/my_job"))    # 最近失效原因、起止序号、被移除记录
```

## 命令行

```bash
python -m blockgemm generate --out samples --force
python -m blockgemm plan --manifest samples/manifest.json --case medium_gemm
python -m blockgemm run --manifest samples/manifest.json --case medium_gemm --workspace runs/medium
python -m blockgemm stop --workspace runs/medium --reason scheduler_preemption
python -m blockgemm resume --manifest samples/manifest.json --case medium_gemm --workspace runs/medium
python -m blockgemm status --workspace runs/medium
python -m blockgemm report --workspace runs/medium
python -m blockgemm diagnostics --workspace runs/medium
python -m blockgemm inspect --manifest samples/manifest.json --case medium_gemm --workspace runs/medium
python -m blockgemm demo --manifest samples/manifest.json --out runs/demo
```

`run` 与 `resume` 对同一工作区都是幂等的：已通过校验的前缀不会被重新计算；内容或静态配置变化时自动执行失效分析。`--stop-after-blocks N` 用于确定性地演示“发布 N 块后收到停止请求”，实际进程也可通过 `stop` 子命令、`meta/STOP.json`、SIGINT 或 SIGTERM 触发边界停止。

## 内核设计

设有效运算为 `C = A_eff @ B_eff`。A/B 先按转置标志形成有效视图；BMM 在批维度最外层展开。输出按批、行块、列块排序，得到全局连续块序号。

单个输出块执行：

1. 从输出块对应的 `MC×NC` 区域建立 `float64` 输出缓存累加器。
2. 按 `KC` 切分 K 方向；每次读取 `MC×KC` A 面板与 `KC×NC` B 面板。
3. 在缓存面板内按 `MR×NR` 寄存器块计算局部 K 切片点积并累加到输出缓存。
4. K 全部完成后才把输出块转换为输入 dtype，计算块产物摘要并发布。

部分寄存器或输出缓存中间值不会写入检查点。对越界块，`row_end=min(M, ...)`、`col_end=min(N, ...)` 显式裁剪，记录 `tail_flags={"m":...,"n":...,"k":...}`。BMM 每个批次的块带独立 `shard_id=batch-XXXX`；所有批次共享单调全局序号。

默认将 BLAS 线程数设为 1（仅在调用方未显式设置时），避免多线程归约顺序引入跨运行差异。

## 分块方案选择

候选方案枚举 `MR/NR/MC/KC/NC`，先过滤工作集超预算者：

- L1 估计：`MR*KC + KC*NR + MR*NR` 个元素。
- L2 估计：`MC*KC + KC*NC + MC*NC` 个元素。

可行候选的确定性分数包含：

- 预测 DRAM 访存量相对理想下界的归一化值；
- L1/L2 压力；
- 输出块容量浪费；
- 并行块数相对目标 16 的短缺惩罚；
- 估计微内核调用开销。

分数保留 15 位后按方案参数稳定排序，保证相同形状、dtype、批量和预算选择同一方案。分块方案指纹只含 `MR/NR/MC/KC/NC`；预算、累加策略、校验配置、形状/转置指纹进入静态版本指纹。

## 检查点与恢复协议

每个块的发布顺序如下，全部在同一个工作区文件锁内完成：

1. 校验待发布序号必须等于当前连续前缀长度；拒绝重复、落后、空洞和跨方案记录。
2. 将块产物写入同目录临时文件，`flush` + `fsync`，再 `os.replace` 到 `blocks/NNNNNNNNNN.npy`。
3. 生成包含记录自校验摘要的 JSON 记录。
4. 将记录写入临时文件，`fsync` 后原子替换为 `checkpoints/NNNNNNNNNN.json`。
5. 记录文件是提交标记；没有有效记录的孤立块产物永远不会被恢复逻辑使用。

记录包含：分片标识、批索引、块坐标、行列区间、输入形状与形状指纹、方案与方案指纹、累加策略、缓存预算、静态/全局版本指纹、A/B/块输入摘要、块指纹、产物数组摘要、产物文件摘要、尾块标记、NaN/Inf 计数、抽样误差报告、单调序号和记录校验和。

恢复扫描只接受从 0 开始连续、JSON 完整、校验和正确、块产物存在且摘要一致的前缀。损坏、半写、文件名与内部序号不符、重复序号及前缀之后的多余记录都不会被复用；从最早无效序号开始重建。

## 停止与恢复一致性

- 停止请求在块边界检查；正在计算的输出块会完成、校验并原子发布后再退出。
- 停止后不保证生成最终输出或汇总报告；不会持久化半完成的寄存器/缓存累加值。
- `resume` 清除上次停止标记，从连续有效前缀之后继续。
- 完整运行与“停止 N 块后恢复”的最终 `result.npy`、`summary.json`、检查点记录文件和块 `.npy` 文件在本机逐字节相同。

## 版本指纹与失效传播

失效判定分两层：

- **静态变化**：形状、转置标志、分块方案、累加策略、L1/L2 预算、校验配置或内核版本变化。无法兼容旧块布局，因此最早受影响序号为 0，全部块失效。
- **内容变化**：静态配置相同，则比较每块的 A 行阶段摘要、B 列阶段摘要和块输入摘要。第一个不一致块即最早受影响块；该块及其全部下游块失效。其之前的块只有在其实际使用的输入行/列未变化时才保留。

失效会删除从最早序号开始的记录和块产物，并移除旧最终输出/汇总，避免中断期间暴露旧版本结果；`meta/invalidation.json` 和查询接口保留原因、起止序号、最早受影响块坐标及 A/B 哪一侧变化。重建完成后，结果与在全新工作区对当前输入做全量构建逐字节一致。

## 并发与错误码

发布在 `fcntl.flock`（POSIX）或 `msvcrt.locking`（Windows）保护的临界区内比较连续序号。所有错误对象都含 `shard_id`、`block_coord`、`reason` 和 `details`。

| 错误码 | 触发条件 |
|---|---|
| `E_STALE_CHECKPOINT` | 发布序号落后/重复/不连续，或与前一记录/当前执行器方案不一致 |
| `E_INVALID_SHAPE` | 非法形状、形状清单与实际不符、缺少前置块/前置块产物、非法产物形状 |
| `E_NUMERIC_MISMATCH` | 抽样块与高精度参考的绝对/相对误差超容差 |
| `E_INVALID_CONFIG` | 无缓存可行方案、非法预算、非法累加策略等配置错误 |
| `E_WORKSPACE` | 工作区文件锁获取超时 |

同一工作区允许多个发布者竞争，但只有连续序号的下一位能够提交；其余的稳定返回 `E_STALE_CHECKPOINT`，不会覆盖已提交状态。

## 数值精度与 NaN/Inf

- 抽样序号由总块数确定性生成，默认首、中、尾三块；也可配置 `verify_blocks`。
- 参考实现使用 `longdouble`，先把 K 按固定叶大小分组，再将叶结果按相邻二次幂配对归约。
- 报告包含块序号、分片、行列区间、最大绝对偏差、最大相对偏差、容差和违规元素数。
- 输入含 NaN/Inf 时不做静默替换，直接按 IEEE 语义参与计算；参考检查跳过该块的全有限比较，但记录 A/B 非有限值计数并将块标记为受影响。

## 本地确定性演示与实测结果

本次验证环境：Python 3.14.4、NumPy 2.5.5、Linux 本机；代码兼容 Python 3.10+，Windows 路径使用 `msvcrt` 与 `.bat` 脚本，未依赖 POSIX 专用服务。实际 Windows 机器可离线运行相同命令；当前执行环境不是 Windows，因此 Windows 锁实现未在此机器上实机执行。

### 自动化测试

```text
$ python3 -m pytest -q
..........................                                               [100%]
26 passed
```

覆盖内容：GEMM/BMM 与 NumPy 对照、转置、非方阵、尾块、三档方案选择、停止恢复字节一致、损坏/重复检查点忽略、产物损坏检测、并发发布竞争、内容局部失效、静态配置失效、稳定错误码、NaN/Inf 传播和数值不匹配。

### 完整演示

```bash
python3 -m blockgemm generate --out samples --force
python3 -m blockgemm demo --manifest samples/manifest.json --out runs/demo
```

连续执行两次后，`runs/demo/demo-summary.json` 均为：

```text
SHA-256 b6d3a2e2da3e2317824f7fe9160d3dfd1566f98f947e317ae04525a72a0affe9
```

验收布尔项全部为 `true`：

```json
{
  "all_cases_match_numpy_reference": true,
  "stop_resume_output_byte_identical": true,
  "stop_resume_summary_byte_identical": true,
  "stop_resume_checkpoint_records_byte_identical": true,
  "stop_resume_block_artifacts_byte_identical": true,
  "invalidation_matches_fresh_build": true
}
```

| 样本 | 块数 | 选中方案 `MR/NR/MC/KC/NC` | 输出数组摘要 |
|---|---:|---|---|
| small_gemm | 1 | 4/4/64/32/64 | `8b8129ad0fd5ae107bdba72aa0188f6d6382a72a9f82e5689c36b88a74c68ffe` |
| medium_gemm | 2 | 8/8/256/32/256 | `e48d57d04b453a3c473d7188e273902cf73c3028c9c8c7203f78c34a19200be7` |
| large_gemm | 16 | 8/8/256/128/256 | `31639e3341b725b62687dd5d313b2afef6cea8ac0b3907dd8c90f006eac7ba1a` |
| transpose_gemm | 1 | 4/4/96/32/96 | `89935ac71759222bf8dd67513a1a1b4de8f96726c59e75e95f44b753e29575db` |
| bmm_batch3 | 3 | 8/8/96/32/64 | `e9aadcf0b669bc63507acb31ab1b81a6821a2360f14a2aa416f4e825d783295b` |
| bmm_transpose_batch2 | 2 | 8/4/96/32/64 | `5f64075126af2f76f847fc0277b24e90d5c6197b76719388ef3974e9ec093d22` |
| bmm_a_transpose_batch2 | 2 | 8/4/96/32/64 | `f0fa5083400fc42cdd8ef5b6e3b408439189541523cc65a7fecfa56d54ed26f7` |
| bmm_b_transpose_batch2 | 2 | 8/4/64/32/96 | `bbebff4fbeb21aa1f0c101b5b054422978301c665b5a08f0ae91f718bcaa8e02` |
| nan_gemm | 1 | 4/4/64/32/64 | `1d215eee8c5e53dd41420795301663ba2c47058923e3090aa8f8686e2d9f9824` |
| inf_bmm_batch2 | 2 | 4/4/64/32/64 | `2827eb45284a5682392688b86337486c7c05673e0b0dba9a9e92eb9372f1a793` |

### 停止恢复实测

`medium_gemm` 在第 1 个块发布后收到确定性停止请求，恢复时复用 1 块、计算 1 块：

```text
baseline output SHA-256 = 6b2909b8fd6f2abcf0ce0097d4d020dad6b03787089fd6f51904e75ac52601de
resumed  output SHA-256 = 6b2909b8fd6f2abcf0ce0097d4d020dad6b03787089fd6f51904e75ac52601de
baseline summary SHA-256 = 9c4cc44529a6f7ebdb173fe83bf2864617d58626bc137b445f565e11fa42b653
resumed  summary SHA-256 = 9c4cc44529a6f7ebdb173fe83bf2864617d58626bc137b445f565e11fa42b653
```

检查点 JSON 文件与块 `.npy` 文件也逐字节一致。

### 失效传播实测

`small_gemm` 在 32 KiB L2 预算下被切成 4 块。修改第二列块使用的一个 B 元素后：

- 最早受影响序号：`1`（`block_coord=[0,1]`）；
- 原因：`input_content_changed`，`b_stage_changed=true`；
- 保留并复用块 0，失效块 1–3；
- 重建后的输出与汇总报告同“干净工作区对修改后输入全量构建”逐字节一致。

完整机器可读结果位于 `runs/demo/demo-summary.json`；各样本最终矩阵位于 `runs/demo/cases/<case>/output/result.npy`。

## 设计边界

- 当前 BMM 输入为 `A(batch,M,K)` 与 `B(batch,K,N)`（按转置标志调整存储形状），不支持广播批次。
- 支持 `float32` 与 `float64`；内部累加统一使用 `float64`。
- 执行器按全局连续序号发布；并发发布受锁和序号比较保护，竞争失败者会收到 `E_STALE_CHECKPOINT`，调用方可重新读取前缀后继续。未实现跨进程分块任务调度器。
- 汇总报告刻意不包含时间戳、路径和执行耗时，以保证完整运行与恢复运行的报告字节一致。

