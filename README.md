# 本地量化误差传播审计与校准回滚引擎

这是一个完全本地、确定性的模型量化审计工具。它不加载真实模型、不使用 GPU、不访问网络，只依赖 NumPy 与 PyYAML。输入是固定随机种子生成的线性层权重 `.npz`、校准激活 `.npy` 与 YAML 量化配置；输出包含逐层量化误差、误差上界、传播归因、预算判定、可回滚的版本历史与故障业务码。

## 已实现能力

- 对称/非对称量化、独立权重/偏置缩放与零点、反量化、量化域越界钳制及审计。
- NaN/Inf、零缩放因子的 `E_QUANT_INVALID` 失败路径，错误详情携带层名。
- 全精度参考前向、量化前向、逐层局部误差与传播误差，以及显式绝对误差上界。
- 一阶敏感度与逐层扰动重放双路归因，排序差异标记待复核，闭合容差检查。
- 逐层/总预算占用、余量、越界清单、上游误差占比与建议动作。
- 内容指纹驱动的增量重算；未变化前缀复用，变化层及下游重算，并与全量影子运行逐字节核验。
- 原子审计记录、哈希链、事务恢复、跨平台文件锁、外部版本锚点、自动/手动回滚。
- 版本回退与内容篡改区分，并发发布只允许连续版本，拒绝重复/落后发布。
- 确定性 6 层演示，固定摘要，并注入全部关键业务码与版本整体替换场景。

## Windows 快速开始

要求 Python 3.11 或更高版本。以下命令在仓库根目录执行。

```bat
py -3.11 -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -e ".[test]"
.venv\Scripts\python.exe -m pytest -q
.venv\Scripts\python.exe -m quant_audit --pretty demo --workspace demo_run --data-dir demo_data --report artifacts\demo_report.json --force --faults
```

也可直接运行：

```bat
scripts\setup_windows.bat
scripts\run_demo_windows.bat
```

PowerShell 版本：`scripts\setup_windows.ps1`、`scripts\run_demo_windows.ps1`。

`demo` 会重新生成固定随机种子的 6 层权重与干净/离群校准集，执行一次全量校准、一次从第 3 层开始的增量校准、一次低精度离群样本导致的预算越界回滚，并在隔离副本中演示历史整体替换、HEAD 回退、摘要篡改、参数指纹篡改、缺失阶段文件、NaN、零缩放、归因缺口、增量缓存不一致、过期发布与非法回滚。

## CLI

```text
python -m quant_audit init|import --workspace WS --weights W.npz --calibration C.npy --config Q.yaml [--force]
python -m quant_audit calibrate --workspace WS --mode full|incremental [--base-version N] [--config Q.yaml] [--calibration C.npy]
python -m quant_audit query --workspace WS [--version N] [--layers-only]
python -m quant_audit history --workspace WS
python -m quant_audit rollback --workspace WS [--to-version N]
python -m quant_audit demo --workspace demo_run --data-dir demo_data --report artifacts/demo_report.json --force --faults
python -m quant_audit generate-demo-data --data-dir demo_data --seed 20261120 --force
```

`--pretty` 可放在命令前后。成功输出为 JSON 且 `"ok": true`；业务失败退出码为 `2`，输出 `"ok": false` 与错误码。

## 输入格式

权重 `.npz` 至少包含：

```text
layer_names          # 层序清单，例如 ["linear_00", ..., "linear_05"]
linear_00.weight     # shape (out, in)
linear_00.bias       # shape (out,)
...
```

也支持 `<layer>/weight`、`weight_0`、`W0` 等本地别名。校准 `.npy` 为二维数值数组，shape 为 `(samples, first_layer_input)`；超过 2 维或空数组会被拒绝。

配置示例：

```yaml
schema_version: 1
calibration:
  ratio: 1.0
quantization:
  bits: 8
  mode: symmetric          # symmetric | asymmetric
  zero_point: exact        # exact | zero | clamp
  layers:
    linear_02: {bits: 6, mode: asymmetric, zero_point: exact}
budget:
  metric: relative_l2       # relative_l2 | relative_max | absolute_max
  per_layer:
    linear_00: 0.06
    linear_01: 0.06
    linear_02: 0.06
    linear_03: 0.06
    linear_04: 0.06
    linear_05: 0.06
  total: 0.05
attribution:
  closure_tolerance: 0.95
  upstream_share_threshold: 0.5
runtime:
  verify_incremental: true  # 兼容字段；影子全量核验始终强制执行
```

`calibration.ratio` 从样本头开始确定性截取。`zero_point: exact` 计算零点并钳制到量化范围；`zero_point: zero` 强制零点为 0；`clamp` 是 `exact` 的兼容别名。对称模式恒为零点 0。预算必须为正数。

## 算法要点

- 对称：`scale = max(abs(x)) / (2^(b-1)-1)`，`zp = 0`。
- 非对称：`scale = (max-min)/(2^b-1)`，`zp = clamp(round(-min/scale))`。
- 量化/反量化：`q = clip(round(x/scale)+zp, qmin, qmax)`，`x_hat = (q-zp)*scale`。
- 逐层局部噪声：`e_l = f_ref_input(Wq_l,bq_l) - f_ref_input(W_l,b_l)`。
- 一阶归因：将 `e_l` 经由下游全精度权重链传播。
- 重放归因：对每个前缀长度，构造“前 `k` 层量化、其余全精度”的输出，相邻前缀差即该层贡献，天然望远镜求和到端到端偏差。
- 闭合：两种归因分别检查 `sum(delta_l)` 与端到端偏差；超容差返回 `E_ATTRIBUTION_GAP`。公开贡献按重放分数归一化，和与端到端 L2 偏差闭合。
- 预算：占用率 `error / budget`；总预算使用端到端同一误差指标。若某层越界且上游贡献占比达到阈值，诊断标记 `cannot_fix_by_raising_bits_alone`。
- 增量：指纹由层参数摘要、位宽、模式、零点策略、校准选择内容摘要和层序组成；最早变化层及下游重算，前缀张量从版本缓存复用，并强制以内存全量影子运行验证，配置或 CLI 不能关闭该安全核验。

更完整的公式、状态机与持久化说明见 `docs/ARCHITECTURE.md`。

## 审计与版本

每个校准版本包含版本号、前序哈希、输入/层参数/预算配置指纹、完整结果、结果摘要、阶段张量归档摘要与哈希链记录。发布使用工作区文件锁，只接受 `HEAD.version + 1`，落后或重复发布返回 `E_STALE_VERSION`。

记录、阶段 NPZ、HEAD 与事务标记均通过“同目录临时文件 + `fsync` + `os.replace`”写入。若在记录与 HEAD 之间中断，下次查询依据事务标记完成提交；若在记录写入前中断，则丢弃事务且不出现半写记录。工作区根目录的外部锚点用于识别整个审计目录被旧版本替换。

正常版本推进被接受；审计目录版本回退返回 `E_VERSION_ROLLBACK`，结果摘要、记录哈希或参数指纹不一致返回 `E_AUDIT_TAMPERED`。预算越界版本仍保留在历史中供审计，但活动指针自动回退到最近一个全部层合规的版本。

## 业务码

| 业务码 | 含义 |
|---|---|
| `E_QUANT_INVALID` | 零/非有限缩放因子、NaN/Inf 激活或非法量化输入 |
| `E_ATTRIBUTION_GAP` | 一阶或重放归因未在容差内闭合 |
| `E_INCREMENTAL_MISMATCH` | 增量结果与全量影子运行不一致 |
| `E_STALE_VERSION` | 发布基线过期或目标版本重复 |
| `E_INVALID_STAGE` | 缺失前置阶段结果、阶段归档或目标版本不存在 |
| `E_VERSION_ROLLBACK` | 审计版本号回退或整体历史被旧副本替换 |
| `E_AUDIT_TAMPERED` | 记录哈希、结果摘要、参数指纹或锚点不一致 |
| `E_INVALID_ROLLBACK` | 请求回滚到不存在的或预算不合规的版本 |
| `E_NO_VALID_VERSION` | 当前没有可用的合规版本 |
| `E_INVALID_INPUT` / `E_INVALID_CONFIG` | 输入数据或 YAML 配置非法 |

## 实际验证结果

验证记录见 `docs/VERIFICATION.md` 与 `artifacts/demo_report.json`。本次实际运行结果：

- `pytest -q`：`25 passed`。
- 演示固定摘要：`08d8aae842ddc42b6e8607ed5738f7aded94fe2573ed0c24724bef948b2e829a`；重复完整运行摘要一致。
- v1 全量、v2 增量均预算合规；v2 复用 `linear_00..02`，仅重算 `linear_03..05`，并通过全量影子核验。
- v3 的 2-bit 离群校准端到端相对 L2 误差 `1.762345847827907`，全部 6 层越界；活动版本自动回滚到 v2，查询误差与预算状态和历史 v2 完全一致。
- 历史整体替换、HEAD 回退返回 `E_VERSION_ROLLBACK`；摘要/参数指纹篡改返回 `E_AUDIT_TAMPERED`；缺失阶段文件返回 `E_INVALID_STAGE`。
- 异常注入覆盖 `E_QUANT_INVALID`（NaN 与零缩放）、`E_ATTRIBUTION_GAP`、`E_INCREMENTAL_MISMATCH`、`E_STALE_VERSION`、`E_INVALID_ROLLBACK`。

## 目录

```text
quant_audit/       引擎、存储、CLI、演示
tests/             量化、归因、增量、存储/并发/中断、CLI、端到端测试
scripts/           Windows BAT/PowerShell 脚本
docs/              架构与验证记录
demo_data/         由固定种子生成的本地样本（可删除后重建）
demo_run/          演示工作区与审计历史（可删除后重建）
artifacts/         实际运行报告
```

## 环境说明

当前任务执行容器是 WSL2 Ubuntu，实际命令使用 Python 3.14.4、NumPy 2.5.5、PyYAML 6.0.3 运行。代码中的文件锁包含 Windows `msvcrt` 分支，且路径、原子替换与脚本均按 Windows 兼容方式编写；本次物理执行记录来自 WSL2 环境。

