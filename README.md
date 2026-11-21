# BlockLab · 激活与转置重排分块算子工作台

BlockLab 是一个从零构建、完全本地运行的确定性分块算子工作台。它读取固定随机种子生成的 `.npz` 张量分片和 YAML 配置，执行 **激活计算 → 转置重排 → 稳定归约** 三阶段流水线，并提供块级原子检查点、优雅停止/恢复、内容指纹驱动的增量失效、真实文件锁并发发布保护，以及本地 HTTP 交互界面与 CLI。

工作区不依赖 GPU、Docker、外部服务或真实数据集；运行时读取与写入均限制在当前项目根目录内。

## 快速开始

环境要求：Python 3.10+、NumPy、PyYAML。

```bash
python3 --version
python3 -m pip install -r requirements.txt

# 固定种子可重建样本（重复执行得到相同 NPZ 字节）
python3 samples/generator.py --out samples

# 导入一个分片
python3 -m blocklab --workdir work import \
  --shard samples/tensor_4d_gelu_swap.npz \
  --config configs/gelu_swap.yaml \
  --job gelu-swap

# 运行全部阶段；中断后再次执行同一命令即可恢复
python3 -m blocklab --workdir work run --job gelu-swap

# 查询状态 / 指纹 / 诊断 / 失效事件
python3 -m blocklab --workdir work status --job gelu-swap
python3 -m blocklab --workdir work fingerprints --job gelu-swap
python3 -m blocklab --workdir work diagnostics --job gelu-swap
python3 -m blocklab --workdir work invalidations --job gelu-swap

# 导出确定性结果摘要
python3 -m blocklab --workdir work export --job gelu-swap --out work/exports/gelu-summary.json
```

启动本地交互界面：

```bash
python3 -m blocklab --workdir work serve --host 127.0.0.1 --port 8765
# 浏览器打开 http://127.0.0.1:8765/
```

界面支持导入样例、运行全部或单阶段、请求停止、恢复、查看流水线进度、块检查点、块级数值诊断、版本指纹、失效传播时间线和结果摘要导出。服务只允许绑定 loopback 地址。

## 样本

`samples/generator.py` 使用 `seed=20260217`，并以固定 ZIP 时间戳写出确定性 NPZ。形状清单为 `samples/shape_manifest.json`。

| 分片 | 源形状 | 目标形状 | 布局/配置 | 覆盖点 |
|---|---:|---:|---|---|
| `tensor_4d_gelu_swap.npz` | `[2,3,7,5]` | `[2,7,3,5]` | 4-D / `gelu_swap` | 头序列互换、奇数头维、尾块、`±1e4` |
| `matrix_silu_tile.npz` | `[9,11]` | `[9,11]` | 2-D / `silu_tile` | 奇数列、`4×5` 非整除瓦片、`±1e4` |
| `tensor_4d_sigmoid_tile.npz` | `[2,3,6,7]` | `[2,3,6,7]` | 4-D / `sigmoid_tile` | 瓦片重排、奇数头维、极端值 |
| `tensor_4d_gelu_diag.npz` | `[2,2,5,3]` | `[2,5,2,3]` | 4-D / `gelu_swap_diag` | NaN、±Inf、全零行、极端值 |

配置样例位于 `configs/`：三种激活、两种重排模式、不同块大小，以及用于演示失效传播的 `mean` 和 swap 变体。YAML 支持常用字段别名和大小写归一化。

## 算子语义

### 分块激活

- 4-D 张量按 `[B,H,S,D]` 解释并展平为 `(B*H*S, D)` 矩阵；2-D 直接作为矩阵。
- 由 `row_block × col_block` 生成块序列，最后一行/列自动裁剪，支持奇数列与非整除。
- GELU 使用 Abramowitz & Stegun 7.1.26 误差函数近似；SiLU/Sigmoid 使用免溢出的稳定形式。
- 每块先选取有限元素的块内参考极值并减去，再恢复浮点参数求值。
- 有限输出必须落在合法值域；NaN/Inf/全零行在对应块产生 `E_NUMERIC` 诊断。NaN/Inf 会保留在产物中，绝不静默零替换。

### 重排

- `swap_head_seq`：4-D 输出 `[B,S,H,D]`；2-D 输出矩阵转置形状。
- `tile`：按块大小切瓦片。完整内部瓦片由源行优先顺序映射到目标列优先顺序；最后一行/列尾块保持原位，确保非整除形状仍可无损、无重叠覆盖。
- 每个重排块记录 `source_region`、`target_region` 与源/目标瓦片坐标。
- 实际目标形状必须同时匹配形状清单和 YAML 的可选 `target_shape`；否则返回 `E_SHAPE_MISMATCH`，并附 `declared_shape` 与 `actual_shape`。

### 稳定归约

- 支持 `sum`（`math.fsum`）、`mean`、`l2`（迭代 `math.hypot`）。
- 不直接连乘大值；NaN/Inf 不计入有限聚合，但保留在重排张量与诊断中。
- 每块输出 `blocks/<sequence>-<key>.json` 块摘要，最终 `summary.json` 给出全量聚合与固定 SHA-256。

## 检查点、恢复与并发

每个作业目录：

```text
work/jobs/<job_id>/
  job.json
  state.json
  invalidations.jsonl
  locks/{meta,run,activation,reorder,reduce}.lock
  checkpoints/<stage>/e000001/
    seq-00000001-<block-key>.json
    blocks/00000001-<block-key>.npy|json
  archive/
  quarantine/
  outputs/{activation.npy,reordered.npy,summary.json,*.meta.json}
```

检查点记录包含分片/作业标识、阶段、块坐标、行列区间、输入内容指纹、阶段计划指纹、配置版本、产物 SHA-256、状态和单调序号。产物先原子写，记录后原子写；两者都经过同目录临时文件、文件 `fsync`、`os.replace` 和目录 `fsync`。中断最多留下可忽略临时文件或未引用产物，不会产生半条记录。

恢复只接受从序号 1 开始连续、字段完整、指纹匹配、产物摘要匹配的前缀。损坏、截断、重复序号及其后的记录被忽略；下次发布前隔离到 `quarantine/`。

阶段发布在 `fcntl.flock` 文件锁内重新扫描有效前缀，要求请求序号恰为“有效块数 + 1”，且块键等于预定位置。落后、重复、跳号或跨块错位返回 `E_STALE_CHECKPOINT`。

停止请求通过作业目录 `STOP` 文件或 CLI `SIGINT/SIGTERM` 标志传递。执行器在完成当前块的全部原子写入后退出；恢复后不复算已完成块。阶段中途或阶段之间停止，最终激活张量、重排张量和摘要均与从零完整运行逐字节一致。

## 增量失效

版本指纹链：

```text
activation_plan = H(input_digest, activation, block sizes, config)
reorder_plan    = H(activation_plan, reorder mode, target shape, block sizes, config)
reduce_plan     = H(reorder_plan, sum strategy, tolerances, config)
stage_content   = H(stage plan, ordered block product digests/status)
version         = H(input, config, all plans, all stage contents)
```

| 变化 | 最早失效阶段 | 下游失效 | 保留复用 |
|---|---|---|---|
| 输入内容、激活类型、块大小 | `activation` | `reorder`, `reduce` | 无 |
| 重排模式、目标形状 | `reorder` | `reduce` | `activation` |
| 求和策略、数值容差 | `reduce` | — | `activation`, `reorder` |

`GET /api/jobs/<id>/invalidations` 和 CLI `invalidations` 可查看原因、旧/新指纹、受影响阶段、块数量和区间。失效阶段目录归档后以新纪元重建；未受影响阶段沿用原检查点与内容摘要。

## CLI

```text
python -m blocklab [--root ROOT] [--workdir WORKDIR] [--json] <command>

init
samples [--out samples]
import --shard PATH --config PATH [--job ID] [--manifest PATH] [--shard-id ID]
run --job ID [--stage activation|reorder|reduce|all] [--stop-after-stage S]
    [--stop-after-blocks N] [--block-delay-ms MS] [--resume]
status --job ID
fingerprints --job ID
diagnostics --job ID [--stage S]
blocks --job ID [--stage S]
invalidations --job ID
stop --job ID
resume --job ID [--stage ...] [--block-delay-ms MS]
export --job ID [--out PATH]
serve [--host 127.0.0.1] [--port 8765]
demo [--workdir demo_work] [--report artifacts/demo_report.json]
```

全局选项可放在子命令前或后。失败时返回码为 2，并输出业务码与详情。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/api/health` | 服务健康与版本 |
| `GET` | `/api/samples` | 本地样例、配置列表 |
| `GET` | `/api/jobs` | 作业概览 |
| `POST` | `/api/import` | `{sample_id}` 或 `{shard_path,config_path,job_id,manifest_path}` |
| `POST` | `/api/jobs/<id>/run` | `{stage,background,stop_after_stage,stop_after_blocks,block_delay_ms}` |
| `POST` | `/api/jobs/<id>/stop` | 请求优雅停止 |
| `POST` | `/api/jobs/<id>/resume` | 清除停止并恢复 |
| `GET` | `/api/jobs/<id>/status` | 阶段状态、连续块数、下一序号、运行状态 |
| `GET` | `/api/jobs/<id>/fingerprints` | 输入/配置/阶段计划/阶段内容/版本指纹 |
| `GET` | `/api/jobs/<id>/blocks?stage=` | 块记录、坐标、摘要、状态与元数据 |
| `GET` | `/api/jobs/<id>/diagnostics?stage=` | `E_NUMERIC` 诊断上下文 |
| `GET` | `/api/jobs/<id>/invalidations` | 失效事件 |
| `GET` | `/api/jobs/<id>/export[?download=1]` | 保存并返回结果摘要 |

## 业务错误码

| 码 | 场景 |
|---|---|
| `E_NUMERIC` | 块内出现 NaN/Inf/全零行，附分片、块坐标、行列区间、异常值和计数 |
| `E_SHAPE_MISMATCH` | 重排实际形状与形状清单/YAML 声明不一致，附声明与实测形状 |
| `E_INVALID_STAGE` | 未知阶段、跳过前置阶段、前置块未完成，附请求阶段与可用序号 |
| `E_STALE_CHECKPOINT` | 并发发布落后、重复、跳号或跨块错位 |
| `E_BUSY` | 同一作业已有执行器持锁运行 |
| `E_INPUT` / `E_CONFIG` / `E_PATH` / `E_NOT_FOUND` | 输入、配置、路径或资源错误 |

## 可重复演示与验证

```bash
# 完整演示：4 个分片、两种重排、异常链、阶段停止恢复、三类失效范围和四类业务码
python3 scripts/demo.py

# 全套测试 + 演示，并写入 artifacts/verification.json
python3 scripts/verify.py

# 或仅运行测试
python3 -m unittest discover -s tests -v
```

本次实际验证结果（详见 `artifacts/verification.json` 与 `artifacts/demo_report.json`）：

- `python3 -m unittest discover -s tests -v`：**12 项测试全部通过**。
- 固定演示摘要：`99b040858334cf133669d6f1d087d3bc150c1c418e6956694ab8825cc4810d8e`。
- 完整运行摘要 SHA-256：
  - `gelu-swap`: `49c16a5f557c4927d8e7431bd71606979bdc894cfa36305ab3be0beb73fcaff9`
  - `silu-tile`: `9ffb8753bc22cbc087aad1e44842aaeb52aac49959cd2c3bba414261aa207835`
  - `sigmoid-tile`: `b94cfe3249a113211b340e1cf362886c9493e5bd2454bf603cb7e076ca9cb965`
  - `gelu-diag`: `84041db552a85b56febe53434334135b4f24b203d850ae3d365b720f96f01153`
- 阶段间停止后恢复：`activation.npy`、`reordered.npy`、`summary.json` 以及全部块检查点记录与完整运行**逐字节一致**；三阶段检查点序号均连续，激活阶段 28 个块全部复用、未复算。
- 失效范围：`sum→mean` 最早 `reduce`；`tile→swap` 最早 `reorder`；输入内容变化最早 `activation`。
- 业务码实测：`E_NUMERIC`、`E_SHAPE_MISMATCH`、`E_INVALID_STAGE`、`E_STALE_CHECKPOINT`。
- 异常分片产生 13 个数值诊断块，NaN/±Inf 保留在输出中并可由诊断接口查询。

## 项目结构

```text
blocklab/
  activation.py      统一激活内核
  reorder.py         两种重排与瓦片映射
  reduction.py       稳定归约
  checkpoints.py     原子检查点、恢复、flock 发布
  workbench.py       作业状态、指纹、失效传播、查询
  engine.py          三阶段执行、停止/恢复、产物重组
  http_server.py     本地 HTTP API 与静态应用
  static/            离线单页交互界面
  demo.py            确定性演示
samples/             生成器、4 个 NPZ、形状清单
configs/             YAML 算子配置
tests/               12 项验收测试
docs/DESIGN.md       算法与数据模型技术说明
scripts/             demo.py / verify.py 启动脚本
artifacts/           实际演示与验证报告
```

## 假设与边界

- 张量统一转为 C 连续 `float64`；输入限制为 2-D 或 4-D。
- `tile` 模式以完整内部瓦片的行优先→列优先双射重排，尾瓦片保持原位；这是保持非整除形状无损且可验证的具体定义。
- 停止请求不抢占正在执行的块；强制 `SIGKILL` 不承诺生成退出报告，但检查点原子性仍保证恢复前缀一致。
- 检查点序号按阶段、按纪元连续；每次失效归档旧阶段目录并开启新纪元，报告同时暴露纪元与序号。
- 跨机器浮点库差异不属于逐字节保证范围；同一本地 Python/NumPy 环境、同一输入与配置满足逐字节确定性。

