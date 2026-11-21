# RoPE Diagnostics

本地、确定性、可交互的 RoPE 位置编码算子与诊断应用。应用使用固定随机种子生成的 `.npz` 分片，提供三阶段计算流水线、内容寻址缓存、原子检查点、增量重算、峰值内存规划和本地 HTTP/CLI 诊断接口。

本仓库不依赖 GPU、外部模型、数据库、容器或网络服务。HTTP 服务只绑定本机回环地址。

## 交付内容

```text
rope_diag/                  Python 包
  rope.py                   频率生成、分块旋转、旋转角度表
  attention.py              点积打分、因果掩码、稳定 softmax、数值错误定位
  engine.py                 三阶段流水线与从零一致性校验
  memory.py                 执行前峰值内存规划
  artifacts.py              不可变内容寻址产物
  checkpoints.py            文件锁、原子检查点、单调序号
  cache.py                  查询缓存键与原子缓存写入
  queries.py                角度/打分/权重查询与延迟预算
  session.py                会话、配置编辑、停止/恢复、导出
  api.py                    FastAPI 本地接口
  cli.py                    命令行
  static/index.html         浏览器诊断面板
configs/
  rope.yaml                 默认编码配置
  rope_ntk.yaml             NTK 分段缩放示例
  invalid_zero_block.yaml   错误注入配置
data/
  short.npz                 短序列 [1,2,7,8]
  long.npz                  长序列且批量大于 1 [2,3,96,16]
  odd.npz                   奇数头维度 [2,5,11,9]
  manifest.json             形状清单
scripts/
  run_demo.py               一键确定性验收演示
  http_smoke.py             启动真实回环服务并验证 HTTP
  start_server.py           浏览器应用启动器
tests/                      46 项自动化测试
artifacts/demo_summary.json 最近一次演示的可审计摘要
artifacts/http_smoke.json   最近一次 HTTP 验证报告
VALIDATION.md               实测结果与固定摘要
```

## Windows 快速开始

在 PowerShell 中执行：

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt

python -m rope_diag demo
python scripts\http_smoke.py
python -m rope_diag --workspace artifacts\demo_workspace serve --host 127.0.0.1 --port 8765
```

浏览器打开 <http://127.0.0.1:8765>。所有命令都从仓库根目录执行。

Linux/macOS 对应命令：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m rope_diag demo
python scripts/http_smoke.py
python -m rope_diag --workspace artifacts/demo_workspace serve --host 127.0.0.1 --port 8765
```

依赖：Python 3.10+、NumPy、FastAPI、Uvicorn、PyYAML。测试额外需要 `requirements-dev.txt` 中的 `pytest` 与 `httpx`。

## 一键演示

```bash
python -m rope_diag demo
```

演示会重置 `artifacts/demo_workspace` 与 `artifacts/demo_resume_workspace`，并完成：

1. 生成固定种子 `20261122` 的三个分片。
2. 执行完整三阶段计算，逐阶段保存原子检查点。
3. 从零重算并比较旋转角度表、旋转张量、注意力权重与统计摘要。
4. 查询角度、打分与权重；验证首次查询未命中、第二次命中且 payload 字节一致。
5. 注入 `E_LATENCY_BUDGET`、`E_NUMERIC`、`E_INVALID_CONFIG`、`E_INVALID_STAGE`、`E_STALE_CHECKPOINT`、`E_MEMORY_BUDGET`。
6. 将 `rotation_block_size` 从 2 改为 4；复用三份频率产物，仅重算旋转与打分，并与从零全量构建比对。
7. 在频率阶段后优雅停止，再从检查点恢复；最终摘要与从未停止的完整运行一致。
8. 导出 ZIP 并核验文件摘要。

结果写入 `artifacts/demo_summary.json`。`python scripts/run_demo.py` 是等价的脚本入口。

## 浏览器应用

```bash
python -m rope_diag --workspace artifacts/demo_workspace serve
# 或
python scripts/start_server.py --workspace artifacts/demo_workspace --port 8765
```

面板支持：导入本地 manifest/YAML、运行、停止、恢复、从零校验、配置编辑与增量重算、分片/阶段状态、角度/打分/权重查询、调度决策查看、错误注入和 ZIP 导出。页面不加载任何外部前端资源。

## 命令行

全局 `--workspace` 放在子命令之前。

```bash
python -m rope_diag --workspace my_ws init --manifest data/manifest.json --config configs/rope.yaml --reset
python -m rope_diag --workspace my_ws run
python -m rope_diag --workspace my_ws status
python -m rope_diag --workspace my_ws status --shard-id odd --stage block_rotation
python -m rope_diag --workspace my_ws query angles --shard-id odd --position 4 --channel 0,1,8
python -m rope_diag --workspace my_ws query score_summary --shard-id long --batch 0 --head 0 --query-position 10 --top-k 5
python -m rope_diag --workspace my_ws query weight_stats --shard-id odd --top-k 5
python -m rope_diag --workspace my_ws config set rotation_block_size=4
python -m rope_diag --workspace my_ws verify
python -m rope_diag --workspace my_ws stop
python -m rope_diag --workspace my_ws resume
python -m rope_diag --workspace my_ws export
python -m rope_diag generate-data --base .
```

失败时 CLI 输出 `{"ok": false, "error": {"code", "message", "details"}}` 到 `stderr`，返回码为 2。

## 本地 HTTP API

成功响应统一为 `{"ok": true, "data": ...}`；业务错误统一为 `{"ok": false, "error": {"code", "message", "details"}}`。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/` | 交互式诊断面板 |
| GET | `/api/health` | 服务与会话健康检查 |
| POST | `/api/session/init` | 导入本地 manifest/config；可 reset 工作区 |
| GET | `/api/status` | 会话、阶段、配置版本、复算报告 |
| GET | `/api/shards` | 分片形状与内容指纹 |
| GET | `/api/fingerprints` | 配置、分片、角度表、阶段产物指纹 |
| GET | `/api/checkpoints` | 当前配置版本的单调检查点记录 |
| GET | `/api/schedule` | 可观测内存调度决策 |
| POST | `/api/config` | 编辑配置并默认增量重算 |
| POST | `/api/config/validate` | 仅校验配置，不修改会话 |
| POST | `/api/run` | 执行流水线；可设置 `stop_after_stage` |
| POST | `/api/stop` | 请求优雅停止 |
| POST | `/api/resume` | 清除停止标志并从检查点恢复 |
| POST | `/api/verify` | 从零全量重算并逐字节比较 |
| POST | `/api/query` | `angles`、`score_summary`、`weight_stats` 查询 |
| POST | `/api/export` | 生成结果 ZIP |
| GET | `/api/export/download/{filename}` | 下载导出 ZIP |

示例：

```bash
python -m rope_diag --workspace artifacts/demo_workspace serve --port 8765

curl -s http://127.0.0.1:8765/api/status
curl -s -X POST http://127.0.0.1:8765/api/query \
  -H 'Content-Type: application/json' \
  -d '{"shard_id":"odd","query_type":"angles","options":{"position":4}}'
curl -s -X POST http://127.0.0.1:8765/api/config \
  -H 'Content-Type: application/json' \
  -d '{"patch":{"rotation_block_size":4},"run":true,"verify":true}'
```

## 计算语义

### 分块旋转

输入查询/键形状为 `[B, H, S, D]`。配置中的 `rotation_block_size` 是通道块大小，必须是正偶数。每个块内相邻通道组成旋转对：

```text
(x[2i], x[2i+1]) -> (x[2i]cosθ - x[2i+1]sinθ,
                     x[2i]sinθ + x[2i+1]cosθ)
```

频率按全局通道对索引计算；分块大小决定传播/调度粒度，不改变标准 RoPE 的数学结果。奇数 `D` 时最后一个通道不旋转，并写入频率产物元数据的 `unrotated_channels` 与 `odd_head_dim_recorded`。旋转角度表形状为 `[S, D]`，同一对的两个通道共享角度；角度表数组摘要即角度表指纹。

### 频率缩放

令 `Drot = D - (D % 2)`、`P = Drot / 2`，通道对索引 `i ∈ [0, P-1]`：

```text
standard_i = base_frequency ** (-2i / Drot)
angle[pos, i] = pos * inverse_frequency_i

none:          inv_i = standard_i
linear:        inv_i = standard_i / scaling_factor
ntk_piecewise: ntk_base = base_frequency * scaling_factor ** (Drot / (Drot - 2))
               inv_ntk_i = ntk_base ** (-2i / Drot)
               x_i = i / max(P - 1, 1)
               inv_i = inv_ntk_i        if x_i < 0.5
                       standard_i / scale if x_i >= 0.5
```

该实现明确将“NTK 分段缩放”定义为高频段 NTK-aware 基频缩放、低频段线性插值的分段组合；`Drot=2` 时使用 `ntk_base=base*scaling_factor`，避免除零。

### 打分与归一化

`scores[b,h,i,j] = dot(q_rot[b,h,i], k_rot[b,h,j])`，不额外乘 `1/sqrt(D)`，因此诊断值就是配置所述的点积。因果模式将 `j > i` 设为 `-inf`。每行先减去该行有限分数最大值，再执行 `exp` 和归一化：

```text
shifted = score - row_max
z = sum(exp(shifted))
weights = exp(shifted) / z
```

点积按行归约而不是块矩阵乘，避免改变行块大小时累加顺序变化。出现 NaN、Inf、非有限权重或零/非有限归一化因子时，返回带 `shard_id`、`row_block_index`、`step_index` 的 `E_NUMERIC`。

## 确定性、缓存与增量

- 数组摘要综合 dtype、shape 与 C 顺序原始字节；配置版本是规范化 JSON 的 SHA-256。
- 阶段产物存储在 `artifacts/<stage>/<shard>/<artifact_digest>/`，同一内容只写一次。
- 查询缓存键包含分片内容指纹、配置版本、角度表指纹、查询类型与选项摘要。
- 缓存 payload 与绕过缓存重新计算的 payload 使用同一规范化 JSON 编码，摘要必须相同。
- 配置字段映射到最早受影响阶段：
  - `base_frequency`、`scaling_mode`、`scaling_factor` → 频率生成
  - `rotation_block_size` → 分块旋转
  - `causal_mask`、`numeric_tolerance` → 打分归一化
  - 延迟/内存/行块大小 → 不改变数学产物，仅重新发布复用检查点
- 增量执行后，应用从零重算频率、旋转、打分与统计摘要并逐字节比较；差异返回 `E_INCREMENTAL_MISMATCH` 和 `differing_stage`。

## 检查点、停止与恢复

每个分片/配置版本有独立的检查点目录，阶段顺序固定为：

```text
1 frequency_generation
2 block_rotation
3 scoring_normalization
```

记录含阶段名、分片内容指纹、配置版本、产物摘要、前序摘要与单调序号。发布过程使用跨平台文件锁（POSIX `flock` / Windows `msvcrt.locking`），记录和状态均通过同目录临时文件 + `fsync` + 原子替换写入。落后、重复、跳跃或跨阶段错位发布返回 `E_STALE_CHECKPOINT`。

停止请求在阶段或调度块边界生效，已完成阶段保留检查点；中断的阶段不会发布半成品检查点。恢复时从当前配置版本最后连续检查点继续。应用不承诺强杀进程后生成退出报告。

## 峰值内存规划

执行前由 `memory.py` 计算输入、输出、角度表、cos/sin 表与工作区的峰值估计：

- 旋转阶段可按批次切块。
- 打分阶段可按平坦行块切块。
- 决策记录 `mode`、`chunk_count`、`fixed_bytes`、`working_bytes`、`estimated_peak_bytes` 与预算明细，可通过 `/api/schedule` 或 `/api/status` 查看。
- 固定输出本身已超预算时，在分配大数组前返回带预算明细的 `E_MEMORY_BUDGET`。

默认演示中 `long` 的分数输出本身约 1.04 MiB，而完整行工作区会超过 4 MiB 预算，因此调度器选择 `row_blocks`（16 行/块，36 块），估计峰值为 1,309,184 B。

## 稳定错误码

| 错误码 | 场景 |
|---|---|
| `E_INVALID_CONFIG` | 未知字段、非法缩放系数、零/奇数分块、非法预算等 |
| `E_INVALID_INPUT` | 分片/清单/查询参数格式错误 |
| `E_INVALID_STAGE` | 非法阶段、缺失前置产物、当前状态禁止操作 |
| `E_STALE_CHECKPOINT` | 检查点落后、重复、跳跃或跨阶段错位；分片内容已变 |
| `E_NUMERIC` | NaN/Inf、非有限权重、零或非有限归一化因子 |
| `E_LATENCY_BUDGET` | 查询超预算，附 `query_id`、`elapsed_ms`、`budget_ms` |
| `E_MEMORY_BUDGET` | 规划峰值超预算，附固定/最小工作区/所需字节 |
| `E_INCREMENTAL_MISMATCH` | 增量产物与从零构建不一致，附差异阶段 |
| `E_BUSY` | 运行中重复启动或编辑配置 |
| `E_NOT_FOUND` / `E_IO` | 本地文件不存在或持久化完整性错误 |

## 数据格式与固定样本

`.npz` 至少包含 `q`/`k`（也接受 `query`/`key`），两者形状一致且为浮点四维。`data/manifest.json` 记录 `id`、相对文件路径、形状、随机种子和用途。

| 分片 | 形状 | 覆盖点 |
|---|---|---|
| `short` | `[1,2,7,8]` | 短序列 |
| `long` | `[2,3,96,16]` | 长序列、批量 > 1 |
| `odd` | `[2,5,11,9]` | 奇数头维度、最后一维不旋转 |

重新生成样本：

```bash
python -m rope_diag generate-data --base .
```

## 测试

```bash
pip install -r requirements-dev.txt
pytest -q
python scripts/run_demo.py
python scripts/http_smoke.py
```

当前实测：`46 passed`，一键演示 17/17 检查通过，回环 HTTP 检查全部通过。固定摘要、错误注入证据与复现记录见 [VALIDATION.md](VALIDATION.md)。

## 边界说明

- 本地导入使用工作区可访问的文件路径，不上传数据到外部服务。
- Uvicorn 多工作进程可共享内容寻址产物与检查点锁，但会话写状态更适合单进程服务；默认单 worker 即可满足本地诊断。
- 逐字节一致性在同一执行环境与同一 NumPy 构建内验证；跨 CPU/BLAS 平台的浮点末位不作为兼容承诺。
- 强制终止进程后不生成退出报告；再次加载会把遗留 `running` 状态恢复为 `stopped`，由用户显式 `resume`。
