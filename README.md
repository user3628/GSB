# prefalign — 本地 LLM 偏好对齐数据编辑与增量失效应用

纯本地、确定性、无容器、无外部服务、无真实模型的偏好样本对（preference pair）
四阶段处理应用。核心能力是**单样本编辑 / source 分片替换 / 配置变更后的增量
失效传播**，并保证增量结果与从零全量构建逐字节一致。

## 1. 环境与安装

- Python 3.11（通过 `uv python install 3.11` 安装 CPython 3.11.18），虚拟环境位于 `.venv/`
- 依赖：NumPy、Flask、pytest、PyYAML（见 `requirements.txt`），仅安装期联网，运行期不访问网络

```bash
uv venv --python 3.11 .venv
uv pip install -r requirements.txt
```

## 2. 目录结构（Windows 风格逻辑路径）

所有配置与持久化记录中的路径均使用 Windows 反斜杠风格
（如 `workspace\\state\\manifest.json`），由 `prefalign/core/paths.py`
在运行时映射到本机原生路径，Windows 与 POSIX 上行为一致。

```
config/default.yaml          # 质量权重/分折/特征规则/配比权重/存储路径
data/shards/                 # JSONL 输入分片 + GENERATION_META.json（生成种子）
prefalign/core/              # 四阶段、哈希、检查点、流水线、存储、路径、错误码
prefalign/web/app.py         # 本地 Flask HTTP 接口
prefalign/cli.py             # 命令行接口
scripts/generate_demo_data.py# 固定种子生成 4 个分片（60 条，含异常样本）
scripts/run_demo.py          # 确定性验收演示
workspace/state|artifacts|checkpoints|locks/   # 工作区状态
tests/                       # pytest 套件（46 个用例）
```

## 3. 确定性演示数据

`.venv/bin/python scripts/generate_demo_data.py`，固定种子 `20240917`，
生成 4 个分片共 60 条，覆盖：

- 中英混排（含全角空格、混合脚本，用于检验 NFKC 与空白折叠）
- 跨分片重复 prompt（human/rlhf/synth 三个分片共享同一 prompt）
- 共享 prompt 的不同偏好对（同组 3 个 pair，组内序号 0/1/2）
- 异常样本：空白 prompt 与三字段全空（每分片 2 条，共 8 条）

## 4. 四个阶段

| 阶段 | 模块 | 关键产物 |
| --- | --- | --- |
| 1 规范化与分组 | `stage_normalize.py` | NFKC+空白折叠+小写、64 位 SimHash 指纹、`group_id`、组内 `pair_ordinal`；异常样本落 `E_INVALID_SAMPLE` 诊断并保留 pair_id |
| 2 质量打分 | `stage_quality.py` | 长度惩罚、相对词汇包含、来源先验三因子，按可配置权重在对数域合成；零因子/NaN/Inf 落 `E_NUMERIC`（含 pair_id 与因子名），分值按有效位数取整 |
| 3 奖励特征 | `stage_features.py` | 固定哈希 64 维单位嵌入；`concat/diff/normalized` 三种 pair 规则；按 prompt 组确定性分折（同组不跨折）；维度不符/空折落 `E_FEATURE_INVALID`（含 group_id/fold） |
| 4 分层采样 | `stage_sampling.py` | source×lang 分层、空白切分 token 估计、最大余数法配额（余数并列按层名字典序裁决）；零权重/权重和为零/空层落 `E_ALLOCATION_INVALID` |

所有哈希为纯 Python FNV-1a 64 位实现，不依赖进程级 `hash()` 随机化；
所有 JSON 产物用排序键、固定分隔符的规范化 JSON 落盘，保证跨运行逐字节一致。

## 5. 检查点、停止恢复与并发保护

- 每阶段完成时原子写检查点（临时文件 + `fsync` + `os.replace`），含阶段名、
  输入分片摘要、配置版本/指纹、产物摘要、单调序号（1..4），中断不留半写记录。
- 优雅停止：停止标记在每个不可拆分样本单元边界被检查；恢复时忽略损坏/不完整
  记录，从最近一致检查点继续，最终序号连续无重复。
- 并发发布：`fcntl.flock` 文件锁 + 序号比较，仅允许序号连续推进；落后/重复/
  跨阶段错位返回 `E_STALE_CHECKPOINT`，非法跳转或缺前置产物返回 `E_INVALID_STAGE`。

## 6. 版本指纹与增量失效

每个阶段指纹由「输入内容摘要 + 该阶段配置子树指纹 + 上游阶段指纹」复合而成：

- 样本编辑 / 分片替换 → 最早失效阶段 `normalize`，全链重建
- 质量权重变化 → 复用 `normalize`，从 `quality` 重建
- 分折种子/特征规则变化 → 复用前两阶段，从 `features` 重建
- 配比权重变化 → 仅重建 `sampling`

失效原因、受影响阶段链与新指纹写入 `workspace/state/invalidation_log.json`，
并可通过状态接口观测；旧版本下游产物一律删除，禁止复用。

## 7. 命令行用法

```bash
.venv/bin/python -m prefalign --root . import --demo      # 导入分片
.venv/bin/python -m prefalign --root . run                # 增量构建
.venv/bin/python -m prefalign --root . status             # 阶段状态与指纹
.venv/bin/python -m prefalign --root . edit edit.json     # 编辑单样本
.venv/bin/python -m prefalign --root . replace web f.jsonl --alias shard_03_web_v2.jsonl
.venv/bin/python -m prefalign --root . stop               # 请求停止
.venv/bin/python -m prefalign --root . resume             # 恢复
.venv/bin/python -m prefalign --root . verify             # 增量 vs 全量逐字节比对
.venv/bin/python -m prefalign --root . export             # 导出混合集
.venv/bin/python -m prefalign --root . demo               # 端到端确定性演示
```

## 8. HTTP 接口（默认 127.0.0.1:5050）

`POST /shards/import`、`POST /pipeline/run`、`GET /pipeline/status`、
`GET /pipeline/artifacts/<stage>`、`POST /samples/edit`、
`POST /sources/<source>/replace`、`POST /stop`、`POST /resume`、
`POST /verify`、`POST /mixture/export`、`GET /mixture`、`GET /health`。

启动：`.venv/bin/python -m prefalign.web.app --root . --port 5050`

## 9. 业务错误码

| 码 | 触发场景 |
| --- | --- |
| `E_INVALID_SAMPLE` | 空 prompt 或 prompt/chosen/rejected 规范化后全空 |
| `E_NUMERIC` | 质量因子为零、NaN 或 Inf |
| `E_FEATURE_INVALID` | 特征维度不符或某折内无 prompt 组 |
| `E_ALLOCATION_INVALID` | 层权重为零、权重和为零或层无可用样本 |
| `E_STALE_CHECKPOINT` | 落后/重复/错位的检查点发布 |
| `E_INVALID_STAGE` | 非法阶段跳转或缺失前置产物 |
| `E_INCREMENTAL_MISMATCH` | 增量与全量产物摘要不一致（附首个差异阶段与位置） |

## 10. 实际验证结果

- `scripts/run_demo.py`：4 分片全量构建 → 单样本编辑触发全链失效 →
  停止/恢复 → 并发发布返回 `E_STALE_CHECKPOINT` → 增量与全量四阶段摘要
  全部 `matched`（60 样本）。
- `.venv/bin/python -m pytest`：**46 passed**，覆盖四阶段、确定性、异常诊断、
  检查点原子性、停止恢复、增量失效、全量一致性、多进程并发发布、HTTP 全流程、
  Windows 风格路径解析。
