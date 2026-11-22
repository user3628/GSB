# MMCoop 本地确定性多模态协同推理工作区

本项目从零实现一个本地可运行的多模态数据编辑与分阶段推理应用。它使用固定种子样本和固定哈希编码，不依赖真实模型、GPU、容器、外部服务或运行时网络。

核心能力：

- 文本规范化、固定哈希 64 维单位嵌入、token 字符偏移；
- 64 维视觉特征跨模态投影、单位归一化与余弦相似度；
- 相似度/算子置信度/语言一致性加权融合、对数贡献与权重重归一化；
- 按样本汇总 `consistent` / `weak` / `conflict` 及诊断；
- 命令行与本地 HTTP 的数据导入、文本/特征编辑、分阶段构建、状态与结果查询；
- 原子检查点、真实文件锁、连续序号发布、优雅停止/恢复；
- 内容摘要驱动的增量失效与全量结果逐字节对照；
- 5 类参数注入与稳定业务错误码。

## 快速开始

要求 Python 3.11。项目根 `.venv` 已按该版本创建。

Linux / WSL / macOS：

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements.txt
source .venv/bin/activate
python -m mmcoop init-demo --fresh-workspace
python -m mmcoop build
python -m mmcoop status
python -m mmcoop results
```

完整实测版本锁定见 `requirements.lock`（CPython 3.11.18）。

Windows PowerShell（不使用容器）：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m mmcoop init-demo --fresh-workspace
python -m mmcoop build
python -m mmcoop serve --host 127.0.0.1 --port 8000
```

安装为可编辑包后也可使用 `mmcoop` 控制台命令：

```bash
uv pip install --python .venv/bin/python -e . --no-deps
```

## 固定样本

`data/` 已由固定种子生成：

- `data/texts.jsonl`：5 条文本，含中英混排、重复内容（`t02`/`t05`）；
- `data/images.json`：6 条图像记录，每条含 64 维特征、PNG、`text_id` 与来源算子；
- `data/png/*.png`：24×24 确定性小图；
- `data/manifest.json`：种子、关联统计和全部文件 SHA-256。

样本包含一对多关联（`t01` 与 `t02` 各关联两图）和缺失关联（孤立文本 `t04`）。重新生成：

```bash
python -m mmcoop init-demo --seed 20261123 --fresh-workspace
```

## 四阶段

1. **文本编码**：NFKC、全半角统一、空白折叠、小写化；按 token 执行固定 SHA-256 哈希并归一化为 64 维单位向量，同时保存 token 起止偏移。空文本、纯标点或零向量返回 `E_EMPTY_INPUT`，超长文本返回 `E_TEXT_TOO_LONG`。
2. **投影对齐**：按 `config/projection_matrix.json` 将 64 维视觉特征投影到文本空间并归一化，计算余弦相似度。维度/数值异常返回 `E_PROJECTION_INVALID`，缺失文本引用返回 `E_MISSING_REF`；注入 NaN 相似度返回 `E_NUMERIC`。
3. **融合打分**：融合相似度归一分、来源算子置信度、声明语言与检测语言一致性。`log_contributions` 给出每个分量的对数值、归一权重和对数贡献；被跳过分量与重归一化权重重记录在 `weights`。无权可用返回 `E_WEIGHT_ZERO`。
4. **结果聚合**：按文本汇总一对多图像结论，冲突优先于弱一致，弱一致优先于一致；孤儿文本输出 `weak` 并附 `missing_image_link` 诊断。

配置位于 `config/default.yaml`，投影矩阵可独立修改。`structure_version`、矩阵、权重、阈值、算子置信度、语言元数据或文本/特征变化都会驱动内容摘要变化。

## 常用命令

```bash
# 分阶段推进
python -m mmcoop build --through-stage text_encoding
python -m mmcoop build --through-stage projection_alignment
python -m mmcoop build --through-stage fusion_scoring
python -m mmcoop build --through-stage aggregation

# 编辑
python -m mmcoop edit-text t03 --text "新的中英 mixed 文本"
python -m mmcoop edit-image i01 --feature-file feature.json
python -m mmcoop edit-image i01 --operator vision --text-id t01

# 状态、版本、失效、结果、完整性
python -m mmcoop status
python -m mmcoop version
python -m mmcoop invalidations
python -m mmcoop results
python -m mmcoop checkpoints
python -m mmcoop integrity

# 注入
python -m mmcoop inject --kind missing_ref --sample-id i01

# 优雅停止与恢复
python -m mmcoop stop
python -m mmcoop resume

# 完整验收演示并写报告
python -m mmcoop demo --report artifacts/demo_report.json

# 启动 HTTP
python -m mmcoop serve --host 127.0.0.1 --port 8000
```

全局参数 `--config`、`--data`、`--workspace` 放在子命令之前，例如：

```bash
python -m mmcoop --config config/default.yaml --data data --workspace workspace build
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/api/status` | 阶段状态、序号、版本指纹、失效原因、停止状态 |
| GET | `/api/version` | 配置版本、数据指纹、阶段输入指纹与产物摘要 |
| GET | `/api/results` | 聚合结果、三类结论与当前是否最新 |
| GET | `/api/invalidations` | 增量失效阶段、分量和原因码 |
| GET | `/api/checkpoints` | 有效检查点链与扫描诊断 |
| GET | `/api/checkpoints/integrity` | `normal` / `normal_advance` / `rollback` / `tamper` 裁决 |
| POST | `/api/demo/reset` | 固定种子重新生成演示数据 |
| POST | `/api/data/import` | 从本地路径或 JSON 记录导入；支持 PNG base64 |
| PATCH | `/api/data/texts/{text_id}` | 编辑文本、语言或来源通道 |
| PATCH | `/api/data/images/{image_id}` | 编辑特征、来源算子或关联文本 |
| POST | `/api/build` | 同步构建；`background=true` 后台构建；支持 `through_stage` |
| GET | `/api/build/status` | 后台构建状态 |
| POST | `/api/stop` | 请求协作式停止 |
| POST | `/api/resume` | 清除停止请求并恢复 |
| POST | `/api/inject` | 执行单个参数注入并返回稳定业务错误 |

示例：

```bash
curl -s http://127.0.0.1:8000/api/version
curl -s -X POST -H 'content-type: application/json' \
  -d '{"kind":"similarity_nan","sample_id":"i01"}' \
  http://127.0.0.1:8000/api/inject
```

## 错误码

| 错误码 | 触发条件 |
| --- | --- |
| `E_EMPTY_INPUT` | 空文本、纯标点、零嵌入 |
| `E_TEXT_TOO_LONG` | 规范化文本超过 `max_chars` |
| `E_PROJECTION_INVALID` | 矩阵/特征维度错误、非有限数值、零投影范数 |
| `E_MISSING_REF` | 图像关联的 `text_id` 不存在 |
| `E_NUMERIC` | 注入的 NaN 相似度 |
| `E_INVALID_OPERATOR` | 来源算子不在置信度配置 |
| `E_WEIGHT_ZERO` | 可用权重和为零，或全部分量不可用 |
| `E_INCREMENTAL_MISMATCH` | 增量产物与从零全量产物字节不一致 |
| `E_STALE_CHECKPOINT` | 落后、重复、跨阶段/上游错位或快照完整性失败 |
| `E_INVALID_STAGE` | 前置阶段缺失或目标阶段非法 |

错误响应统一带 `code`、`stage`、`sample_id`、`field` 和 `diagnostics`。注入失败不会发布新检查点。

## 持久化与恢复

默认工作区为 `workspace/`：

```text
workspace/
  checkpoints/       # 每阶段原子检查点，序号严格连续
  artifacts/         # 按 SHA-256 寻址的不可变阶段产物
  state/published.json
  state/last_run.json
  locks/publish.lock # 真实跨进程文件锁
  quarantine/        # 发布前隔离的重复/损坏序号文件
  stop.request
```

同一序号只允许一个有效记录。扫描器忽略临时文件、截断 JSON、校验和错误、产物摘要错误、重复序号及链断裂之后的记录，并从最近一致链继续。详细裁决规则见 `docs/architecture.md`。

## 实际验证结果

本机 CPython 3.11.18，未使用 GPU、模型、容器或运行时网络。

```text
pytest: 45 passed
HTTP: uvicorn 127.0.0.1:8765 进程级验证通过
integrity: normal
data_fingerprint: 547c58bb0e6f55cf27227b0df90c39eaa6d1ed93b00314f0169fe9aff891e2cd
version_fingerprint: ba4abfe84fd99f04b603276b6b3a148826dc2a289b2d3156c91a899a1808d89d
pipeline_fingerprint: e62a0f88b5f145e982cb60bcdfd474a45f99419454486d6491b7df80f5e0438a
```

四阶段产物摘要：

```text
text_encoding        1fe72e573bbb679aa75b0a9a97ea272bccaafaa51fc1c3257a6696a1c751ac9e
projection_alignment c8d11d0319bea293cf7a99b0b4b4627b17cbfb754f0a31f34af527079a1dd6df
fusion_scoring       a0aa001a7509b7ecf5b2b70ff9c62f7a1c6cceab3f4d08f062f693de84193e42
aggregation          e62a0f88b5f145e982cb60bcdfd474a45f99419454486d6491b7df80f5e0438a
```

演示聚合：`consistent=2`、`weak=2`、`conflict=1`；缺失关联 `t04`；重复内容组 `t02/t05`。

完整演示报告见 `artifacts/demo_report.json`，其中记录：

- 全量构建与另一个空工作区从零构建 `full_scratch_match=true`；
- 修改视觉特征后最早失效阶段为 `projection_alignment`，`text_encoding` 复用；
- 修改文本内容后最早失效阶段为 `text_encoding`；
- 恢复输入后增量摘要回到全量基线；
- 5 个注入分别返回 `E_PROJECTION_INVALID`、`E_INVALID_OPERATOR`、`E_TEXT_TOO_LONG`、`E_NUMERIC`、`E_MISSING_REF`，且检查点序号均未推进；
- 停止后检查点序号不变，恢复构建与独立全量构建摘要一致；
- 最终完整性裁决为 `normal`。

运行回归：

```bash
.venv/bin/python -m pytest -q
```
