# 图文跨模态检索索引工作台（cmr workbench）

本地可运行的图文跨模态检索索引工作台与诊断台。不依赖任何真实视觉语言模型、
GPU 或外部服务：视觉描述子、文本特征、投影对齐、倒排/向量索引、双向检索、
版本指纹、检查点恢复与异常注入全部在本地确定性完成。

## 环境

- Python 3.11（本机验证 3.11.18），本地虚拟环境 `.venv`
- 依赖：NumPy、Pillow、FastAPI、uvicorn（测试另用 pytest、httpx），见 `requirements.txt`
- 不使用 Docker / 容器；除 `pip install` 外无需网络

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt   # 如 PyPI 不可达: -i https://mirrors.aliyun.com/pypi/simple/
```

## 快速开始

```bash
.venv/bin/python -m cmr init                 # 生成 config / 投影矩阵 / 6 个 PNG 分片 + pairs.jsonl
.venv/bin/python -m cmr build                # 四阶段增量构建（features→projection→index→recall）
.venv/bin/python -m cmr query-text "红色三角形 red triangle" -k 3
.venv/bin/python -m cmr query-image pair:pair_01 -k 3      # 或传 PNG 路径
.venv/bin/python -m cmr status               # 阶段指纹 / 索引规模 / 失效报告 / 注入记录
.venv/bin/python -m cmr inject nan_projection
.venv/bin/python -m cmr serve --port 8371    # 诊断页面 http://127.0.0.1:8371/
.venv/bin/python -m cmr verify               # 验收套件（27 项检查，独立临时工作区）
.venv/bin/python -m pytest tests/ -q         # 28 项端到端测试
```

数据目录 `data/`（config.json、projection.json、fragments/、pairs.jsonl）与状态目录
`state/`（分片产物、检查点、索引、召回报告、注入记录、文件锁）相互独立，均可读写。

## 架构

```
cmr/
  config.py         配置与按阶段的配置摘要（grid/ngram/投影/排序权重分量）
  samples.py        固定种子样本生成器（PNG 分片 + JSONL 清单 + 数据驱动投影矩阵）
  image_features.py 分块颜色矩(均值/标准差) + 边缘方向直方图 → 224 维
  text_features.py  NFKC + 小写 + 空白折叠 → 加权词袋/字符n-gram → 1024 维
  projection.py     投影校验(E_PROJECTION_INVALID 带矩阵位置) + 单位归一化(1e-9, E_NUMERIC)
  indexer.py        文本/视觉倒排表 + (分片,向量号) 寻址稠密索引
  retrieval.py      双向检索：余弦 + 倒排命中加权组合，top-k 分数明细
  pipeline.py       四阶段流水线：版本指纹、失效传播、检查点、停止/恢复
  checkpoint.py     原子写(tmp+rename+fsync)、fcntl 文件锁、单调序号发布保护
  inject.py         异常注入（快照/恢复，不污染已发布索引）
  server.py         FastAPI 接口 + 诊断页面
  cli.py / verify.py / service.py
```

### 关键设计

- **特征维度先声明**：`image_dim = grid²·(3+3+orient_bins) = 224`，
  `text_dim = bow_bins + ngram_bins = 1024`；清单 meta 行声明维度，不一致 → `E_DIM_MISMATCH`。
- **投影矩阵**：`data/projection.json`，64×224 / 64×1024。由配对样本互协方差 SVD
  确定性导出（PLS 式对齐），使跨模态余弦有语义；维度不符或含 NaN/Inf →
  `E_PROJECTION_INVALID`（附阶段名与 `image[r][c]` 位置）。
- **版本指纹**：`stage_fp = H(stage, 上游指纹, 该阶段配置分量)` 链式推进；
  输入指纹 = H(清单 + 各图像字节)。任一变化定位最早失效阶段，下游全部重建，
  未受影响的上游分片产物经检查点复用。查询前强制新鲜度校验，绝不用旧索引应答。
- **检查点**：每条记录含 分片/阶段/阶段指纹/分片指纹/产物摘要/单调序号，
  原子落盘；发布须经文件锁且序号连续，否则 `E_STALE_CHECKPOINT`；
  缺前置阶段产物 → `E_INVALID_STAGE`。
- **优雅停止**：`state/STOP_REQUESTED` 标志（或 `--stop-after N`），
  在分片×阶段粒度上停；恢复后最终索引摘要与召回摘要和从零运行逐字节一致。
- **异常注入**：6 种注入（维度不符清单 / NaN 投影 / 缺失分片 / 空查询 / k 越界 /
  非法图像向量），各返回稳定业务码并写入 `state/injections/INJ-*.json`
  （无时间戳，重复执行逐字节一致）；注入前后快照恢复，已发布索引不受污染。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 诊断页面（索引规模、阶段指纹、失效原因与重建范围、注入记录、检查点、双向查询） |
| POST | `/api/build?from_stage=` | 触发（分阶段）构建 |
| POST | `/api/pairs/import` | 导入 JSONL 清单 `{jsonl}` |
| POST | `/api/query/text` | `{text,k}` → top-k 图像 + 分数明细 |
| POST | `/api/query/image` | `{image,k}`（`pair:<id>` 或路径）或 `{vector,k}` |
| GET | `/api/status` | 阶段状态、当前/已发布指纹、失效报告、注入记录、检查点 |
| POST | `/api/inject/{kind}` | 异常注入 |
| POST | `/api/stop` `/api/resume` | 停止请求 / 恢复构建 |
| POST | `/api/publish` | 检查点发布（锁 + 序号校验） |

错误约定：业务错误返回 HTTP 422 + `{error:{code,message,stage,position,detail}}`。

## 实测验证结果（2026-11-20，本机 Python 3.11.18）

- `cmr verify`：**27/27 通过** —— 冷构建、跨工作区索引摘要一致、双向检索 top-k、
  重复查询逐字节一致、空转重建 up_to_date、caption 变更重建范围
  `[features,projection,index,recall]`、排序权重变更范围 `[recall]`、6 种注入
  业务码与阶段正确且记录可复现、注入不污染索引、优雅停止/恢复后摘要与从零一致、
  过期/越级发布被拒（`E_STALE_CHECKPOINT` / `E_INVALID_STAGE`）。
- `pytest`：**28/28 通过**（特征确定性与维度、检索、错误契约、失效范围、
  停止/恢复、检查点单调性、注入记录文件级字节一致、HTTP 状态/查询/诊断页）。
- 默认 6 个样本召回校验 6/6 通过（文本查图像、图像查文本 top-1 均命中自身分片）。
- HTTP 服务于 `127.0.0.1:8371` 实测：状态、双向查询、注入、停止/恢复、
  诊断页与分片静态资源均正常。

说明：导入全新图文对后，投影矩阵（配置产物）不会自动重导，跨模态余弦对新样本
可能失准——召回校验阶段会如实报告；重新生成投影矩阵（配置变更）会触发
projection→index→recall 的失效重建。倒排命中权重为 tf 归一化分量，
视觉分块哈希重复时可超过 1.0，属预期的评分分量。

任务原始要求见 `TASK.md`。
