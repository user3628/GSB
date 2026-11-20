# 说话人分离与识别增量编辑工作台（diarwb）

本地、确定性、无外部服务依赖的说话人分离/识别工作台。仅依赖 NumPy（SciPy 为可选，
运行时核心不强制），不使用 GPU、容器或任何真实分离/声纹模型。输入由固定随机种子
生成器产出的 16 kHz / 单声道 / 16 位 PCM WAV 分片与 JSON 清单，提供 CLI 与本地
HTTP 两套接口，支持分段编辑、说话人改名/合并、增量重算、版本化快照、停止恢复与
诊断查询。

## 1. 环境准备

要求 Python 3.10+（开发与验证使用 3.14）。

```bash
python -m venv .venv
# Windows: python -m venv .venv && .venv\Scripts\activate
.venv/bin/pip install -r requirements.txt   # numpy、scipy 标准发行版
```

标准库 `http.server` 与 `threading` 随 Python 提供，无需联网。

## 2. 一分钟快速体验

```bash
ROOT=$(mktemp -d)
.venv/bin/python -m diarwb --root "$ROOT" generate     # 生成 3 个确定性分片+清单
.venv/bin/python -m diarwb --root "$ROOT" import       # 导入并校验一致性
.venv/bin/python -m diarwb --root "$ROOT" run-all      # 分离 + 识别
.venv/bin/python -m diarwb --root "$ROOT" status       # 阶段状态与版本指纹
.venv/bin/python -m diarwb --root "$ROOT" query --shard shard-000 --kind overlaps
.venv/bin/python -m diarwb --root "$ROOT" export       # 导出分段与重叠段
```

完整端到端演示（含全部错误码注入与固定摘要）：

```bash
.venv/bin/python tools/run_demo.py
```

测试：

```bash
.venv/bin/python -m unittest discover -s tests -v
```

## 3. 本地 HTTP 接口

```bash
.venv/bin/python -m diarwb --root "$ROOT" serve --host 127.0.0.1 --port 8765
```

基于 `ThreadingHTTPServer`，所有写操作经工作区文件锁串行化。主要 POST 路由：

| 路由 | 作用 |
| --- | --- |
| `/import` `/run-all` `/run-stage` | 导入、全量运行、单阶段运行 |
| `/status` `/timeline` `/query` | 阶段指纹、时间轴、分段/相似度/重叠查询 |
| `/draft` `/draft/discard` `/publish` | 草稿编辑、回退、带版本号发布 |
| `/unify-identity` | 跨分片统一说话人身份 |
| `/config` | 查询/修改配置（驱动增量重算） |
| `/stop` `/recover` `/consistency` `/export` | 停止、恢复、一致性校验、导出 |
| `/health` | 存活探测 |

## 4. 核心设计与确定性保证

- **分离**（`separation.py`）：汉宁窗 STFT → 种子化低频基函数初始化掩码 → 固定次数
  迭代 soft 幅值掩码 → OLA iSTFT 重建；波形量化为 int16 定点，逐采样点一致。
- **嵌入**（`embedding.py`）：帧级 log-mel 能量 → 均值/方差/极值/漂移统计 →
  种子哈希投影（确定性 QR 正交化）→ L2 归一化 → Q14 定点序列化，逐字节一致。
- **识别**（`recognition.py`）：参考段嵌入均值 + 种子微扰构成质心；通道经确定性
  全局一对一指派消除掩码置换歧义，再以余弦相似度 + 可配置 margin 判定；低于 margin
  的通道标记 `low` 且 `assigned_speaker=null`，保留在诊断中，不静默归类。
- **重叠段**：在多人共占的参考帧内，以每通道自适应能量阈值确认后合并相邻帧输出。
- **时间轴**（`timeline.py`）：毫秒整数分段，切分/相邻合并/改名/说话人合并，单调
  版本号 + 操作来源 + 影响区间，强制非负、start<end、不越界不变量。
- **指纹与缓存**：缓存键 = 分片内容指纹 + 配置版本 + 编辑版本；编辑提交即清空旧缓存。
- **增量与快照**（`pipeline.py`/`workspace.py`）：按阶段版本指纹识别最早受影响阶段，
  仅重算其下游；快照经临时文件 + `os.replace` 原子写，含摘要 digest 与高水位记录；
  快照被整体替换时区分 rollback / tamper / 正常推进。
- **并发发布**：跨平台文件锁（POSIX `fcntl`，Windows `msvcrt`）+ 版本号连续推进校验，
  草稿与已发布版本隔离，草稿可丢弃回退。

## 5. 结构化错误码

| 代码 | 触发场景 |
| --- | --- |
| `E_IMPORT_VALIDATION` | 样本总数/时长上界/分段合法性与清单不一致 |
| `E_INVALID_CONFIG` | 阈值、迭代次数等配置越界 |
| `E_LATENCY_BUDGET` | 查询超预算，返回 `query_id`/`elapsed_ms`/`budget_ms` |
| `E_INVALID_STAGE` | 前置阶段缺失或非法阶段跳转 |
| `E_STALE_VERSION` | 发布版本落后/重复/不连续 |
| `E_STALE_SNAPSHOT` | 快照回退或篡改（`details.kind` 区分） |
| `E_INCREMENTAL_MISMATCH` | 增量结果与全量重建不一致并附差异摘要 |
| `E_STOPPED` | 收到停止请求，完成当前不可拆分单元后退出 |
| `E_NOT_FOUND` / `E_INTERNAL` | 资源不存在 / 兜底内部错误 |

## 6. 目录结构

```
diarwb/        核心包（音频、分离、嵌入、识别、时间轴、流水线、工作区、CLI、HTTP）
tests/         单元测试与多进程并发测试
tools/         确定性端到端演示
requirements.txt
docs/DESIGN.md 技术说明与验证记录
```
