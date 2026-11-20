# 表格数据降维与聚类诊断工作台

这是一个不依赖容器、完全本地运行的确定性工作台。它围绕三档表格样本执行：

1. 列语义核对、缺失填充、标准化与独热编码；
2. PCA、截断 SVD、固定随机投影三候选降维与规模感知取舍；
3. KMeans / Ward 层次聚类、自动簇数搜索、显式空簇重定位；
4. 轮廓系数、SSE、重构误差、累计解释方差、误差带与回退建议；
5. 基于阶段指纹的增量重算、全量一致性校验；
6. 原子 checkpoint、文件锁、序号/账本校验、优雅停止与恢复；
7. CLI 与绑定 `127.0.0.1` 的本地 HTTP 工作台。

所有样本均由固定种子在本地生成，不访问任何外部数据集。

## 环境与安装

当前验证环境：

- Python `3.14.4`（要求 `>=3.11`）
- `numpy==2.3.5`
- `scikit-learn==1.9.1`
- 无 Docker、无容器环境

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install -e .
```

主依赖在 `requirements.txt` 与 `pyproject.toml` 中均固定版本。工作区目录：

```text
samples/    三档 CSV、manifest.json、generation_summary.json
state/      默认 checkpoint、账本、内容寻址产物与停止标记
state/demo/ 演示专用状态
outputs/    默认运行报告与最近一次运行/导入记录
demo/       三档演示固定报告、增量报告与演示摘要
src/dimcluster/
tests/
```

## 快速开始

```bash
# 生成并核对三档样本
dimcluster generate-samples --pretty
dimcluster verify-samples --pretty

# 环境探针：可写目录、原子替换、文件锁、回环地址空闲端口
dimcluster env-check --pretty

# 默认工作区完整构建（可同时做增量/全量一致性校验）
dimcluster run --mode auto --verify-full

# 查询
dimcluster status
dimcluster fingerprints
dimcluster rationale --tier small
dimcluster report --pretty

# 三档确定性演示：停止→恢复→全量校验→簇数增量→非法配置注入
dimcluster demo --pretty
```

全局参数 `--root` 必须放在子命令之前，例如：

```bash
dimcluster --root /path/to/workspace status
```

自定义配置：

```bash
dimcluster run --config config.example.json \
  --set clustering.n_clusters=4 \
  --set reduction.compute_budget=low \
  --verify-full
```

`--set` 的值会优先按 JSON 解码，例如整数、布尔值、`null`。

## 样本与分档语义

| 档位 | 行数 | 列数 | 预编码维度 | 缺失率 | 主要内容 |
|---|---:|---:|---:|---:|---|
| small | 400 | 8 | 13 | 0.0165625 | 连续、类别、冗余列、缺失列 |
| medium | 4000 | 24 | 46 | 0.02784375 | 9 连续 + 4 类别 + 缺失/冗余 |
| large | 20000 | 48 | 101 | 0.029890625 | 20 连续 + 8 类别 + 缺失/冗余 |

清单记录文件名、行数、列数、CSV SHA-256、每列声明语义、缺失率、类别取值、冗余来源及非空统计。生成算法使用潜在簇结构与独立随机噪声，冗余列保留相关性但不完全相同。

本次生成摘要：

- manifest 文件 SHA-256：`e5f270eae4f109409f60eb5719564cb55873c92f0459894a5da75ed140146491`
- generation_summary.json 文件 SHA-256：`110999cd910e768c934f84f5a371a8093e8b30581a8d24e75aa4b07e02f8b779`
- generation summary 内部 `summary_sha256` 字段：`b8ff1e3f7f0c482bf09f8745977dfbf2ea95252243b45a9bb49c70734070677e`
- CSV SHA-256：
  - small：`5667afd1aa7f2b2a84aa05625238adb1f7f5f216a6b2eae2d51a2dfe881ef988`
  - medium：`6dafe6bc34c6ce5f0ea7fce59223c1bc577b9c0c04c0230c3107eb9c4babc1f9`
  - large：`aa59e160c476523877eaf3f376fa164a5f8214694d051c0b77ebe7a4355af63e`

## 预处理与确定性

- 连续列：按清单声明核对非空数值比例后，使用 `median`/`mean`/`zero` 填充，再标准化。
- 类别列：排序后的众数或显式常量填充，排序后的类别做 one-hot；超过 `max_onehot_cardinality` 报 `E_INVALID_CONFIG`。
- 缺失列：从非空统计识别，默认追加 `missing__<col>` 指示列。
- 冗余列：保留并记录来源；可选 `drop_exact_duplicate_features=true` 删除完全相同的编码列。
- 数组统一 `float64`、小端、C 顺序、12 位量化；PCA/SVD 载荷做符号归一化；随机状态固定。
- 降维产物为 `reduced.npy`、`loadings.npy`、`mean.npy`、逐行重构误差与元数据。相同输入与配置的重复运行逐字节一致（测试覆盖三种候选）。

## 规模感知降维取舍

规则版本：`size-aware-v1`。先拟合一组确定性的 2..cap 前缀曲线，再根据：

- 行数；
- 原始/编码维度；
- 缺失比例；
- `compute_budget`；
- 方差目标；
- 重构误差上限；
- 候选算法先验；
- 归一化计算成本；
- 维度效率

计算候选分数。每档记录全部候选的目标维度、指标、分数分解、入选/淘汰状态与淘汰主因。

规模规则：

- small：基础 cap 5，避免小样本使用明显过高维度；
- medium：基础 cap 10；
- large：基础 cap 6，在重构误差上限内优先低维表示；
- `compute_budget=low` 降 cap、提高成本权重并提升随机投影先验；`high` 反向调整；
- 缺失率高于 10% 时允许额外一维鲁棒空间；
- 自动目标维度取满足方差目标和重构约束的最小 k；不满足时确定性回退到 cap；
- 分数相同按候选名字典序决胜。

本次结果：

| 档位 | 选定降维 | k | 重构误差 | 累计解释方差 | 聚类 |
|---|---|---:|---:|---:|---|
| small | PCA | 3 | 0.134313 | 0.853330 | Ward, k=2 |
| medium | 截断 SVD | 7 | 0.131820 | 0.851800 | KMeans, k=5 |
| large | 截断 SVD | 6 | 0.193334 | 0.785300 | KMeans, k=5 |

large 的目标维度低于 medium，且重构误差低于 0.30 约束。small 的单簇占比 0.765 超过 0.72 阈值，因此报告明确标记该档并建议回退到其他簇数/降维候选。

## 聚类与诊断

KMeans 为确定性 Lloyd 实现：

- k-means++ 初始化使用固定随机种子；
- 每轮记录最大簇心移动量、簇大小、空簇列表与收敛状态；
- 空簇按 `relocate_farthest` 从其他簇选取离簇心最远的可移动点并重定位，事件写入记录；
- 最终标签按簇心字典序重新编号（`lexicographic_centroid_v1`），重复运行编号稳定；
- 层次聚类使用 Ward，单次合并过程如实记录，不伪造 Lloyd 迭代。

自动簇数在 `[k_min, k_max]` 上以固定子样本轮廓系数选择，平局取较小 k。诊断报告包含：

- 轮廓系数及规模相关误差带；
- 簇内平方和、每样本惯性及误差带；
- 重构误差及逐行标准误带；
- 累计解释方差及有界误差带；
- 单簇占比及二项误差带；
- 轮廓阈值/单簇占比阈值触发项；
- 备用降维候选、备用簇数及依据。

误差带是局部稳定性近似，不是总体保证；大样本轮廓计算使用确定性子样本。

## 增量重算与一致性

阶段顺序：

```text
preprocess -> reduce -> cluster -> diagnose -> report
```

每阶段输入指纹由上游产物摘要与当前阶段配置共同生成。`run --mode auto` 会逐阶段比较：

- 输入指纹；
- 阶段配置指纹；
- checkpoint 产物摘要。

命中则复用，否则只从最早失配阶段开始重算，并继续复用未受影响的上游产物。修改：

- 预处理方式：从 preprocess 重算；
- 目标维度/候选/预算：从 reduce 重算；
- 簇数/聚类算法：从 cluster 重算；
- 诊断阈值：从 diagnose 重算；
- 最终 report 汇总重算。

`--verify-full` 会在隔离临时工作区执行一次从零构建，并：

- 对最终报告做规范 JSON 字节比较；
- 对每档聚类标签做允许重排的划分等价比较；
- 不一致时返回 `E_INCREMENTAL_MISMATCH`，附字段级差异、两份报告哈希与差异数量。

## 持久化、停止恢复与并发

每个阶段完成后写入：

```json
{
  "schema_version": "state-record-v1",
  "sequence": 7,
  "stage": "cluster",
  "tier": "medium",
  "state": "complete",
  "input_fingerprint": "...",
  "config_fingerprint": "...",
  "config_version": "...",
  "artifact_digest": "...",
  "artifacts": {"labels.npy": {"path": "...", "sha256": "...", "bytes": 32000}},
  "predecessors": [{"stage": "reduce", "tier": "medium", "artifact_digest": "..."}],
  "record_digest": "..."
}
```

- 产物先以内容寻址方式原子写入，checkpoint JSON 最后原子替换，随后写账本条目；
- checkpoint 不跨文件拼装，不会暴露半写 JSON；
- 全局序号在文件锁内取当前最大序号 +1；
- 账本保存每条记录的序号、记录摘要、输入/配置指纹与产物摘要；
- 可区分 checkpoint 被替换、序号回退、账本冲突与产物摘要不匹配；
- 并发发布同一版本时只有一个成功，其余返回 `E_STALE_CHECKPOINT`；
- 缺失前置阶段返回 `E_INVALID_STAGE`。

停止语义：`stop` 只写停止标记；运行器在完成当前不可拆分的“分档单元”（该档四个阶段）后退出。若报告尚未生成，不声称存在退出报告。`resume` 清除停止标记并从最近一致 checkpoint 继续；`state/active_config.json` 保存当前配置，CLI/HTTP 在未显式传入配置时据此恢复，因此跨进程恢复不会丢失簇数、维度或预处理配置。查询报告时会检查其配置版本，拒绝返回其他配置的旧报告；全量/重置运行开始前会删除旧报告文件。演示已验证：停止后恢复的最终报告与从零全量构建逐字节一致。

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_INVALID_CONFIG` | 维度、簇数、枚举值、阈值或请求体非法 |
| `E_INVALID_STAGE` | 前置阶段/产物缺失，或阶段不合法 |
| `E_STALE_CHECKPOINT` | 重复发布、基于旧序号的落后发布，或锁等待超时 |
| `E_SEQUENCE_REGRESSION` | checkpoint 序号低于账本头 |
| `E_VERSION_REPLACED` | checkpoint 记录被整体替换或读取后被他人更新 |
| `E_DIGEST_MISMATCH` | 产物/记录摘要不匹配 |
| `E_INCREMENTAL_MISMATCH` | 增量结果与干净全量构建不一致 |

## HTTP 工作台

```bash
dimcluster serve --port 0
# 输出 http_ready JSON，含实际回环地址与端口
```

服务只允许绑定 `127.0.0.1`、`localhost` 或 `::1`，默认端口 0 由操作系统选择未占用端口。根路径 `/` 提供无外部依赖的交互页面。

主要接口：

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/api/health` | 版本、配置指纹、运行状态 |
| GET | `/api/status` | 阶段状态、序号头、停止标记、最近结果 |
| GET | `/api/fingerprints` | 每档阶段配置/输入/checkpoint 指纹 |
| GET | `/api/report` | 诊断报告 |
| GET | `/api/rationale?tier=small` | 降维/聚类取舍与诊断理由 |
| GET | `/api/runs/latest` | 最近一次运行/错误 |
| POST | `/api/import` | 校验并导入本地 manifest |
| POST | `/api/run` | 触发增量或全量运行，支持 `background` |
| POST | `/api/resume` | 清除停止标记并恢复 |
| POST | `/api/stop` | 请求当前分档完成后停止 |
| POST | `/api/config/validate` | 配置校验 |

示例：

```bash
curl -s http://127.0.0.1:PORT/api/report
curl -s -X POST http://127.0.0.1:PORT/api/stop \
  -H 'content-type: application/json' -d '{"reason":"manual"}'
curl -s -X POST http://127.0.0.1:PORT/api/resume \
  -H 'content-type: application/json' -d '{"verify_full":true}'
```

## 测试与本次验证

```bash
.venv/bin/python -m unittest discover -s tests -t . -v
.venv/bin/dimcluster demo --pretty
```

本次实际结果：

- 自动化测试：`28` 项全部通过；
- 三档样本：`400×8`、`4000×24`、`20000×48`，行数/列数/哈希/分档摘要全部匹配；
- 三种降维候选重复运行：`reduced.npy` 与 `loadings.npy` 逐字节一致；
- 空簇重定位：强制非空/重复点场景触发重定位，最终簇非空且标签稳定；
- 停止→恢复：停止时已完成 small 档 4 个阶段、未写报告；恢复后复用 small 并完成 medium/large；
- 恢复报告与全量构建：
  - 规范字节 SHA-256：`61c8dbc48d546aa021d34fdf7d9315fad4edd4a01cd8e9828edc7a16caf9d4c7`
  - 报告内部内容 SHA-256：`ed722ecfc806d5e325dda8e6d19ced236683e344f9b13d13fbcf30ca34cd9e5c`
  - `report_byte_identical=true`，标签在重排意义下等价；
- 簇数从 auto 改为 4：复用 6 个 stage，重算 7 个 stage，最早重算阶段为 `cluster`；
- 增量报告与干净全量报告字节一致；
- 非法 `reduction.target_dimension=1` 返回 `E_INVALID_CONFIG`；
- 并发双进程发布：一个 `ok`、一个 `E_STALE_CHECKPOINT`；
- 序号回退、整体替换、产物摘要篡改分别识别为 `E_SEQUENCE_REGRESSION`、`E_VERSION_REPLACED`、`E_DIGEST_MISMATCH`；
- 原子替换与文件锁探针通过；HTTP 回环绑定、页面与 JSON API 实测通过；
- HTTP 后台运行→请求停止→恢复实测通过，恢复过程保留当前服务配置；
- CLI 跨进程：带自定义簇数/分档配置运行并停止后，不重复传配置直接 `resume` 成功复用 4 个上游 stage、完成其余 5 个 stage，报告生成且状态完整性为真；
- 本次演示耗时约 `14s`，默认三档完整构建约 `7.9s`（当前 20 核 Linux/WSL2 环境）。
- 重复运行演示后，`demo_summary.json`、固定报告、增量报告三个文件的 SHA-256 均逐字节不变；`demo_summary.json` SHA-256 为 `af5ab83b94fdd28d50a6af4e4985f7b9df879be790f9a712c8ee130110f55130`。

完整机器可读结果见 `demo/demo_summary.json`，固定报告见 `demo/fixed_report.json`，增量报告见 `demo/incremental_report.json`。

## Windows 说明

跨平台锁与原子替换实现：

- POSIX：`fcntl.flock` + `os.replace` + 文件/目录 `fsync`；
- Windows：`msvcrt.locking` + `os.replace`，目录 `fsync` 自动跳过；
- 锁文件不删除，保持稳定锁身份；
- 所有阶段记录与数组均先写同目录临时文件，再原子替换。

本次执行环境是 Linux/WSL2，因此原生 Windows 锁定与替换未在本机实测；`env-check` 会在目标操作系统上执行实际探针。Windows 上建议使用短路径工作区，并以：

```powershell
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m pip install -e .
.venv\Scripts\dimcluster.exe env-check --pretty
.venv\Scripts\dimcluster.exe demo --pretty
```

进行本机确认。

## 实现边界

- 轮廓误差带为解析近似，不是统计保证；大样本轮廓使用固定子样本。
- 本项目不启动后台守护进程；HTTP 运行可由同步请求或进程内后台线程触发。
- 强制终止进程时不承诺生成退出报告；恢复只依赖已完成的原子 checkpoint。
- “逐字节一致”以相同 Python/numpy/scikit-learn 版本与相同本地运行配置为前提；数组已量化并规范符号/字节序以降低平台差异。
