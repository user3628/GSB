# GSB 概率校准与诊断工作台

本地、确定性、可恢复的梯度提升二分类概率校准工作台。项目不依赖 Docker 或外部服务，所有读写路径均限制在项目根目录内。工作台同时提供 Python 命令行、Flask 本地 HTTP API 和单页诊断界面。

## 交付内容

- `data/`：固定种子生成的两档 NPZ 样本、`manifest.json` 与 `checksums.json`。
- `gsb/`：分箱、直方图 GBDT、Platt/等距校准、定点分批归约、诊断、原子检查点、失效传播、CLI、HTTP 与服务层。
- `templates/`、`static/`：本地可视化诊断工作台，无 CDN、字体或外部脚本依赖。
- `tests/`：42 项 pytest 测试，覆盖核心算法、错误码、分批一致性、停止恢复、增量全量一致性和并发发布。
- `state/demo/`：演示运行留下的真实阶段检查点。
- `out/demo_report.json`、`out/demo/`：固定摘要演示报告与概率/诊断导出。

## 环境

实测环境：

```text
Python 3.11.18
numpy 2.2.6
flask 3.1.2
pytest 8.3.5
```

安装 Python 3.11 与隔离环境：

```bash
uv python install 3.11
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements.txt
```

也可以使用已有离线 wheel 源：

```bash
uv pip install --python .venv/bin/python --find-links /path/to/wheels -r requirements.txt
```

后续命令均以 `.venv/bin/python -m gsb` 为入口。如已安装项目，也可以直接使用 `gsb`。

## 快速开始

```bash
# 1. 校验或生成固定种子数据；重复运行文件摘要不变
.venv/bin/python -m gsb generate-data
.venv/bin/python -m gsb verify-data

# 2. 导入小规模样本并设置演示配置
.venv/bin/python -m gsb import --dataset small
.venv/bin/python -m gsb config \
  --set bins=20 --set rounds=10 --set learning_rate=0.12 \
  --set max_depth=3 --set min_leaf=20 \
  --set calibrator=platt --set calibration_folds=3 \
  --set diagnostic_bins=10

# 3. 分阶段完整训练
.venv/bin/python -m gsb train --until-stage diagnostics

# 4. 查询状态、可靠度表与指纹
.venv/bin/python -m gsb status
.venv/bin/python -m gsb reliability

# 5. 验证批大小/批顺序一致性并导出
.venv/bin/python -m gsb compare-batches --batch-sizes 13,97,800
.venv/bin/python -m gsb export --out-dir out/export
```

单配置变更并在同一次操作中完成“失效链增量重建 + 全量从零比对”：

```bash
.venv/bin/python -m gsb config --set calibrator=isotonic --verify
```

如果增量结果与全量结果有任何字节差异，命令返回 `E_INCREMENTAL_MISMATCH`，并在 `details.differences` 中给出 `boosting`、`calibration`、`diagnostics` 或 `run_digest` 阶段。

## 本地 HTTP

获取空闲本地端口但**不启动服务**：

```bash
.venv/bin/python -m gsb free-port
```

启动仅监听回环地址的本地工作台：

```bash
.venv/bin/python -m gsb serve --host 127.0.0.1 --port 8765
# 浏览器打开 http://127.0.0.1:8765/
```

`--port` 省略时会自动选取空闲端口并打印 URL。项目测试使用 Flask in-process test client，不启动外部服务。

主要 HTTP 接口：

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/api/health` | 进程、Python 版本与路径边界检查 |
| GET | `/api/datasets` | 两档样本与当前导入项 |
| POST | `/api/import` | `{ "dataset": "small" }` 导入并校验声明 |
| GET/PUT | `/api/config` | 查询/保存配置；保存时计算失效链 |
| POST | `/api/reconfigure` | 单入口配置变更、增量重建、默认全量比对 |
| POST | `/api/train` | 后台训练；`wait=true` 时同步返回 |
| POST | `/api/stop` | 请求在当前提升轮完成后停止 |
| POST | `/api/resume` | 清除停止请求并从持久化轮次继续 |
| GET | `/api/status` | 阶段状态、序号、指纹、失效原因、进度 |
| GET | `/api/reliability` | 可靠度表、指标与高精度误差界 |
| POST | `/api/compare-batches` | 正反批序与不同批大小的摘要比对 |
| POST | `/api/verify-incremental` | 当前产物与从零全量构建逐字节比对 |
| GET | `/api/export/probabilities` | 下载概率 CSV |
| GET | `/api/export/diagnostics` | 下载完整诊断 JSON |

所有 API 错误均返回：

```json
{"ok": false, "error": {"code": "E_...", "message": "...", "details": {}}}
```

## 固定样本

`gsb.data` 使用固定种子和显式 `numpy.random.Generator` 生成数据。NPZ 使用固定 ZIP 时间戳、STORED 压缩和规范数组布局，因此重复生成时文件 SHA-256 不变。

| 档位 | 行 | 列 | 缺失列 | 正例率 | NPZ SHA-256 |
|---|---:|---:|---|---:|---|
| small | 800 | 12 | `[2, 7]` | 0.36625 | `324672ef73ce6f6586f223f8d3431eb7f1deefa508c6c208f89b4014cb051c9e` |
| medium | 8000 | 32 | `[3, 9, 17, 25]` | 0.414375 | `1aca735a6f39a33628ed444b0ac65c042161f5ddc2dce10e4296ef57acbc62db` |

`manifest.json` 记录行数、列数、缺失列下标、正例率、文件摘要、相关冗余列对与生成器版本。加载时严格核对形状、缺失列观测值、正例率、校验和与标签取值；数据文件摘要不匹配返回 `E_INVALID_PARAM`，Inf 返回 `E_NUMERIC`。

## 算法

### 1. 确定性分箱

- 每个特征只在非缺失有限值上计算 `bins - 1` 个线性分位点。
- 边界按升序去重；缺失值固定映射到桶 `0`，有限值映射到 `1..len(edges)+1`。
- 分箱边界与分箱矩阵均进入 `binning` 内容摘要；同一输入和 `bins` 产生完全相同的边界、矩阵和 SHA-256。
- `bins <= 0`、`learning_rate <= 0` 或 `learning_rate > 1` 在训练前返回 `E_INVALID_PARAM`。

### 2. 直方图 GBDT

- 初值使用带 Laplace 修正的正例率 logit。
- 每轮计算 `g = p - y`、`h = max(p(1-p), 1e-12)`。
- 每个节点对每个特征的离散桶累计一阶和二阶直方图，以 `G_L^2/(H_L+l2) + G_R^2/(H_R+l2) - G^2/(H+l2)` 选择分裂。
- 叶值 `-G/(H+l2) * learning_rate`，按固定特征顺序、桶顺序和严格增益比较决胜。
- `rounds`、`learning_rate`、`max_depth`、`min_leaf`、`l2` 均在 `boosting` 输入指纹中。

### 3. 训练集外折校准

- 按类别和 `SHA256(seed:label:row)` 稳定排序后轮转分配折，得到确定性的分层外折。
- 每折排除该折样本训练独立的同配置 booster，对留出折产生原始分数。
- 所有样本的 OOF 原始分数先做定点量化，再用全部 OOF 分数与标签拟合校准器。
- 校准器类型、折数、折种子、Platt 迭代参数和定点位数都属于 `calibration` 阶段输入。

Platt 使用 `p = sigmoid(a*s+b)`，在对数域用稳定 `logaddexp` NLL 做带回溯线搜索的 Newton 迭代；记录迭代数、收敛标志、`a`、`b` 与最终 NLL。

等距回归先按分数稳定排序并按同分样本聚合，再运行 PAVA 保序回归；每个唯一分数的拟合值通过线性插值得到单调映射。记录合并次数（迭代数）和全部 knot。

单一类别输入返回：

```json
{"code": "E_CALIBRATION_INFEASIBLE", "details": {"sample_count": 800}}
```

### 4. 定点分批归约

`PredictionAccumulator` 的量化位数为 `quantize_decimals`，默认 12：

- 原始分数与概率均量化为整数 `q = round(value * 10^d)`。
- 每个样本显式记录 `sample_id`、原始分数 q、概率 q、标签、loss q 和 Brier q。
- 接受批之前先完成全部校验，确认无批内重复、无历史重复且数值有效后才提交状态。
- 拒绝批不改变已有计数、累加和或摘要。
- 最终按 `sample_id` 排序，概率向量、Log loss 与 Brier 的和均由整数累加得到。

因此批大小、批顺序与批数不会影响最终概率向量和指标；累加器仍会记录实际批数和样本数。零批大小、批内重复或历史重复返回 `E_BATCH_STATE`，错误详情带 `preserved_state`。

### 5. 诊断与精度

- 诊断使用 OOF 校准概率与真实标签，按配置 `diagnostic_bins` 生成等宽可靠性表。
- 报告 Log loss、Brier、ECE、最大分箱偏差。
- 定点值、量化步长和有效小数位写入 `precision`。
- 本地高精度参考实现使用 Python `decimal`、60 位精度和 `Decimal.ln()`，从概率的精确二进制值计算参考 Log loss/Brier。
- 误差上界由“概率量化传播 + 逐项 loss 定点舍入 + 浮点运算余量”构成；报告同时给出观测误差和上界。
- 任何 NaN、Inf 或越界概率返回 `E_NUMERIC`，并附 `sample_index` 与 `stage`；不会静默裁剪。

## 阶段、指纹与失效

固定阶段顺序：

```text
binning -> boosting -> calibration -> diagnostics
```

每个检查点包含：

```json
{
  "stage": "calibration",
  "seq": 3,
  "input_fingerprint": "...",
  "upstream_fingerprint": "...",
  "config_version": "...",
  "content_fingerprint": "...",
  "payload_sha256": "...",
  "accumulator": {"batches": 3, "samples": 800},
  "version_fingerprint": "..."
}
```

失效传播规则：

| 变化 | 最早失效阶段 |
|---|---|
| 样本内容、manifest、`bins` | `binning` |
| `rounds`、`learning_rate`、`max_depth`、`min_leaf`、`l2` | `boosting` |
| `calibrator`、折数、折种子、Platt 参数、定点位数 | `calibration` |
| `diagnostic_bins` | `diagnostics` |

查询接口会展示各阶段有效/失效状态、输入/版本/内容指纹、失效字段的新旧值及原因。失效阶段检查点会从可发布集合移除，旧校准参数无法被新的原始分数复用。

## 原子持久化与并发

- 每个 payload 先写入同目录临时文件，`flush + fsync + os.replace` 后再更新注册表；临时文件与目标文件同卷同目录，兼容 Windows 替换语义。
- JSON 注册表与独立阶段检查点也必须完整写入后才可见；中断最多留下完整的孤立 payload，不会留下半写检查点。
- 内容寻址 payload 文件名包含内容 SHA-256。
- 注册表更新由跨进程文件锁保护（POSIX `fcntl`，Windows `msvcrt`）。
- 全局序号只允许连续加一。落后、重复、错误 `expected_seq` 或跨阶段上游指纹错位返回 `E_STALE_CHECKPOINT`。
- 缺少前置阶段、未知阶段或非法跳转返回 `E_INVALID_STAGE`。
- `python -m gsb self-check` 会验证原子临时文件位于目标同目录/同卷。

## 停止与恢复

- 停止请求写在 `state/stop.request`。
- 主提升和每一折校准都在每个完整提升轮后原子写入进度，再检查停止请求。
- 收到请求后立即完成当前轮并退出，不发布未完成阶段；强制进程终止时可能没有最终退出报告。
- `resume` 清除停止请求，校验进度输入指纹后继续。已完成的轮次不会重训。
- 恢复终态与同配置从零全量运行的原始分数、校准参数、诊断表和 run digest 逐字节一致。

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_INVALID_PARAM` | 参数、数据声明或形状非法 |
| `E_CALIBRATION_INFEASIBLE` | 校准输入只有一个类别，附样本数 |
| `E_BATCH_STATE` | 零批、重复样本、批大小不一致或未完成归约 |
| `E_NUMERIC` | NaN、Inf、概率越界或定点溢出，附位置与阶段 |
| `E_STALE_CHECKPOINT` | 序号落后/重复或跨阶段指纹错位 |
| `E_INVALID_STAGE` | 未知阶段、非法跳转或前置检查点缺失 |
| `E_INCREMENTAL_MISMATCH` | 增量与全量摘要/参数/诊断不一致，附差异阶段 |
| `E_TRAIN_BUSY` | 当前进程已有后台训练 |

## 演示与实测结果

运行完整演示（两档全量、Platt→Isotonic 增量重建、3 轮后停止恢复、多批大小正反批序）：

```bash
.venv/bin/python -m gsb demo
```

本次实测固定摘要：

```text
demo_digest = 05db7e815c15d8f3473c0e78b1e072b575b6d17e0fd1ad563ce150f4c6195443
small Platt run_digest      = c4d7fc37268859ce47b12c1953c5956fb7468a4dabe4c8ef9d280b902fc96a8c
small Isotonic run_digest   = d9f09a38ca5111cef9a891f9b9087ceea48ea74d4e4625eaa10a3990cfaa7e65
medium Platt run_digest     = 5796d60c82aa747a7e20d083f775fb1cf552135fee78be1bab960a00b9514ce4
```

质量指标（OOF 校准后）：

| 运行 | Log loss | Brier | ECE | Log loss 误差上界 |
|---|---:|---:|---:|---:|
| small Platt | 0.515856 | 0.171336 | 0.014531 | `1.19537e-9` |
| small Isotonic | 0.498966 | 0.165562 | `1.76e-13` | `1.1898e-9` |
| medium Platt | 0.542386 | 0.182029 | 0.016963 | `1.19558e-8` |

一致性与恢复：

```text
calibrator switch earliest invalid stage = calibration
binning/boosting fingerprints before/after switch = identical
incremental full comparison = equal
stop at persisted round = 3
stop/resume run digest = c4d7fc...96a8c (equals from-scratch)
all batch sizes/orders = byte-identical
```

分批摘要：

```text
small probability_vector_digest = 2ecc05a380dbe57baea2b399d182a7c9f0149eeaf2fb887dbf88130379d9cac8
small result_digest             = 47a2c32d59077434715dc003c75fc10fb8073dea160a443aba915d3d7104457c
medium probability_vector_digest = 3fcc256c74a1734b97d020e5527be22ab8c72e805e38a90e8f0c5ff6b890e43c
medium result_digest             = 3bf552a846f3c9eadf9128ab821a6f8f2d4f93f11777211ab0dcb7261ac689e0
```

测试：

```text
$ .venv/bin/python -m pytest -q
..........................................  [100%]
42 passed
```

## 设计边界

- 这是单机本地工作台，服务默认只绑定 `127.0.0.1`，未提供远程多用户鉴权。
- 一个进程内只允许一个后台训练；跨进程写检查点由文件锁和序号保护。
- 进程被强制杀死后不承诺退出报告；未完成阶段只能从最后原子写入的轮次进度恢复。
- NPZ 字节确定性依赖固定 NumPy/Python 写入实现；模型与检查点确定性由固定运行环境以及规范 JSON/定点序列化保证。
