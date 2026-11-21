# 本地确定性 AIGC 生成图像质量评估组件（IQA）

一个不依赖真实生成模型、GPU、外部服务或网络数据集的本地图像质量评估组件。输入由固定随机种子
生成的小尺寸 RGB PNG 分片，按四个阶段完成评估：解码标准化 → 分维度指标 → 文本-图像提示词
一致性 → 加权融合与分级。所有产物内容寻址、可增量重建、可在停止后恢复，并保证同输入同配置下
逐字节可复现。

## 1. 环境与安装

要求 Python 3.10+。本机实际验证版本：

| 组件 | 验证版本 |
| --- | --- |
| Python | 3.14.4 |
| NumPy | 2.5.5 |
| Pillow | 13.0.0 |
| pytest | 9.2.0 |

安装依赖（版本以本机已装为准，无需联网下载数据集）：

```bash
python3 -m pip install -r requirements.txt
# 或仅安装运行依赖
python3 -m pip install numpy pillow
```

仅使用 Python 标准库实现 HTTP 服务、文件锁（`fcntl`）、原子写与 JSON 持久化，无额外服务依赖。

## 2. 目录结构

```
iqa/                     评估组件源码
  errors.py              稳定业务码与 BusinessError
  hashing.py             FNV-1a、SHA-256 指纹、规范化序列化、浮点确定性舍入
  config.py              权重/阈值/融合域配置与配置版本指纹
  injection.py           四类异常参数注入入口
  vocab.py               NFKC 归一化、中英切分、哈希词表与未知基元回退
  normalize.py           阶段1：解码、尺寸校验、固定归一化与摘要
  metrics.py             阶段2：清晰度/色彩/结构保真
  semantic.py            阶段3：文本-图像提示词一致性
  fusion.py              阶段4：融合、分级、期望区间比对
  checkpoints.py         原子检查点、文件锁、单调序号、并发发布保护
  pipeline.py            四阶段编排、增量失效、停止恢复、摘要与诊断
  cli.py / __main__.py   命令行接口
  http_app.py            回环 HTTP 诊断接口
scripts/
  generate_samples.py    固定种子样本与元数据重建脚本
  demo.py                一键复现实验（全量/停止恢复/异常注入）
tests/                   pytest 测试套件（27 个用例）
work/                    默认本地工作目录（运行后生成）
```

工作目录布局：`images/`（PNG 分片）、`references/`（参考图）、`metadata.json`、
`checkpoints/`（每图每阶段一条记录）、`state/`（序号、文件锁、停止标志）、
`outputs/`（`summary.json` 固定摘要、`run_report.json` 运行态报告、`diagnostics.json`）。

## 3. 确定性样本

样本由 `np.random.default_rng(20261122)` 固定种子生成，共 6 张 48x36 RGB PNG 与 1 张参考图，
覆盖几何色块、渐变、条纹、噪声、全黑与尺寸声明不符场景：

```bash
python3 scripts/generate_samples.py work
# 或
python3 -m iqa --work-dir work generate
```

元数据字段：`image_id`、`file`、`prompt`（中英混排）、`declared_width/height`、
`reference_file`（可为 `null`）、`expected_range`、`operator`。
其中 `img_002_gradient` 的期望区间故意偏高以制造越界，`img_006_dim_mismatch` 声明宽度不符。

## 4. 四阶段设计

### 阶段1 解码与标准化（`iqa/normalize.py`）
- 校验实际宽高与声明一致，不符返回 `E_DIM_MISMATCH`，上下文含 `image_id/declared/actual`。
- 固定规则 `float64 = uint8 / 255.0`；输出宽高、通道数、有效像素数、逐通道均值/方差。
- 空图或含 NaN/Inf 像素返回 `E_NUMERIC`；全黑图像显式标记 `is_all_black=true`。
- 摘要经统一浮点舍入与规范化 JSON 序列化，同输入同配置逐字节一致。

### 阶段2 分维度指标（`iqa/metrics.py`）
- 清晰度：拉普拉斯响应的归一化方差 `var/(1+var)`。
- 色彩一致性：无参考时用色偏与饱和度分布；有参考时用每通道 8-bin 直方图 L1 距离。
- 结构保真：仅在参考图存在时计算局部梯度方向余弦一致性；缺参考时 `score=null` 并写明
  跳过原因，而不是填零。参考文件缺失返回 `E_MISSING_REFERENCE`。
- 每个指标都输出 `score`、`valid_pixels`、`skipped_reason`。

### 阶段3 提示词一致性（`iqa/semantic.py` + `iqa/vocab.py`）
- Unicode NFKC、全半角统一、空白折叠；按中文单字与拉丁连续串切分。
- 词项经 FNV-1a 哈希映射到颜色/纹理基元；词表缺失时回退到显式 `unknown` 基元并累计
  `fallback_count`，再从图像提取对应颜色亲和度或纹理统计量，合成 0~1 分数。
- 词表变化通过 `vocab_version` 进入阶段输入指纹。

### 阶段4 融合与分级（`iqa/fusion.py`）
- 跳过维度不参与；可用维度权重重新归一化并记录 `active_weight_sum`、`renormalized_weights`。
- 支持对数域（默认，几何平均）与线性域；输出各维度贡献 `contributions`。
- 阈值可配置，分 `excellent/good/poor` 三级；并列时按固定维度顺序确定性裁决，语义维度不参与破并。
- 可用维度全零或可用权重和为零返回 `E_NUMERIC`（含 `image_id` 与维度名）；负权重返回
  `E_INVALID_CONFIG`。最后与 `expected_range` 比对，输出越界清单与偏差量。

## 5. 业务码

| 业务码 | 触发阶段 | 含义 |
| --- | --- | --- |
| `E_DIM_MISMATCH` | 阶段1 | 声明尺寸与实际像素不符 |
| `E_MISSING_REFERENCE` | 阶段2 | 声明的参考图文件不存在 |
| `E_NUMERIC` | 阶段1/4 | NaN/空图，或全零分/零权重和 |
| `E_INVALID_CONFIG` | 配置/阶段4 | 负权重、阈值非法、区间非法等 |
| `E_STALE_CHECKPOINT` | 检查点发布 | 落后、重复或跨样本错位发布 |
| `E_INVALID_STAGE` | 检查点发布 | 前置阶段产物缺失或指纹不匹配 |

## 6. 检查点、增量失效与并发保护

- 每张图每阶段原子写入（临时文件 + `fsync` + `os.replace`），中断不会留下半写记录；记录含
  阶段名、`image_id`、输入指纹、配置版本、产物摘要与全局单调序号。
- 优雅停止：`stop.flag` 协作式标志，流水线在完成当前不可拆分样本单元（一次阶段发布）后退出；
  恢复后清除标志继续，序号连续推进，最终摘要与一次从零运行逐字节一致。
- 版本指纹：各阶段记录含 `input_fingerprint`、`product_digest`、`fingerprint`。图像/参考图/
  提示词/词表/权重/阈值变化会在最早受影响阶段被识别，该阶段及全部下游失效重建，未受影响上游复用。
- 并发发布：`fcntl.flock` 真实文件锁 + 样本内序号比较；落后/重复/跨样本错位返回
  `E_STALE_CHECKPOINT`，缺前置阶段返回 `E_INVALID_STAGE`。

## 7. 命令行用法

```bash
# 全量运行（默认对数域）
python3 -m iqa --work-dir work run
# 线性域 + 自定义权重/阈值
python3 -m iqa --work-dir work run --fusion-domain linear \
    --weights weights.json --thresholds thresholds.json
# 异常注入（四类）
python3 -m iqa --work-dir work run --inject nan_pixels --inject-image img_001_blocks
python3 -m iqa --work-dir work run --inject negative_weight --inject-image img_001_blocks
# 状态、版本指纹、逐维分数、诊断、停止、恢复
python3 -m iqa --work-dir work status
python3 -m iqa --work-dir work scores
python3 -m iqa --work-dir work diagnostics
python3 -m iqa --work-dir work stop
python3 -m iqa --work-dir work resume
```

## 8. HTTP 诊断接口（仅监听回环地址）

```bash
python3 -m iqa --work-dir work serve --host 127.0.0.1 --port 8765
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/status` | 各阶段状态、序号与指纹 |
| GET | `/fingerprints` | 版本指纹矩阵 |
| GET | `/scores` | 逐维分数、跳过原因与融合细节 |
| GET | `/diagnostics` | 业务码与越界/失效报告 |
| POST | `/run` | 触发分阶段评估（支持 `?injection=&image_id=`） |
| POST | `/stop` / `/resume` | 请求停止 / 清除标志并恢复 |
| POST | `/import` | 重新导入确定性样本 |

端口被占用时通过 `--port` 改写。

## 9. 一键复现实验与测试

```bash
python3 scripts/demo.py      # 全量 + 停止恢复逐字节比对 + 四类异常注入
python3 -m pytest            # 27 个用例
```

## 10. 实际验证结果

- 完整四阶段运行处理 6 张图，产生 20 条检查点（5 张有效图 × 4 阶段；尺寸不符图在阶段1终止）。
- 两次全量运行的 `outputs/summary.json` 逐字节一致；停止后恢复的摘要与全量运行逐字节一致，
  序号从 1 连续推进到 20。
- 四类异常注入分别稳定返回 `E_DIM_MISMATCH`、`E_MISSING_REFERENCE`、`E_NUMERIC`、
  `E_INVALID_CONFIG`，并保留 `image_id` 等诊断上下文。
- `pytest` 结果：**27 passed**。
