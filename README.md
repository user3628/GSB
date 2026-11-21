# Deterministic Pose Pipeline

一个完全本地、确定性的四阶段关键点与姿态估计数据处理管线。它使用固定随机种子生成的 PNG 人体简笔画和 JSON 真值清单，完成热图生成、峰值解码、骨架装配、量化验证与增量失效传播。实现不依赖深度学习框架、预训练权重、GPU、Docker 或外部服务。

核心特性：

- 5 个本地样例分片、7 个姿态实例、103 个可见关键点、2 个不可见关键点，来源标签中英混排。
- 四阶段产物均使用内容指纹和不可变摘要；变更会从最早受影响阶段向下游精确失效。
- 检查点使用原子替换、全局连续序号和记录链校验；损坏、半写、重复序号记录会被忽略。
- 发布使用真实跨进程文件锁（POSIX `flock` / Windows `msvcrt.locking`），过期发布返回 `E_STALE_CHECKPOINT`。
- 阶段间停止后恢复的最终 `report.json` 和 `poses.json` 与一次完整运行逐字节一致。
- 同时提供 CLI 与标准库 HTTP JSON API。

## 环境与快速开始

要求 Python 3.10+、NumPy、Pillow。

```bash
# 在仓库根目录直接运行即可，无需安装第三方 CLI 框架

# 生成确定性样例（同 seed 同文件字节）
python3 -m pose_pipeline generate --out examples/demo_data

# 导入并全量构建
python3 -m pose_pipeline build \
  --workspace demo_workspace \
  --manifest examples/demo_data/manifest.json

# 查看状态、指纹、失效原因
python3 -m pose_pipeline status --workspace demo_workspace
python3 -m pose_pipeline fingerprints --workspace demo_workspace
python3 -m pose_pipeline invalidations --workspace demo_workspace

# 导出姿态结果与验证报告
python3 -m pose_pipeline export --workspace demo_workspace --out demo_export

# 运行完整验收演示
python3 scripts/run_demo.py --out demo_workspace

# 运行测试
python3 -m unittest discover -s tests -v
```

如需安装命令行入口，可执行 `python3 -m pip install -e .`；直接使用 `python3 -m pose_pipeline` 不依赖安装步骤。

样例数据固定标识：

```text
dataset_digest = 677037a02e2a989ca6cd0bddf58164ffa6090f8bd54d679aa70bf1430c33c84e
baseline report_digest = 7fcc54df67bdf80895efa4735607d68e7bfc93b1f417fb94619aebc5f63485ae
```

## 四阶段语义

| 阶段 | 输入 | 输出 | 失效起点条件 |
|---|---|---|---|
| `preprocess` | PNG 内容、图像尺寸、生成参数、真值坐标、逐实例可见性、高斯半径、对比度、热图尺寸 | 灰度/对比度图、逐关键点 `float32` 热图、不可见标记 | 图像/真值/可见性/生成参数/预处理配置变化 |
| `decode` | 已提交预处理产物、候选上限、峰值阈值、亚像素质心半径 | 带分片、关键点、像素/归一化坐标、响应值的候选和空记录 | 解码配置变化或上游失效 |
| `assembly` | 已提交解码产物、骨架、图像尺寸、几何约束 | 姿态实例、剔除实例、违规连接对和偏差 | 骨架定义或几何参数变化 |
| `validation` | 已提交装配产物、清单真值、来源标签、置信区间、匹配阈值 | 逐实例/逐关键点误差、RMSE、区间越界标记、分组摘要 | 区间、来源标签、坐标约定、验证配置变化 |

坐标约定为 `normalized_topleft_xy`。像素坐标换算为 `normalized * (size - 1)`，图像原点在左上角。

### 热图与解码

- 可见真值关键点以 `gaussian_radius` 为高斯标准差、`3*sigma` 截断生成热图；同关键点有多实例时取逐像素最大值。
- 某关键点在所有实例中均不可见时，热图为全零，并写入 `invisible=true`、`zero=true` 和 `invisible_keypoints`。
- 局部极大值使用 3x3 邻域与阈值检测；响应值降序，同值以关键点名称和坐标决胜。
- 亚像素修正使用峰值邻域的响应加权质心。
- 每个关键点即使没有候选也保留 `empty=true` 记录，原因可能为 `invisible_zero_heatmap`、`zero_response`、`no_local_maximum` 或 `candidate_limit_zero`。

### 骨架装配

- 在骨架相邻关键点的候选之间建立关联边，边超过 `association_max_edge_fraction * image_diagonal` 时不连接。
- 连通分量形成姿态实例；同一分量出现重复关键点名视为多实例冲突，返回 `E_POSE_INVALID`。
- 有效关键点低于 `min_valid_keypoints`、骨架含未知关键点名、实例明显重叠均返回 `E_POSE_INVALID`，装配阶段不写半成品。
- 对称边长度比例和关节角度违反约束的实例会被剔除，同时记录连接对、观测值和偏差量；该情况不导致整阶段错误。

### 量化与指标

- 预测实例与清单真值实例按归一化坐标平方误差做确定性一一匹配。
- RMSE 同时输出像素与归一化坐标版本；误差只在“真值可见且有显式候选”的关键点上计算。
- 缺失候选标记为 `missing_candidate`，不插值、不推断；不可见真值标记为 `ignored_invisible`。
- 姿态置信度为显式匹配关键点响应均值。实例置信度或单关键点响应落在清单期望区间外时，记录越界实例和 `out_of_interval_keypoints`。
- 分组统计按来源标签、关键点名称以及二者组合输出；命中要求误差不超过 `hit_error_px` 且响应在期望区间内。缺失候选计入命中率分母。

## 清单格式

完整样例见 `examples/demo_data/manifest.json`。最小结构如下：

```json
{
  "schema_version": 1,
  "coordinate_convention": "normalized_topleft_xy",
  "generation": {"seed": 20261122},
  "keypoints": ["head", "neck", "left_wrist"],
  "skeleton": [["head", "neck"]],
  "shards": [
    {
      "shard_id": "shard_01",
      "image": "images/shard_01.png",
      "width": 128,
      "height": 128,
      "source_label": "中文源 / studio-en",
      "keypoints": ["head", "neck", "left_wrist"],
      "skeleton": [["head", "neck"]],
      "expected_pose_confidence": {"min": 0.4, "max": 1.0},
      "instances": [
        {
          "instance_id": "pose_0001",
          "keypoints": {
            "head": {"x": 0.5, "y": 0.2, "visible": true},
            "neck": {"x": 0.5, "y": 0.3, "visible": true},
            "left_wrist": {"x": 0.35, "y": 0.6, "visible": false}
          },
          "expected_pose_confidence": {"min": 0.5, "max": 1.0}
        }
      ]
    }
  ]
}
```

- 分片级 `skeleton` 存在时覆盖顶层 `skeleton`；生成器为每个分片显式写出骨架。
- 分片级置信区间是默认值，实例级置信区间优先。
- `visible=false` 的真实关键点仍保留坐标，但不会绘制/生成热图，也不会计算误差。

## 配置

默认配置见 `pose_pipeline/config.py`，常用项：

| 字段 | 默认值 | 作用 |
|---|---:|---|
| `gaussian_radius` | `2.2` | 热图高斯标准差，必须非负 |
| `contrast_factor` | `1.10` | 灰度对比度扰动因子 |
| `heatmap_width/height` | `null` | 覆盖热图尺寸；必须与图像尺寸相同 |
| `max_candidates` | `4` | 每个关键点最多候选数 |
| `peak_threshold` | `1e-6` | 局部极值响应阈值 |
| `subpixel_radius` | `2` | 亚像素质心邻域半径 |
| `min_valid_keypoints` | `5` | 实例有效关键点下限 |
| `association_max_edge_fraction` | `0.23` | 候选关联最长边占图像对角线比例 |
| `symmetry_ratio_tolerance` | `0.30` | 对称边长度相对偏差容忍度 |
| `min/max_joint_angle` | `5/180` | 关节角度约束 |
| `hit_error_px` | `1.5` | 关键点命中误差阈值 |
| `round_digits` | `10` | 验证摘要舍入位数；中间产物固定 10 位，不随此配置变化 |

JSON 配置可使用部分字段，例如：

```bash
python3 -m pose_pipeline build --workspace demo_workspace \
  --config examples/configs/negative_radius.json
```

非法注入配置位于 `examples/configs/`：负高斯半径、热图尺寸不一致、过大多实例关联半径、过低有效关键点下限。

## 版本指纹与增量失效

每个阶段输入指纹由规范化 JSON 计算 SHA-256。阶段指纹显式覆盖：

- `preprocess`：图像 SHA-256、声明尺寸、生成参数、真值坐标、可见性、关键点集合、预处理配置版本。
- `decode`：已提交预处理产物摘要、解码配置版本。
- `assembly`：已提交解码产物摘要、骨架、关键点集合、图像尺寸、空间关联与几何配置版本。
- `validation`：已提交装配产物摘要、真值、来源标签、期望置信区间、坐标约定、验证配置版本。

构建时按阶段顺序比较当前指纹与已提交记录。第一个不匹配阶段即“最早受影响阶段”，该阶段及下游进入计划；上游已提交产物继续复用。`invalidations` 保留本次构建起点的原因、受影响分片、计划和最终最新指纹。

```bash
python3 -m pose_pipeline invalidations --workspace demo_workspace
python3 -m pose_pipeline invalidations --workspace demo_workspace --refresh
```

`--refresh` 用于在尚未构建时查看待发生失效；默认读取最近一次构建报告。

## 检查点、停止与恢复

每次“分片 + 阶段”提交产生一个不可变记录：

```json
{
  "sequence": 1,
  "shard_sequence": 1,
  "shard_id": "shard_01",
  "stage": "preprocess",
  "input_fingerprint": "...",
  "config_version": "...",
  "artifact_digest": "...",
  "upstream_digest": null,
  "previous_record_digest": "",
  "record_digest": "..."
}
```

- 记录和产物均使用同目录临时文件、`fsync`、`os.replace` 原子提交。
- 恢复只接受从序号 1 开始、`previous_record_digest` 连续匹配的最长有效链；损坏、半写、重复和断链记录被忽略。
- `sequence` 全局连续推进；`shard_sequence` 是分片内阶段序号；阶段提交必须存在匹配的上一阶段产物。
- 停止请求在每个原子“分片阶段”之间检查；当前阶段会完成。`resume` 先清除停止标记再继续。
- 手工演示停止后可恢复：

```bash
python3 -m pose_pipeline stop --workspace demo_workspace
python3 -m pose_pipeline resume --workspace demo_workspace
```

测试钩子 `--stop-after-units N` 可确定性地在第 N 个单元后停止，用于逐字节一致性验证。

## CLI

```text
pose-pipeline generate     生成确定性 PNG、manifest、ground_truth
pose-pipeline import       导入清单与图片到工作区
pose-pipeline build        全量或增量构建；支持 --to-stage、--stop-after-units
pose-pipeline status       查看阶段状态、检查点、导出状态
pose-pipeline fingerprints 查看当前版本指纹
pose-pipeline invalidations 查看失效原因与最新指纹
pose-pipeline errors       查看持久化业务错误
pose-pipeline stop         请求优雅停止
pose-pipeline resume       清除停止标记并恢复
pose-pipeline export       导出 poses.json 与 report.json
pose-pipeline serve        启动本地 HTTP API
pose-pipeline demo         运行完整验收演示
```

严格单阶段构建用于验证非法跳转：

```bash
pose-pipeline build --workspace demo_workspace \
  --strict-shard shard_01 --strict-stage validation
# 缺少前置阶段时返回 E_INVALID_STAGE，并附 current_stage
```

## HTTP API

```bash
python3 -m pose_pipeline serve --workspace demo_workspace --host 127.0.0.1 --port 8765
```

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/import` | `{"manifest_path": "..."}` |
| `POST` | `/api/generate` | `{"out_dir": "...", "seed": 20261122}` |
| `POST` | `/api/build` | 支持 `to_stage`、`stop_after_units`、`strict_*`、`background` |
| `POST` | `/api/stop` | 请求停止 |
| `POST` | `/api/resume` | 清除停止并恢复 |
| `GET` | `/api/status` | 各阶段状态、检查点、当前导出状态 |
| `GET` | `/api/fingerprints` | 最新分阶段指纹 |
| `GET` | `/api/invalidations` | 最近失效原因；`?refresh=1` 重新检查 |
| `GET` | `/api/keypoint-errors` | 展平的逐关键点误差 |
| `GET` | `/api/errors` | 持久化错误 |
| `GET` | `/api/poses` | 姿态导出 |
| `GET` | `/api/report` | 验证报告 |
| `GET` | `/api/checkpoints` | 已接受记录链与诊断 |
| `GET` | `/api/uncommitted` | 已完成但发布被拒、仍未提交的产物 |
| `POST` | `/api/export` | 可选 `out_dir` 导出文件 |

后台构建示例：

```bash
curl -s -X POST http://127.0.0.1:8765/api/build \
  -H 'Content-Type: application/json' \
  -d '{"background": true, "stop_after_units": 7}'
curl -s http://127.0.0.1:8765/api/status
curl -s -X POST http://127.0.0.1:8765/api/resume -d '{}'
```

## 业务错误码

| 错误码 | 场景 |
|---|---|
| `E_INVALID_PARAM` | 负高斯半径、热图尺寸不一致、非法阶段内参数；含 `stage` 与 `shard_id` |
| `E_POSE_INVALID` | 未知骨架关键点、有效关键点不足、多实例冲突；含违规详情 |
| `E_INVALID_STAGE` | 阶段跳转、缺失前置产物；`details.current_stage` 给出当前已达阶段 |
| `E_STALE_CHECKPOINT` | 序号落后/跳号、重复发布、跨分片/阶段错位发布 |
| `E_INVALID_MANIFEST` | 清单结构、坐标、可见性、区间或路径非法 |
| `E_INVALID_CONFIG` | 配置 JSON 字段或类型非法 |
| `E_NOT_FOUND` | 工作区、清单、图片或未生成导出不存在 |

## 确定性说明

- 所有 JSON 使用 UTF-8、键排序、紧凑分隔符和固定浮点舍入写入摘要；导出使用固定键排序的缩进 JSON。
- 算术仅依赖 NumPy/Pillow 本地 CPU 实现；没有时间戳、随机 UUID、进程号或绝对工作区路径进入最终导出。
- 相同清单、图像、配置、Python/NumPy/Pillow 版本下，全量构建与停止恢复构建的最终产物逐字节一致。
- 验证摘要中的 `report_digest` 覆盖除自身外的全部报告字段；`poses.json` 单独带 `export_digest`。

## 文档与验证

- 详细设计：`docs/DESIGN.md`
- 实际执行结果：`VALIDATION.md`
- 自动测试：`tests/test_pipeline.py`、`tests/test_concurrency.py`
- 演示脚本：`scripts/run_demo.py`
- 示例数据：`examples/demo_data/`

```bash
python3 -m unittest discover -s tests -v
python3 scripts/run_demo.py --out demo_workspace
```
