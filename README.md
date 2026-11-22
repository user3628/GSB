# SegVal 本地实例分割预测校验与并行调度工作台

这是一个从零构建、完全本地运行的实例分割结果校验系统。它不依赖真实模型、预训练权重、GPU、外部数据集或网络下载。输入是固定随机种子生成器产出的 JSON 分片；真值只来自这些本地分片。

工作台提供：

- 归一化多边形解码、鞋带公式面积、像素包络框、越界/顶点不足/零面积/自交诊断。
- 按类别和 IoU 的唯一贪心匹配，区分命中、漏检、重复预测与背景误报。
- 按分片、类别、来源算子聚合质量指标；分母为零时显式返回 `N/A` 并计数。
- 全局预算、单工作进程配额、并行进程上限三重约束下的分片接纳与调度。
- 本地 `spawn` 进程池执行；并行与单进程导出逐字节一致。
- 原子阶段检查点、不可变单调序号日志、跨进程真实文件锁、协作式停止与恢复。
- 基于阶段输入指纹的增量失效传播，以及增量与全量构建的逐字节校验。
- CLI、仅监听回环地址的 HTTP API，以及内置单页可视化诊断台。

## 快速开始

要求 Python 3.10+、NumPy；无需其他第三方包。

```bash
python3 -m pip install -r requirements.txt

# 1. 生成固定种子演示数据（4 个分片，sparse/medium/dense/mixed 四种密度）
python3 -m segval.cli generate-demo --out demo_data --seed 20261123 --shards 4

# 2. 初始化工作区、导入分片并以 2 个进程完整运行
python3 -m segval.cli setup-demo \
  --workspace .workbench/demo \
  --data-dir demo_data \
  --run --workers 2

# 3. 启动可视化工作台，只监听 127.0.0.1
python3 -m segval.cli serve --workspace .workbench/demo --host 127.0.0.1 --port 8765
```

浏览器打开 `http://127.0.0.1:8765/`。也可以直接使用 `python3 run_workbench.py <子命令>`。

## 演示数据

`demo_data/` 已包含生成的 4 个分片：

| 分片 | 密度 | 样本 | 预测 |
|---|---|---:|---:|
| `shard-0001` | sparse | 2 | 4 |
| `shard-0002` | medium | 3 | 16 |
| `shard-0003` | dense | 3 | 43 |
| `shard-0004` | mixed | 2 | 33 |

类别包含 `person`、`car`、`bicycle`、`dog`。数据同时包含命中、漏检、高置信重复、低置信背景误报、顶点不足、自交、零面积和越界预测。生成器位于 `segval/demo_generator.py`，相同 seed 与分片序号输出逐字节一致。

单个分片结构：

```json
{
  "schema_version": 1,
  "shard_id": "shard-0001",
  "shard_index": 1,
  "density_label": "sparse",
  "samples": [
    {
      "image_id": "img-0001-0000",
      "width": 640,
      "height": 480,
      "ground_truth": {
        "person": [[[0.12, 0.20], [0.30, 0.20], [0.30, 0.52], [0.12, 0.52]]]
      },
      "predictions": [
        {
          "prediction_id": "p-0001-0000-0000",
          "class_name": "person",
          "confidence": 0.91,
          "polygon": [[0.13, 0.21], [0.29, 0.21], [0.29, 0.51], [0.13, 0.51]],
          "source_operator": "augment-rotate"
        }
      ]
    }
  ]
}
```

## 核心流程

### 1. 解码与几何

- 归一化 `(x, y)` 按样本宽高换算：`x * width`、`y * height`。
- 面积使用 NumPy 数组上的鞋带公式；包络框为像素域 `[min_x, min_y, max_x, max_y]`。
- 越界策略：`clamp` 钳制到画面并记录诊断；`reject` 标记实例无效。
- 顶点少于 3、面积为零、自交、非有限坐标均记录原因；无效实例不参与匹配。
- 面积、坐标、包络框按配置精度量化，负零归一为 `0.0`，重复运行逐字节稳定。

### 2. 唯一匹配

对每个样本、每个类别：

1. 在有效真值与同类别有效预测之间计算像素域多边形 IoU。
2. 按 `(-IoU, ground_truth_id, prediction_id)` 排序；这是同分时的确定性仲裁。
3. 按 IoU 从高到低贪心，真值和预测各最多使用一次。
4. `IoU >= iou_threshold` 为命中；未匹配真值为漏检。
5. 未匹配有效预测若置信度 `>= duplicate_confidence_threshold` 记为重复预测，否则记为背景误报。

默认策略 `greedy_global_iou` 在 IoU 完全相同时按真值 ID、预测 ID 仲裁；配置可切换为 `greedy_global_iou_confidence_tiebreak`，在 IoU 同分时先比较预测置信度，再按 ID 仲裁。两种策略都保证重复运行稳定。

IoU 的交集面积由“耳切三角化 + 凸多边形裁剪”计算，不把多边形近似为包围框。默认 IoU 阈值为 `0.5`。

### 3. 聚合

输出 `by_shard`、`by_shard_class`、`by_shard_class_source`、`by_class` 和 `totals`。统计包含：

- `hits`、`misses`、`duplicates`、`background_false_positives`；
- `eligible_ground_truth`、`eligible_predictions`、无效实例数；
- `hit_rate`、`miss_rate`、`duplicate_rate`、`background_false_positive_rate`、`average_iou`；
- 分母为零时值为 `null`、状态为 `N/A`，并列入 `na_metrics`、计入 `na_metric_count`。

漏检没有预测来源算子，因此在来源维度归入哨兵行 `__unmatched__`，避免被重复分配到多个真实来源。聚合严格从按 `shard_index,image_id,class_name` 排序后的样本级明细累加。

### 4. 预算调度

每个分片接纳前的保守预估值：

```text
estimate = sample_count + sum_over_samples(total_ground_truth_count * prediction_count)
```

真实候选评估数只统计同类别有效对，因此不会超过预估值。调度在以下三重约束下按 `shard_index` 升序接纳：

- 全局 `max_evaluations`；
- 每个逻辑 worker lane 的 `per_worker_quota`；
- `max_workers` 并行上限。

未接纳分片会给出 `E_GLOBAL_BUDGET_EXHAUSTED`、`E_PER_WORKER_QUOTA_EXCEEDED` 或 `E_WORKER_CAPACITY_EXHAUSTED` 及原因。任何时刻 `reserved_evaluations <= max_evaluations`，且真实累计评估数也受总预算约束。

## 检查点、停止与恢复

每个分片有 `decode`、`match`、`aggregate` 三个检查点。发布时：

- 获取 `.checkpoint.lock`：POSIX 使用 `fcntl.flock`，Windows 使用 `msvcrt.locking`。
- 校验前置阶段；缺失前置返回 `E_INVALID_STAGE`。
- 校验分片内容未被并发更新；重复或落后发布返回 `E_STALE_CHECKPOINT`。
- 分配全局单调 `sequence`，先写不可变 `checkpoints/_journal/<sequence>.json`，再原子更新当前阶段快照。
- 临时文件与目标同目录，`fsync` 后 `os.replace`；中断不会留下半写 JSON。

收到停止请求后，worker 在每个不可拆分样本边界检查标志。当前样本完成后退出，不发布不完整的匹配/聚合阶段；已完成的解码检查点保留。`resume` 清除停止标志并继续，最终导出与从零构建一致。

## 增量失效

阶段输入指纹：

| 阶段 | 指纹输入 |
|---|---|
| decode | 分片完整内容摘要 + 解码配置 |
| match | decode 产物摘要 + 类别集合 + IoU 阈值 + 匹配策略 + 重复置信阈值 |
| aggregate | match 产物摘要 + 聚合 schema |
| schedule | 预算配置 + 接纳/未处理分片 + 聚合摘要 |

任一输入变化只使最早受影响阶段及下游失效。上游检查点继续复用；失效阶段、原因、期望/现有指纹和序号可在 `fingerprints`、`status`、HTTP `/api/fingerprints` 中查询。

```bash
# 导入修改后的单分片，只重建该分片失效链路
python3 -m segval.cli update-shard \
  --workspace .workbench/demo \
  --file runtime/shard-0003-updated.json \
  --workers 2

# 与临时全新工作区的全量构建逐字节比较
python3 -m segval.cli verify-incremental --workspace .workbench/demo
```

若不一致，返回 `E_INCREMENTAL_MISMATCH`，`details.first_differences` 包含首个差异的 `byte_offset`、行列、上下文和两侧摘要。`verify-incremental --inject-mismatch` 可显式验证该错误路径。

## CLI

```text
init                         初始化工作区
generate-demo                生成固定种子分片
setup-demo                   生成/导入演示数据，可立即运行
import-shard                 校验并原子导入分片
update-shard                 单分片更新并只重建失效链
run --stage ...              触发 all/decode/match/aggregate/schedule
status                       状态、预算、失效链、聚合摘要
fingerprints                 阶段版本指纹与失效原因
budget                       接纳/未处理/lane 配额
samples                      解码多边形与匹配分类（供可视化）
stop / resume                协作式停止与恢复
export                       导出 matches/aggregates/schedule/diagnostics/bundle
verify-incremental           增量-全量逐字节一致性校验
config-show / config-set     查看或原子更新配置
serve                        启动回环 HTTP 工作台
doctor                       检查 Python/NumPy/锁/spawn/原子替换/回环
probe-checkpoints            注入 E_INVALID_STAGE / E_STALE_CHECKPOINT 并并发发布
```

示例：

```bash
python3 -m segval.cli status --workspace .workbench/demo
python3 -m segval.cli budget --workspace .workbench/demo
python3 -m segval.cli export --workspace .workbench/demo --kind matches --output runtime/matches.json
python3 -m segval.cli stop --workspace .workbench/demo --reason demo_stop
python3 -m segval.cli resume --workspace .workbench/demo --workers 2
python3 -m segval.cli probe-checkpoints
python3 -m segval.cli doctor
```

## HTTP API

服务器只接受回环地址，其他 host 返回 `E_INVALID_HOST`。所有响应为 JSON，入口页为无外部依赖的单文件 HTML/JS 可视化台。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/` | 可视化工作台 |
| GET | `/api/status` | 状态、预算、无效链、聚合摘要 |
| GET | `/api/fingerprints` | 配置与阶段指纹 |
| GET | `/api/budget` | 预算裁决与未处理原因 |
| GET | `/api/samples` | 样本多边形和命中/重复/误报分类 |
| GET | `/api/export?kind=...` | 导出结果 |
| GET | `/api/doctor` | 环境能力检查 |
| POST | `/api/shards` | 导入分片 |
| POST | `/api/update-shard` | 单分片增量更新 |
| POST | `/api/run` | `{"stage":"all","workers":2}` |
| POST | `/api/stop` | 请求停止 |
| POST | `/api/resume` | 恢复 |
| POST | `/api/config` | 配置增量补丁 |
| POST | `/api/verify-incremental` | 增量-全量校验 |
| POST | `/api/init-demo` | 导入 4 个内置演示分片 |
| POST | `/api/probe-checkpoints` | 检查点故障注入探测 |

可视化台提供预算/失效链/分片阶段/类别/来源算子表格，以及 GT 与预测多边形的颜色叠加诊断。

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_INVALID_IOU_THRESHOLD` | IoU 阈值非法 |
| `E_INVALID_CONFIDENCE_THRESHOLD` | 重复置信阈值非法 |
| `E_INVALID_MATCH_STRATEGY` | 未知匹配策略 |
| `E_INVALID_BUDGET` | 预算字段为负或 worker 上限非法 |
| `E_INVALID_CONFIG` / `E_INVALID_OOB_POLICY` | 配置不合法 |
| `E_INVALID_SHARD` | 分片结构不合法 |
| `E_INVALID_STAGE` | 缺少前置阶段或阶段顺序错误 |
| `E_STALE_CHECKPOINT` | 重复/落后发布或分片被并发更新 |
| `E_INCREMENTAL_MISMATCH` | 增量与全量导出不一致 |
| `E_GLOBAL_BUDGET_EXHAUSTED` | 分片未接纳：剩余全局预算不足 |
| `E_PER_WORKER_QUOTA_EXCEEDED` | 分片预估超过单 worker 配额 |
| `E_WORKER_CAPACITY_EXHAUSTED` | 所有 worker lane 均无法容纳 |
| `E_WORKER_CRASH` | 工作进程异常的结构化记录 |
| `E_WORKSPACE_BUSY` | 真实文件锁被其他运行占用 |

## 测试与验证

```bash
python3 -m unittest discover -v
python3 scripts/run_validation.py
```

当前实际结果：

- `27` 项单元测试全部通过。
- 串行与 2 进程并行的 `matches.json`、`aggregates.json`、`schedule.json` 逐字节一致。
- 停止后恢复的 `matches.json`、`aggregates.json` 与从零完整构建逐字节一致。
- 增量更新只重建 `decode -> match -> aggregate` 失效链，增量与全量摘要一致。
- 检查点并发发布得到连续序号 `1,2,3,4`；缺失前置和重复发布分别返回 `E_INVALID_STAGE`、`E_STALE_CHECKPOINT`。
- 注入预算 `180` 时只接纳预估为 `10` 和 `47` 的两个分片；实际评估 `28`，未超过预算。
- 非法阈值返回 `E_INVALID_IOU_THRESHOLD`；负预算返回 `E_INVALID_BUDGET`。

完整机器可读结果见 `VALIDATION_RESULTS.json`，人工摘要见 `VALIDATION.md`。

## 跨平台说明

运行时代码没有容器依赖，进程池显式使用 `spawn`，文件锁按平台选择 `fcntl.flock` 或 `msvcrt.locking`，原子发布使用 `os.replace`。`doctor` 会报告当前后端和能力。本次实际验证环境为 WSL2/Linux、Python 3.14.4、NumPy 2.2.1；当前工作区没有 Windows 执行环境，因此 Windows 分支已实现但未在真实 Windows 内核上执行。`doctor` 的 Python 版本、NumPy、锁、`spawn`、原子替换与回环绑定检查均通过。
