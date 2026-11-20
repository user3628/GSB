# 目标检测候选匹配与并行调度管线（本地确定性）

纯本地、无网络、无预训练模型的目标检测候选框匹配管线。输入是固定随机种子
（NumPy `default_rng`）生成的确定性 JSON 样本，输出按类别的 IoU 匹配结果、
来源算子质量统计、预算约束下的分片调度、可恢复的分片检查点，以及结果验证工具。

## 环境

- Python 3.10+（开发验证于 Python 3.14）、NumPy（`requirements.txt`）
- 仅使用标准库 `threading` / `json` / `hashlib`，锁使用 `fcntl`（POSIX）或
  `msvcrt`（Windows，独占字节锁），无需任何外部服务
- 不使用 Docker

```bash
pip install -r requirements.txt
```

## 目录结构

```
detpipe/
  errors.py       结构化错误码（E_INVALID_CONFIG / E_STALE_CHECKPOINT / ...）
  versioning.py   规范化 JSON + SHA-256 指纹（样本/阈值/预算版本）
  config.py       PipelineConfig 与严格参数校验
  generate.py     确定性样本生成器（6 图、3 类、3 算子、跨图重复候选）
  matching.py     归一化->像素 IoU、按类别贪心唯一匹配、稳定决胜
  stats.py        候选分类、类别/算子维度统计、按 image_id 确定性归约
  scheduler.py    总预算 + 每线程配额的评估前接纳控制
  checkpoint.py   原子写、跨平台文件锁、连续序号发布、停止信号、链恢复
  pipeline.py     本地线程池、在途去重、完成序缓冲发布、停止/恢复、rebase
  validator.py    从分片记录重建全局统计、指纹校验、失效可观测
scripts/
  generate_samples.py  生成样本
  run_pipeline.py      运行管线（CLI）
  validate_results.py  验证/查询（CLI）
  stop_inject.py       独立进程注入 STOP 文件
  demo.py              全流程确定性演示
tests/                 unittest 测试套件（24 个）
```

## 快速开始

```bash
# 1. 生成确定性样本
python3 scripts/generate_samples.py --out run/samples.json

# 2. 运行管线（并行，3 线程，每 2 张图一个分片）
python3 scripts/run_pipeline.py \
  --samples run/samples.json --workspace run/ws \
  --workers 3 --shard-size 2 --report run/report.json

# 3. 验证持久化结果并重建全局统计
python3 scripts/validate_results.py --workspace run/ws

# 4. 一键完整演示（完整运行/并行一致性/停止恢复/错误码/版本失效复用）
python3 scripts/demo.py
```

## 关键设计

### IoU 匹配（`matching.py`）
- 归一化 `xywh` 先按图像宽高换算为像素框，再计算交并比
- 逐真值框在同类别候选中贪心选择 IoU 最高且未被占用的候选；
  排序键为 `(-iou, -score, candidate_index)`，同分同 IoU 用候选索引稳定决胜
- 输出匹配对、未匹配真值、重复候选、背景候选（高分低 IoU）、低置信候选
- 阈值默认 0.5，可通过 `--iou-threshold` / `--score-threshold` 配置

### 统计（`stats.py`）
- 未匹配候选三分：`duplicate`（IoU 达标但落选）、`background`（误报 FP）、
  `low_confidence`（低分噪声），三者与 matched 之和等于候选总数
- 同时输出 `by_class` 与 `by_source`（命中 hits、误报 false_positives、
  duplicates、low_confidence、avg_iou）
- 归约前按 `image_id` 全局排序，并行与单线程输出逐字节一致

### 预算调度（`scheduler.py` / `pipeline.py`）
- 分片以图像为单位；接纳发生在任何候选评估之前，单锁原子记账
- 同时约束全局 `max_evaluations` 与每线程 `worker_quota`
- 超全局预算或所有线程配额都放不下的分片，以 `status=unprocessed` 落盘并带原因码
  （`E_BUDGET_EXHAUSTED` / `E_WORKER_QUOTA`），累计评估永不超预算

### 并行与确定性（`pipeline.py`）
- 本地线程池；领取分片时维护在途集合去重，完成结果进入“完成序缓冲”，
  仅当下一分片就绪时按序号连续发布，保证磁盘记录严格连续
- 工作线程不使用超时杀线程；停止只在分片之间被观测，当前不可拆分分片必须做完
- 工作线程异常被捕获为结构化 `E_WORKER_FAILED` 错误对象回传

### 持久化、停止与恢复（`checkpoint.py`）
- 每个分片通过 `os.replace` 原子写为 `shards/shard_000001.json`
- 记录含分片序号、样本指纹、阈值版本、预算版本与结果摘要
- 停止：进程内回调或独立进程创建 `<workspace>/STOP`（见 `stop_inject.py`）
- 恢复：`recover()` 校验序号连续性与全部指纹，截断首个损坏/失配分片及其尾部，
  已发布分片不重算

### 版本变化检测（`versioning.py` / `pipeline.rebase` / `validator.py`）
- 样本内容、IoU/得分阈值、预算配置分别有独立 SHA-256 指纹
- `rebase()` 识别受影响分片：单图变化只失效其所在分片，其余复用；
  阈值/预算变化影响全部分片
- 验证接口对陈旧分片返回 `E_VERSION_MISMATCH` 并定位到具体字段，
  禁止用旧版本结果冒充新版本

### 并发发布保护与错误码
- 跨进程发布在 `shards/.lock` 上用独占锁串行化
- 仅允许发布“下一个连续序号”：落后/重复 -> `E_STALE_CHECKPOINT`，
  缺前置分片 -> `E_INVALID_STAGE`；错误均携带 `shard_index`
- 预算非法或阈值越界 -> `E_INVALID_CONFIG`，携带字段名 `field`

| 错误码 | 触发场景 | 携带定位 |
| --- | --- | --- |
| `E_INVALID_CONFIG` | 预算为负、阈值越界、workers/shard_size 非法 | `field` |
| `E_STALE_CHECKPOINT` | 重复/落后序号发布 | `shard_index` |
| `E_INVALID_STAGE` | 跳号发布、缺前置分片 | `shard_index` |
| `E_VERSION_MISMATCH` | 样本/阈值/预算指纹不一致 | `field`/`shard_index` |
| `E_BUDGET_EXHAUSTED` | 分片放不下全局预算 | `shard_index` |
| `E_WORKER_QUOTA` | 分片放不下任何线程配额 | `shard_index` |
| `E_WORKER_FAILED` | 工作线程内部异常 | `shard_index` |

Windows 上文件锁通过内置 `msvcrt.locking` 对同一 `.lock` 文件加独占字节锁实现，
与 POSIX 的 `fcntl.flock` 行为对齐；两条代码路径都只依赖标准库。

## 验证结果（实际执行）

- `python3 -m unittest discover -s tests`：24 个测试全部通过，覆盖 IoU 匹配、
  稳定决胜、候选分类、统计分区、预算/配额、原子写、发布错误码、跨进程竞态、
  并行/顺序逐字节一致、停止恢复、rebase 部分复用
- 演示固定数据集（seed=20260520）完整运行全局摘要的规范化统计 SHA-256：
  `4e37b13a198b12148cda7e02839410e01cd1d0dfbdf95769f611174113471b52`
  （参考字节保存在 `run/expected_global_stats.json`，由验证流程生成）
- 已实测：并行(3 线程)与单线程统计 `report_bytes` 完全相同；
  分片间停止后恢复的统计与一次完整运行逐字节一致；
  预算 30 时累计评估 23 且后续分片标记 `E_BUDGET_EXHAUSTED`；
  单图变更后仅分片 2 失效、分片 0/1 复用，重建结果与全新运行一致
