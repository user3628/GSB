# 实际验证记录

验证日期：2026-11-23（Asia/Shanghai）。所有验证均在当前本地工作区执行，无 Docker、无网络访问。

## 环境

```text
Python 3.14.4
NumPy 2.2.1
Linux
```

`python3 -m pipeline.cli doctor --workspace .` 验证 Python 版本、NumPy 导入、工作区写权限和 `flock` 文件锁，返回 `status: ok`。

## 数据生成与结构核对

```bash
python3 -m pipeline.cli generate-data --out data --seed 20261123
python3 -m pipeline.cli verify-data --index data/index.json --output data/verification.json
```

实际结果：

```text
batch_count: 4
row_count: 8800
tier_counts: {"large": 8000, "small": 800}
empty_doc_count: 22
passed: true
```

各批类别数分别为 4、4、7、7；doc_id 全局唯一；全部批次 `content_digest_matches_index: true`。

## 完整两档运行

```bash
python3 -m pipeline.cli run \
  --workspace demo_workspace \
  --config data/config.yaml \
  --data data/index.json
```

实际结果：

```text
status: ok
doc_count: 8800
accepted_batches: batch_01_small, batch_02_small, batch_03_large, batch_04_large
rejected_batches: []
checkpoint_sequence: 16
results_digest: 46de86a3392706d57a1efade512ea2adfb2d1439d7dff3f79c5332c11d6d90f8
report_digest: 1d1fe103eb206f52a8d02a66e6e3a37ba5fc1d9bdbe1f2831a7bac87418c930d
```

汇总：

```text
metric_eligible docs: 8778
correct: 8768
accuracy: 0.9988607883344726
weighted precision: 0.9978039798641846
weighted recall: 0.9990528381029701
weighted F1: 0.9984280184581058
E_EMPTY_DOC: 22
review_required: 19
calibration_review_class_count: 2
```

所有空正文均保留为 `decision: ERROR`、`predicted_label: null`、`error_code: E_EMPTY_DOC`，未从缺失信息推断类别；每条的 `score_evidence` 仍指向显式分数行。两个小批的稀有 `fraud` 类因校准正样本低于下限被标记 `E_CALIBRATION_LOW_SAMPLES`，相关预测保留但标记复核。

最终产物 SHA-256：

```text
4f78fbfa0b9c0c615151582e68709addfd9e3a9e823b30be44598004589f7f33  demo_workspace/results.jsonl
035fc1a0825eb19afb12087587e6a6fb1930d2810a0f564eab4eff387176bcbb  demo_workspace/summary.json
7e80bba9bbcf1a099aced82ac204a3b45f977755dfd1947ae78162619021db88  demo_workspace/calibrations.json
6870a2a555f5b86dcc428cb748361ec8af184a2771227c694ac330b5df196877  demo_workspace/parameters.json
c4dade599bbb7e97102e4a9ebd24da4b1673c941f9ed7fb7f0b8ac8bd3f5c0e6  demo_workspace/report.json
```

检查点完整性：

```text
record_count: 16
first_sequence: 1
last_sequence: 16
contiguous: true
```

重复运行同一工作区时全部阶段命中缓存，检查点序号保持 16，未追加失效记录。

## 自动化测试

```bash
python3 -m unittest discover -v
```

实际结果：`Ran 22 tests ... OK`。覆盖 YAML 流式集合、特征规范化、确定性 NB、未知词、零平滑、空类别、FPR 阈值、低样本、不可达目标、零样本分层、定点加权、预算调度、原子检查点、双进程抢锁、阶段跳转、跨批错位、并行一致性、停止恢复、上游复用、阶段失效定位、增量替换和首个差异报告。

## 并行一致性

使用同一配置分别以 `--execution-workers 1` 与 `--execution-workers 4` 从零运行，并执行：

```bash
python3 -m pipeline.cli compare --left <sequential-workspace> --right <parallel-workspace>
```

实际结果：`status: ok`。`results.jsonl`、`summary.json`、`calibrations.json`、`parameters.json`、`schedule.json`、`report.json` 均逐字节一致。

## 优雅停止与恢复

```bash
python3 -m pipeline.cli run ... --stop-after-batches 1
python3 -m pipeline.cli run ...
python3 -m pipeline.cli compare --left <resumed-workspace> --right <fresh-workspace>
```

停止时返回 `status: stopped`、`error_code: E_STOPPED`，首个窗口的四个阶段全部完成，未生成不完整最终结果。恢复后的最终六个结果文件与从零全量运行逐字节一致；恢复后的检查点记录 `1..20` 连续，无截断或半写记录。

## 注入异常

非法平滑强度：

```text
smoothing: -1.0
exit status: 2
error_code: E_INVALID_SMOOTHING
```

不可达误报率上限（阈值上限 0、目标 FPR 0）：

```text
status: ok（校准阶段完成并保留待复核结论）
每类 target_met: false
每类 review_reasons 包含: E_FPR_UNREACHABLE
保留最优阈值: 0.0
report.review_required_count > 0
```

## 单批增量替换

自动化测试将 `batch_01_small` 中数条正文替换为带新增词项的内容，执行 `replace-batch`：

- 最早受影响阶段为 `features`；
- 只有该批产生 `invalidate` + 四个阶段重建记录；
- `batch_02_small` 的检查点输入指纹和产物摘要保持不变；
- 增量最终六文件与临时工作区从零全量运行逐字节一致；
- 人为改动一条结果后，比较器返回 `E_INCREMENTAL_MISMATCH`，并报告首个差异的 `batch_01_small`、`doc_id` 和 `results.value` 字段。

另在完整 8,800 条数据副本上替换 `batch_01_small` 的 6 条正文并执行 `replace-batch`，实际结果：

```text
initial: status=ok, doc_count=8800, checkpoint_sequence=16
replacement: status=ok
earliest_stage: features
incremental results_digest: 22d184197d81d36fd068dacc7efd0572a69934e05b981633aeea88c1a905627c
fresh full results_digest: 22d184197d81d36fd068dacc7efd0572a69934e05b981633aeea88c1a905627c
compared files: results.jsonl, summary.json, calibrations.json, parameters.json, schedule.json, report.json
comparison: ok（逐字节一致）
```
