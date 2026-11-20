# 实际验证记录

验证日期：2026-11-21（Asia/Shanghai）。所有命令均在当前工作区执行。

## 环境

```text
Python 3.14.4
numpy 2.5.5
Pillow 13.0.0
OS: Linux local workspace
```

## 自动化测试

```bash
python3 -m unittest discover -s tests -v
```

结果：

```text
Ran 20 tests in 13.791s
OK
```

覆盖范围：

- PNG/清单逐字节确定性、画像摘要确定性、报告逐字节确定性；
- 双标签、单标签、空标签、未知标签、NaN/Inf、尺寸不符、互斥冲突；
- `E_DIM_MISMATCH`、`E_UNKNOWN_LABEL`、`E_NUMERIC`、`E_MISSING_FIELD`、
  `E_SCORE_LENGTH` 五类注入与未涉及样本不受影响；
- 像素、真值、分数、阈值、分块参数变化的最早阶段识别与增量重建；
- 双进程并发发布仅允许一个连续序号提交，其余返回 `E_STALE_CHECKPOINT`；
- 检查点目录回退与索引摘要篡改被拒绝；
- 停止文件、恢复运行、报告哈希一致；
- HTTP 仪表盘/运行/报告/注入/清除注入接口；本地导入后重新运行。

## 端到端演示

```bash
python3 -m workbench demo --seed 20261121 --stop-after 5
```

关键结果（完整结构见 `reports/demo_summary.json`）：

```text
baseline.status                    completed
baseline.published                 28
baseline.failures                  3
baseline.conflicts                 1
baseline.metrics.micro             TP=6 FP=2 FN=1 P=0.75 R=0.857142857143 F1=0.8
baseline.metrics.macro             P=0.833333333333 R=0.833333333333 F1=1.0 AP=1.0
baseline.cooccurrence_consistency  0.8
baseline.report_sha256             19f497ba8165b0b6c699cb738641898a77700382d66ec85a922e622a5b29b60f

stop_resume.stopped_status         stopped
stop_resume.stopped_reason         stop_after_checkpoints
stop_resume.stopped_published      5
stop_resume.resumed_published      23
stop_resume.resumed_reused         5
stop_resume.resumed_report_sha256  19f497ba8165b0b6c699cb738641898a77700382d66ec85a922e622a5b29b60f
stop_resume.byte_identical         true

injection.operation                score_length s08_multi label=cat
injection.error_code               E_SCORE_LENGTH
injection.earliest_invalidated     score
injection.invalidated_stages       score, metrics, report
injection.unaffected_sample        s01_dual
injection.unaffected_unchanged     true
injection.report_after_injection   05745b4b00ca054bf8b2ced75f76ece8d6d089f62783eadb77f42a36f217eec4
injection.restored_report_sha256   19f497ba8165b0b6c699cb738641898a77700382d66ec85a922e622a5b29b60f
injection.restored_identical       true
```

## 失败定位

基线报告中的硬失败：

- `s04_unknown` / stage 2 / `E_UNKNOWN_LABEL`，详情标签 `unicorn`；
- `s05_nan` / stage 2 / `E_NUMERIC`，详情标签 `cat`、值 `NaN`；
- `s06_dim_mismatch` / stage 1 / `E_DIM_MISMATCH`，声明 `39x34`，实际 `32x32`。

冲突：

- `s07_conflict` / `E_LABEL_CONFLICT` / 标签对 `["cat", "dog"]`，
  kinds 为 `["ground_truth", "prediction"]`。

## 哈希文件

```bash
cat reports/report.sha256
# 19f497ba8165b0b6c699cb738641898a77700382d66ec85a922e622a5b29b60f
```

同一输入与配置清空输出后重新完整运行、以及停止后恢复运行，均得到该报告哈希。

