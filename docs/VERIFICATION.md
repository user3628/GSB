# 实际验证记录

日期：2026-11-23（Asia/Shanghai）  
工作区：`/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-115`

## 环境

- Python `3.14.4`
- NumPy `2.2.1`
- PyYAML `6.0.3`
- 虚拟环境：`.venv`，`--system-site-packages`，项目以 `pip install -e . --no-deps` 安装
- 无 Docker、无 GPU、无外部服务调用

## 全量测试

命令：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
16 passed
```

覆盖范围：

- 固定随机种子生成器重复执行后，清单、参考摘要和全部 `.npy` 文件哈希一致；
- 清单拒绝 `CON` 等 Windows 设备名及尾随点/空格等不可移植帧路径；
- 外观阶段重建逐字节一致，空框、越界、全零区域返回 `E_INVALID_REGION` 并携带序列与帧；
- 搜索窗口完全越界、全零候选区域、NaN 直方图返回 `E_NUMERIC` 并携带网格信息；
- 重复发布、跨序列发布、链摘要不匹配返回 `E_STALE_CHECKPOINT`；
- 截断检查点被扫描器忽略，隔离后可从未损坏前缀第 1 帧继续；
- 两个独立进程竞争同一序号，一个成功，另一个返回 `E_STALE_CHECKPOINT`；
- 第 10 帧真值变化后，保留 0–9 帧，失效阶段为 `appearance`，原因 `reference_truth_changed`；
- 搜索比例变化后，最早失效帧为 0，阶段为 `candidates`；
- 候选阶段和轨迹阶段在缺少当前外观阶段时返回 `E_INVALID_STAGE`，轨迹阶段在缺少候选产物时同样拒绝；
- 独立候选阶段重复执行产物逐字节一致，候选产物被篡改或删除时轨迹阶段返回 `E_INVALID_STAGE`；
- `all` 完成后候选产物刷新为全帧审计，阶段 3 可复用且不重算已发布帧；
- `demo` CLI 的短输出使用存在的 `baseline_summary_sha256`，不再触发缺键异常；
- 非法阶段跳转和缺失前置帧诊断返回 `E_INVALID_STAGE`；
- 3 个序列完整运行与停止恢复的 bundle、summary、检查点树逐字节一致；
- HTTP 健康、序列、帧、诊断、报告、停止/恢复和错误码。

## 默认 40 帧演示

命令：

```bash
.venv/bin/python -m tracklab --root . demo --json
```

关键输出：

```text
verification_passed: true
byte_identical: true
checkpoint_trees_identical: true
checkpoint_file_count: 120
replayed_frames: 0
full_checkpoint_tree_digest: b03c165212662b6e7fd5d5579848b38f3b418ea4f86c5545c37996a53fddab36
resumed_checkpoint_tree_digest: b03c165212662b6e7fd5d5579848b38f3b418ea4f86c5545c37996a53fddab36
baseline_summary_sha256: 180aa109787bd85d74f76590ce592a3f27a57bef97fb93d7bd68e97f23c95b7b
resumed_summary_sha256: 180aa109787bd85d74f76590ce592a3f27a57bef97fb93d7bd68e97f23c95b7b
demo_summary_sha256: f4798346c52af8b571ad20156759ac84377bf4e1e6d8def022aa37601becece3
```

候选阶段产物核对：

| 序列 | 审计帧数 | `preview_frame` | 候选阶段指纹前缀 |
|---|---:|---|---|
| `seq_uniform` | 40 | `null` | `9fe329e980423eb4` |
| `seq_scaling` | 40 | `null` | `da562cd4ceccf37e` |
| `seq_direction` | 40 | `null` | `962fa69be38de5e8` |

阶段 2 产物位于 `runs/artifacts/<seq>/candidates.json`，包含已发布帧候选摘要、阶段/配置指纹和未完成轨迹的下一帧预览；完整运行后为全帧审计快照。

停止点：

| 序列 | 停止时已发布帧 | 状态 |
|---|---:|---|
| `seq_uniform` | 12 | stopped |
| `seq_scaling` | 9 | stopped |
| `seq_direction` | 20，正处于遮挡区间 | stopped |

方向突变序列恢复后：

```text
occlusion start_frame: 18
anchor_frame: 17
end_frame: 23
recovery_frame: 24
recovered: true
```

逐序列指标：

| 序列 | 帧数 | 平均 IoU | 最大中心误差 | 模板更新 |
|---|---:|---:|---:|---:|
| `seq_uniform` | 40 | 0.889919094744 | 0.047227328154 | 40 |
| `seq_scaling` | 40 | 0.926048470625 | 0.053989686023 | 39 |
| `seq_direction` | 40 | 0.912873860998 | 0.160147418335 | 34 |

固定运行总摘要：

```text
mean_iou: 0.909613808789
run_bundle_sha256: 2df6ba5a3337c1e566e1a4083b001b23a4fc32784cf3bee3e532427649eed2e0
summary_sha256: 180aa109787bd85d74f76590ce592a3f27a57bef97fb93d7bd68e97f23c95b7b
```

生成物：

- `reports/run_bundle.json`
- `reports/summary.json`
- `reports/demo_summary.json`
- `reports/sequences/*.json`
- `runs/snapshots/full/`
- `runs/snapshots/resume/`
- `runs/checkpoints/`（恢复运行后的最终活动链）
- `runs/artifacts/<seq>/appearance.json`
- `runs/artifacts/<seq>/candidates.json`
- `runs/artifacts/invalidations/`（测试或变更演示时生成）

## 最终验收命令

```bash
.venv/bin/python -m tracklab --root . validate
.venv/bin/python -m tracklab --root . verify
.venv/bin/python -m tracklab --root . status
```

实际结果：清单校验 `ok=true`、3 个序列各 40 帧且样本上界 128；活动检查点链均为连续 40 帧且 `complete=true`；报告重算摘要仍为 `180aa109787bd85d74f76590ce592a3f27a57bef97fb93d7bd68e97f23c95b7b`。

## 错误码抽查

测试中实际断言：

```text
E_INVALID_REGION  empty_box / box_outside_image / all_zero_region
E_NUMERIC         search_window_outside_frame / all_candidates_invalid_regions / non_finite_histogram
E_STALE_CHECKPOINT duplicate_or_rollback / state_replacement_or_chain_mismatch / cross-sequence
E_INVALID_STAGE   missing appearance/candidates prerequisite / tampered candidates artifact / unsupported stage / skipped diagnostics
```

所有业务错误均保留 `sequence_id`、`frame_index`（适用时）和诊断上下文；运行期错误同时持久化到 `runtime/diagnostics/errors/`。
