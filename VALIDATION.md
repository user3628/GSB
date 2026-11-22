# 实际验证记录

验证环境：

```text
Python 3.14.4
NumPy 2.5.5
Pillow 13.0.0
Linux 本地文件系统；代码同时提供 Windows msvcrt 文件锁分支与 run_pipeline.bat
```

## 1. 自动化测试

```bash
python -m unittest discover -v
```

结果：

```text
Ran 43 tests in 6.344s

OK
```

关键断言：

- NFKC、全角数字、中文单字/拉丁连续串、别名去重、非法裸拉丁算子、缺失参数、链长超限均返回稳定业务码；
- 并行和单线程结果 PNG 与固定摘要逐字节一致；
- 查询缓存第二次命中，强制重算与缓存 payload 逐字节一致；
- 第一条 parse 检查点后停止，恢复摘要与从零运行一致；每任务检查点 seq 严格为 `1..N`；
- 输入像素、指令、调度预算、延迟预算、期望摘要变化分别从 `parse`、`execute`、`verify` 最早阶段重建；
- 4 个独立进程同时发布同一任务首检查点，仅 1 个成功，3 个返回 `E_STALE_CHECKPOINT`；
- 原子替换失败时旧文件保持完整且临时文件清理；
- HTTP 导入、执行、状态、查询缓存、PNG 下载、停止、恢复、单任务解析错误上下文与 CLI 全流程通过。

## 2. 干净演示工作区

```bash
python -m gspipe init-demo --workspace demo_workspace
```

```text
shard_count: 4
task_count: 6
config_version: 628c30d959a155591eb4745b75ade101a95119170e30a921a0214ef6783465ad
```

```bash
python -m gspipe run --workspace demo_workspace
```

```text
status: COMPLETED
checkpoints_written: 24
stage checkpoints: parse=6, execute=6, verify=6, export=6
status_counts: PROCESSED=5, UNPROCESSED=1
summary_digest: 886d754242dc2decbbd13dc64640b44c35ba4d9e9a0ebd8a213f009c3bb81a58
export_dir: exports/886d754242dc2decbbd13dc64640b44c35ba4d9e9a0ebd8a213f009c3bb81a58
```

`task_006` 实际判定：

```text
status: UNPROCESSED
reason: LATENCY_BUDGET_EXCEEDED
latency_budget_ms: 0
within_latency_budget: false
elapsed_ms: 9.574056（单次运行示例；实际值随机器变化）
steps_used: 2898
chunks:
  rotate: cumulative_steps=504
  blur:   cumulative_steps=2646
  color_shift: cumulative_steps=2898
```

导出包实际文件：

```text
manifest.json
summary.json
results/task_001.png
results/task_002.png
results/task_003.png
results/task_004.png
results/task_005.png
```

同一工作区再次执行 `run`：

```text
status: COMPLETED
checkpoints_written: 0
summary_digest: 886d754242dc2decbbd13dc64640b44c35ba4d9e9a0ebd8a213f009c3bb81a58
```

## 3. 查询缓存逐字节一致

```bash
python -m gspipe query task_001 --workspace demo_workspace --json
python -m gspipe query task_001 --workspace demo_workspace --json
```

```text
两次 payload 相等: true
payload 长度: 6668 bytes
cache_key: 062f3084ecf2320a6e88a99a48a1c550ca45ca962603979306542c3b9485f27c
```

## 4. 样本确定性哈希

```text
89ce8485115f524e4eec224ff7256d488dd126f5c5b635ab3e5170d1f36f4e04  samples/shards/shard_01.png
9edbbb5658d33ecebcace208f0d9e0b16726514dc6c9a622cf01429278f1bde5  samples/shards/shard_02.png
79b3bcfc7ce61e64d0ea097cbff1d107511208b241e37dfbcda9ba77d9eca2dc  samples/shards/shard_03.png
9a0c6ef869bcd15496ec3fc0a90348a49e8eb112031d921020bc8f0fea4013f4  samples/shards/shard_04.png
6259daad52ee3b22104847949e44946c6bcdecfc6c668b8051ea9fe8c747f0a4  samples/tasks.json
5995a3a7c29b1f41a9f29e6764a1141843163d6213d2202eb3d0824ace5ac5fd  samples/config.json
```

## 5. 增量失效实测示例

完成 6 任务运行后，将 `task_001` 指令中的 `dr=12` 改为 `dr=13`：

```text
task_001 earliest_stage: parse
reason: instruction_changed
task_002 parse checkpoint seq: unchanged
summary_digest: changed
```

随后修改 `execution.scheduler.max_workers`：

```text
all tasks earliest_stage: execute
reason: scheduling_budget_changed
parse checkpoints: reused
summary_digest: unchanged（像素结果相同）
```

随后只把 `task_006.latency_budget_ms` 从 `0` 改为 `2000`：

```text
only task_006 earliest_stage: execute
reason: latency_budget_changed
task_006 final status: PROCESSED
summary_digest: changed
```

随后只修改 `task_003.expected_summary`：

```text
only task_003 earliest_stage: verify
execute checkpoint seq: unchanged
final status: MISMATCH
query invalidations: old/new fingerprints recorded
```

## 6. 停止/恢复实测

测试在第一条 `parse` 检查点发布后写入停止请求：

```text
首次运行: STOPPED, checkpoints_written=1, parse checkpoints=1
恢复运行: COMPLETED
恢复摘要 == 从零完整运行摘要: true
恢复结果 PNG == 从零完整运行 PNG: true
每个任务检查点阶段顺序: parse,execute,verify,export
每个任务检查点 seq: 1,2,3,4
```
