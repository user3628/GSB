# 实际验证记录

验证日期：2026-11-23（Asia/Shanghai）

## 环境

- Python 3.14.4（满足 `>=3.10`）
- NumPy 2.2.1
- PyYAML 6.0.3
- pytest 8.3.4
- 本地 `.venv`，使用 `--system-site-packages` 创建，再以 `pip install -e . --no-deps --no-build-isolation` 安装
- 无 Docker、无外部服务、无 GPU、无网络依赖

依赖版本固定在 `requirements.txt` 和 `pyproject.toml`。

环境备注：共享系统 site-packages 中存在与本项目无关的 `pandas`/`cryptography` 依赖声明冲突，`pip check` 会报告这两项；工作台不导入这些包，项目自身导入、编译和测试均正常。

## 样本生成

命令：

```bash
.venv/bin/python scripts/generate_samples.py --output data
```

输出：

```text
generated 5 shards in data
  small: 600 rows x 10 columns, 2 shards
  medium: 6000 rows x 40 columns, 3 shards
```

两档均包含 4 个类别列；中档包含相关冗余数值列；manifest 与配置已落盘。

## 自动化测试

命令：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
35 passed in 11.84s
```

覆盖点：

- 样本契约与逐字节生成确定性。
- 类型推断、取值域、缺失统计和列声明冲突。
- 三种编码、全局独热域对齐、目标编码折诊断与不可见类别回退。
- 当前行标签不会进入本行目标编码。
- 单类别训练折 `E_FOLD_INVALID`。
- 三种选择和阈值校验。
- 全局预算、单进程配额、并发上限与未处理原因。
- 进程池/单进程摘要逐字节一致。
- 停止后恢复与完整运行摘要一致、序号连续。
- 工作进程异常的 `E_WORKER_FAILED` 与成功分片保留。
- 检查点落后、重复、依赖缺失、跨阶段回退。
- 原子替换失败时旧记录保持完整。
- 真实跨进程文件锁阻塞。
- 选择/预算/编码/样本内容变化的失效阶段与上游复用。
- 导出只含选择特征、CLI、HTTP、导入样本。

## 完整运行与恢复演示

命令：

```bash
.venv/bin/python scripts/demo.py --root . --workdir artifacts/demo --force
```

关键结果：

```text
完整运行 result_digest:
b783753fc920cf35cf30c2ff94add5318d122cda431974e4340d4fe1341130a1

完整运行检查点: 11 条，序号 1..11，连续
停止时状态: stopped
停止时检查点: 3 条，序号 1..3，连续
恢复后检查点: 11 条，序号 1..11，连续
恢复后 result_digest:
b783753fc920cf35cf30c2ff94add5318d122cda431974e4340d4fe1341130a1
恢复复用: type_inference=True, encoding_records=2
完整运行与恢复的各档 payload 完全相等
```

逐文件复核：

```text
exports/selection.json
artifacts/demo/full/exports/selection.json
artifacts/demo/resume/exports/selection.json
以上三个文件的 SHA-256 均为
3855705664f7faf1daaead9b26e611acb348b056a87fa1252ff90e37d3f38088

exports/medium/encoding_summaries.json 与完整运行/恢复产物逐字节相同
exports/medium/selected.csv 与完整运行/恢复产物逐字节相同
```

检查点实际序列：

```text
1 type_inference
2..6 encoding shard 0..4
7 encoding STAGE
8 selection tier medium
9 selection tier small
10 selection STAGE
11 aggregate
```

默认完整运行的选择结果：

- medium：6000 行，编码后 56 列，保留 53 列，淘汰 3 列。
- small：600 行，编码后 26 列，保留 26 列，淘汰 0 列。
- medium `selected_matrix_digest`：`a3b13e46f41f8f02046d72cf2810a4d7c36a5074dde7a54388a660617c81aef7`
- small `selected_matrix_digest`：`07a5ca553afb13a56b265d2d91d32b0344bebecd1afe7364c37f7d7386e2c0b8`

## 目标编码诊断演示

演示用 `target` 编码、5 折、平滑强度 10，完整运行摘要：

```text
b1114602410b42a533c8557e2f238b25af7f8933a6a9dc96216b4bc14c8ca2b0
```

`city=Beijing` 在不同折的训练统计下得到不同编码值：

```text
fold 0: 0.401041666667
fold 1: 0.397619047619
fold 2: 0.438034188034
fold 3: 0.388157894737
fold 4: 0.387061403509
```

回退字段为 `current_training_fold_global_mean`。诊断文件：

```text
artifacts/demo/target/exports/small/encoding_summaries.json
```

## 限额调度演示

将全局评估预算降为 200000 后：

```text
global_used = 125000
admitted_shards = [0, 3, 4]
unprocessed:
  medium_01.csv: GLOBAL_BUDGET, estimated_evaluations=110000
  medium_02.csv: GLOBAL_BUDGET, estimated_evaluations=110000
```

所有已接纳分片均满足单进程配额和并发上限；未处理分片不消耗特征评估预算。

## 状态与查询延迟

`status` 查询结果：

```text
status = complete
affected_earliest_stage = null
checkpoints = {count: 11, first_sequence: 1, last_sequence: 11, continuous: true}
query.latency_ms ≈ 4.2 ms
query.budget_ms = 250.0
query.within_budget = true
```

HTTP 冒烟验证包含：`GET /api/health`、`GET /api/status`、`GET /api/version`、`PUT /api/config`、`POST /api/run`、`POST /api/resume`、`POST /api/stop`、`GET /api/export`。响应包含查询延迟头，后台运行可被状态接口观测。

## 复现命令

```bash
.venv/bin/python -m pytest -q
.venv/bin/python scripts/demo.py --root . --workdir artifacts/demo --force
.venv/bin/python -m feature_workbench --root . status
```

上述命令不访问网络，不启动容器，不依赖未授权目标。

