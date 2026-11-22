# 表格特征编码与选择工作台

这是一个仅依赖本地 CPU 的确定性工作台，面向传统机器学习建模前的表格特征准备。它把流程拆成四个可恢复阶段：

1. `type_inference`：按声明和非空值统计推断列类型、缺失情况和取值域。
2. `encoding`：按分片并行执行独热、目标编码或哈希编码，并按配置填充缺失值。
3. `selection`：在编码后的全局特征空间执行方差过滤、互信息排序或递归消除。
4. `aggregate`：按固定顺序归约并导出选择后的特征矩阵、标签和诊断摘要。

工作台同时提供 CLI 和本地 HTTP API，并实现预算裁决、真实文件锁、原子检查点、优雅停止、恢复、版本失效和并发发布保护。运行过程不需要 Docker、外部服务、网络、账号或 GPU。

## 已生成数据

固定种子 `20260317` 的生成器已产出：

- 小档：600 行 × 10 列，2 个分片。
- 中档：6000 行 × 40 列，3 个分片；包含相关/完全复制的冗余数值列。
- 每档均有 4 个类别列：`city(8)`、`channel(5)`、`segment(4)`、`device(3)`。
- 含缺失值的数值列和类别列，以及一个二分类 `label`。
- `data/manifest.json` 记录文件、行数、列数、缺失列下标、类别基数和内容摘要。
- `data/config.yaml` 是完整的编码、选择、预算和检查点配置。

## 快速开始

### Windows PowerShell

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip install -e . --no-deps --no-build-isolation
.\.venv\Scripts\python.exe scripts\generate_samples.py --output data
.\.venv\Scripts\python.exe -m feature_workbench run --mode process
.\.venv\Scripts\python.exe -m feature_workbench status
```

### Linux / macOS

```bash
python3 -m venv --system-site-packages .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install -e . --no-deps --no-build-isolation
.venv/bin/python scripts/generate_samples.py --output data
.venv/bin/python -m feature_workbench run --mode process
.venv/bin/python -m feature_workbench status
```

若使用系统解释器且依赖已经存在，可直接执行 `python3 -m feature_workbench ...`。Python 最低版本为 3.10；本项目实际验证版本见 `docs/VALIDATION.md`。

## CLI

全局选项应放在子命令之前：

```bash
python3 -m feature_workbench --root . status
python3 -m feature_workbench --root . run --mode process
python3 -m feature_workbench --root . run --mode sequential --until-stage encoding
python3 -m feature_workbench --root . run --mode sequential --stop-after-shards 2
python3 -m feature_workbench --root . resume --mode process
python3 -m feature_workbench --root . budget
python3 -m feature_workbench --root . version
python3 -m feature_workbench --root . export
python3 -m feature_workbench --root . stop --reason maintenance
python3 -m feature_workbench --root . configure --set encoding.strategy=target --set selection.method=mutual_info --set selection.threshold=0.0
python3 -m feature_workbench --root . import-samples /path/to/sample-directory
python3 -m feature_workbench --root . serve --host 127.0.0.1 --port 8765
```

退出码 `2` 表示结构化的业务错误；错误 JSON 中的 `error.code` 是稳定业务码。

## HTTP API

服务默认只监听 `127.0.0.1`。所有接口返回 JSON，并附带 `X-Query-Latency-Ms` 与 `X-Query-Within-Budget` 响应头。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查 |
| GET | `/api/status` | 阶段状态、版本、预算、停止状态、后台运行状态 |
| GET | `/api/version` | 版本指纹与最早失效阶段 |
| GET | `/api/budget` | 接纳顺序、并发度、预算消耗、未处理分片原因 |
| GET | `/api/config` | 读取配置 |
| GET | `/api/result` | 最近一次后台运行结果或结构化错误 |
| GET | `/api/export` | 读取已提交的最终选择摘要 |
| PUT | `/api/config` | 以 `{"changes":{"dotted.key":value}}` 原子修改配置 |
| POST | `/api/import` | 以 `{"source_dir":"..."}` 导入样本目录 |
| POST | `/api/run` | 启动后台分阶段运行 |
| POST | `/api/resume` | 清除停止请求并恢复 |
| POST | `/api/stop` | 请求优雅停止 |
| POST | `/api/export` | 必要时执行流水线并物化导出 |

示例：

```bash
curl -s -X POST http://127.0.0.1:8765/api/run \
  -H 'Content-Type: application/json' \
  -d '{"until_stage":"selection","worker_mode":"process"}'
curl -s http://127.0.0.1:8765/api/status
curl -s -X POST http://127.0.0.1:8765/api/stop \
  -H 'Content-Type: application/json' -d '{"reason":"demo"}'
curl -s -X POST http://127.0.0.1:8765/api/resume \
  -H 'Content-Type: application/json' -d '{"worker_mode":"process"}'
```

## 配置摘要

`data/config.yaml` 的主要字段：

```yaml
seed: 20260317
encoding:
  strategy: one_hot        # one_hot | target | hash
  missing:
    numeric: median        # mean | median | mode | zero | constant
    categorical: constant  # mode | constant
    constant: __MISSING__
  target:
    folds: 5
    smoothing: 10.0
    fold_seed: 20260317
  hash:
    n_features: 8
selection:
  method: variance         # variance | mutual_info | rfe
  threshold: 0.01          # variance/MI: >=0; rfe: (0,1] 保留比例
budget:
  global_feature_evaluations: 1000000
  per_worker_evaluations: 250000
  max_workers: 4
  query_latency_ms: 250
checkpoint:
  atomic_writes: true
```

预算成本模型为：

```text
estimated_evaluations = rows * max(1, numeric_columns + sum(categorical_cardinality))
```

分片按全局 `shard_index` 顺序接纳，使用固定 first-fit 规则分配到最多 `max_workers` 个配额通道。超过单通道配额或全局余额的分片不执行，并在预算报告中给出 `PER_WORKER_QUOTA` 或 `GLOBAL_BUDGET` 原因。

## 文件与目录

- `feature_workbench/`：工作台实现。
- `data/`：确定性 CSV 分片、`manifest.json`、`config.yaml`。
- `.workbench/`：运行检查点、版本基线与产物缓存（可重建）。
- `exports/`：默认导出；`selection.json` 是最终摘要，各档目录含选中特征矩阵、标签和编码诊断。
- `artifacts/demo/`：完整运行、停止恢复和目标编码演示的独立产物。
- `scripts/generate_samples.py`：固定种子样本生成入口。
- `scripts/demo.py`：一键演示完整运行、停止恢复、目标编码诊断和限额调度。
- `tests/`：35 项自动化测试。

## 确定性与恢复

- 同一输入与配置产生相同的编码摘要、选择清单和最终 `result_digest`。
- 进程池与单进程模式按分片序号归约，输出逐字节一致。
- 停止请求只阻止后续分片/阶段；当前不可拆分单元完成后退出。
- 检查点先写临时文件、`fsync` 后原子替换，不会暴露半写 JSON。
- 发布使用真实 OS 文件锁（POSIX `fcntl`，Windows `msvcrt`），并比较单调序号与依赖链。
- 目标编码仅使用当前训练折标签；不可见类别回退到该折训练集的全局均值。

当前完整演示的最终摘要为：

```text
b783753fc920cf35cf30c2ff94add5318d122cda431974e4340d4fe1341130a1
```

## 测试与文档

```bash
python3 -m pytest -q
python3 scripts/demo.py --root . --workdir artifacts/demo --force
```

自动化测试结果、实际摘要、预算和恢复记录见 `docs/VALIDATION.md`；架构、版本映射、锁与错误码见 `docs/TECHNICAL.md`。

