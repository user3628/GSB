# Deterministic Image Workbench

一个完全本地、可中断恢复的多模态图像编辑任务批处理与结果验证工作台。项目从零实现，只依赖 Python 标准库、NumPy 与 Pillow；不使用容器、真实生成模型、外部图像服务或网络资源。

核心能力：

- `crop`、`resize`、`rotate`、`color_shift` 四个确定性算子，浮点域计算、显式半入取整、8 位钳制。
- 有序算子链解析与可达性校验，错误包含 `task_id`、算子位置和具体原因。
- 任务级三重预算裁决：全局步数预算、逻辑 worker 步数配额、并行 worker 上限。
- spawn 进程并行执行，按 `task_id` 确定性归约；串行与并行报告逐字节一致。
- 四阶段原子检查点：`parsed`、`executed`、`size_checked`、`exported`。
- POSIX `flock` / Windows `msvcrt.locking` 真实文件锁与连续序号发布保护。
- 内容指纹驱动的增量失效传播与可查询失效历史。
- 命令行与本地 HTTP API：导入、分阶段执行、状态、预算、版本、停止、恢复、报告查询。

## 快速开始

要求 Python 3.10+、NumPy、Pillow。Windows、Linux、macOS 均可本地运行。

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt

python -m image_workbench generate-demo --out demo
python -m image_workbench plan --workspace demo
python -m image_workbench run --workspace demo --fresh
python -m image_workbench status --workspace demo
python -m image_workbench versions --workspace demo
```

`demo/` 中已保留一次完整运行结果。重新生成会清理该目录下的运行时检查点、产物与报告。

也可以安装为命令：

```bash
pip install -e .
image-workbench plan --workspace demo
```

## 演示数据

固定随机种子 `20261123`，生成 4 个 64x64 RGB PNG 分片和 5 条任务：

| task | 输入 | 链长 | 预算结果 | 说明 |
|---|---|---:|---|---|
| `t010` | `shard_000.png` | 3 | 接纳，10 步 | crop + color_shift + resize |
| `t020` | `shard_001.png` | 3 | 接纳，10 步 | rotate + resize + color_shift |
| `t030` | `shard_000.png` | 3 | 接纳，10 步 | 与 `t010` 共享输入，独立计费 |
| `t040` | `shard_002.png` | 7 | 拒绝 | 18 步，同时超过 worker 配额与全局剩余预算 |
| `t050` | `shard_003.png` | 2 | 链拒绝 | 64x64 上请求 `(80,80)` crop，空交集 |

调度配置为 `max_workers=3`、`global_step_budget=30`、`worker_step_quota=12`。成本公式：

```text
estimated_steps = max(
  min_task_steps,
  chain_length * steps_per_operator + ceil(input_width * input_height / pixels_per_step)
)
```

接纳顺序为 `priority` 降序、`task_id` 升序；每条任务分配到能容纳其配额且已用步数最小的逻辑 worker slot。

## 数据与输出布局

```text
demo/
  inputs/shard_000.png ... shard_003.png
  tasks.jsonl
  config.yaml
  checkpoints/<task_id>/seq-00000001-parsed.json ...
  checkpoints/_quarantine/<task_id>/...
  artifacts/parsed/<task_id>.json
  artifacts/executed/<task_id>.png
  artifacts/size_checked/<task_id>.json
  artifacts/exported/<task_id>.png
  reports/schedule.json
  reports/verification.json
  reports/versions.json
  reports/run_summary.json
  metadata/worker_results/<task_id>.json
  metadata/invalidation_events.json
  .locks/checkpoints/<task_id>.lock
  .locks/run.lock
  control/STOP
```

## 任务清单格式

`tasks.jsonl` 每行一个 JSON 对象。`operations` 是权威有序链；`instruction` 会进入版本指纹并保留在报告中，不做非确定性 NLP 推断。

```json
{
  "task_id": "t010",
  "input": "shard_000.png",
  "instruction": "Crop 左上局部区域，light warm color shift，然后 bilinear 缩放到 48x32。",
  "allowed_operators": ["crop", "color_shift", "resize"],
  "priority": 0,
  "operations": [
    {"op": "crop", "x": 4, "y": 6, "width": 52, "height": 44},
    {"op": "color_shift", "red": 18.5, "green": 7.25, "blue": -12.75},
    {"op": "resize", "width": 48, "height": 32, "mode": "bilinear"}
  ],
  "expected_output": {
    "width": 48,
    "height": 32,
    "sha256": "fe6436189e9b76d2346b6f93f8606334fb9b5393012c73189eeae4a56d293eeb"
  }
}
```

兼容别名：`operator_chain` / `operators` 作为链字段，`operator_params` 可给字符串算子列表补充参数；`input_shard` / `shard` 作为输入字段。`expected_output.sha256` 是规范 RGB 像素摘要，不是 PNG 文件字节摘要。

## 调度配置格式

```yaml
version: demo-v1
max_workers: 3
global_step_budget: 30
worker_step_quota: 12
task_timeout_seconds: 30.0
max_chain_length: 8
reachability:
  rotate_angle_min: -180.0
  rotate_angle_max: 180.0
  max_dimension: 512
cost_model:
  steps_per_operator: 2
  pixels_per_step: 1024
  min_task_steps: 1
export:
  format: png-rgb-v1
```

配置解析使用内置 `yaml_lite`，支持本项目需要的嵌套映射、列表、注释、引号与流式 JSON 集合，不额外依赖 PyYAML。

## 确定性语义

- 输入解码：Pillow 读取并统一为 RGB `uint8`；内容指纹基于宽高与原始 RGB 像素。
- `crop`：显式 `x,y,width,height`，与当前尺寸求交后执行；空交集为 `E_CHAIN_INVALID`。
- `resize`：`nearest` 或 `bilinear`；像素中心映射 `(dst+0.5)*src/dst-0.5`，边界钳制。
- `rotate`：固定角度逆映射、最近邻采样、边界钳制，输出保持当前尺寸；正角度视觉上逆时针。
- `color_shift`：RGB 浮点加性偏移后钳制至 `[0,255]`，最终 `floor(x+0.5)` 取整。
- PNG：自实现无元数据、filter 0、zlib level 9 的规范 RGB PNG 编码，跨运行稳定。
- 并行归约：所有任务完成后按 `task_id` 排序生成报告，报告不包含时间戳、worker PID、复用标记或检查点序号，因此并行、串行、恢复运行可逐字节一致。

## 四阶段检查点与恢复

每阶段先原子写产物、再在真实文件锁内发布检查点记录。记录包含：

```text
schema_version, task_id, stage, stage_index, seq, input_digest,
config_version, stage_fingerprint, upstream_fingerprint,
fingerprint_components, artifact_path, artifact_sha256,
output_digest, output_size, ok, error, record_sha256
```

- 记录文件先写同目录临时文件、`fsync`、再 `os.replace`，不会留下半写 JSON。
- 每任务序号从 1 连续推进；发布时在锁内重算 `next_seq`，并校验调用者提供的期望序号。
- 损坏、字段缺失、校验和不符、文件名/阶段错位、重复序号均会被忽略；修复时移入 `checkpoints/_quarantine/`，从最近连续记录继续。
- 发布下游阶段必须存在指纹匹配的前置阶段记录与有效前置产物，否则 `E_INVALID_STAGE`。
- 并发发布同一序号时只有一人成功，其余返回 `E_STALE_CHECKPOINT`；跨任务错放产物同样返回 `E_STALE_CHECKPOINT`。

停止请求（CLI `stop` / HTTP `POST /api/stop`）只写 `control/STOP`。执行器在任务边界检查该文件，完成当前不可拆分任务后退出，不生成最终报告。`resume` / `POST /api/resume` 清除停止标记并从检查点继续。

## 版本指纹与增量失效

每阶段指纹均由规范 JSON SHA-256 计算，上游指纹进入下游组件：

| 阶段 | 关键组件 |
|---|---|
| `parsed` | 输入像素摘要、指令、允许算子、有序链、链长/可达性/预算配置、配置版本 |
| `executed` | parsed 指纹、输入摘要、算子内核版本 |
| `size_checked` | executed 指纹、声明尺寸 |
| `exported` | size_checked 指纹、声明摘要、导出格式、PNG 编码版本 |

发生变更时，查询接口会给出：

- 最早失效阶段；
- 受影响的下游阶段；
- 变化的组件名；
- 期望与记录中的阶段指纹；
- 去重后的持久化失效历史。

仅修改声明尺寸时，`parsed` 与 `executed` 记录及其序号保持不变，只重建 `size_checked` 及下游；仅修改声明摘要时只重建 `exported`。修改输入、指令、算子链、预算或可达性规则会从 `parsed` 开始失效。

## CLI

```bash
python -m image_workbench generate-demo --out demo
python -m image_workbench import --workspace demo --shards PATH --tasks tasks.jsonl --config config.yaml
python -m image_workbench plan --workspace demo
python -m image_workbench run --workspace demo --fresh
python -m image_workbench run --workspace demo --sequential
python -m image_workbench run --workspace demo --until-stage executed
python -m image_workbench stop --workspace demo
python -m image_workbench resume --workspace demo
python -m image_workbench status --workspace demo
python -m image_workbench versions --workspace demo
python -m image_workbench reports --workspace demo
python -m image_workbench serve --workspace demo --host 127.0.0.1 --port 8765
```

退出码：`0` 完成/部分阶段完成，`1` 业务或配置错误，`2` 请求停止，`3` 完整运行已结束但存在未完成任务（如 worker 异常）。

## HTTP API

```bash
python -m image_workbench serve --workspace demo --port 8765

curl http://127.0.0.1:8765/api/health
curl http://127.0.0.1:8765/api/budget
curl http://127.0.0.1:8765/api/status
curl http://127.0.0.1:8765/api/versions
curl -X POST http://127.0.0.1:8765/api/run \
  -H 'Content-Type: application/json' -d '{"until_stage":"executed"}'
curl -X POST http://127.0.0.1:8765/api/stop -d '{}'
curl -X POST http://127.0.0.1:8765/api/resume \
  -H 'Content-Type: application/json' -d '{"until_stage":"exported"}'
curl http://127.0.0.1:8765/api/reports?name=verification.json
```

导入接口接收本机路径：

```bash
curl -X POST http://127.0.0.1:8765/api/import \
  -H 'Content-Type: application/json' \
  -d '{"shards_dir":"/path/shards","tasks_file":"/path/tasks.jsonl","config_file":"/path/config.yaml"}'
```

HTTP 服务默认仅绑定 `127.0.0.1`，无认证，不应暴露到不可信网络。

## 稳定错误码

| 错误码 | 含义 |
|---|---|
| `E_CHAIN_INVALID` | 未知/不允许算子、链过长、参数非法、空 crop 交集、角度越界等 |
| `E_TASK_INVALID` | 任务清单或字段非法 |
| `E_INPUT_MISSING` | 分片缺失或无法解码 |
| `E_CONFIG_INVALID` | 调度配置非法 |
| `E_BUDGET_GLOBAL_EXCEEDED` | 全局步数预算不足 |
| `E_WORKER_QUOTA_EXCEEDED` | 无 worker slot 能满足单进程配额 |
| `E_STALE_CHECKPOINT` | 检查点序号落后、重复或跨任务错位 |
| `E_INVALID_STAGE` | 非法阶段跳转、前置阶段或前置产物缺失 |
| `E_ARTIFACT_INVALID` | 产物缺失、越界或摘要不符 |
| `E_SIZE_MISMATCH` | 输出尺寸与声明不符 |
| `E_DIGEST_MISMATCH` | 输出像素摘要与声明不符 |
| `E_EXPORT_VERIFY` | 导出 PNG 重读校验失败 |
| `E_TASK_TIMEOUT` | 任务超过执行时间预算（协作式检查，不强杀进程） |
| `E_WORKER_PROCESS_EXITED` | worker 异常退出且未发布任务结果 |
| `E_INCOMPLETE` | 任务未达到最终导出阶段 |

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖算子确定性、链校验、预算裁决、真实文件锁并发、损坏/重复记录恢复、并行/串行字节一致、停止/恢复、增量失效与 HTTP 全流程。当前 21 项测试全部通过。

## 实际验证摘要

固定样本完整运行结果：

```text
config_version:       dc048b68a64e22d2c4a4c4873cc77b6d2cc11737b6e77474796489f4551d2626
verification_sha256:  7947cedb4a9c7a6dfe885c7abfec09804aacc7f05b41e0425e8a3458ae5806b0
summary_sha256:       45dc629c63d2e970af5d2d62f3a85be2ee20e7fc15c98e91eff3d5135b09f1b6
run_summary file:     7e2f84bcacd4a0dd103a42f1b109f84c348113db8e99e300b5c165e11e377774
tasks:                total=5 accepted=3 verified=3 rejected_chain=1 rejected_budget=1
budget:               reserved 30/30 steps; worker slots 10/12 each
outputs:              t010=fe6436...93eeb  t020=c8df5d...d2b6  t030=6e26a3...f5ec
```

详细命令与恢复、并发、失效传播证据见 `docs/VERIFICATION.md`；设计细节见 `docs/DESIGN.md`。
