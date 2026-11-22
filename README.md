# GSPipe：本地确定性生成式图像编辑管线

GSPipe 不依赖生成模型、GPU、网络或容器。它从固定随机种子生成的 RGB PNG 分片与 JSON 任务清单出发，完成四个阶段：

1. `parse`：Unicode 归一化、中英混排分词、算子链生成与静态校验；
2. `execute`：在三重预算下调度，用确定性整数/定点内核分块执行；
3. `verify`：复核期望尺寸与编辑摘要；
4. `export`：结合延迟预算结果生成可查询、可导出的固定摘要。

支持 Windows 命令行、本地 HTTP API、逐阶段检查点、优雅停止/恢复、增量失效、真实跨进程文件锁、执行缓存和查询缓存。

## 环境准备（Windows）

要求 Python 3.10+、NumPy、Pillow：

```bat
py -3.10 -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

仓库自带 `run_pipeline.bat`，也可直接使用：

```bat
run_pipeline.bat init-demo --workspace demo_workspace
run_pipeline.bat run --workspace demo_workspace
```

管线完全在本地执行；HTTP 默认只监听 `127.0.0.1`，不访问外部服务。

## 快速开始

```bat
python -m gspipe init-demo --workspace demo_workspace
python -m gspipe run --workspace demo_workspace
python -m gspipe status --workspace demo_workspace
python -m gspipe query task_001 --workspace demo_workspace
python -m gspipe serve --workspace demo_workspace --port 8765
```

`init-demo` 生成 4 个 PNG 分片和 6 条任务。也可从已有文件导入：

```bat
python -m gspipe import --workspace demo_workspace --shards-dir samples\shards --tasks samples\tasks.json --config samples\config.json
```

一次完整运行的固定摘要（只取决于像素结果、状态与归一化摘要，不含耗时）：

```text
886d754242dc2decbbd13dc64640b44c35ba4d9e9a0ebd8a213f009c3bb81a58
```

状态统计为 `PROCESSED: 5`、`UNPROCESSED: 1`。`task_006` 延迟预算为 `0 ms`，因此必然被真实耗时判定为超预算；它仍完成执行并记录分块耗时，但不会进入最终结果。

## 样本数据

`samples/shards` 与 `samples/tasks.json` 由固定种子 `20261123` 生成；`python -m gspipe write-samples --output samples` 可重建完全相同的字节。

| task_id | 分片 | 指令要点 | 期望输出 | 延迟预算 |
| --- | --- | --- | --- | --- |
| `task_001` | `shard_01.png` | 调暖 + 裁剪右上区域 | `110x82` | 2000 ms |
| `task_002` | `shard_02.png` | 裁剪后双线性缩放 | `120x120` | 2000 ms |
| `task_003` | `shard_03.png` | 旋转 90° + 模糊 | `176x128` | 2000 ms |
| `task_004` | `shard_04.png` | 裁剪 + 最近邻缩放 + 模糊 | `144x120` | 2000 ms |
| `task_005` | `shard_01.png` | 调色 + 轻度模糊 | `160x120` | 2000 ms |
| `task_006` | `shard_02.png` | 旋转 + 强模糊 + 调色 | `144x112` | 0 ms（必然超预算） |

分片包含渐变、确定性噪声、矩形几何色块和对角条纹。像素指纹基于解码后的 `RGB8` 像素，而非 PNG 压缩字节；重新编码但像素不变不会触发失效。

## 四阶段与工作区

```text
workspace/
  inputs/shards/*.png
  inputs/tasks.json
  config.json
  .gspipe/
    artifacts/{json,png}/...       # 内容寻址产物
    checkpoints/<task_id>/records/ # 不可变阶段检查点
    cache/{execution,query}/...    # 执行与查询缓存
    metrics/...                    # 非确定性耗时，独立于版本指纹
    state/                         # 锁、停止请求、失效事件、最新导出
  exports/<summary_digest>/        # summary.json, manifest.json, results/*.png
```

每个任务在每个阶段完成后发布一条检查点，字段包括：

```text
task_id, stage, seq, generation, input_fingerprint, config_version,
fingerprint_components, dependencies, artifact_digest,
artifact_type, artifact_path
```

`seq` 每任务单调递增且只能加一。`generation` 表示失效后的重建代际；未受影响的上游阶段保留旧 generation 并复用。

## 指令语法

### 归一化与分词

解析前执行 Unicode NFKC、BOM/零宽字符清理、连续空白折叠。NFKC 同时统一全角/半角拉丁字母、数字和标点。分词规则为中文单字、拉丁连续串、数字与标点。

### 算子与参数

算子按文本出现顺序执行，参数使用显式 `key=value`：

```text
crop x=0 y=0 w=96 h=80
resize w=144 h=120 mode=nearest|bilinear
rotate angle=-360..360（整数）
color_shift dr=-255..255 dg=-255..255 db=-255..255
blur radius=0..32
```

中文别名：`裁剪/裁切`、`缩放/调整尺寸`、`旋转`、`调色/调暖`、`模糊/高斯模糊`。若同一算子已有显式拉丁标签，对应中文词只作为描述，不会产生重复算子。

严格规则：裸拉丁连续串必须是受支持算子、参数键或枚举值（`nearest`、`bilinear`）。`sharpen`、`sharpen x=1` 等不会被静默忽略，而会返回 `E_INVALID_CHAIN`。中文自然语言可作描述，但参数必须显式给出，避免猜测坐标或尺寸。

规范摘要示例：

```text
color_shift(dr=12,dg=6,db=-8)>crop(x=20,y=12,w=110,h=82)
```

### 静态校验

`parse` 阶段校验算子受支持且在 `allowed_ops` 中、必需参数存在、类型与范围合法、无未知或重复参数、链长不超限、上游输出可被下游接受、输出不超过图像上限。非法算子、参数缺失、链长超限等返回 `E_INVALID_CHAIN`，错误对象包含 `task_id`、`operator`、`reason` 和上下文。

## 确定性内核

| 算子 | 固定规则 |
| --- | --- |
| `crop` | 显式整数 `x,y,w,h`；越界或非正尺寸返回 `E_INVALID_PARAM`。 |
| `resize` 最近邻 | 像素中心映射 `floor((2*dx+1)*src_w/(2*dst_w))`，随后钳制。 |
| `resize` 双线性 | Q16 定点像素中心坐标、整数权重、固定 32 位权重和与四舍五入。 |
| `rotate` | 整数角度，Q16 三角采样，以图像中心旋转，保持画布尺寸；越界采样填黑，即固定边界裁剪。 |
| `color_shift` | 整数定点 RGB 增量，逐通道 `clip(0,255)`。 |
| `blur` | 可分离整数 box blur，边缘复制填充，固定窗口和与半上取整。 |

PNG 编码器不使用 Pillow 元数据：每行 filter 0、zlib level 9、固定 IHDR，因此同一像素数组产生完全相同的 PNG 字节。Pillow 仅用于严格 RGB PNG 解码。

## 调度、延迟与并发

调度以任务为单位，按 `task_id` 升序确定性接纳。操作步数估计：

```text
crop         ceil(w*h / 64)
resize       ceil(dst_w*dst_h / 32)
rotate       ceil(area / 32)
color_shift  ceil(area / 64)
blur         ceil(area*(2*radius+1) / 128)
```

三重约束：

1. `execution.scheduler.total_step_budget`：全局已接纳步数上限；
2. `execution.scheduler.worker_step_quota`：每个确定性工作道的步数配额；
3. `execution.scheduler.max_workers`：工作道与并发上限。

执行时每个工作道使用一个本地线程，只处理分给该道且不超配额的任务。真实延迟从任务执行前用 `perf_counter_ns` 计时，任务完整结束后比较 `elapsed_ns <= latency_budget_ms * 1_000_000`。**没有使用 `future.result(timeout=...)` 或线程取消来替代延迟判定。** 超预算任务记为 `UNPROCESSED / LATENCY_BUDGET_EXCEEDED`，分块耗时仍保留。

每个算子是执行块；记录块的操作、估计步数、累计步数、耗时与输出像素指纹。所有并行结果最终按 `task_id` 排序归约；测试证明并行与单线程输出 PNG 和导出摘要逐字节一致。

## 版本指纹与增量失效

| 阶段 | 主要指纹组件 |
| --- | --- |
| `parse` | 输入像素指纹、输入尺寸、NFKC 后指令、`allowed_ops`、解析配置版本 |
| `execute` | parse 产物摘要、内核版本、调度预算版本、任务延迟预算 |
| `verify` | execute 产物摘要、期望尺寸、期望摘要、校验配置版本 |
| `export` | verify 产物摘要、导出配置版本 |

重新规划时从 `parse` 开始比较。找到最早不一致阶段后，该阶段及全部下游进入新 generation 重建；未受影响的上游产物继续复用。输入像素、指令/算子参数、调度预算、延迟预算、校验配置等变化都会落到对应最早阶段。

失效事件保存在 `state/invalidations`，查询接口返回 `earliest_stage`、原因、变化组件、旧/新指纹、旧/新代际和检测序号。

## 检查点并发保护与恢复

检查点发布在真实 OS 文件锁下完成：POSIX 使用 `fcntl.flock`，Windows 使用 `msvcrt.locking`，锁文件为 `.gspipe/state/locks/checkpoints.lock`。

发布时校验阶段顺序、连续 `seq`、代际、依赖产物摘要与产物完整性。落后、重复、跨任务错位、序号跳号返回 `E_STALE_CHECKPOINT`；非法阶段跳转、缺失前置阶段或前置产物损坏返回 `E_INVALID_STAGE`。

记录与产物使用同目录临时文件、`fsync`、`os.replace`（Windows 重试）和目录 `fsync`。进程在任意写入点中断时，目标只会是旧完整文件或新完整文件；残留临时文件不以 `.json` 结尾，不会被加载。

停止请求写入 `state/stop.request`。运行器在任务边界检查；已开始的任务和已提交到工作道的任务完成后再退出。`resume` 清除标记并从有效检查点继续。测试验证恢复后的固定摘要与结果 PNG 和一次从零完整运行一致。

## 缓存

- 执行缓存键 = 输入像素指纹 + 规范算子链 + 内核配置版本；
- 查询缓存键 = 任务内容、输入像素、完整配置版本、检查点记录快照、运行时指标快照和失效事件；
- 缓存原子写入；PNG 内容寻址并在读取时校验 SHA-256。

查询缓存命中响应与强制重算响应逐字节相同。HTTP 使用 `X-GSPipe-Cache: HIT|MISS` 暴露命中状态。

## CLI

```text
python -m gspipe write-samples --output samples
python -m gspipe init-demo --workspace demo_workspace [--force]
python -m gspipe import --workspace demo_workspace --shards-dir samples/shards --tasks samples/tasks.json
python -m gspipe run --workspace demo_workspace [--stages parse,execute] [--sequential] [--resume]
python -m gspipe resume --workspace demo_workspace
python -m gspipe stop --workspace demo_workspace
python -m gspipe status --workspace demo_workspace
python -m gspipe fingerprints --workspace demo_workspace
python -m gspipe query task_001 --workspace demo_workspace [--recompute] [--json]
python -m gspipe export --workspace demo_workspace
python -m gspipe config --workspace demo_workspace [--file config.json]
python -m gspipe serve --workspace demo_workspace --host 127.0.0.1 --port 8765
```

`--stages` 必须是 `parse,execute,verify,export` 的连续片段，例如仅 `parse`、`parse,execute`、`verify,export`。从中间阶段开始时，所有前置检查点必须已是当前版本，否则返回 `E_INVALID_STAGE`。

## 本地 HTTP API

| 方法 | 路径 | 功能 |
| --- | --- | --- |
| `GET` | `/v1/health` | 健康信息与端点列表 |
| `POST` | `/v1/import` | JSON + base64 PNG 分片 + 任务清单导入 |
| `POST` | `/v1/demo/init` | 初始化演示数据 |
| `POST` | `/v1/run` | 触发阶段；body：`stages`、`parallel`、`resume` |
| `POST` | `/v1/resume` | 清除停止标记并恢复 |
| `POST` | `/v1/stop` | 请求优雅停止 |
| `GET` | `/v1/status` | 状态、指纹、分块耗时、预算与失效历史 |
| `GET` | `/v1/fingerprints` | 精简版本指纹视图 |
| `GET` | `/v1/query/<task_id>` | 原始查询缓存 payload |
| `POST` | `/v1/query` | body：`task_id`、`recompute` |
| `GET` | `/v1/parse/<task_id>` | 单独解析并返回算子链，失败时直接返回 `E_INVALID_CHAIN` |
| `GET` | `/v1/results/<task_id>.png` | 当前有效结果 PNG |
| `POST` | `/v1/export` | 生成确定性导出包 |
| `GET` | `/v1/export/latest` | 最新导出清单 |
| `GET` | `/v1/export/summary` | 最新固定摘要 |
| `GET/PUT` | `/v1/config` | 查看/替换工作区配置 |

启动与示例：

```bat
python -m gspipe serve --workspace demo_workspace --port 8765
curl http://127.0.0.1:8765/v1/health
curl -X POST http://127.0.0.1:8765/v1/run -H "Content-Type: application/json" -d "{\"stages\":\"parse,execute,verify,export\"}"
curl http://127.0.0.1:8765/v1/status
curl http://127.0.0.1:8765/v1/query/task_001
curl -X POST http://127.0.0.1:8765/v1/stop -H "Content-Type: application/json" -d "{\"reason\":\"maintenance\"}"
curl -X POST http://127.0.0.1:8765/v1/resume -H "Content-Type: application/json" -d "{}"
```

HTTP 导入格式：

```json
{
  "shards": [{"name": "shard_01.png", "png_base64": "iVBORw0KGgo..."}],
  "tasks": [{"task_id": "task_001", "input": "shard_01.png", "...": "..."}],
  "config": {"execution": {"scheduler": {"max_workers": 3}}}
}
```

## 错误码

| 业务码 | HTTP | 含义 |
| --- | --- | --- |
| `E_INVALID_CHAIN` | 422 | 算子、参数、链长或上游尺寸非法 |
| `E_INVALID_PARAM` | 422 | 内核参数越界或类型非法 |
| `E_INVALID_STAGE` | 409 | 阶段跳转、前置检查点或产物缺失 |
| `E_STALE_CHECKPOINT` | 409 | 序号落后、重复、跳号、跨任务或旧代际发布 |
| `E_INVALID_INPUT` | 422 | PNG、配置、请求体或路径输入非法 |
| `E_INVALID_TASKS` | 422 | 任务清单字段、重复 ID 或引用非法 |
| `E_TASK_NOT_FOUND` | 404 | 任务不存在 |
| `E_RESULT_UNAVAILABLE` | 409 | 结果陈旧、未处理或尚未导出 |
| `E_LOCK_TIMEOUT` | 503 | 运行/检查点文件锁等待超时 |
| `E_ARTIFACT_CORRUPT` | 500 | 内容寻址产物缺失或哈希不一致 |

错误统一为结构化 JSON：`{"ok":false,"error":{"code","message","task_id","operator","stage","details","context"}}`。

## 测试与验证

```bat
python -m unittest discover -v
```

当前 43 项测试覆盖：归一化/分词/严格链校验、五种内核确定性、三重预算、并行与单线程一致性、固定摘要、超预算任务、查询缓存、停止恢复、增量失效、检查点并发锁、HTTP 与 CLI 端到端（含 `/v1/parse/<task_id>` 的 `E_INVALID_CHAIN` 结构化错误）。详细实际运行记录见 [VALIDATION.md](VALIDATION.md)。

## 运行语义说明

- 延迟分类使用真实墙钟耗时；预算处在机器噪声边界时结果可能随硬件变化。演示使用 `0 ms` 与 `2000 ms` 两端，避免抖动。
- 工作道是按步数配额模拟的确定性执行/归约单元；线程实际调度不参与结果归约顺序。
- 内核算法变化时应提升 `execution.kernel_version`，以主动失效执行缓存和下游产物。
