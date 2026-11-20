# 确定性图像修复与格式互操作工作台

这是一个完全本地、无 GPU、无外部模型、无网络服务依赖的图像工作台。它把小型 PNG/JPEG 分片、修复掩码、风格参考、风格强度和修复迭代参数组织为内部工程模型，并提供：

- 确定性的邻域扩散修复与参考图颜色统计迁移；
- 内部无损工程 JSON、PNG、JPEG + 配套 JSON 描述之间的双向转换；
- JPEG 透明通道/元数据/颜色量化降级的显式记录与回补；
- schema v1 旧工程清单的稳定升级；
- 分阶段版本指纹、最早失效阶段定位和上游产物复用；
- 原子阶段记录、连续序号快照发布、跨进程发布锁和优雅停止；
- CLI、FastAPI 本地 HTTP API 和无前端构建步骤的交互页。

## 快速开始

### Windows 11 / Windows 10 本地

前置条件：安装 Python 3.11，并确保 `py -3.11` 可用。

```bat
setup_windows.bat
```

该脚本创建 `.venv311`、安装锁定依赖、生成 `data/` 确定性样本并运行完整验收演示。

```bat
run_demo.bat
run_server.bat
```

`run_server.bat` 会打开 `http://127.0.0.1:8000` 并启动本地 Uvicorn 服务。服务只监听回环地址，不访问外部网络。

### 手工命令（Windows / macOS / Linux 通用）

```bash
py -3.11 -m venv .venv311            # Windows
# python3.11 -m venv .venv311       # Linux/macOS
source .venv311/bin/activate        # Windows: .venv311\Scripts\activate.bat
python -m pip install -r requirements.txt
python -m image_workbench generate-sample --out data
python -m image_workbench demo
python -m uvicorn image_workbench.api:app --host 127.0.0.1 --port 8000
```

## 确定性样本

`python -m image_workbench generate-sample --out data` 使用固定种子 `20261120` 生成：

- 5 个图像分片：`style_ref.png`、`fragment_a.png`、`fragment_b.jpg`、`fragment_c.png`、`fragment_d.jpg`；
- `manifest.json`：schema v2 工程清单，含归一化矩形掩码、风格参考、强度、迭代轮数和输出格式偏好；
- `legacy_manifest.json`：schema v1 旧清单，掩码使用像素 `x/y/width/height`，强度为整数百分比，分片无 ID；
- `legacy_mask_descriptor.json`：旧清单配套像素掩码说明；
- `sample_summary.json`：样本语义摘要和每个文件的摘要。

样本摘要基于生成后的 RGB 像素和清单内容，不依赖 JPEG 压缩后的二进制字节，因此不会因不同平台的 libjpeg 编码细节改变。

固定值：

```text
sample_digest            = 444291c68e0861de51c62283d222107eb85829772f02538b6cae9185e9d35351
pipeline_summary_digest  = 692aee759bb4c936733fa9b39f6fc4901ec97d5a501779d63e4737c9a91a587c
```

## 内部模型与格式互操作

### 内部无损模型

内部工程 JSON 的每个分片包含：

- `fragment_id`；
- `base64-raw-rgb-u8` 像素矩阵、尺寸和像素摘要；
- 归一化半开矩形掩码 `[x0, y0, x1, y1]`、掩码像素摘要；
- `style_reference`、`style_strength`、`repair_iterations`、`output_format`；
- 完整版本元数据 `schema_version`、`codec_version`、`workbench_version`、来源格式/来源 schema 等；
- 显式格式降级项 `loss_items`。

内部工程 JSON 与模型往返时，像素、掩码、风格强度、迭代轮数、输出偏好和版本元数据逐字段一致。

### 三种交换表示

| 表示 | 像素 | 掩码 | 元数据 | 往返行为 |
|---|---|---|---|---|
| PNG | RGB，无损 | alpha 通道，掩码像素 alpha=0 | 描述器嵌入 PNG 文本块 | 像素与掩码零损失 |
| JPEG | RGB，有损量化 | 必须写入配套 `*.iwb.json` | 必须写入配套描述器 | 掩码/强度/元数据无损；像素量化显式记录 |
| JSON 描述 | 引用 PNG/JPEG 像素载荷 | 归一化矩形 + 掩码摘要 | 完整 | 与 PNG/JPEG 像素载荷组合重建模型 |

JPEG 导出会写入三条显式降级项：

- `jpeg_alpha_channel_dropped`：JPEG 无 alpha，修复掩码改由描述器运输；
- `jpeg_metadata_externalized`：版本元数据外置到 sidecar；
- `jpeg_pixel_quantization`：JPEG 颜色有损，导入 RGB 可能与内部源矩阵不同。

从 JPEG + 描述器回补时，会校验：

1. 描述器存在且 kind/版本/自摘要合法；
2. 图像尺寸与描述器尺寸一致；
3. JPEG 降级标记存在；
4. 描述器掩码栅格化摘要匹配。

任一项失败返回 `E_FORMAT_LOSS`，`details.lost_items` 列出丢失项。尺寸不匹配时掩码像素集合不会被猜测性恢复。

## 修复与风格迁移算法

### 确定性修复

`repair_pixels` 只更新掩码覆盖的像素：

1. 未掩码像素是固定 Dirichlet 边界；
2. 掩码区域先以已知像素的逐通道均值播种；
3. 每一轮对掩码内像素使用固定顺序的 8 邻域平均值更新，边界使用实际存在的邻域数量；
4. 固定 `repair_iterations` 轮后，在 float64 域做有限性校验、`[0,255]` 截断和 half-up 取整；
5. 全图掩码且无已知像素时返回 `E_NUMERIC`，不会编造结果。

算法不使用随机数，同一像素、掩码和轮数逐字节稳定。

### 确定性风格迁移

`style_transfer_pixels` 对 RGB 通道分别计算目标与参考图的均值和总体标准差（`ddof=0`），执行：

```text
transferred = (target - target_mean) / max(target_std, 1e-12) * ref_std + ref_mean
mixed       = target * (1 - strength) + transferred * strength
```

随后校验有限性、截断到 8 位范围并 half-up 取整。强度必须落在 `[0,1]`；越界或 NaN/Inf 返回 `E_NUMERIC`，错误包含 `fragment_id` 和 `pixel`。

## 阶段指纹与增量失效

阶段固定为：

```text
source -> repair -> style -> export
```

指纹依赖如下：

| 阶段 | 输入依赖 | 配置依赖 |
|---|---|---|
| `source` | 分片 ID、像素摘要、版本元数据摘要 | 解码器版本、schema 版本 |
| `repair` | source 指纹、掩码像素摘要 | 扩散算法版本、迭代轮数 |
| `style` | repair 指纹、参考分片 ID、参考像素摘要 | 均值/标准差算法版本、强度、epsilon |
| `export` | style 指纹、输出格式偏好 | 完整格式能力配置 |

运行时比较新旧阶段指纹，找到第一个不一致阶段：

- 该阶段及其全部下游标记 `invalidated` 并重建；
- 更早阶段标记 `reused`，直接读取缓存产物；
- `GET /api/status`、`GET /api/fragments/{id}` 和 CLI `status` 暴露每个阶段的指纹、复用/失效状态和原因。

已经验证的原因包括：像素变化、掩码变化、风格参考内容变化、强度变化、迭代轮数变化、输出格式变化、格式能力配置变化。

## 快照、原子写入、并发与停止

### 阶段记录

每个分片每个阶段完成后，服务原子写入：

```text
runtime/snapshots/stages/<sequence>/<fragment_id>/<stage>.json
```

记录包含 `snapshot_sequence`、输入指纹、配置指纹、产物摘要、阶段状态和失效原因。写入使用同目录临时文件、`fsync` 和 `os.replace`；中断只会留下可清理临时文件，不会污染正式记录。

### 快照发布

`runtime/snapshots/current.json` 包含快照序号、所有阶段记录、运行摘要和整体摘要。发布在跨进程文件锁内执行：

- 首次发布必须是序号 1；
- 后续只能发布 `current + 1`；
- 回退、重复、跳号返回 `E_STALE_SNAPSHOT`；
- 候选内容与 `digest` 不符，或当前快照被整体替换后摘要不符，返回 `E_SNAPSHOT_DIGEST`；
- 正常推进返回 `published / normal_advance`。

另有一个原子高水位文件记录已发布的最大序号和摘要。若 `current.json` 被整体替换成较旧的有效快照，状态查询会报告 `rollback`，而不是把它误判为正常；发布方仍可从高水位之后继续按连续序号恢复。

Unix 使用 `fcntl.flock`，Windows 使用 `msvcrt.locking`。

### 停止与恢复

`POST /api/control/stop` 或 CLI `stop` 写入跨进程停止请求。运行器只在分片边界检查停止请求，因此当前不可拆分分片会完成全部四个阶段后才退出。`POST /api/control/resume` 或 CLI `resume` 清除请求。

## CLI

所有命令都支持全局 `--root` 与 `--runtime`，例如：

```bash
python -m image_workbench --root . --runtime runtime status
```

常用流程：

```bash
python -m image_workbench generate-sample --out data
python -m image_workbench import --manifest data/manifest.json
python -m image_workbench run
python -m image_workbench status
python -m image_workbench fragment fragment_a
python -m image_workbench convert --fragment fragment_a --to png --out runtime/exports
python -m image_workbench convert --fragment fragment_a --to jpeg --out runtime/exports
python -m image_workbench convert --fragment fragment_a --to json --out runtime/exports
python -m image_workbench import-exchange --image runtime/exports/fragment_a.jpg --descriptor runtime/exports/fragment_a.jpeg.iwb.json
python -m image_workbench export-project --out runtime/internal-project.json --artifact style
python -m image_workbench import-project --project runtime/internal-project.json
python -m image_workbench config --file config/format_config.json
python -m image_workbench stop
python -m image_workbench resume
python -m image_workbench snapshot status
python -m image_workbench demo
```

旧版清单升级：

```bash
python -m image_workbench import \
  --manifest data/legacy_manifest.json \
  --legacy \
  --legacy-config config/legacy_rules.json
```

升级后的 schema v2 清单写入 `runtime/upgraded_legacy_manifest_v2.json`。`config/legacy_rules.json` 可配置坐标精度、坐标原点（`top-left` / `bottom-left`）、强度百分比基数、ID 前缀/盐和默认迭代轮数。默认规则使用图像尺寸归一化并保留旧像素掩码；ID 为带盐哈希生成的稳定 `legacy-*` 标识，强度按可配置百分比基数归一化。

## HTTP API

交互页：`GET /`。

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/api/health` | 版本、Python、工作区 |
| POST | `/api/project/bootstrap-sample?run=true` | 载入样本并可选运行 |
| POST | `/api/project/import` | 导入 v2 或 v1 清单 |
| POST | `/api/project/import-internal` | 导入内部无损工程 JSON |
| POST | `/api/project/export` | 导出内部工程 JSON |
| POST | `/api/run` | 增量或强制完整运行 |
| GET | `/api/status` | 工程、阶段、指纹、快照总状态 |
| GET | `/api/fragments/{id}` | 单分片状态与失效预览 |
| GET | `/api/fragments/{id}/fingerprints` | 阶段版本指纹 |
| PATCH | `/api/fragments/{id}` | 修改强度、轮数、掩码、参考或输出偏好 |
| PUT | `/api/config/format` | 替换格式能力配置 |
| POST | `/api/convert` | 导出 PNG/JPEG/JSON 描述 |
| POST | `/api/import-exchange` | PNG/JPEG + sidecar 回补 |
| POST | `/api/control/stop` | 请求优雅停止 |
| POST | `/api/control/resume` | 恢复 |
| GET | `/api/snapshots` | 当前快照状态 |
| POST | `/api/snapshots/inspect` | 分类候选快照 |
| POST | `/api/snapshots/publish` | 发布候选快照 |

错误统一返回：

```json
{
  "ok": false,
  "error": {
    "code": "E_FORMAT_LOSS",
    "message": "...",
    "fragment_id": "fragment_a",
    "pixel": [0, 0],
    "details": {"lost_items": ["mask"]}
  }
}
```

主要错误码：

| 错误码 | HTTP | 含义 |
|---|---:|---|
| `E_FORMAT_LOSS` | 422 | 格式能力不足、描述缺失/尺寸不符/掩码不可回补 |
| `E_NUMERIC` | 422 | NaN、Inf、强度越界、无法修复 |
| `E_STALE_SNAPSHOT` | 409 | 快照回退、重复或跳号 |
| `E_SNAPSHOT_DIGEST` | 409 | 快照摘要不符 |
| `E_VALIDATION` | 400 | 结构或参数非法 |
| `E_NOT_FOUND` | 404 | 分片/文件不存在 |
| `E_BUSY` | 409 | 同一进程已有运行任务 |

## 实际验证结果

本会话实际使用的解释器为 CPython `3.11.18`。

```text
pytest: 43 passed
demo:   45 / 45 checks passed
```

固定摘要再次确认：

```text
sample_digest            444291c68e0861de51c62283d222107eb85829772f02538b6cae9185e9d35351
pipeline_summary_digest  692aee759bb4c936733fa9b39f6fc4901ec97d5a501779d63e4737c9a91a587c
```

演示脚本实际注入并捕获：

```text
JPEG without descriptor                E_FORMAT_LOSS
JPEG descriptor dimension mismatch     E_FORMAT_LOSS
invalid style strength                 E_NUMERIC   fragment_a, pixel [0,0]
NaN pixel matrix                       E_NUMERIC   fragment_a, pixel [3,2]
stale duplicate snapshot publication   E_STALE_SNAPSHOT
stale gap snapshot publication         E_STALE_SNAPSHOT
snapshot digest mismatch publication   E_SNAPSHOT_DIGEST
```

真实 Uvicorn 服务在 `127.0.0.1:8767` 的验证结果：

```text
GET  /api/health                      200
GET  /                                 200, 14814 bytes
POST /api/project/bootstrap-sample    200, digest 692a...587c
GET  /api/status                      200, 5 fragments, snapshot 1 normal
POST /api/convert (JPEG)              200, three explicit loss items
PATCH invalid strength                422, E_NUMERIC, fragment_a, pixel [0,0]
POST /api/control/stop                200, stop_requested=true
POST /api/control/resume              200, stop_requested=false
```

CLI 也实际执行了 `import -> run -> status -> convert`，运行摘要同为 `692aee75...587c`。

### Windows 说明

代码刻意只使用跨平台标准库/依赖，并提供 `setup_windows.bat`、`run_demo.bat`、`run_server.bat`。Windows 文件锁走 `msvcrt.locking`，原子写入在关闭文件句柄后调用 `os.replace`。本次执行环境是 WSL，且当前会话的 Windows 互操作不可用，无法在这里实际启动 `cmd.exe` 执行 `.bat`；上述实际数字来自同一代码在 Python 3.11 下的 WSL/Linux 运行。Windows 本机入口就是上述三个批处理文件。

## 代码结构

```text
image_workbench/
  model.py         内部模型、字段校验、工程序列化
  masks.py         归一化矩形、alpha/像素掩码互转
  codecs.py        PNG/JPEG/JSON 描述器编解码与降级校验
  manifest.py      schema v2 清单导入
  legacy.py        schema v1 升级与 v2 导出
  algorithms.py    确定性修复与颜色统计迁移
  numeric.py       NaN/Inf、强度、8 位量化校验
  fingerprints.py  四阶段版本指纹与失效原因
  pipeline.py      增量执行、缓存、停止边界
  snapshots.py     原子阶段记录、快照分类与跨进程锁
  service.py       会话持久化与业务编排
  api.py           FastAPI 路由与统一错误响应
  cli.py           CLI
  demo_data.py     固定种子样本生成
  demo.py          45 项验收演示
  static/index.html 本地交互页
tests/             43 项自动化测试
config/            格式能力与旧版升级规则样例
data/              已生成的确定性样本
setup_windows.bat / run_demo.bat / run_server.bat
```

## 已知边界

- JPEG 颜色量化是格式本身的损失，不能伪装成无损；工作台只保证显式记录、掩码零差异和元数据无损。
- 风格迁移使用参考图整图 RGB 统计量，不做语义分割或真实生成模型推理。
- 工作台面向本地单用户演示；路径访问不做多租户隔离，不应暴露到不可信网络。
- 快照序号由当前快照推导，跨进程并发发布有锁保护；若多个进程同时准备同一序号，只有一个发布成功，其他得到 `E_STALE_SNAPSHOT`。
