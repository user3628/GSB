# incseg — 语义与实例分割增量更新引擎

纯本地、确定性、可复现的四阶段分割流水线与增量构建工作台。不依赖任何真实数据集、
预训练模型、GPU 或容器；样本数据由固定随机种子生成器离线产出。

## 环境要求

- Python 3.10+（Windows / Linux / macOS 均可）
- 依赖：`numpy`、`Pillow`（`pip install -r requirements.txt`，可使用 venv）

```bash
python -m venv .venv
.venv/Scripts/activate        # Windows；POSIX 用 source .venv/bin/activate
pip install -r requirements.txt
```

## 快速开始

```bash
# 1. 生成确定性样本工作区（4 分片、多类别、每分片 >=2 个同类实例区域）
python -m incseg --workspace ws init --tiles 4 --seed 42

# 2. 全量构建（4 阶段 x 4 分片 = 16 个不可拆分单元）
python -m incseg --workspace ws run

# 3. 查看各阶段状态 / 版本指纹 / 失效原因
python -m incseg --workspace ws status [--tile tile_000]

# 4. 单分片配置变更后的增量重建 + 与从零全量构建逐字节比对
python -m incseg --workspace ws set-config --tile tile_002 --set min_area=80
python -m incseg --workspace ws rebuild --tile tile_002
python -m incseg --workspace ws verify

# 5. 优雅停止与恢复（停止请求在阶段单元间生效）
python -m incseg --workspace ws stop
python -m incseg --workspace ws resume

# 6. 导出掩码与摘要 / 启动本地 HTTP 服务（仅回环）
python -m incseg --workspace ws export --out output
python -m incseg --workspace ws serve --host 127.0.0.1 --port 8765

# 7. 一键完整演示（全量 -> 停止恢复 -> 增量重建 -> HTTP）
python -m incseg --workspace demo_workspace demo --with-http
# 或 scripts/run_demo.sh / scripts/run_demo.bat
```

运行测试：`python -m unittest discover -s tests -v`（或 `scripts/run_tests.*`）。

## 四阶段流水线

每个分片按固定顺序执行四个阶段，每阶段是不可拆分单元，完成后原子写入检查点：

| 阶段 | 输入 | 配置项 | 产物 |
| --- | --- | --- | --- |
| `classify` | 分片 RGB PNG | `color_map`、`smooth_weight`、`smooth_iterations` | `labels.png`（8-bit 标签图） |
| `instances` | 标签图 | `connectivity`、`min_area` | `instances.png`（16-bit）、`instances.json` |
| `postprocess` | 标签图 | `refine_iterations`、`fusion_weight`、`class_priority` | `labels.png`（细化后） |
| `quantize` | 细化标签图 + 实例产物 | 调色板（源自 `color_map`） | `semantic.png`、`instance.png`、`summary.json` |

- **语义分类**：逐像素按配置颜色映射打分（负平方距离归一化到 [0,1]），叠加可配置
  邻域平滑权重（8 邻域同类占比），同步迭代保证与顺序无关；平票取最小类别 id。
- **实例分离**：按类别升序、行主序扫描做连通域标记（4/8 连通），面积小于
  `min_area` 的区域被过滤（实例 id 置 0）；实例按发现顺序连续编号，输出每实例的
  类别、像素数与外接框。
- **边界后处理**：可配置迭代次数的同步细化；候选类别取邻域多数，平票按
  `class_priority` 裁决，再平票取最小类别 id（完全确定）；采用阈值为
  `fusion_weight * 有效邻居数`。
- **量化输出**：语义掩码为调色板 PNG（索引=类别 id，颜色来自配置），实例掩码为
  16-bit 灰度 PNG；摘要含每类像素占比（配置的类别全部列出，禁止静默丢弃）、
  实例数与边界像素数（4 邻域存在异类的像素）。

同一输入与配置必产生逐字节一致的产物：所有随机性来自 `numpy.random.RandomState`
（跨版本稳定），PNG 编码参数固定，JSON 排序序列化，浮点占比保留 6 位小数。

## 检查点与恢复

- 记录位置：`checkpoints/<tile_id>/NNNNNN_<stage>.json`，包含分片标识、阶段名、
  单调序号、输入内容指纹、配置版本、产物摘要（逐文件 sha256 清单）与引擎版本。
- 原子写入：临时文件 + `os.replace`，任意位置中断不会留下半写记录；恢复时损坏、
  不完整、重复或序号不连续的记录被隔离（重命名为 `.corrupt`/`.gap`/`.dup`）并忽略，
  从最近一致检查点继续。
- 优雅停止：停止请求（`stop` 命令 / HTTP `/stop`，写入 `state/STOP`）在阶段单元
  之间生效——当前单元完成后退出；不承诺强制终止后生成退出报告。恢复后最终掩码与
  摘要与一次从零完整运行逐字节一致，检查点序号连续推进、无跳跃与重复。

## 增量失效传播

阶段指纹 = `sha256(tile_id, stage, engine_version, 上游内容摘要, 阶段配置子集)`。
任一输入变化时定位最早受影响阶段，该阶段及全部下游产物失效并重建，未受影响上游
产物继续复用：

| 变化 | 最早失效阶段 |
| --- | --- |
| 分片图像内容 | `classify`（原因 `input_changed`） |
| `color_map` / `smooth_weight` / `smooth_iterations` | `classify`（`config_changed`） |
| `connectivity` / `min_area` | `instances`（`config_changed`） |
| `refine_iterations` / `fusion_weight` / `class_priority` | `postprocess`（`config_changed`） |
| 上游阶段失效 | 下游各阶段（`upstream_invalidated`） |
| 产物被篡改/丢失 | 对应阶段（`artifact_mismatch`） |

失效原因与各阶段指纹可通过 `status` 命令或 HTTP `GET /tiles/{id}`、`GET
/invalidations` 查询。`verify` 在独立临时工作区从零全量重建基准，与增量产物逐字节
比对（sha256）；不一致时返回 `E_INCREMENTAL_MISMATCH` 并附差异文件清单，增量结果
不会写入校验基准。

## 并发发布保护与错误码

多进程发布检查点时经文件锁（POSIX `fcntl` / Windows `msvcrt`）串行化并比较序号，
仅允许序号连续推进：

| 错误码 | 含义 |
| --- | --- |
| `E_STALE_CHECKPOINT` | 落后/重复/跳号的检查点发布（HTTP 409） |
| `E_INVALID_STAGE` | 缺失前置阶段检查点时发布下游阶段 |
| `E_INVALID_CONFIG` | 越界阈值、缺失/非法颜色映射等非法配置 |
| `E_INCREMENTAL_MISMATCH` | 增量与全量逐字节比对不一致（附差异清单，HTTP 409） |
| `E_WORKSPACE` | 工作区/分片/图像缺失等一般错误 |

## HTTP 接口（仅监听回环地址）

`python -m incseg --workspace ws serve --host 127.0.0.1 --port 8765`

- `GET /health`、`GET /tiles`、`GET /tiles/{id}`（阶段状态与指纹）
- `GET /invalidations`（各分片失效原因）
- `GET /tiles/{id}/export?kind=semantic|instance|summary`（导出掩码/摘要）
- `POST /run`、`POST /stop`、`POST /resume`、`POST /verify`
- `POST /tiles/import`（`image_path` 或 `image_base64`，`like` 复制模板配置）
- `POST /tiles/{id}/rebuild`（单分片增量更新入口）

绑定非回环地址会被拒绝（`E_WORKSPACE`）。

## 配置格式

```json
{
  "config_version": "1.0.0",
  "seed": 42,
  "tiles": [{
    "tile_id": "tile_000",
    "image": "tiles/tile_000.png",
    "color_map": {"#1c1c20": 0, "#dc2828": 1, "#28c83c": 2, "#3250dc": 3},
    "class_names": {"0": "background", "1": "red", "2": "green", "3": "blue"},
    "class_priority": [1, 2, 3, 0],
    "connectivity": 8,
    "min_area": 24,
    "smooth_weight": 0.6,
    "smooth_iterations": 2,
    "refine_iterations": 2,
    "fusion_weight": 0.5
  }]
}
```

## 工作区布局

```
ws/
  config.json                 # 工作区配置
  tiles/                      # 输入分片 PNG
  artifacts/<tile>/<stage>/   # 各阶段产物
  checkpoints/<tile>/         # 原子检查点记录
  locks/                      # 文件锁
  state/                      # STOP 标记、last_run.json、临时校验区
```

## 实际验证结果（本仓库交付时运行）

- `python -m unittest discover -s tests`：**21 个测试全部通过**（约 3 秒），覆盖：
  同种子逐字节一致、重复运行全复用、摘要/实例/边界正确性、实例掩码与清单一致、
  损坏/截断检查点忽略与恢复、`E_STALE_CHECKPOINT`、`E_INVALID_STAGE`、产物篡改检测、
  六类配置变更的失效传播矩阵、图像变更全链重建、重建后序号连续、停止恢复逐字节
  一致、外部停止标志、`E_INCREMENTAL_MISMATCH` 差异清单、11 种非法配置、HTTP 全端点。
- `python -m incseg --workspace demo_workspace demo --with-http`：全部检查通过——
  全量 16 单元；停止于第 5 单元后恢复，产物与不间断构建逐字节一致，序号连续；
  `tile_002 min_area 24->80` 后 `classify` 复用、`instances/postprocess/quantize`
  重建；`verify` 12 个文件逐字节比对一致；HTTP 8 个端点全部 200。
- 固定摘要（seed=42, tile_000）：background 7683 (0.833659)、red 792 (0.085938)、
  green 348 (0.037760)、blue 393 (0.042643)，实例数 4，边界像素 560。
