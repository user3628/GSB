# Voxel Engine — 多视角图像 → 体素化场景表示重建引擎

本仓库从零实现一个本地可运行的确定性重建引擎：输入固定随机种子生成的多视角
PNG 分片与一份 JSON 场景工程清单（相机内外参、地标投影、体素参数均显式声明、
视为不可变输入，**不求解相机位姿**），按四个阶段构建体素化三维场景表示，并支持
持久化检查点、版本指纹失效传播、增量/全量一致性校验、并发安全发布、本地 HTTP
与命令行交互。不依赖 GPU、外部服务、账号或线上素材；第三方依赖仅 NumPy 与
Pillow。

## 环境准备

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt   # numpy, pillow
```

所有命令均在仓库根目录执行（Windows 下将 `.venv/bin/python` 换成
`.venv\Scripts\python`，路径读写、原子替换与文件锁均已做跨平台处理）。

## 快速开始

```bash
# 1. 生成固定种子样本（>=4 个视角分片 + 场景工程清单 + 备用替换分片）
.venv/bin/python -m apps.cli generate --out data/sample

# 2. 导入工作区并全量构建
.venv/bin/python -m apps.cli import --workspace ws --manifest data/sample/manifest.json
.venv/bin/python -m apps.cli build  --workspace ws --mode full

# 3. 查询阶段状态 / 版本指纹 / 稀疏度统计 / 失效原因
.venv/bin/python -m apps.cli status --workspace ws

# 4. 增量入口：整体替换一个分片 / 修改单项配置，仅重建失效链路
.venv/bin/python -m apps.cli replace-view --workspace ws --view view_02 \
    --image data/sample/shards_alt/view_02.png
.venv/bin/python -m apps.cli build --workspace ws --mode incremental
.venv/bin/python -m apps.cli set-config --workspace ws --set voxel_resolution=0.4
.venv/bin/python -m apps.cli build --workspace ws --mode incremental

# 5. 增量 vs 从零全量：逐字节一致性校验
.venv/bin/python -m apps.cli verify --workspace ws

# 6. 阶段间停止 → 恢复 → 续建
.venv/bin/python -m apps.cli build --workspace ws2 --mode full --stop-after voxelize:view_01
.venv/bin/python -m apps.cli recover --workspace ws2
.venv/bin/python -m apps.cli build --workspace ws2 --mode resume

# 7. 导出全局体素摘要与校验报告；启动本地 HTTP 接口
.venv/bin/python -m apps.cli export --workspace ws
.venv/bin/python -m apps.cli serve --workspace ws --port 8765

# 8. 一键确定性演示（生成固定摘要）
.venv/bin/python -m scripts.run_demo --dir demo_out
```

## 样本生成器与清单

`voxel_engine/generator.py` 以 `numpy.RandomState(seed)` 固定抽签顺序生成：

- `shards/view_XX.png`：同一合成几何体（立方体角点 + 球面点 + 平面网格 +
  离群地标）在 5 个固定位姿与逐视角明暗下渲染的 64×48 RGB 视图；
- `shards_alt/view_02.png`：同一视角的替代渲染（不同明暗与噪声流），用于演示
  分片整体替换；
- `manifest.json`：场景工程清单，逐分片声明相机外参（world→camera 旋转/平移）、
  内参（fx/fy/cx/cy）、该视角可见地标投影坐标（含声明深度与场景节点归属），
  以及全局参数：`voxel_resolution`、`truncation_distance`、`fusion_radius`、
  `sparsity_threshold`、`voxel_feature_dim`、`ray_step`、`max_ray_depth`、
  `scene_origin`、`scene_size`。清单即内外参与参数声明文件，引擎不估计位姿。

同一 seed 重新生成逐字节一致（PNG 编码与时间无关，浮点均按固定顺序运算）。

## 四阶段语义

### 1. 视角投影采样（projection，每视角）

对每个声明地标，像素 `(u,v)` 与内外参确定世界射线
`P(t) = C + t·Rᵀ·[(u-cx)/fx, (v-cy)/fy, 1]`，`C = -Rᵀ·t`。在声明深度
±`truncation_distance` 窗口内按 `ray_step` 采样，逐样本重投影读取最近邻图像
颜色；落在视锥外（深度 ≤0 或超过 `max_ray_depth`）或图像边界外的样本被裁剪并
计入 `clipped_count`。输出带分片标识、世界坐标采样点与颜色值的贡献记录
（`artifacts/projection/<view>.<fp>.ve1`），记录顺序 = 地标声明序 × 采样序，
同输入同配置逐字节一致。

### 2. 各视角体素贡献体（voxelize，每视角）

采样点按 `index = floor((p - scene_origin)/voxel_resolution)` 量化；向截断距离
内的体素（中心距 ≤ `truncation_distance`）泼溅，权重 `k(d)=max(0,1-d/trunc)`。
同一体素被同视角多次命中时按记录到达序确定性合并（颜色累加、命中计数、权重
累加、最小深度）；样本深度超过目标体素最小深度一个截断距离以上判为遮挡，跳过
并计入 `occluded_skips`。颜色出现 Inf/NaN 或量化坐标为负/越界时返回
`E_VOXEL_INVALID`（附分片标识与体素坐标）。产物头记录每视角占用体素数
`occupied_voxels` 与被遮挡跳过数。

### 3. 场景体素场融合（fusion，全局）

按分片序、视角内体素坐标升序、邻域偏移升序的固定顺序，把各视角局部块融合为
全局场。权重归一化显式可复算：

```
color(g)  = Σ_v k(|v-g|)·count(v)·color_avg(v) / norm(g)
norm(g)   = Σ_v k(|v-g|)·count(v)        （即体素权重 weight(g)）
k(d)      = max(0, 1 - d/fusion_radius)  （d=0 时 k=1；radius=0 仅同体素）
```

融合仅消费「归一化体素块」（`color_avg = color_sum/count`，唯一规范化点），
因此落盘后再读回的块与内存中逐位一致——这是增量与全量逐字节一致的关键。

### 4. 稀疏化与摘要（sparsify，全局）

按 `weight/max_weight >= sparsity_threshold` 剪枝；剪枝后出现零权重体素或空
场景时返回 `E_SPARSE_EMPTY` 并保留剪枝前后计数。摘要含全局体素占用、颜色摘要
（均值/最小/最大）与稀疏度统计（前后计数、剪枝率、阈值、包围盒）。

## 版本指纹与失效传播

每个阶段实例的版本指纹由其内容输入链式构成：

```
fp(projection:view) = H(stage, cfg₁(view), sha256(image))
fp(voxelize:view)   = H(stage, cfg₂, fp(projection:view))
fp(fusion)          = H(stage, cfg₃, [fp(voxelize:view)…])
fp(sparsify)        = H(stage, cfg₄, fp(fusion))
```

`cfg*` 为各阶段精确依赖的配置子集（projection：像素内容哈希、内外参、
`ray_step`/`max_ray_depth`/`truncation_distance`；voxelize：`voxel_resolution`/
`truncation_distance`/场景边界/特征维度；fusion：`fusion_radius`；sparsify：
`sparsity_threshold`）。任一分片像素、内外参或上述参数变化时，最早受影响阶段
及其全部下游指纹改变并被重建，未受影响上游产物继续复用（绝不重算）；失效原因
（`config_changed`/`input_changed`/`upstream_changed`，含旧新指纹）写入
`state/invalidation.json`，可经 CLI/HTTP 查询。禁止以旧版本结果冒充新结果：
状态接口对每个实例同时暴露 `desired_fingerprint` 与 `published_fingerprint`。

## 检查点、恢复与并发发布

- 每次阶段发布在真实文件锁（POSIX `flock` / Windows `msvcrt.locking`）保护下
  完成：产物与检查点记录均经 tmp+fsync+`os.replace` 原子写入，任意位置中断
  不会留下半写记录；产物路径内容寻址（含指纹），历史记录引用的产物不被覆盖。
- 检查点记录含阶段名、实例、输入分片内容指纹、配置版本与快照、上游指纹、
  产物摘要、单调序号与自校验 checksum。
- 恢复时忽略损坏/不完整/序号重复的记录，校验从 seq 1 起的连续链（含产物摘要
  比对），折叠出有效状态（上游指纹不匹配的记录成为孤儿），从最近一致检查点
  继续；已发布阶段产物不被重复计算。
- 并发发布仅允许序号连续推进：落后/重复/跨阶段错位发布返回
  `E_STALE_CHECKPOINT`；缺失前置阶段产物返回 `E_INVALID_STAGE`；锁等待超时
  返回结构化 `E_LOCK_TIMEOUT`（附锁路径与超时毫秒），不以固定延迟冒充锁。

## 增量与全量一致性

`verify` 在临时目录用当前输入从零全量重建，与当前（可能多次增量后的）状态做
逐字节比对：贡献记录摘要、融合场摘要、全局体素场摘要、稀疏度统计。任何不一致
返回 `E_INCREMENTAL_MISMATCH` 并附首个差异阶段与体素坐标（投影阶段附记录下
标）。演示与测试均覆盖「多次连续增量后仍与一次全量一致」。

## 稳定业务错误码

| 代码 | 含义 |
| --- | --- |
| `E_VOXEL_INVALID` | 颜色 Inf/NaN 或量化体素坐标为负/越界（附分片标识与体素坐标） |
| `E_SPARSE_EMPTY` | 剪枝后零权重体素或空场景（附剪枝前后计数） |
| `E_INCREMENTAL_MISMATCH` | 增量与全量逐字节比对失败（附首个差异阶段与体素坐标） |
| `E_STALE_CHECKPOINT` | 落后/重复/跨阶段错位发布 |
| `E_INVALID_STAGE` | 缺失前置阶段产物或阶段未就绪 |
| `E_LOCK_TIMEOUT` | 文件锁等待超时（结构化，附锁路径与超时） |
| `E_STOPPED` | 已请求停止，需 recover + resume 继续 |
| `E_MANIFEST_INVALID` / `E_NOT_FOUND` | 清单非法 / 输入缺失 |

## HTTP 接口

`python -m apps.cli serve --workspace ws --port 8765`（仅标准库
`http.server`）。业务错误统一返回 HTTP 409 + 结构化错误体。

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/health` | 存活检查 |
| `POST /api/import` | 导入分片与场景清单 `{manifest_path}` |
| `POST /api/build` | 分阶段构建 `{mode: full\|incremental\|resume, stop_after?}` |
| `GET /api/status` | 各阶段状态与版本指纹、稀疏度统计 |
| `GET /api/invalidations` | 失效原因与指纹、稀疏度统计 |
| `POST /api/stop` | 请求停止（阶段间生效） |
| `POST /api/recover` | 执行恢复 |
| `POST /api/view/replace` | 整体替换分片 `{view_id, image_path}` |
| `POST /api/config` | 单配置变更 `{updates: {...}}` |
| `POST /api/verify` | 增量 vs 全量一致性校验 |
| `GET /api/export/summary` | 导出并返回全局体素摘要与校验摘要 |

## 目录结构

```
voxel_engine/            # 引擎库
  generator.py           # 固定种子样本生成器
  manifest.py            # 清单模式与校验
  stages/                # 四阶段：projection/voxelize/fusion/sparsify
  artifacts.py           # 产物规范化二进制编解码
  checkpoints.py         # 检查点发布/扫描/恢复（原子、有效状态折叠）
  locking.py             # 跨平台真实文件锁（结构化超时）
  pipeline.py            # 版本指纹、失效传播、分阶段构建
  engine.py              # CLI/HTTP 共用门面（导入/构建/校验/导出）
apps/cli.py              # 命令行入口
apps/http_server.py      # 本地 HTTP 接口
scripts/run_demo.py      # 确定性端到端演示
tests/                   # unittest 测试套件（55 项）
```

## 测试与验证

```bash
.venv/bin/python -m unittest discover -s tests -t .
```

本机实际验证结果（Python 3.11.18，numpy 2.4.6，pillow 13.0.0）：

- `55` 项测试全部通过（生成器确定性、投影采样、体素化、融合权重手算复算、
  稀疏化错误路径、检查点损坏/重复/截断恢复、锁超时、失效传播、增量-全量
  一致、并发发布拒绝、停止-恢复、HTTP 端到端）。
- 确定性演示 `python -m scripts.run_demo --dir demo_out`：5 视角样本，停止于
  `voxelize:view_01` 后恢复续建，摘要与参考全量一致；`view_02` 整体替换、
  `voxel_resolution 0.5→0.4`、`sparsity_threshold 0.02→0.05` 三次增量后均通过
  逐字节校验；两次完整运行产出相同固定摘要
  `demo_digest = dbb6ad84754c0a5d46958058a8d5e360821505b987d261775e20eb646e051c1f`。

## 备注

- 路径全部经 `pathlib` 处理、原子写经 `os.replace`、锁实现按 `os.name` 分派，
  Windows 上可读写运行（本仓库在 Linux 上完成验证）。
- 工作区目录（`checkpoints/`、`artifacts/`、`state/`、`locks/`、`exports/`）
  即本地临时工作区，用于阶段检查点、文件锁与恢复实验。
