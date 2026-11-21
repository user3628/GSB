# recon3d — 本地确定性多视角图像三维重建管线

纯本地、确定性、无 GPU / 无预训练模型 / 无外部站点依赖的多视角图像三维几何
数据处理与结果验证工具。固定随机种子产出可复现样本，四个阶段通过内容指纹、
原子检查点与规范化序列化，保证跨运行、停止恢复、增量重建与全量重建**逐字节
一致**。

## 环境准备

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt   # numpy, fastapi, uvicorn
```

要求 Python 3.11+（开发与验证在 3.14 完成，亦兼容 3.11+）。

## 一分钟快速验证

```bash
python -m recon3d.demo                      # 一键确定性演示（含错误码/停止恢复/增量）
python -m unittest tests.test_pipeline -v   # 12 项测试
```

## 目录结构

```
recon3d/
  canonical.py     # 确定性 JSON、SHA-256 指纹、原子写、纯标准库 PNG、文件锁
  errors.py        # 业务错误码与结构化异常链路
  generator.py     # 固定种子样本生成器（5 分片 / 8 真值点）
  features.py      # 阶段一：规范化 + 局部峰角点 + HOG 描述子
  triangulation.py # 阶段二：位姿校验 + 视线最小二乘交会 + 全局一致性匹配
  geometry.py      # 阶段三：重投影裁剪 + 确定性体素降采样
  packaging.py     # 阶段四：PLY 点云 + 真值验证摘要
  fingerprint.py   # 各阶段内容摘要构成的版本指纹链
  checkpoint.py    # 原子检查点、单调序号、文件锁并发发布保护、恢复扫描
  query.py         # 延迟预算 + 内容寻址缓存
  pipeline.py      # 编排：优雅停止、恢复、增量失效重建
  api.py / cli.py  # FastAPI HTTP 接口 / 命令行
tests/test_pipeline.py
```

## 确定性样本

`recon3d/generator.py` 以种子 `20261121` 生成：

- 8 个真值 3D 点，分布在两个旋转 45° 的方形环上（满足 ≥6 点要求）；
- 5 个灰度 PNG 分片：3 个 32×32 宽视角（不同方位角/俯仰角光心，8 点全可见
  且像素可分），1 个 24×24 与 1 个 16×16 同光心变焦近景色，体现平移/缩放/
  明暗变化；
- 清单 `samples/manifest.json` 逐分片声明旋转矩阵 `R`、平移 `t`、内参 `K`、
  可见真值点像素/世界坐标、采样密度（体素尺寸）与算法偏好。

同种子重复生成，清单与所有 PNG 逐字节一致。真值点云仅由生成器产出，是验证
唯一真值来源。

## 四个阶段

1. **特征提取** (`features.py`)：零均值/单位方差规范化 → 3×3 局部峰角点检测
   + 非极大值抑制 → 9×9 窗 Sobel 梯度、16 方向箱线性插值投票、L2 归一化的
   HOG 描述子。空白/恒定/零梯度图返回 `E_EMPTY_FEATURE`（带分片标识），绝不
   静默产出空清单。
2. **多视角三角化** (`triangulation.py`)：校验旋转正交性（`R·Rᵀ=I`、
   `|det R|=1`）与正焦距；像素经内参/位姿转世界系视线，枚举跨视角组合并按
   最小二乘交会后的最大重投影误差做**全局一对一排他分配**，再以视线最小二乘
   求三维坐标，记录参与视角数与逐视角/最大重投影误差。非正交位姿、零/负焦距、
   单视角均返回 `E_INVALID_POSE` 且不输出未验证坐标。
3. **几何裁剪与降采样** (`geometry.py`)：按可配置重投影阈值剔除并记录原因；
   体素键 `floor(点/尺寸)` 确定性降采样，同体素保留误差最小（平局按 track_id）
   的代表点。裁剪后低于下限、或严格模式下出现重复体素键返回
   `E_GEOMETRY_DEGENERATE` 并保留诊断。
4. **结果封装** (`packaging.py`)：导出 ASCII PLY 点云，与真值做一对一最优
   匹配，输出 RMSE/最大误差/覆盖率/逐点误差的验证摘要。

## 一致性与可靠性机制

- **版本指纹与增量失效** (`fingerprint.py`)：样本指纹 = 清单 + 全部 PNG 字节；
  每阶段输入指纹串联上游产物摘要与该阶段配置。分片像素/位姿/内参/阈值/密度任一
  变化都会改变对应阶段指纹，定位最早受影响阶段，令其与全部下游失效重建，未变化
  上游复用。增量结果与同输入从零全量重建逐字节比对，不一致返回
  `E_INCREMENTAL_MISMATCH`（附差异点数量）。
- **原子检查点与恢复** (`checkpoint.py`)：产物 `tmp→fsync→rename` 原子写；
  journal 记录阶段名、分片标识、输入指纹、配置版本、产物摘要、单调序号与记录
  摘要。恢复时忽略损坏/不完整/序号重复记录，只接受从 1 连续递增的最长前缀。
- **优雅停止**：停止请求在当前不可拆分单元完成后于阶段边界生效
  （`E_STOPPED`），恢复从最近一致检查点继续。
- **并发发布保护**：跨进程 `flock` + 分片内序号比较；输入指纹未变的重复/落后
  发布拒绝为 `E_STALE_CHECKPOINT`，非法阶段或缺失前置产物为 `E_INVALID_STAGE`，
  错误均带阶段名与分片标识。
- **延迟预算与缓存** (`query.py`)：每次查询带预算与查询标识，各检查点比对已耗时，
  超时返回含 `query_id` 与 `elapsed_ms` 的 `E_LATENCY_BUDGET`（HTTP 503）而非
  阻塞。缓存键含样本指纹、位姿配置版本、阶段产物摘要与查询参数，阶段指纹变化后
  键自然改变、旧缓存不命中；缓存值即冷计算规范化字节，命中与冷计算逐字节一致。

## 业务错误码

| 错误码 | 触发条件 |
| --- | --- |
| `E_EMPTY_FEATURE` | 空白/恒定/零梯度分片，无有效特征 |
| `E_INVALID_POSE` | 旋转非正交、零/负焦距、点仅单视角 |
| `E_GEOMETRY_DEGENERATE` | 裁剪/降采样后点数过低或严格模式重复体素键 |
| `E_LATENCY_BUDGET` | 查询超过延迟预算（带 query_id / elapsed_ms） |
| `E_STALE_CHECKPOINT` | 重复/落后/跨分片错位的检查点发布 |
| `E_INVALID_STAGE` | 非法阶段跳转或缺失前置阶段产物 |
| `E_INCREMENTAL_MISMATCH` | 增量与全量重建产物不一致（附 diff_count） |
| `E_STOPPED` | 优雅停止请求生效 |

## CLI 用法

```bash
python -m recon3d.cli generate --out samples
python -m recon3d.cli run --samples samples --run runs/demo --force
python -m recon3d.cli run --samples samples --run runs/demo --stop-after triangulation:global
python -m recon3d.cli resume --samples samples --run runs/demo
python -m recon3d.cli status --samples samples --run runs/demo
python -m recon3d.cli query --run runs/demo --type cloud_summary --budget 1.0
python -m recon3d.cli export --run runs/demo
```

## HTTP 接口（仅绑定 127.0.0.1）

```bash
RECON3D_PORT=8077 python -m recon3d          # 或 uvicorn recon3d.api:app
```

`POST /samples/import`、`POST /rebuild`、`GET /stages`、`GET /invalidation`、
`POST /stop`、`POST /recover`、`POST /query`、`GET /export`、`GET /health`。
业务异常统一渲染为 `{"error":{code,message,...}}` 并带对应 HTTP 状态码。

## 实际验证结果（本地，种子 20261121）

- 样本确定性：重复生成清单与 5 张 PNG 逐字节一致；
- 特征召回：5 个分片检测峰数与可见真值点数完全相等；
- 三角化：8 条轨道全部多视匹配正确，3D RMSE ≈ 0.0845，最大误差 ≈ 0.1343，
  覆盖率 100%；
- 停止/恢复：与从零全量重建的全部阶段产物、PLY、验证摘要逐字节一致；
- 增量重建：修改分片像素与误差阈值后，仅受影响分片及下游重建，与全量重建
  差异点数量 0；
- 延迟预算：零预算查询稳定返回 `E_LATENCY_BUDGET`（含 elapsed_ms）；缓存命中与
  冷计算逐字节一致，阶段指纹变化后旧缓存不命中；
- 并发：双进程发布经文件锁串行化，序号 1..N 连续无重复；损坏 journal 行被正确截断；
- 错误码：非正交位姿、零焦距均返回 `E_INVALID_POSE` 并附视角标识。

`python -m unittest tests.test_pipeline` 全部 12 项通过。
