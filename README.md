# GridPipe：确定性表格与图表结构管线

本项目从零实现一个完全本地、可复现的 PNG 表格/图表结构化管线。样本由固定随机种子生成器产出，不依赖 OCR 模型、外部数据集、GPU、容器或线上服务。运行时仅依赖 `numpy` 与 `Pillow`。

## 快速开始

Linux / WSL：

```bash
bash scripts/setup_offline.sh
.venv/bin/gridpipe demo --root .
```

Windows PowerShell（需要在 `wheelhouse/` 放入与当前 CPython 版本和 Windows 架构匹配的 `numpy`、`Pillow` wheel）：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup_offline.ps1
.\.venv\Scripts\gridpipe.exe demo --root .
```

常规流程：

```bash
.venv/bin/gridpipe init --root . --clean-work
.venv/bin/gridpipe generate --root . --seed 20261122
.venv/bin/gridpipe import --root .
.venv/bin/gridpipe run --root . --workers 2
.venv/bin/gridpipe status --root .
.venv/bin/gridpipe budget --root .
.venv/bin/gridpipe fingerprints --root .
.venv/bin/gridpipe export --root .
```

停止与恢复：

```bash
# 另一个终端或 HTTP 控制面发出停止请求
.venv/bin/gridpipe stop --root .
# 当前不可拆分分片阶段完成后退出
.venv/bin/gridpipe resume --root . --workers 2
```

本地 HTTP 服务：

```bash
.venv/bin/gridpipe serve --root . --host 127.0.0.1 --port 8765
```

主要接口：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/api/import` | 导入并校验本地清单与 PNG |
| POST | `/api/run` | 启动全部或指定阶段，可设置 `workers`、`stop_after` |
| POST | `/api/stop` | 请求优雅停止 |
| POST | `/api/resume` | 恢复当前运行 |
| GET | `/api/status` | 状态、阶段计数、错误、未处理分片 |
| GET | `/api/fingerprints` | 各分片四阶段版本指纹 |
| GET | `/api/budget` | 接纳顺序、预算消耗与未处理原因 |
| GET | `/api/shards/<id>` | 单分片结构化结果 |
| POST/GET | `/api/export` | 写出或查询结构化导出 |

## 目录结构

```text
data/                固定种子生成器产出的 PNG、manifest.json、generation.json
work/                检查点、内容寻址缓存、运行状态、停止请求
output/              export/tables/charts/validation/budget 与验证报告
wheelhouse/          本地 wheel 包，安装时使用 --no-index
gridpipe/            应用代码
tests/               pytest 验收测试
scripts/             离线安装与演示脚本
```

## 四阶段管线

1. **区域检测**：灰度化、阈值化、确定性 8 连通域标记，再对连通域内部做行/列投影。表格候选按多组完整网格线评分，图表候选按外框面积选择。
2. **表格结构**：在表格框内寻找行/列投影峰；对每个相邻单元格的分隔线段做缺失检测，用并查集恢复跨行/跨列合并；按首行背景亮度识别表头。
3. **图表结构**：定位绘图框，在框外读取刻度像素，用内置 5×7 位图模板解码刻度数字；验证刻度间距，再做像素到数据坐标的线性映射。柱系列由颜色连通块提取，折线系列由颜色密度窗口定位标记点。
4. **验证汇总**：按清单真值计算表格/图表区域 IoU、单元格行列与合并跨度准确率、表头准确率、各系列数值 MAE/最大误差。

中英混排文本由确定性位图块模拟，仅用于视觉样本；项目不宣称 OCR 能力。序列名使用稳定的 `bar-N` / `line-N` 标识，而不是识别图例文字。

## 调度、预算与确定性

- 每个分片的预估成本为 `ceil(width * height * tier_factor)`，且强制不小于实际像素数。
- 只有同时满足“单分片不超过 worker 配额”和“累计预留不超过总预算”的分片才被接纳。
- 未接纳分片保留 `worker_pixel_quota_exceeded` 或 `total_pixel_budget_exceeded` 原因。
- 已接纳分片由本地 `ProcessPoolExecutor` 按阶段并行执行；父进程按分片序号和阶段序号排序后发布检查点。
- `--workers 1` 只改变执行并发，不改变业务配置或运行 ID。串行与并行的导出逐字节一致。
- 不使用线程超时、看门狗或“强制终止失控计算”的语义。停止只发生在不可拆分任务边界。

## 检查点与发布保护

- 每个 `(分片, 阶段)` 产物先内容寻址写入缓存，再用同目录临时文件、`fsync`、`os.replace` 原子发布记录。
- 记录包括：分片 ID/序号、阶段、输入指纹、配置版本、产物摘要、全局单调序号、前一记录摘要链。
- POSIX 使用 `flock`，Windows 使用 `msvcrt.locking`；发布在同一工作区文件锁内完成。
- 序号必须严格等于上一条加一；同阶段的分片序号必须递增；同类阶段不能重复发布；缺失前置阶段返回 `E_INVALID_STAGE`。
- 恢复时只接受从序号 1 开始、摘要链连续、产物摘要可验证的记录前缀。损坏、半写、孤立或重复记录被忽略。若重新发布同一内容，后续孤立记录可在摘要链重新连通后被重新接纳。

## 版本指纹与增量失效

配置或输入变化会通过阶段指纹映射到最早失效阶段：

| 变化 | 最早失效阶段 |
|---|---|
| PNG 像素摘要、检测阈值/投影参数 | `detection` |
| 表格切分容差、清单声明行列数 | `table` |
| 图表映射/刻度参数 | `chart` |
| 预算、真值、接纳集合 | `validation` |

阶段链为 `detection -> table -> chart -> validation`。运行时优先复用内容寻址缓存；表格配置变化时检测缓存继续复用，而表格、图表、验证重建；预算变化只重建验证。

## 业务错误码

| 错误码 | 触发条件 |
|---|---|
| `E_INVALID_REGION` | 未检测到候选、区域越界或零面积 |
| `E_TABLE_STRUCTURE` | 行列数冲突、方向冲突、非矩形合并、跨度越界、零面积单元格 |
| `E_CHART_AXIS` | 轴范围零跨度、刻度非均匀、刻度不可解码、数据点越界；诊断保留已提取合法序列 |
| `E_BUDGET` | 预算/大小档配置非法 |
| `E_STALE_CHECKPOINT` | 序号落后/重复/不连续、跨分片错位、指纹产物冲突 |
| `E_INVALID_STAGE` | 非法阶段跳转或缺失前置产物 |
| `E_WORKER` | 工作进程未处理异常的结构化记录 |

## 实际验收结果

本工作区已执行 `.venv/bin/python -m pytest -q`，结果为 **20 passed**。完整演示由 `.venv/bin/gridpipe demo --root .` 生成，`output/verification.json` 与 `output/VERIFICATION.md` 记录全部结果：

- 固定种子：`20261122`，5 个分片；4 个接纳，第 5 个因总预算拒绝。
- 表格区域 IoU 均值：`1.0`
- 图表区域 IoU 均值：`1.0`
- 单元格行列/合并准确率均值：`1.0`
- 图表系列 MAE 均值：`0.089076`
- 停止于检查点序号 `2`，恢复后完成于序号 `16`。
- 完整运行、串行运行、停止/恢复运行的 `export.json` SHA-256 均为：
  `8865f8e4ca99cf67da2143e4ffb3d494e29224af1c1a09ebec091c02c287a27c`
- 注入并验证：`E_INVALID_REGION`、`E_TABLE_STRUCTURE`、`E_CHART_AXIS`、`E_STALE_CHECKPOINT`、`E_INVALID_STAGE`。

## 环境说明与限制

当前执行环境实际是 **WSL2 内的 Ubuntu/Linux**（Python 3.14.4），并非原生 Windows。代码对 Windows 做了 `spawn` 进程上下文和 `msvcrt` 文件锁分支，但本工作区无法声称已在原生 Windows 上实测。`wheelhouse/` 当前包含 Linux CPython 3.14 的离线 wheel；原生 Windows 需要补充匹配平台的 wheel 后运行 `scripts/setup_offline.ps1`。

这是确定性模拟管线，不是通用自然图像 OCR 系统。检测与提取依赖生成器约定的色块、边框和位图刻度；真实扫描件、旋转、抗锯齿、复杂图例文字等不在本项目验证范围。

