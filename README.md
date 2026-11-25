# matbench — 确定性纹理与材质生成诊断工作台

`matbench` 是一个纯本地、固定种子、无 GPU/无网络素材依赖的纹理与材质管线诊断工作台，
既可作为命令行/HTTP 工作台运行，也可由上游材质服务以 Go 库形式集成。
仅依赖 Go 标准库与一个纯 Go 的 SQLite 驱动（`modernc.org/sqlite`，Windows 免 CGO）。

## 四阶段管线

| 阶段 | 包 | 职责 |
| --- | --- | --- |
| 控制向量 `control` | `internal/control` + `internal/textnorm` | NFKC 子集规范化、空白折叠、全半角统一、小写化、分词、固定哈希 32 维单位向量 |
| 噪声合成 `noise` | `internal/noise` | 确定性可平铺值噪声、逐八度叠加、Kahan 补偿求和、单/多线程逐字节一致 |
| 调色量化 `palette` | `internal/palette` | 色卡校验、调色板最近邻匹配、确定性重采样、8 位量化与材质参数误差界 |
| 诊断汇总 `report` | `internal/pipeline` | 聚合分词、逐八度误差依据、参数有效位数、版本指纹与失效轨迹 |

## 快速开始（Windows / Linux / macOS）

需要 Go 1.22+，确认 `go version` 可用。Windows 终端（PowerShell/CMD）命令等价。

```bash
go mod download
go build -o matbench ./cmd/matbench        # Windows: -o matbench.exe

./matbench demo -dir workspace             # 固定种子生成 recipes.json 与 4 张参考色卡 PNG
./matbench run  -dir workspace -workers 1  # 单线程完整运行
./matbench run  -dir workspace -workers 4  # 多线程八度合成，结果逐字节一致
./matbench status -dir workspace           # 查看阶段检查点、序号与指纹
```

### 增量编辑（精确失效，非全量重算）

```bash
# 仅改噪声频率：control 复用，noise 及其下游重建
./matbench incr -dir workspace -id MAT-0003 -field noise_frequency=3.5
# 改描述词：control 及其全部下游重建
./matbench incr -dir workspace -id MAT-0001 -field description="new 全新 finish"
# 调整分辨率 / 替换色卡
./matbench incr -dir workspace -id MAT-0002 -field target_resolution=56
./matbench incr -dir workspace -id MAT-0002 -field card_name=card-C.png

./matbench invalidations -dir workspace -id MAT-0003   # 失效原因与新旧指纹
./matbench material -dir workspace -id MAT-0003 -stage noise   # 逐八度误差依据
./matbench report -dir workspace                                # 导出诊断报告
```

### 优雅停止与恢复

```bash
./matbench stop -dir workspace     # 请求停止（写入 STOP 哨兵文件）
./matbench run  -dir workspace     # 完成当前不可拆分材质单元后提交退出
./matbench resume -dir workspace   # 复用检查点续跑，产物与一次连续运行逐字节一致
```

### 本地 HTTP 工作台

```bash
./matbench serve -dir workspace -addr 127.0.0.1:8080
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/import` | 生成固定种子清单与色卡 |
| POST | `/api/v1/run` | 分阶段生成（支持 `workers`、`only_ids`、`force_all`） |
| POST | `/api/v1/incremental` | 提交增量补丁，自动校验与全量结果逐字节一致 |
| GET  | `/api/v1/status` | 全部有效检查点、序号、输入/产物指纹 |
| GET  | `/api/v1/materials/{id}?stage=` | 单阶段产物与版本指纹 |
| GET  | `/api/v1/materials/{id}?kind=invalidations` | 失效轨迹 |
| POST | `/api/v1/stop` / `/api/v1/resume` | 停止 / 恢复 |
| POST | `/api/v1/export` | 导出诊断报告 JSON |

## 工作区布局

```
workspace/
├── recipes.json                 # 固定种子配方清单（4 条中英混排材质）
├── cards/card-{A..D}.png        # 固定种子参考色卡
├── matbench.db                  # SQLite 阶段检查点/失效轨迹/运行元数据
├── matbench.lock                # 跨进程真实文件锁
├── STOP                         # 停止哨兵（请求停止时出现）
└── artifacts/
    ├── textures/MAT-*_texture.png
    └── reports/diagnostic_report.json
```

## 业务错误码

| 代码 | 触发条件 |
| --- | --- |
| `E_EMPTY_PROMPT` | 描述词规范化/分词后为空（空串或仅标点），携带 material_id |
| `E_HASH_DEGENERATE` | 哈希分量出现 NaN/Inf 或 32 维向量全零 |
| `E_NOISE_DEGENERATE` | 八度数为零或振幅全零（诊断标注，不静默产出空白纹理） |
| `E_PALETTE_EMPTY` | 色卡提取不到任何颜色 |
| `E_CARD_DIM_MISMATCH` | 色卡实际像素尺寸与声明尺寸不符/色卡缺失 |
| `E_QUANT_ALL_ZERO` | 量化后纹理全通道为零 |
| `E_INCREMENTAL_MISMATCH` | 增量结果与从零全量结果首个差异（含 material_id 与阶段） |
| `E_STALE_CHECKPOINT` | 序号回退/重复/跨材质错位或同序号指纹被篡改 |

更多设计与验证细节见 [TECHNICAL_DESIGN.md](TECHNICAL_DESIGN.md)。
