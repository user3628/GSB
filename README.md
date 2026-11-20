# mmpipe：本地确定性多模态协同推理数据管线与结果验证工具

纯本地、确定性、可恢复的四阶段多模态协同推理管线。不依赖真实模型、GPU 或任何
外部服务；文本嵌入由固定哈希函数生成，视觉投影矩阵与样本由固定随机种子生成器产出，
同一输入与配置在任意进程/机器上产生**逐字节一致**的阶段产物。

## 四个协同阶段

| 阶段 | 模块 | 输入指纹覆盖 |
| --- | --- | --- |
| 1. 文本编码 `text_encoding` | `mmpipe/text_encoding.py` | 文本 id/text/lang + 嵌入配置 |
| 2. 视觉对齐 `visual_alignment` | `mmpipe/visual_alignment.py` | 图像特征、文本嵌入、投影矩阵 |
| 3. 融合打分 `fusion_scoring` | `mmpipe/fusion_scoring.py` | 对齐结果、算子置信度、融合权重 |
| 4. 结果聚合 `aggregation` | `mmpipe/aggregation.py` | 融合结果、文本样本、阈值配置 |

关键保证：

- **确定性**：NFKC + 空白折叠 + 小写化 → BLAKE2b 带符号特征哈希 → 64 维 L2 单位嵌入；
  所有 JSON 产物经规范化序列化（键排序、固定分隔符、浮点往返一致）。
- **可恢复**：每阶段原子发布检查点（临时文件 + fsync + `os.replace`），含阶段名、
  输入指纹、配置版本、产物摘要、单调序号与校验和；损坏/断链记录在打开工作区时隔离，
  从最近一致检查点继续。
- **失效传播**：文本、视觉特征、投影矩阵、融合权重、算子置信度任一变化，经版本指纹
  识别最早受影响阶段，级联失效其全部下游；未受影响上游直接复用。
- **优雅停止**：停止信号（进程内事件 + `stop.flag` 文件）只在样本单元边界生效，
  在途单元跑完、未开始单元取消，阶段不发布；恢复后结果与一次完整运行逐字节一致。
- **并发发布保护**：跨进程文件锁 + 序号比较，重复/落后发布返回 `E_STALE_CHECKPOINT`，
  缺前置阶段返回 `E_INVALID_STAGE`，已发布产物不可覆盖。
- **稳定错误码**：`E_MISSING_INPUT` / `E_SHAPE_MISMATCH` / `E_NUMERIC` /
  `E_INVALID_STAGE` / `E_STALE_CHECKPOINT`，均带阶段与样本标识。

## 环境准备（Python 3.11，无 Docker）

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt   # numpy/fastapi/uvicorn 固定版本
.venv/bin/pip install -e .
```

Windows 兼容性：文件锁在 `mmpipe/platform_io.py` 中对 `msvcrt`（Windows）与
`fcntl`（POSIX）分别实现；原子替换统一使用 `os.replace`；线程池使用标准库
`ThreadPoolExecutor`，三端均可用。

## 生成确定性样本

```bash
.venv/bin/python -m mmpipe.samplegen_cli samples
```

产出 `samples/texts.jsonl`（4 条文本，含 en/zh/zh-en 中英混排）、
`samples/images.json`（4 条图像描述，含 1 个冲突样本）、
`samples/pipeline_config.json`（投影矩阵、融合权重、阈值、算子置信度）与
`samples/generation_params.json`（随机种子等全部复现参数，种子 `20251120`）。

## CLI 用法

```bash
export PATH="$PWD/.venv/bin:$PATH"
mmpipe -w <workspace> init                      # 初始化/清理遗留 tmp
mmpipe -w <workspace> import                    # 导入样本与配置
mmpipe -w <workspace> run                       # 完整运行（已发布阶段自动复用）
mmpipe -w <workspace> run --stop-after 2 --unit-delay 0.1   # 演示优雅停止
mmpipe -w <workspace> stop                      # 请求停止（写 stop.flag）
mmpipe -w <workspace> resume                    # 清除停止标志并恢复
mmpipe -w <workspace> status                    # 阶段状态/指纹/失效信息
mmpipe -w <workspace> product <stage>           # 查看某阶段产物
mmpipe -w <workspace> aggregate                 # 查看聚合结论
mmpipe -w <workspace> events                    # 运行事件日志
mmpipe-verify -w <workspace> check              # 验证工具：28 项结构/一致性检查
mmpipe-verify compare <a.json> <b.json>         # 逐字节比对两份产物
mmpipe-serve -w <workspace> --port 8765         # 本地 HTTP 接口（仅 127.0.0.1）
```

## HTTP 接口

`POST /workspace/init`、`POST /import`、`POST /run`、`POST /stop`、
`GET /run/status`、`POST /resume`、`GET /status`、`GET /stages/{stage}`、
`GET /aggregate`、`GET /events`、`GET /health`。

## 一键演示与验收

```bash
.venv/bin/python demo/run_demo.py     # 14 个场景，含停止恢复/失效/错误码/并发/HTTP/崩溃自愈
bash demo/cli_demo.sh                 # CLI 手动路径 + 逐字节比对
bash demo/http_demo.sh                # HTTP 手动路径
.venv/bin/python -m unittest discover -s tests -v   # 27 个单元/集成测试
```

## 工作区布局

```
<workspace>/
  config.json                 当前管线配置
  inputs/                     已导入文本/图像/导入清单
  checkpoints/cp_NNNNNN.json  不可变检查点（信封含 checksum）
  checkpoints/quarantine/     自愈时隔离的损坏/断链检查点
  products/<stage>_NNNNNN.json 不可变阶段产物
  current/<stage>.json        当前有效产物
  state.json                  阶段状态缓存（可由检查点重建）
  logs/events.jsonl           运行事件（追加写）
  logs/invalidations.jsonl    失效事件（原因/组件/新旧指纹/被取代序号）
  locks/workspace.lock        跨进程文件锁
  stop.flag                   停止请求标志
```

更多设计细节见 `docs/design.md`，原始任务书见 `docs/requirements_original.md`。
