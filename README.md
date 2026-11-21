# 车内多音区语音活动仲裁与指令归属引擎

一个完全本地、确定性的领域工作台：对固定种子生成的车内 4 音区拼接 WAV 做短时能量/频谱平坦度滞回 VAD、跨音区到达时延差仲裁、毫秒级区间编辑、增量重算、缓存查询、原子快照与并发发布保护。

运行时不访问网络，不使用 Docker、真实 VAD、声学模型、预训练权重、GPU 或外部服务。

## 快速开始

```bash
# 当前环境已预装 NumPy / FastAPI / uvicorn / pytest
python3 -m cabin_voice --workspace ./demo_workspace demo \
  --summary ./artifacts/demo_summary.json

python3 -m cabin_voice --workspace ./demo_workspace serve \
  --host 127.0.0.1 --port 8765
```

浏览器打开 `http://127.0.0.1:8765/`。页面可初始化演示、执行增量/全量重算、查询活跃音区/TDOA/归属依据、编辑优先级与词表、切分合并区间、请求停止/恢复、校验快照链并导出结果。

仓库内 `demo_data/manifest.json` 与 `demo_data/wavs/*.wav` 是固定种子 `20261122` 生成的可直接导入样本。也可重新生成：

```bash
python3 -m cabin_voice generate-data --root ./demo_data --seed 20261122
python3 -m cabin_voice --workspace ./demo_workspace init-demo --seed 20261122 --reset
python3 -m cabin_voice --workspace ./demo_workspace run
```

## 已实现能力

- 导入校验：WAV 必须为 16 kHz、单声道、16 位 PCM；校验 WAV 帧数、清单 `total_samples`、四音区连续偏移之和、完整覆盖与标注时长上界。
- VAD：可配置帧长/帧移、汉宁窗、对数能量与频谱平坦度加权、高进低出滞回、最短语音段/最短静音段后处理；尾部越界显式补零并记录补零帧数。
- 仲裁：按活跃区间起止毫秒计算两两到达时延差，依据声速与四音区几何先验、参考词表命中强度和优先级打分；分差小于复核间距时标记 `review`，不强行归属。
- 编辑：优先级、完整优先级映射、词表命中新增/更新/删除、活动区间切分/合并、阶段配置修改；每次编辑单调推进编辑版本并记录前后值与影响范围。
- 查询与缓存：活跃音区、TDOA 明细、归属依据；缓存键包含内容指纹、编辑版本、配置指纹、阶段产物摘要与查询参数；超预算返回 `E_LATENCY_BUDGET`。
- 增量重算：按内容/配置/上游产物摘要识别最早受影响阶段，优先级与词表编辑只重算仲裁；每次增量结果与从零全量构建做规范化字节比较，差异返回 `E_INCREMENTAL_MISMATCH` 和首个差异偏移。
- 快照：每阶段以临时文件 + `fsync` + `os.replace` 原子写入，记录阶段、序号、内容指纹、配置版本、编辑版本、产物摘要与链摘要；区分 `E_SNAPSHOT_ROLLBACK` 与 `E_SNAPSHOT_TAMPERED`。
- 并发发布：使用真实 `fcntl.flock` 文件锁和连续版本检查；落后、重复、跨分片错位发布返回 `E_STALE_VERSION`；待提交编辑可显式发布、重试或丢弃。
- 停止/恢复：停止在分片边界生效，未完成的阶段不落快照；恢复后摘要与完整运行一致。

## 固定演示结果

`artifacts/demo_summary.json` 记录了实际运行结果，固定摘要如下：

- 初始结果摘要：`bd6805bff75fda4d082a7ea7a3f44cd65c7416cab8c005bb9aa419f982d71b05`
- 优先级编辑后结果摘要：`ae204f49645338d693e01964565003b616746f7ccf25f0913dddbb79168c9ef8`
- 演示总摘要：`c936747303992e79b2fa8f21e2feceaad5e3e16a78d1f5cf049a846629d2209f`

演示还验证了：首次查询为缓存未命中、第二次命中且结果字节一致；`budget_ms=0` 返回 `E_LATENCY_BUDGET`；优先级编辑后 `vad` 复用、仅 `arbitrate` 重算且全量摘要一致；停止后恢复摘要不变；4 条快照链校验通过。

## 测试

```bash
python3 -m pytest -q
```

当前实际验证：Python 3.14.4 上 28 项测试全部通过；Python 3.11.9 上 27 项核心、并发和文件锁测试通过，1 项 HTTP 路由测试因该解释器未安装 FastAPI 而自动跳过。两端分别从零运行完整演示，得到相同的清单摘要、初始结果摘要、编辑后摘要与总摘要。HTTP 已在 Python 3.14.4 的 `127.0.0.1:8877` 实际启动并用 `curl` 验证健康检查、导入、重算、查询、504 预算错误、快照校验与工作台页面。

## 主要接口

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/api/demo/init` | 生成并导入固定种子演示 |
| `POST` | `/api/import` | 导入清单与 WAV |
| `POST` | `/api/run` | 增量/全量执行阶段 |
| `GET` | `/api/status` | 阶段状态、版本与摘要 |
| `GET` | `/api/query/{active|tdoa|assignment}` | 毫秒时刻查询 |
| `POST` | `/api/edits/direct` | 直接编辑 |
| `POST` | `/api/edits/prepare` | 暂存编辑 |
| `POST` | `/api/edits/{id}/publish` | 连续版本发布 |
| `POST` | `/api/edits/{id}/retry` | 冲突后显式重试 |
| `POST` | `/api/edits/{id}/discard` | 丢弃未提交编辑 |
| `POST` | `/api/stop` / `/api/resume` | 优雅停止与恢复 |
| `GET`/`POST` | `/api/snapshots/verify` | 快照链校验 |
| `POST` | `/api/export` | 导出活动、TDOA 与归属结果 |

详细设计与不变量见 [docs/TECHNICAL.md](docs/TECHNICAL.md)。
