# 实际验证记录

验证日期：2026-11-22（Asia/Shanghai）

## 1. 环境

```text
Python       3.14.4
NumPy        2.5.5
soundfile    0.14.0
pytest       9.2.0
OS           本地 Linux 工作区，无 Docker / GPU / 外部服务
```

依赖安装：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install pytest -e .
```

## 2. 测试套件

命令：

```bash
.venv/bin/pytest -q
```

结果：

```text
........................................                                 [100%]
40 passed
```

覆盖内容：

- v2 JSON、WAV+侧车、单元标注、v1 清单四种往返。
- v1 稳定 ID、可配置分隔符/稳定 ID 前缀/缺省共振峰/cents 换算、非整数百分比拒绝。
- WAV 采样率与声道错配的 `E_FORMAT_MISMATCH` 及 declared/actual 上下文。
- 清单总量上界、可选严格相等策略。
- 负区间、反向区间、越界区间、NaN/Inf/越界音高。
- 特征、建模、量化确定性与量化误差界。
- 样本替换、单元编辑、结构版本升级、音高编辑的增量/全量逐字节一致性。
- `E_INCREMENTAL_MISMATCH` 注入检测。
- 检查点单调性、重复/跳号、缺失前置产物、原子中断、双进程重复发布。
- 命名阶段执行、停止后完成当前分片、恢复并与全量构建一致。
- 本地 HTTP 页面、状态、检查点、构建、往返、错误与停止/恢复。

## 3. 演示数据

固定随机种子：`20261122`。

```text
shard_001       19,360 samples  你 / 好 / world / pause
shard_002       19,520 samples  音 / 色 / clone / sil
shard_003       18,880 samples  lab / 语 / 嗯 / 停顿
v1-acff54dd9b2d 20,480 samples  旧 / 版 / legacy / 停顿（版本 1 导入）
total           78,240 samples
format          16000 Hz, mono, 16-bit PCM
```

生成命令：

```bash
.venv/bin/python scripts/generate_demo.py --data-dir data
```

`data/index.json` 记录种子、样本总数、分片顺序、清单归属、单元符号与异常清单。
再次在临时目录运行生成器，9 个数据文件 SHA256 全部一致。

## 4. 全量基准

命令：

```bash
.venv/bin/python -m tonelab.cli demo --data-dir data \
  --workspace workspace/demo --output artifacts/baseline_summary.json
```

实际结果：

```text
full deterministic fingerprint: 78ea332a5ebb02cc8882c72c2a4731d3d1d324c7e26994f106230e01e21df527
baseline summary fingerprint:   555e0a91b700b91bcb81c4c90ecfa964f6253a56dd9fc572cb0909e02c22d5dd
truncated frames:               7
checkpoints:                     16 (sequence 1..16, continuous)
project export files:            22
```

连续运行两次演示，`summary_fingerprint` 与全量确定性指纹均完全相同。

项目级关键导出摘要：

```text
project_v2.json            115b04a7f45df817b1e599a69b54300482d48c995404083d7b18667b13fac51e
v1/manifest_v1.json        5ec3c363d452bbc38a2d256298f57633110786613578a4a27bbd22bbc0b6daab
units/shard_001.units.json 9bb3810728e2eb185351fce17b5a4c6deb01adba1f531f2734b07abd7e317e94
wav/shard_001.wav          090241a0ba2f4878a0d44fa19d05fed4e2b86d79c0eeb70ae8558850d0283d5f
```

## 5. 三种格式往返

```text
json_v2:
  first_sha256  = 58cfc58079c1562dbb277a7819daf6e84f4c1fa5a44c971a518005ac65c2a7fd
  second_sha256 = 58cfc58079c1562dbb277a7819daf6e84f4c1fa5a44c971a518005ac65c2a7fd
  stable_export = true, differences = []

wav:
  stable_export = true, differences = []
  WAV header re-inspected as 16000 Hz / mono / PCM_16

units:
  stable_export = true, differences = []

json_v1:
  stable_manifest = true, pcm_equal = true
  first_sha256  = 75a829a1052a4885ef08bc017d5d978fecf5ae469f07aef0b0ca7b1d6fee1dd9
  second_sha256 = 75a829a1052a4885ef08bc017d5d978fecf5ae469f07aef0b0ca7b1d6fee1dd9
  max pitch delta = 0.0050000000000000044
  error bound     = 0.005
```

## 6. 增量与全量逐字节一致性

```text
no-op incremental == full:
  78ea332a5ebb02cc8882c72c2a4731d3d1d324c7e26994f106230e01e21df527

sample replacement:
  incremental = full = 489a00070937299fa88db605fdadab566901e76670afed16d30799b7ea9e51e9
  earliest invalidation = shard_001.feature
  differences = []

unit edit:
  incremental = full = 232dcdbf889f8c286c9d9c34ab912f9ddd68f84bdebb06ad5cad1dd2e7a2d29e
  earliest invalidation = shard_002.model, feature reused
  differences = []

version upgrade:
  incremental = full = c4d08f68c46fa9879ebbc1a1200464b327cd014e1d5b1b7f3864c54e027ee1ae
  earliest invalidation = v1-acff54dd9b2d.model, feature reused
  differences = []
```

每个校验同时比较 4 分片 × 4 阶段的 16 份产物字节与 22 个项目导出文件。

## 7. 异常注入

```bash
.venv/bin/python -m tonelab.cli validate --manifest data/anomaly_timeline.json
# exit 1
```

```text
E_TIMELINE_INVALID
shard_id   = anomaly_timeline
unit_index = 0
position   = {"kind":"unit","unit_index":0,"field":"end_ms"}
context    = start_ms=0, end_ms=1260, duration_ms=1210, overflow_ms=50
```

```bash
.venv/bin/python -m tonelab.cli validate --manifest data/anomaly_numeric.json
# exit 1
```

```text
E_NUMERIC
shard_id = anomaly_numeric
position = {"kind":"pitch_point","index":2,"component":"value"}
context  = value_repr="'NaN'"
```

版本 1 校验：

```text
project_id = demo-aurora-v1
shard_id   = v1-acff54dd9b2d
units      = 旧 / 版 / legacy / 停顿
declared   = 20480
actual     = 20480
```

## 8. HTTP 工作台

`tests/test_http.py` 启动真实 `ThreadingHTTPServer` 并验证：

- `/` 返回交互页面。
- `/api/health`、`/api/status`、`/api/checkpoints` 正常。
- 增量构建摘要与 CLI 全量基准一致。
- WAV 往返 `stable_export=true`。
- 越界单元索引返回 HTTP 404 与 `E_NOT_FOUND`。
- 停止后恢复成功。

手工启动：

```bash
./scripts/run_server.sh
# http://127.0.0.1:8765/
```

## 9. 复现清单

```bash
.venv/bin/pytest -q
.venv/bin/python -m tonelab.cli build --workspace workspace/demo --mode full
.venv/bin/python -m tonelab.cli roundtrip --workspace workspace/demo
.venv/bin/python -m tonelab.cli consistency --workspace workspace/demo \
  --mutation unit --shard shard_002 --unit-index 1 --end-ms 620
.venv/bin/python -m tonelab.cli checkpoints --workspace workspace/demo
```

