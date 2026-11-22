# AudioQ Workbench

本地、确定性、可恢复的音频质量评估工作台。项目只使用 Python 标准库、NumPy 与 Flask，不访问网络，不依赖真实模型、数据库、账号或 GPU。

## 快速开始

```bash
python3 -m pip install numpy flask pytest

# 生成固定种子样本、清单、v2 配置与 v1 旧配置，并执行完整验收演示
python3 -m audioq demo --root .

# 运行测试
pytest -q

# 启动本地 HTTP 诊断工作台
python3 -m audioq serve \
  --manifest demo/manifest.json \
  --config demo/configs/config_v2.json \
  --state-dir demo/state_ui \
  --host 127.0.0.1 --port 5000
```

浏览器打开 `http://127.0.0.1:5000`。演示摘要位于 `demo/demo_summary.json`，完整报告位于 `demo/state_full/report.json`。

也可以直接生成样本而不运行评估：

```bash
python3 scripts/generate_demo.py --root .
```

## 演示数据

`demo/samples/` 中四个 WAV 均为 16 kHz、单声道、16-bit PCM：

- `clean_speech.wav`：带语句内停顿的清晰合成语音。
- `low_snr_burst.wav`：低信噪比，并叠加突发噪声。
- `clipped_distortion.wav`：严重削顶失真。
- `all_silent.wav`：全静音。

`demo/manifest.json` 按分片声明样本数、采样率、语言、顺序和 MOS 参考区间。`demo/configs/config_v2.json` 是结构版本 2 配置；`config_v1_legacy.json` 是结构版本 1 旧配置，权重为整数百分比且不包含 SNR 门限。

## 常用命令

```bash
# 完整运行；已有且仍有效的阶段会被复用
python3 -m audioq run --manifest demo/manifest.json \
  --config demo/configs/config_v2.json --state-dir demo/state_cli

# 确定性地运行两个分片后停止，再恢复
python3 -m audioq run --manifest demo/manifest.json \
  --config demo/configs/config_v2.json --state-dir demo/state_cli --stop-after 2
python3 -m audioq resume --manifest demo/manifest.json \
  --config demo/configs/config_v2.json --state-dir demo/state_cli

# 查询阶段状态、链式版本指纹和失效原因
python3 -m audioq status --manifest demo/manifest.json \
  --config demo/configs/config_v2.json --state-dir demo/state_cli

# v1 -> v2，自动回补缺失的 SNR 门限
python3 -m audioq convert \
  --input demo/configs/config_v1_legacy.json \
  --output demo/configs/legacy_as_v2.json --target-version 2

# v2 -> v1，输出显式 downgrade_items / downgrade_restore
python3 -m audioq convert \
  --input demo/configs/config_v2.json \
  --output demo/configs/v2_as_v1.json --target-version 1

# 优雅停止与导出报告
python3 -m audioq stop --state-dir demo/state_cli --reason manual
python3 -m audioq export-report --manifest demo/manifest.json \
  --config demo/configs/config_v2.json --state-dir demo/state_cli
```

所有业务错误均以 JSON 对象返回，例如 `{"ok":false,"code":"E_LENGTH_MISMATCH","shard_id":"clean","observed":{...}}`。进程退出码为 `2`。

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/` | 本地诊断工作台界面 |
| `GET` | `/api/status` | 分片、四阶段状态、版本指纹、失效记录与停止请求 |
| `POST` | `/api/run` | 完整或增量运行；可传 `stop_after_shards` |
| `POST` | `/api/resume` | 清除停止请求并恢复 |
| `POST` | `/api/stop` | 请求在当前不可拆分分片完成后停止 |
| `GET` | `/api/shards/<id>` | 单分片指标、融合贡献、精度和错误明细 |
| `GET` | `/api/report` | 当前版本的完整诊断报告 |
| `POST` | `/api/report/export` | 原子导出报告 |
| `POST` | `/api/convert` | v1/v2 配置互转与旧版回补 |
| `POST` | `/api/import` | 导入本地分片清单/配置路径 |
| `GET` | `/api/checkpoints` | 检查点序号、损坏项、重复与缺口 |

## 测试与验收

```bash
pytest -q
python3 -m audioq demo --root .
```

当前实现包含 37 个测试，覆盖配置互转、逐帧指标、融合与误差界、原子检查点、多进程发布、损坏恢复、增量失效、HTTP/CLI 和完整演示。实测结果见 [TECHNICAL.md](TECHNICAL.md) 与 `artifacts/verification.json`。

更详细的格式、算法与一致性协议见 [TECHNICAL.md](TECHNICAL.md)。
