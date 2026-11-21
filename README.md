# 本地确定性训练/推理一体化数据处理管线

纯本地运行的四阶段管线（特征化 → 线性模型拟合 → 推理判定 → 结果汇总），
内置内容指纹、增量失效传播、原子检查点、崩溃恢复、并发发布保护与
增量/全量逐字节一致性校验。不使用 Docker、GPU 或任何外部服务。

## 环境要求

- Python 3.10+（开发验证使用 Python 3.14）
- 依赖：`pip install -r requirements.txt`（NumPy、PyYAML）
- Linux/macOS/Windows 均可，Windows 可直接使用 `run_demo.bat`、`run_tests.bat`

## 快速开始

```bash
pip install -r requirements.txt

# 端到端演示：生成数据 -> 全量运行 -> 单样本编辑/单分片替换的增量校验
# -> 损坏检查点恢复 -> 全部错误码 -> 多进程并发发布保护
python scripts/run_demo.py

# 单元与集成测试（21 个用例）
python -m unittest discover -s tests -v

# 本地 HTTP 接口演示（默认 127.0.0.1:8765）
python scripts/http_demo.py
```

演示输出存档：`docs/demo-output.txt`、`docs/http-demo-output.txt`；
设计细节见 `docs/TECHNICAL_DESIGN.md`。

## 命令行

```bash
python -m pipeline.cli generate --workspace ./ws
python -m pipeline.cli run --workspace ./ws --through-stage stage4_summary
python -m pipeline.cli status --workspace ./ws
python -m pipeline.cli validate --workspace ./ws --scope stage4_summary
python -m pipeline.cli edit-sample --workspace ./ws --sample-id train-0003 --text "新文本" --label 1
python -m pipeline.cli replace-shard --workspace ./ws --kind train --shard train-shard-02.jsonl --from ./new.jsonl
python -m pipeline.cli import --workspace ./ws --kind config --path ./pipeline.yaml
python -m pipeline.cli serve --workspace ./ws --host 127.0.0.1 --port 8765
```

训练侧变更（样本、规范化、特征维度、学习率、轮数）会改变模型版本，编辑后
需先重建到 `stage2_model`，再刷新推理请求的版本引用：

```python
from pipeline.pipeline import Pipeline
from pipeline.data_ops import edit_training_sample, refresh_inference_model_versions

pipe = Pipeline("./ws")
edit_training_sample(pipe.ws, "train-0003", new_text="...", new_label=1)
pipe.run(through_stage="stage2_model")
refresh_inference_model_versions(pipe.ws)
pipe.run()
pipe.validate_incremental()
```

## HTTP 接口

仅监听 `127.0.0.1`，请求/响应均为 JSON：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/generate` | 生成 3 训练分片 + 2 推理分片 + YAML 配置 |
| POST | `/import?kind=train\|infer\|config` | 导入本地文件 |
| POST | `/run` | body `{"through_stage": "stage4_summary"}`，自动复用/失效重建 |
| GET | `/status` | 检查点序号、各阶段指纹、失效事件、隔离记录 |
| POST | `/validate` | body `{"scope": "stage1_features|...|stage4_summary"}` |
| POST | `/edit-sample` | body `{"sample_id","text","label"}` |
| POST | `/stop` | 请求优雅停止 |

## 工作区布局

```
ws/
├── config/pipeline.yaml          # 规范化/特征/模型/推理/资源预算
├── data/train/*.jsonl            # 训练分片（sample_id/text/label/lang/source）
├── data/infer/*.json             # 推理分片（request_id/text/ref_model_version/期望区间）
├── artifacts/stage{1..4}_*.json  # 四阶段产物，内嵌 artifact_summary 摘要
├── checkpoints/record-000001.json # 原子检查点（序号、阶段、输入/配置指纹、产物摘要）
├── quarantine/                   # 失效或损坏后隔离的旧记录
├── state/workspace.lock          # 跨进程文件锁
├── state/invalidation.log        # 失效原因事件流
└── tmp/                          # 全量一致性校验的临时构建目录
```

## 确定性保证

- FNV-1a 64 位固定特征哈希 + SHA-256 内容摘要；规范化 JSON（排序键、固定
  分隔符、无 NaN）落盘，相同输入与配置逐字节一致。
- 样本按 ID 固定排序、全批量梯度下降、参数零初始化、逐维定点量化落盘。
- `validate` 在临时目录从零全量构建并与当前增量产物逐字节比对，不一致时
  返回 `E_INCREMENTAL_MISMATCH` 与首个不一致的阶段/产物名。

## 错误码

`E_EMPTY_SAMPLE`、`E_INVALID_TRAIN`、`E_NUMERIC`、`E_MODEL_VERSION_MISMATCH`、
`E_INVALID_STAGE`、`E_STALE_CHECKPOINT`、`E_CORRUPT_CHECKPOINT`、
`E_INCREMENTAL_MISMATCH`、`E_BUDGET_EXCEEDED`、`E_INVALID_CONFIG`。
触发条件与结构化字段见 `docs/TECHNICAL_DESIGN.md` 第 6 节。
