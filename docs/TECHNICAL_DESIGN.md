# 技术设计说明

## 1. 总体架构

纯本地、纯 Python 实现，不依赖 GPU、容器或外部服务。四个阶段顺序推进：

| 阶段 | 模块 | 输入 | 产物 |
| --- | --- | --- | --- |
| `stage1_features` | `pipeline/stages.py::run_stage1` | 训练 JSONL 分片 + 规范化/特征配置 | `artifacts/stage1_features.json` |
| `stage2_model` | `pipeline/stages.py::run_stage2` | stage1 产物 + 模型超参 | `artifacts/stage2_model.json` |
| `stage3_inference` | `pipeline/stages.py::run_stage3` | 推理 JSON 分片 + stage2 模型 | `artifacts/stage3_inference.json` |
| `stage4_summary` | `pipeline/stages.py::run_stage4` | stage3 产物 | `artifacts/stage4_summary.json` |

模块职责：

- `tokenizer.py`：NFKC 规范化、空白折叠、小写化、中文单字/相邻二字、拉丁数字连续串切词。
- `hashing.py`：FNV-1a 64 位特征哈希（纯整数、跨平台一致）与 SHA-256 内容摘要。
- `featurizer.py`：哈希计数、L2 归一化、空样本 `E_EMPTY_SAMPLE`。
- `model.py`：固定顺序全批量逻辑回归梯度下降、逐维 2 的幂定点量化与反量化。
- `checkpoints.py`：原子检查点记录、序号/阶段顺序校验、损坏隔离、失效事件日志。
- `pipeline.py`：失效传播、复用编排、优雅停止、增量 vs 全量逐字节校验、并发发布。
- `server.py` / `cli.py`：本地 HTTP 与命令行接口。
- `generator.py` / `data_ops.py`：确定性样本生成、单样本编辑与单分片替换。

## 2. 确定性与逐字节一致

1. 所有 JSON 经 `atomicio.canonical_json_bytes` 序列化：`sort_keys=True`、
   `ensure_ascii=False`、固定分隔符、固定 `\n` 行尾、`allow_nan=False`。
2. 特征哈希使用 FNV-1a 64 位纯整数运算，避免 Python `hash()` 随机化。
3. 训练样本按 `sample_id`、推理请求按 `request_id` 固定排序后处理；批量梯度下降
   为全批量、样本顺序写入每轮 `loss_history`。
4. NumPy 仅用于确定性稠密矩阵运算，无随机初始化（参数从零向量起步）。
5. 模型参数按维定点量化：`q = round(w / 2^exponent)`，每维记录一个
   `scale_exponents`，落盘只含整数权重与整数指数，消除浮点序列化歧义。
6. 已在两个独立工作区验证四个产物与四条检查点记录全部逐字节一致。

## 3. 指纹与增量失效传播

每条检查点记录保存 `input_fingerprint` 与按阶段裁剪的 `config_fingerprint`：

- stage1 配置子集：`normalize` + `features` + `resources`
- stage2 配置子集：`features` + `model`
- stage3 配置子集：`normalize` + `features` + `inference` + `resources`
- stage4 配置子集：`inference`

模型版本 `mv_<16位摘要>` 仅由 stage1 产物摘要与 stage2 配置子集推导，因此
修改置信阈值不会改变模型版本，而修改学习率/轮数/特征维度/训练数据会改变版本。

运行时从 stage1 起逐阶段比对指纹，定位最早失效阶段，将其及下游检查点移入
`quarantine/`、删除对应产物，并向 `state/invalidation.log` 追加失效原因；
上游未受影响阶段继续复用。可观测原因包括 `TRAIN_DATA_CHANGED`、
`CONFIG_CHANGED`、`MODEL_CONFIG_CHANGED`、`INFERENCE_CONFIG_CHANGED`、
`MODEL_OR_INFER_INPUT_CHANGED`、`MISSING_ARTIFACT`。

## 4. 检查点与恢复语义

- 写入：临时文件 + `fsync` + `os.replace` + 目录 `fsync`，崩溃只可能留下
  `.part` 临时文件，不会出现半写目标记录。
- 恢复：只接受序号从 1 连续、阶段按 `STAGES` 循环匹配、且 `record_digest`
  自洽的最长前缀；损坏记录及其后所有记录整体隔离，重建后序号重新连续。
- 序号重复、跳跃、跨阶段错位均判为损坏前缀；发布路径再以
  `E_STALE_CHECKPOINT` 拒绝落后/重复/错位发布。
- 优雅停止：`threading.Event` 在每个不可拆分阶段单元前后检查，停止请求只在
  当前单元完成并落盘检查点后生效；强制 kill 不承诺报告，但恢复后经
  `validate` 证明与从零全量构建逐字节一致。

## 5. 并发发布保护

所有发布走 `Workspace.lock()`（POSIX `fcntl.flock`，Windows `msvcrt.locking`），
在锁内重新加载有效前缀并比较期望序号与阶段，只有 `序号=前缀长度+1` 且阶段
匹配才允许写入，否则返回 `E_STALE_CHECKPOINT`。`scripts/run_demo.py` 的 7f
与 `tests/test_pipeline.py` 用多进程实际验证仅一个发布成功。

## 6. 错误码

| 错误码 | 触发条件 |
| --- | --- |
| `E_EMPTY_SAMPLE` | 规范化后为空文本，保留 sample_id/request_id 诊断，不中断整批 |
| `E_INVALID_TRAIN` | `epochs==0`（带 epoch=0）或学习率为负 |
| `E_NUMERIC` | 损失/梯度/参数出现 NaN/Inf，带轮次与原因 |
| `E_MODEL_VERSION_MISMATCH` | 请求引用版本与当前有效版本不一致，带 request_id 与两个版本号，禁止替代打分 |
| `E_INVALID_STAGE` | 缺失前置阶段、缺少配置或未知阶段名 |
| `E_STALE_CHECKPOINT` | 落后/重复/跨阶段错位的并发发布 |
| `E_CORRUPT_CHECKPOINT` | 恢复时发现损坏/不完整/重复记录（内部隔离并可查询） |
| `E_INCREMENTAL_MISMATCH` | 增量产物与全量构建逐字节不一致，带首个不一致阶段与产物名 |
| `E_BUDGET_EXCEEDED` | 样本/请求/单样本词项超过资源预算 |
| `E_INVALID_CONFIG` | YAML 配置字段非法 |

## 7. 模型版本刷新约定

训练数据或模型超参变化会产生新模型版本。推理请求内嵌的 `ref_model_version`
属于推理输入，工作流为：先重建到 `stage2_model`，再调用
`data_ops.refresh_inference_model_versions` 把引用刷新为当前有效版本，最后
重建 stage3/stage4。未刷新而直接推理会按要求得到
`E_MODEL_VERSION_MISMATCH`（演示 7d 与对应单测覆盖）。

## 8. 平台说明

开发与验证环境为 Linux + Python 3.14（任务要求 Python >= 3.10）。代码仅使用
标准库 + NumPy + PyYAML，路径全部走 `os.path`/`pathlib` 语义，文件锁对
Windows 提供 `msvcrt` 分支，多进程入口带 `multiprocessing.freeze_support()`，
并附带 `run_demo.bat` / `run_tests.bat` 供 Windows 直接运行。
