# sft-workbench：指令微调数据构造与格式互操作工作台

本项目是一个**纯本地**运行的指令微调数据构造与格式互操作工作台：不依赖任何真实模型、
外部服务或 GPU。输入为固定随机种子生成器产出的本地 JSONL 指令样本分片，系统把团队间
流转的两种结构版本（v1 自由文本 / v2 结构化）统一为内部数据模型，依次完成规范化与
SimHash 近似去重、多因子质量评分、分层配比采样，并提供版本指纹识别、整体替换检测、
阶段检查点持久化、优雅停止恢复与并发发布保护。任务原始需求见 `TASK.md`。

## 环境准备

- Python 3.10+（实际验证使用 Python 3.14.4）
- 依赖：`numpy`、`fastapi`、`uvicorn`（见 `requirements.txt`，验证环境为 numpy 2.2.1 /
  fastapi 0.115.6 / uvicorn 0.34.0）；测试使用 `pytest`

```bash
pip install -r requirements.txt
```

## 快速开始

```bash
# 1. 生成固定种子演示数据（3 个 JSONL 分片）
python3 -m sft_workbench.generator --out-dir data

# 2. 运行完整端到端演示（9 个环节，全部本地完成）
python3 -m sft_workbench.demo demo_run

# 3. 运行测试套件（52 个用例）
python3 -m pytest

# 4. 启动本地 HTTP 服务（默认 127.0.0.1:8377）
SFT_WORKSPACE=workspace python3 -m sft_workbench.server
```

## 目录结构

```
sft_workbench/
  errors.py      # 稳定业务错误码 + 结构化错误（JSON 响应与日志一致）
  models.py      # 统一内部数据模型与内容指纹
  codec.py       # v1/v2 双向编解码、v1 旧质量标注升级规则
  textnorm.py    # Unicode NFKC、空白折叠、小写化与中英文分词
  simhash.py     # 64 位 SimHash、4x16 位桶索引、海明距离
  dedup.py       # 规范化 + 近似去重阶段（E_EMPTY_SAMPLE、距离记录）
  quality.py     # 多因子质量评分（自然对数域合成、零因子回退）
  sampling.py    # 分层配比采样（最大余数法、确定性仲裁）
  checkpoint.py  # 原子检查点、文件锁、整体替换识别（回退/篡改/正常）
  pipeline.py    # 分阶段编排、增量失效传播、优雅停止恢复
  generator.py   # 固定种子样本生成器（3 分片、中英混排、异常样本）
  server.py      # FastAPI 本地 HTTP 接口
  demo.py        # 端到端演示（9 个环节）
tests/           # 52 个 pytest 用例
```

## 核心设计

### 统一内部数据模型与双版本互转（需求 1、3）

内部模型以样本标识、指令文本、输入、输出、输出格式描述、来源、语言、质量标注、
结构版本与内容指纹描述一条样本（`models.InternalSample`）。内容指纹是对规范化内容
字段（不含结构版本）的 SHA-256，因此 v1 → 内部 → v2 → 内部 往返后内容与指纹保持
不变（`tests/test_codec.py::test_v1_to_v2_round_trip_keeps_content_and_fingerprint`）。

- v1 输出为自由文本，导入时按可配置规则（`V1ImportRules.format_hint_pattern`）解析
  `[format:<type>]` 提示行得到结构化输出格式；
- v1 旧质量标注为整数百分比、可能为负、缺少语言一致性字段：导入时按可配置规则做
  百分比换算（`percent_scale`）、负分钳制（`clamp_min/max`）与语言一致性缺省推导，
  并在结果中标注降级项（`downgraded`）与换算依据（`basis`），例如
  `percent/100;clamp=[0,1];lang_consistency=inferred`；
- 编解码失败返回带样本标识与字段名的结构化错误码（`E_DECODE_SAMPLE` /
  `E_UNSUPPORTED_VERSION`），绝不静默丢弃。

### 规范化与 SimHash 近似去重（需求 2）

指令、输入、输出拼接文本经 Unicode NFKC、空白折叠、小写化后，以英文词与中文二元组
为特征计算 64 位 SimHash（blake2b 定长哈希，跨进程稳定）；4 个 16 位桶索引候选对，
在可配置海明距离阈值（默认 3）内确认重复。空文本样本返回带样本标识的
`E_EMPTY_SAMPLE`；重复样本保留首次出现者并记录被去重样本与距离。同一输入与配置
产生逐字节一致的保留清单与去重摘要。

### 多因子质量评分与分层配比采样（需求 4）

长度惩罚、指令/输出词汇重叠率、模板多样性、来源先验四个因子按可配置权重在自然
对数域合成质量分（`exp(Σ wᵢ·ln fᵢ / Σ wᵢ)`，numpy 实现）；任一因子为零时回退到
可配置下限并记录回退原因。随后按 `来源|语言` 分层统计保留样本数与 token 估计，
依据目标权重以最大余数法分配采样数，余数并列时按层键升序做确定性仲裁；分配数受
层内可用样本数约束并确定性再分配。同输入同配置的质量分与分配数逐字节一致。

### 版本指纹与整体替换识别（需求 5）

每个阶段检查点记录输入内容指纹、配置版本指纹、产物摘要与链式累积版本指纹
（任一上游内容或配置变化都会传播到下游指纹）。`CheckpointStore.verify()` 区分三种
情形：`rollback`（序号相对只增日志回退）、`tamper`（指纹或摘要不匹配）、
`normal`（正常推进）；仅正常推进被接受，其余抛出 `E_STALE_CHECKPOINT` 并附期望与
实际指纹。

### 检查点持久化与优雅停止恢复（需求 6）

每个阶段完成时以原子方式（临时文件 + fsync + rename + 目录 fsync）写入检查点，
任意位置中断都不会留下半写记录。停止请求在每个不可拆分样本单元完成后生效；恢复
后最终保留清单、质量分与混合集与一次从零完整运行**逐字节一致**，检查点序号连续
推进。输入或配置变化时，仅失效阶段及其下游被作废并重算（增量失效传播）。

### 并发发布保护与稳定错误码（需求 7）

阶段产物发布在真实文件锁（`fcntl.flock`）保护下比较序号，仅允许序号连续推进；
落后、重复或跨阶段错位发布均被拒绝并返回 `E_STALE_CHECKPOINT`。缺失前置阶段产物
返回 `E_INVALID_STAGE`，非法结构版本返回 `E_UNSUPPORTED_VERSION`。错误码与消息在
JSON 响应（`{"error": {"code", "message", "details"}}`）与日志中保持一致。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/samples/import` | 注册分片并执行导入阶段 |
| POST | `/stages/run` | 执行到指定阶段（`{"stage": "dedup"}` 或 `"all"`） |
| POST | `/stop` / `/resume` | 请求停止 / 清除停止标记并恢复 |
| GET | `/quality` | 质量分查询 |
| GET | `/mixture` | 分层配比与混合集查询 |
| GET | `/fingerprint` | 版本指纹与头指针查询 |
| GET | `/export/mixture` | 导出混合集（JSONL） |
| GET | `/checkpoints` | 检查点链验证结果 |
| GET | `/errors` | 导入与去重阶段的结构化错误 |

## 实际验证结果（本地运行记录）

环境：Python 3.14.4，numpy 2.2.1，fastapi 0.115.6，uvicorn 0.34.0，pytest 8.3.4。

- `python3 -m pytest`：**52 passed**（编解码 16、阶段算法 13、检查点 10、流水线 7、API 6）
- `python3 -m sft_workbench.demo`：9 个环节全部通过，关键输出：
  - 3 个分片摘要：`shard-000 b91d4ebc…`、`shard-001 3d0c83cc…`、`shard-002 7c73855c…`
  - 完整运行：导入 51 条（3 条结构化错误：缺字段 / 非法版本 / 坏 JSON），去重保留 37 条、
    去除 12 条近似重复（含距离 0/1/2），空样本错误 2 条；混合集 12 条
  - 版本指纹：`d4fa8e917ffab9a4…`；重跑逐字节一致：`rerun_identical=true`
  - 停止恢复：导入中途停止（21 个样本单元）后恢复，产物与全量运行逐字节一致，
    检查点序号为 `[1, 2, 3, 4, 5]`
  - 增量失效：去重阈值 3→2 后 `import` 缓存命中，其余阶段重算，版本指纹随之改变
  - 结构互转：v1→内部→v2→内部指纹稳定（`a7713c3631d81baa…`），v1 格式提示往返一致
  - 整体替换识别：`normal / tamper / rollback` 三种情形均正确判定，异常情形返回
    `E_STALE_CHECKPOINT` 并附期望与实际指纹
  - 并发发布：两个进程竞争同一序号，结果为 `[E_STALE_CHECKPOINT, ok]`
  - HTTP 冒烟：缺失前置阶段返回 409 `E_INVALID_STAGE`；完整流程后链验证为 `normal`
- 真实 uvicorn 服务（非测试客户端）同样完成导入 → 全阶段执行 → 指纹/配比/导出查询，
  混合集导出 12 行 JSONL，链验证 `normal`。
