# 搜索查询理解工作台（quworkbench）

本地可运行的三阶段搜索查询理解工作台：查询规范化与分词 → 意图分类与实体槽位抽取 →
阶段产物版本化持久化与增量失效裁决。全部数据由固定种子生成器本地产出，不依赖任何线上数据。

## 环境

- Python 3.11+（本机实际使用 Python 3.14.4：环境中无 python3.11 且无法联网安装，
  按"以本地可获取的稳定版为准"原则执行；代码仅依赖 3.11 起可用的标准库特性）
- 依赖见 `requirements.txt`：`numpy`（对数域打分/softmax）、`fastapi` + `uvicorn`（HTTP 工作台）、
  `pytest` + `httpx`（测试）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## 项目结构

```
quworkbench/
  tokenizer.py   阶段1：NFKC、空白折叠、小写化；拉丁字母数字连写 + 单中文字符切分，带字符偏移
  model.py       阶段2a：对数域多项朴素贝叶斯（先验/条件对数概率、平滑强度、类别权重、<UNK> 回退、softmax）
  slots.py       阶段2b：词典最长匹配（类目/品牌/地域），重叠候选按可配置优先级消解，被抑制候选入诊断
  store.py       阶段3：原子发布（tmp+rename、单行 fsync 日志）、单调序号、flock 文件锁、崩溃恢复
  pipeline.py    版本指纹、失效传播、单分片替换/单配置变更、增量 vs 全量逐字节校验
  generator.py   固定种子(20261123)数据生成：3+ 分片、72 条已标注样本、19 词条槽位词典
  server.py      FastAPI 应用（仅 127.0.0.1 回环）
  cli.py / demo.py
tests/test_workbench.py   34 个用例
```

## 快速开始

```bash
# 生成确定性数据（3 分片 / 中英混排 / 异常样本 / 槽位重叠）
.venv/bin/python -m quworkbench.cli --workspace demo_workspace generate
# 全量构建
.venv/bin/python -m quworkbench.cli --workspace demo_workspace run --full
# 查看各阶段状态与版本指纹
.venv/bin/python -m quworkbench.cli --workspace demo_workspace status
# 单分片替换（只重建该分片失效链）
.venv/bin/python -m quworkbench.cli --workspace demo_workspace replace-shard \
    --shard shard-002 --file demo_workspace/data/variants/shard-002-v2.jsonl
# 单配置变更（只重建受影响阶段及下游）
.venv/bin/python -m quworkbench.cli --workspace demo_workspace change-config \
    --key classify.smoothing_alpha --value 0.5
# 增量 vs 全量逐字节校验
.venv/bin/python -m quworkbench.cli --workspace demo_workspace verify
# 槽位诊断（含被抑制候选及依据）/ 失效原因
.venv/bin/python -m quworkbench.cli --workspace demo_workspace diagnostics --query q-0037
.venv/bin/python -m quworkbench.cli --workspace demo_workspace invalidations
# 崩溃恢复
.venv/bin/python -m quworkbench.cli --workspace demo_workspace recover
# 一键确定性演示（完整运行 + 中断恢复运行，均产出固定摘要）
.venv/bin/python -m quworkbench.cli --workspace demo_workspace demo
# HTTP 工作台（仅回环）
.venv/bin/python -m quworkbench.cli --workspace demo_workspace serve --host 127.0.0.1 --port 8737
```

HTTP 接口：`GET /health`、`GET /status`、`POST /run`、`POST /run/full`、`POST /shards/import`、
`POST /shards/{id}/replace`、`POST /config/change`、`GET /queries/{qid}/slots`、
`GET /invalidations`、`POST /verify`、`POST /recover`、`POST /shutdown`（优雅停止）。
业务错误以 409 + `{"error": {code, stage, reason, ...}}` 返回。

## 版本指纹与失效传播

阶段链 `normalize → classify → slots`。每个阶段的版本指纹由其输入内容摘要构成：

- `normalize`：分片内容指纹 + 规范化配置摘要
- `classify`：上游指纹 + 已标注样本摘要 + 平滑强度 + 类别权重 + 未知标记
- `slots`：上游指纹 + 槽位词典摘要 + 优先级配置摘要

任一输入变化 → 最早受影响阶段指纹变化 → 该阶段及全部下游失效重建，未受影响上游复用
（序号不变即复用）。失效事件（原因、新旧指纹、新序号）追加到 `checkpoints/invalidations.jsonl`，
并经 `GET /invalidations` 与 `status` 可观测。

## 持久化与并发

- 记录：`{seq, stage, shard_id, shard_fingerprint, config_fingerprint, input_digests,
  artifact_digest, artifact_file}`，每行一条追加写入 `records.jsonl`（单次 write + fsync）。
- 产物：`artifact-NNNNNN.json`，先写 `.tmp` 再 `os.replace` 原子更名，中断不留半写记录。
- 恢复：截断损坏尾部、忽略不完整/序号重复记录、要求序号连续且产物摘要匹配，
  从最近一致检查点继续；孤儿产物与 `.tmp` 残留被清理。
- 并发：每个分片一把 `fcntl.flock` 真实文件锁；仅允许序号连续推进；落后/重复/跨分片错位
  → `E_STALE_CHECKPOINT`；缺前置阶段 → `E_INVALID_STAGE`；拒绝后工作区状态不变。

## 稳定业务错误码

`E_EMPTY_QUERY`（空/纯标点查询，带 query_id）、`E_INVALID_CONFIG`（平滑强度为零、
标注样本不足、权重非正，带阶段名与原因）、`E_SLOT_INVALID`（词典缺失/损坏、偏移越界）、
`E_INCREMENTAL_MISMATCH`（附首个差异 query_id 与阶段名）、`E_STALE_CHECKPOINT`、`E_INVALID_STAGE`。

## 实测验证结果（本机，2026-11-23）

- `pytest`：**34 passed**（规范化确定性/偏移、E_EMPTY_QUERY、softmax 归一、E_INVALID_CONFIG、
  最长匹配与抑制诊断、E_SLOT_INVALID、原子发布/恢复、失效传播、增量=全量、4 进程并发发布仅 1 成功、
  HTTP 层）。
- 确定性演示（`cli demo`，种子 20261123）两次执行摘要逐字节一致：
  - 完整运行：`sha256:acc877624d347b887737bc71bb0b489cd2469f725cd6bfea2234f05e66230830`
    （46 查询，41 ok，5 × E_EMPTY_QUERY）
  - 单分片替换 + 平滑强度变更后增量结果：`sha256:3078ea1021a50866565a01e493027cc4df2df1e00580614045ffe7049e67fafc`，
    `verify` 与从零全量构建逐字节一致（match=true）
  - 模拟崩溃（日志尾部截断 + 残留 tmp）→ `recover` → 重跑摘要仍为
    `sha256:3078ea10...`，与增量结果一致
  - 过期序号发布被拒（`E_STALE_CHECKPOINT`）且状态不变
- 失效传播实测：替换 shard-002 仅重建该分片三阶段；`smoothing_alpha` 变更仅重建
  classify+slots（normalize 序号保持 1 复用）；词典变更仅重建 slots。
