# 指令数据质量与诊断工作台

这是一个完全本地、确定性的 LLM 指令微调数据构造与诊断工作台。它从本地 JSONL 样本出发，执行规范化/近似去重、逐样本质量评分、分层配比采样、混合集构造与标注一致性校验，并提供检查点、停止恢复、增量失效、并发发布保护和 HTTP/CLI 诊断接口。

核心目标不是“把低分样本删掉”，而是保留可审计的诊断轨迹：每个分数都能拆到因子、量化误差、来源与阶段指纹；每个混合集都能由相同输入和配置逐字节重建。

## 快速开始

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[test]'

# 生成固定种子演示数据（4 个分片）
python -m qworkbench generate-demo --data-dir data/demo --seed 20261120 --shards 4

# 完整构建
python -m qworkbench run --workdir .workbench/demo

# 查看状态、指纹、误差诊断、配比和一致性
python -m qworkbench status --workdir .workbench/demo
python -m qworkbench diagnostics --workdir .workbench/demo
python -m qworkbench allocation --workdir .workbench/demo
python -m qworkbench consistency --workdir .workbench/demo

# 导出最终混合集
python -m qworkbench export --workdir .workbench/demo --output mixture.jsonl
```

也可以直接启动 HTTP 工作台：

```bash
python -m qworkbench serve --workdir .workbench/http --host 127.0.0.1 --port 8017
# OpenAPI: http://127.0.0.1:8017/docs
```

## 实际环境

| 项目 | 实际值 |
|---|---|
| Python | 3.14.4（要求 >= 3.11） |
| NumPy | 2.5.5 |
| FastAPI | 0.165.0 |
| Uvicorn | 0.54.0 |
| Pydantic | 2.14.4 |
| pytest | 9.2.0 |
| httpx | 0.28.1 |
| 当前工作区文件系统 | WSL2 ext4 |
| 当前锁后端 | `fcntl.flock` |

版本探测和锁探针结果写入 `artifacts/environment.json`。当前运行环境是 WSL2/Linux，因此 Python 进程无法执行 Windows 原生 `msvcrt.locking` 分支；代码已同时实现两个平台分支，并在 WSL 挂载的可写 C: DrvFs 9p 路径上完成了双进程锁争用验证。原生 Windows Python 下会使用 `msvcrt.locking`。

## 演示数据

`data/demo/` 由 `qworkbench.generator.generate_demo` 使用固定种子 `20261120` 生成：

- 4 个 JSONL 分片，共 51 条样本、50 条标注；
- 中英混排、长输出、重复指令模板；
- 跨分片近似重复（SimHash 距离 1）和规范化后完全重复；
- 空输出、极短输出、未知来源、混合 Unicode 等异常样本；
- 标注文件独立为 `data/demo/annotations.jsonl`。

样本内容摘要：`8b24c6c4dafe9b8389b8f347b887fdb8b0ce47a89c021dd507b2fd3d9cdfec22`。

## 四阶段流水线

| 阶段 | 输入 | 关键处理 | 主要产物 |
|---|---|---|---|
| `normalize` | 样本分片 | NFKC、空白折叠、小写化、64 位 SimHash、4×16 位桶候选、海明阈值星形分组 | `01_normalized.jsonl`、`01_duplicate_groups.jsonl`、`01_summary.json` |
| `score` | 规范化保留集 | 长度惩罚、词汇重叠、模板多样性、来源先验、定点对数域加权几何均值 | `02_scores.jsonl`、`02_diagnostics.json` |
| `allocate` | 评分集 | `(source, lang)` 分层、token 估计、目标权重乘积、最大余数/最大赤字分配、确定性抽样 | `03_selected.jsonl`、`03_allocation.json` |
| `mix` | 抽样集 + 标注 | 分桶偏差、Spearman、越界清单、来源权重待复核标记 | `mixture.jsonl`、`04_consistency.json`、`manifest.json` |

所有阶段产物位于工作目录的 `artifacts/<stage>/`，单个阶段记录位于 `checkpoints/`，序号状态位于 `state.json`。

### 去重规则

1. 对 `instruction/input/output` 分别执行 Unicode `NFKC`、Unicode 空白折叠、`strip` 和小写化。
2. 使用分隔符 `␟` 拼接，计算规范化 SHA-256 和 64 位 SimHash。
3. 4 个 16 位桶建立候选索引，仅对候选做海明距离确认。
4. 连通分量内采取确定性星形分组：代表样本 id 最小，只有与代表样本直接满足阈值的样本才被移除。不会出现“链式重复导致与代表距离超过阈值”的隐式行为。
5. 分组、保留集、匹配边和摘要均按固定顺序序列化，输入顺序打乱不会改变输出字节。

### 评分与误差

因子：

- `length_penalty`：短于目标输出 token 数时线性惩罚；超过 `target_tokens × long_output_multiplier` 时反向惩罚。
- `lexical_overlap`：指令与输出 token 集合的 Jaccard 重叠率。
- `template_diversity`：`1 / sqrt(相同规范化指令模板计数)`。
- `source_prior`：配置来源先验；未知来源使用可配置默认先验并写入 `factor_notes`。

加权几何均值全程在对数域和定点量化下组合：

```text
factor_q    = round(factor × 2^Q) / 2^Q
weight_q    = 整数权重，权重单位总和严格等于 2^Q
term_q      = round((w_q / 2^Q) × ln(factor_q) × 2^Q) / 2^Q
log_score_q = Σ term_q
score       = exp(log_score_q)
```

默认 `Q=20`。每个样本输出：

- 各因子的原始值、量化值、对数贡献、加权对数贡献和整数单位；
- 因子量化误差、权重量化误差、项舍入误差及总对数误差上界；
- 相对误差上界与有效十进制位数。

零因子仅在存在已声明回退原因时使用 `quantization.min_factor` 回退，并记录 `fallback_reasons`；没有回退原因的零因子、NaN、Inf、负数或大于 1 的因子会抛出 `E_NUMERIC`，包含样本 id 和因子名。

语料级诊断同时给出顺序求和、成对求和、60 位 Decimal 高精度参考、Higham 浮点误差界和定点误差界。正则聚合值为定点整数 `mean_log_score`。

### 配比

- 层为 `(source, lang)`。
- 层目标权重 = `source_weights[source] × lang_weights[lang]`，缺省键使用 `default_weight`。
- 初始配额向下取整，剩余配额按最大赤字分配；并列时按 `source`、`lang` 字典序升序裁决。
- 每层按质量分降序、id 升序抽样。
- 输出保留数、按指令/输入/输出全文空白切分的 token 估计、理想配额、实际配额和实际采样数。

### 一致性

- 标注可来自独立 `annotations.jsonl`，也可使用样本内 `annotation_score`；独立文件优先。
- 质量分和归一化标注分做等宽分桶，输出分桶均值、偏差、加权绝对偏差、分布 L1 和越界样本。
- Spearman 使用平均秩处理并列值；样本数不足或秩向量恒定会返回原因而不是伪造数值。
- 当某个 source 的平均绝对偏差超过阈值时，其 `weight_status` 标记为 `PENDING_REVIEW`，给出复核建议，但不会丢弃样本或静默调整权重。

## 版本指纹与增量失效

阶段指纹是链式 SHA-256：

```text
normalize = H(algorithm, sample_fingerprint, dedup_config)
score     = H(algorithm, normalize, scoring_config + quantization_config)
allocate  = H(algorithm, score, allocation_config)
mix       = H(algorithm, allocate, annotation_fingerprint, consistency_config)
```

变化影响：

| 变化 | 最早失效阶段 |
|---|---|
| 样本内容 | `normalize` |
| 去重参数 | `normalize` |
| 评分权重、来源先验、长度参数或量化精度 | `score` |
| 配比权重、目标数量、默认权重 | `allocate` |
| 标注数据、一致性阈值、分桶参数 | `mix` |

未受影响的上游检查点复用；最早失效阶段及其全部下游重建。`GET /api/v1/fingerprints` 和 `status` 都会返回期望指纹、检查点指纹、失效原因及最后一次失效事件。

## 检查点、停止与并发发布

- 每阶段完成后先原子写检查点记录，再原子更新 `state.json`；使用同目录临时文件、`fsync` 和 `os.replace`。
- 检查点包含阶段名、单调序号、输入/配置/上游/阶段/产物指纹、配置版本、产物摘要和阶段摘要。
- 停止检查发生在每个不可拆分样本单元之后；中断时不会发布半阶段检查点。
- `resume` 清理停止标记并复用有效上游阶段。停止恢复后的最终混合集与一次完整运行逐字节一致。
- `locks/publish.lock` 保护检查点发布：多进程同时发布同一序号时只有一个成功，其余返回 `E_STALE_CHECKPOINT`。
- 发布阶段 N 时要求阶段 N-1 检查点存在、指纹匹配且产物摘要有效，否则返回 `E_INVALID_STAGE`。

## CLI

```text
qwb generate-demo      生成固定种子 JSONL 分片
qwb doctor             记录版本并执行本机/C: DrvFs 锁争用探针
qwb import-jsonl       导入本地 JSONL 到工作目录
qwb run                运行或增量继续流水线
qwb resume             清除停止请求并恢复
qwb stop               写入优雅停止请求
qwb status             查看阶段状态、指纹和失效原因
qwb diagnostics        查看逐因子误差传播诊断
qwb allocation         查看分层配额与采样明细
qwb consistency        查看标注一致性与复核建议
qwb manifest           查看最终混合集 manifest
qwb export             导出最终 mixture JSONL
qwb serve              启动本地 FastAPI 工作台
```

示例：运行到规范化阶段后停止，再恢复：

```bash
python -m qworkbench run --workdir .workbench/demo --stop-after-stage normalize
python -m qworkbench status --workdir .workbench/demo
python -m qworkbench resume --workdir .workbench/demo
```

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health` | 健康检查 |
| `POST` | `/api/v1/import` | 导入内联样本和可选标注 |
| `POST` | `/api/v1/run` | 前台或后台运行；可指定 `start_stage` 验证前置阶段 |
| `POST` | `/api/v1/control/stop` | 写入停止请求 |
| `POST` | `/api/v1/control/resume` | 清除停止请求并恢复 |
| `GET` | `/api/v1/jobs/latest` | 查看最近后台任务 |
| `GET` | `/api/v1/status` | 阶段状态、摘要、失效历史 |
| `GET` | `/api/v1/fingerprints` | 配置、数据、标注、阶段指纹 |
| `GET` | `/api/v1/diagnostics/numeric` | 误差传播与语料聚合诊断 |
| `GET` | `/api/v1/allocation` | 分层配比明细 |
| `GET` | `/api/v1/consistency` | 一致性报告与越界清单 |
| `GET` | `/api/v1/manifest` | 最终混合集 manifest |
| `GET` | `/api/v1/export` | 下载 `mixture.jsonl` |

示例：

```bash
curl -s -X POST http://127.0.0.1:8017/api/v1/run \
  -H 'content-type: application/json' \
  -d '{"wait":true}'

curl -s http://127.0.0.1:8017/api/v1/status
curl -s http://127.0.0.1:8017/api/v1/diagnostics/numeric
curl -s http://127.0.0.1:8017/api/v1/allocation
curl -OJ http://127.0.0.1:8017/api/v1/export
```

导入示例：

```json
{
  "samples": [
    {
      "id": "local-001",
      "instruction": "解释定点量化误差",
      "input": "Q=20",
      "output": "把因子与对数项分别量化，并报告误差上界。",
      "source": "curated-sft",
      "lang": "zh"
    }
  ],
  "annotations": {"local-001": 4.2}
}
```

## 业务错误码

| 错误码 | 含义 |
|---|---|
| `E_CONFIG` | 非法配置，例如量化位数越界、权重出界、阈值非法 |
| `E_NUMERIC` | NaN/Inf/非法因子，details 包含 `sample_id` 与 `factor` |
| `E_INVALID_INPUT` | 样本、标注或输入文件不合法 |
| `E_INVALID_STAGE` | 缺少前置检查点/产物，或请求阶段不可用 |
| `E_STALE_CHECKPOINT` | 检查点序号落后、重复或非单调 |
| `E_BUSY` | HTTP 后台任务已有一个运行中任务 |
| `E_LOCK_TIMEOUT` | 文件锁超时 |

## 实际验收结果

`artifacts/verification.json` 由 `python scripts/verify_acceptance.py` 生成。本次实际结果：

- 测试：`33 passed, 1 warning in 3.05s`。
- 完整运行：4 阶段完成，`last_seq=4`。
- 最终版本指纹：`2867c079628097d3613eb63e0548b0665d4853e9096c25be134964a08173ce17`。
- 混合集 SHA-256：`c205b9a582ff771278a8375561a79bce86562aca12c6f3478be0562f3ef18ad6`。
- 混合集样本数：30。
- 去重：51 → 41 条，8 个重复组，其中 1 个规范化完全重复组、7 个近似重复组；移除 10 条。
- 评分：41 条；4 条触发零因子回退；最低有效位数 3。
- 配比：保留 41，目标/实际 30，全文 token 估计保留 1579、采样 979。
- 一致性：加权分桶偏差 `0.0555228`，Spearman `0.339416879095`，越界样本 2 条，`legacy-import` 标记 `PENDING_REVIEW`。
- 停止恢复：`normalize` 检查点复用，最终混合集字节完全一致。
- 量化精度从 20 改为 18：最早失效阶段为 `score`，`normalize` 序号仍为 1。
- 业务错误注入：非法量化与越界权重为 `E_CONFIG`；缺前置阶段为 `E_INVALID_STAGE`；NaN 因子为 `E_NUMERIC`；重复发布为 `E_STALE_CHECKPOINT`。
- 锁争用：工作区 ext4 与 `/mnt/c/Users/Public` DrvFs 均通过双进程阻塞探针，等待约 364ms（持有 350ms）。

阶段指纹：

```text
normalize 3cfc3996cb6acf3f77eb2508fd6f6fa01f8ab9a134f8764b93d7bc0d70134279
score     c7b2035347c52c214d87f3ffa12420bb57675ef8916d207001241a0a60486750
allocate  11a996d027320ce300096d085244df0a5c2b2511389de4dfa151646cbd183054
mix       2255c7409b128e8d3ee29d8542d437f180478531613eb1b494231572031a2b18
```

## 测试

```bash
pytest -q
python scripts/verify_acceptance.py
```

测试覆盖：NFKC/去重确定性、星形海明阈值、固定点贡献与回退、NaN/Inf 错误、语料聚合误差界、最大余数并列裁决、Spearman、标注越界、全链路字节确定性、固定摘要、停止恢复、四类增量失效、缺前置阶段、并发发布、HTTP 全流程、HTTP 停止恢复、CLI 与非法配置错误码。

FastAPI TestClient 会显示一条 Starlette/httpx 的上游弃用警告，不影响功能或测试结果。

## 目录

```text
qworkbench/              核心包：配置、去重、评分、配比、一致性、持久化、API、CLI
configs/demo.json        演示配置
data/demo/               确定性样本分片与标注
tests/                   33 个验收测试
scripts/generate_demo.py 重新生成演示数据
scripts/verify_acceptance.py 可复现验收并写入报告
artifacts/environment.json   实际版本与锁探针
artifacts/verification.json  实际验收摘要
.workbench/              本地运行工作目录（已 gitignore）
```

## 设计约束

- 不使用 Docker 或容器。
- 不访问网络目标；运行只读取本地配置和 JSONL。
- 混合集保留全部被抽样样本；低分、未知来源、标注越界和待复核来源都进入诊断产物，而不是被静默删除。
- 产物确定性依赖固定 Python/依赖版本、固定样本、固定配置和固定算法版本；算法或逻辑变化应提升 `ALGORITHM_VERSION` 并更新固定摘要测试。
