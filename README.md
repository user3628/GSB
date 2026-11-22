# 本地 LLM 预训练数据配比与增量去重管线

这是一个不依赖 Docker、外部数据集或网络的本地可运行管线。它从固定随机种子的 JSONL 分片出发，执行四个确定性阶段：

`normalize → allocate → assemble → validate`

实现内容包括 Unicode 规范化与 64 位 SimHash 近似去重、按 `(source, lang)` 的最大余数配比、确定性混合集抽样、原子检查点、增量失效传播、并发发布保护、逐字节增量/全量校验，以及本地 CLI 和 HTTP API。

## 环境

- Python 3.10+
- NumPy

当前验证环境为 Python 3.14.4、NumPy 2.2.1，`python`、`pip`、`pip3` 均可调用。

```bash
pip install -r requirements.txt
```

## 快速演示

一条命令完成演示数据初始化、全量构建和增量/全量一致性校验：

```bash
python pipeline.py demo --workspace runtime --force
python scripts/verify_demo.py
```

也可以按阶段操作：

```bash
python pipeline.py init-demo --workspace runtime --force
python pipeline.py run-stage normalize --workspace runtime
python pipeline.py run-stage allocate  --workspace runtime
python pipeline.py run-stage assemble  --workspace runtime
python pipeline.py run-stage validate  --workspace runtime
python pipeline.py verify-incremental --workspace runtime --no-run
```

`demo/` 中的样本由 `llm_pipeline/generator.py` 使用固定种子 `20261123` 生成。重新生成：

```bash
python pipeline.py generate-demo --output-dir demo
```

演示数据包含 3 个分片、35 条输入记录：中英混排、同一 source 下多个 lang、跨分片近似重复、3 条空文本异常，以及两个最终为空的分层。

## 工作区结构

```text
runtime/
  config.json                    # 配置与 revision
  data/shards/*.jsonl            # 输入分片
  .pipeline/
    checkpoints/                 # 每阶段一个原子检查点文件
    artifacts/<stage>/           # 内容寻址产物
    checkpoints_quarantine/      # 损坏、重复、被替换检查点的隔离区
    locks/publish.lock           # fcntl.flock 真实文件锁
    locks/config.lock
    tmp/                         # 工作区临时目录
    stop.json                    # 协作式停止请求
    last_run.json                # 最近一次运行结果
```

产物没有时间戳，所有 JSON 都使用 UTF-8、键排序、紧凑分隔符的规范编码，因此同一输入、配置、算法版本会得到逐字节一致的产物。

## 四阶段语义

### 1. `normalize`：规范化与 SimHash 去重

对每条记录执行：

1. Unicode `NFKC`；
2. 连续空白折叠为单个空格，并去除首尾空白；
3. `.lower()` 小写化；
4. 计算 64 位 SimHash。

SimHash 特征定义在 `llm_pipeline/dedup.py`：拉丁字母/数字连续串作为词，CJK 单字作为特征，使用 `blake2b` 生成 8 字节特征哈希，再通过 NumPy 64 维加权向量求和得到指纹。每个 64 位指纹拆成 4 个 16 位桶；候选只在至少一个桶完全相同时产生，再以海明距离阈值确认重复。

去重策略是确定性的在线贪心：按样本 `id` 升序访问；最低 `id` 成为代表样本；命中的多个候选中选择海明距离最小者，距离相同再按代表样本 `id` 仲裁。重复项和保留项都不会因分片迭代顺序而漂移。

规范化后为空的样本不进入保留清单，并返回：

```json
{"code":"E_EMPTY_TEXT","id":"demo-0091","source":"archive","lang":"zh"}
```

### 2. `allocate`：source/lang 分层配比

对去重后保留样本按 `source` 与 `lang` 统计：

- 可用样本数；
- 重复数；
- 空文本错误数；
- token 估计。

默认 token 估计为 `len(normalized_text.split())`；可选 `whitespace_plus_cjk` 用于验证 token 估计方式变化会触发失效传播。

每个配置层使用 `source::lang` 标识。分配使用精确 `Fraction` 的最大余数法：

1. 计算理想配额与整数下界；
2. 剩余配额按余数从大到小分配；
3. 余数并列时按 `source`、再按 `lang` 升序仲裁。

当某层容量不足时，缺口在仍有容量的层中按同样规则再分配。配置中存在但无样本的层、以及仅有空文本的层都保留在 `layers` 与校验报告中，状态标为 `empty` 或 `empty_unweighted`，不会静默消失。

### 3. `assemble`：确定性混合集

每层候选按以下排序键抽样：

```text
sha256(assembly.seed + "\0" + sample_id), sample_id
```

前 `allocated` 条被选中；哈希分数相同的极端情况再按样本 `id` 仲裁。最终混合集按 `source`、`lang`、抽样分数、`id` 排序输出，保证逐字节稳定。

### 4. `validate`：一致性校验

校验保留清单摘要、每层分配数与实际组装数、总计、样本唯一性、文本/source/lang 与保留记录的一致性，并输出：

- `keep_list` 摘要；
- `allocation_table` 摘要；
- `mixed_set` 摘要；
- 各阶段产物摘要；
- 各层统计、空层清单、`E_EMPTY_TEXT` 明细。

## 增量失效

每个阶段保存由实际输入构造的 `stage_fingerprint`。变化检测矩阵如下：

| 变化 | 最早失效阶段 | 复用 |
|---|---|---|
| 分片字节内容或分片集合 | `normalize` | 无上游 |
| `dedup.hamming_threshold`、去重/SimHash 算法 | `normalize` | 无上游 |
| `allocation.weights`、`target_samples`、`token_estimator` | `allocate` | `normalize` |
| `assembly.seed`、组装算法 | `assemble` | `normalize`, `allocate` |
| 校验算法 | `validate` | `normalize`, `allocate`, `assemble` |

一旦识别到最早失效阶段，该阶段及其全部下游都会重建；即使重建后的内容摘要偶然与旧值相同，下游也不会错误复用。查询 `status` 或 `fingerprints` 可看到：

下游阶段指纹同时包含上游产物摘要与上游阶段指纹，因此“产物字节相同但上游确实被重建”仍会按依赖链继续失效。

- `invalidation.earliest_stage`
- `invalidation.reasons`（字段级 before/after）
- 当前与期望的阶段指纹
- 已失效下游阶段
- 检查点序号

## 检查点与并发发布

每个检查点包含：

```json
{
  "seq": 1,
  "stage": "normalize",
  "stage_seq": 1,
  "stage_fingerprint": "...",
  "fingerprint_components": {},
  "input_shards": [{"name": "...", "digest": "..."}],
  "input_shard_fingerprint": "...",
  "config_version": "...",
  "config_revision": 0,
  "artifact_digest": "...",
  "artifact_path": "artifacts/normalize/....json",
  "previous_record_digest": null,
  "record_checksum": "..."
}
```

发布协议：

- 产物和检查点都先写临时文件，`fsync` 后 `os.replace`，并 `fsync` 父目录；半写文件永不成为最终记录。
- `publish.lock` 使用 `fcntl.flock(LOCK_EX)`，是跨进程的真实文件锁。
- 全局 `seq` 必须从当前连续前缀的下一个值严格推进；每阶段另有连续 `stage_seq`。
- 检查点通过 `previous_record_digest` 形成哈希链。
- 加载时忽略损坏 JSON、缺字段、校验和不符、产物缺失/摘要错、序号重复、序号断档和阶段序号不连续记录；只采用从 `seq=1` 开始的最长一致前缀。
- 重发布前把活性前缀之外的冲突记录移入 `checkpoints_quarantine/`，因此活跃序号仍连续，不回绕。
- 同一 `seq` 的并发发布只有一个成功；落后、重复或跨阶段输入不匹配返回 `E_STALE_CHECKPOINT`。缺失前置阶段返回 `E_INVALID_STAGE`。拒绝发生在写产物之前，不会覆写已有产物。

## 优雅停止与恢复

```bash
# 另一个终端或进程
python pipeline.py stop --workspace runtime --reason maintenance

# 当前运行会在完成当前不可拆分样本单元后返回 E_STOPPED
python pipeline.py resume --workspace runtime
```

`run` 在每个样本/层单元处理后检查停止请求。停止时不会发布半成品阶段；已完成阶段检查点保留。`resume` 清除停止请求，从最近一致检查点继续。恢复后的保留清单、分配表和混合集与一次从零运行逐字节一致。

确定性测试钩子：

```bash
python pipeline.py run --workspace runtime --stop-after-samples 1 --delay-ms 50
```

该钩子在完成第 N 个不可拆分单元后停止，用于自动化验证。强制 `SIGKILL` 不承诺生成退出报告，这也是设计边界。

## CLI

| 命令 | 说明 |
|---|---|
| `init-demo` | 生成固定种子演示数据并初始化工作区 |
| `generate-demo` | 仅生成 `demo/` 资产与 manifest |
| `demo` | 初始化、全量构建并执行增量/全量校验 |
| `run` | 全量或增量推进所有需要重建的阶段 |
| `run-stage <stage>` | 仅执行前置阶段均有效的单个阶段 |
| `status` | 阶段状态、指纹、失效原因、检查点序号、空文本错误 |
| `fingerprints` | 紧凑版本指纹与失效信息 |
| `import-shard` | 原子导入新分片 |
| `replace-shard` | 单分片替换并默认执行增量重建 |
| `config-change` | 单配置补丁并默认执行增量重建 |
| `stop` / `resume` | 协作式停止与恢复 |
| `export-mixed` | 导出规范 JSONL 混合集 |
| `report [--output FILE]` | 输出或导出最终校验报告 |
| `verify-incremental` | 与从零全量构建逐字节比对 |
| `serve` | 启动本地 HTTP 服务 |

示例：

```bash
# 单分片替换，自动重建并可从返回的 run 对象查看 earliest stage
python pipeline.py replace-shard --workspace runtime \
  --name shard-002.jsonl --path /tmp/new-shard-002.jsonl

# 修改层权重
python pipeline.py config-change --workspace runtime \
  --set 'allocation.weights={"web::zh":6,"web::en":5,"news::zh":4,"news::en":3,"forum::mixed":4,"forum::en":2,"archive::zh":1}'

# 修改海明阈值，最早失效阶段应为 normalize
python pipeline.py config-change --workspace runtime \
  --set dedup.hamming_threshold=6

# 修改 token 估计方式，最早失效阶段应为 allocate
python pipeline.py config-change --workspace runtime \
  --set allocation.token_estimator=whitespace_plus_cjk

python pipeline.py export-mixed --workspace runtime --output runtime/mixed.jsonl
python pipeline.py report --workspace runtime --output runtime/report.json
python pipeline.py verify-incremental --workspace runtime
```

CLI 的成功/失败均输出 JSON；业务失败在 stdout 中包含稳定 `code`。

## HTTP API

```bash
python pipeline.py serve --workspace runtime --host 127.0.0.1 --port 8765
```

仅用于本地验证，无认证。

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 服务与工作区状态 |
| `GET` | `/api/status` | 完整状态、阶段指纹、失效原因、检查点 |
| `GET` | `/api/fingerprints` | 紧凑指纹视图 |
| `GET` | `/api/stages/{stage}` | 单阶段详情 |
| `GET` | `/api/checkpoints` | 连续序号与阶段序号 |
| `GET` | `/api/report` | 校验报告 |
| `GET` | `/api/export/mixed` | 原始 `application/x-ndjson` 混合集 |
| `GET` | `/api/export/report` | 下载规范 JSON 校验报告 |
| `POST` | `/api/run` | `{"force": false, "stop_after_samples": null}` |
| `POST` | `/api/stages/{stage}/run` | 单阶段执行 |
| `POST` | `/api/shards/import` | `{"name":"x.jsonl","path":"..."}` 或 `content` |
| `POST` | `/api/shards/replace` | 同上，默认 `"run": true` |
| `POST` | `/api/config/change` | `{"patch": {...}}` 或 `{"set": {"allocation.target_samples": 20}}` |
| `POST` | `/api/stop` | 写停止请求 |
| `POST` | `/api/resume` | 清除停止请求并恢复 |
| `POST` | `/api/verify-incremental` | `{"run_if_needed": true}` |
| `POST` | `/api/demo/init` | 初始化演示工作区 |

示例：

```bash
curl -s http://127.0.0.1:8765/api/status
curl -s -X POST http://127.0.0.1:8765/api/run \
  -H 'Content-Type: application/json' -d '{}'
curl -s -X POST http://127.0.0.1:8765/api/config/change \
  -H 'Content-Type: application/json' \
  -d '{"set":{"allocation.target_samples":20},"run":true}'
curl -s http://127.0.0.1:8765/api/export/mixed -o mixed.jsonl
```

## 稳定错误码

| 错误码 | 触发场景 |
|---|---|
| `E_EMPTY_TEXT` | 规范化后空文本；错误项包含样本 `id` |
| `E_STALE_CHECKPOINT` | 落后/重复/跨阶段不匹配，或并发竞争失败 |
| `E_INVALID_STAGE` | 非法阶段名、阶段跳转、缺失前置产物 |
| `E_INCREMENTAL_MISMATCH` | 增量与全量三项产物非逐字节一致 |
| `E_INVALID_INPUT` | JSONL、分片、工作区或请求输入非法 |
| `E_INVALID_CONFIG` | 配置字段、权重、算法参数非法 |
| `E_STOPPED` | 已完成当前单元并响应停止请求 |
| `E_STOP_REQUESTED` | 启动前已有未清除的停止请求 |
| `E_VALIDATION_FAILED` | 阶段内部一致性校验失败 |
| `E_LOCK_TIMEOUT` | 文件锁等待超时 |
| `E_NOT_FOUND` | 路由或资源不存在 |

`E_INCREMENTAL_MISMATCH` 的 `details.first_difference` 会给出首个差异产物、位置、样本 `id` 或层 `layer_id`；比较先做完整字节相等，再做差异定位，不使用近似比较。

## 固定演示摘要

完整基准见 `demo/expected.json`。当前固定结果：

| 项目 | 值 |
|---|---|
| 输入 / 去重保留 | 35 / 27 |
| 重复 / 空文本错误 | 5 / 3 |
| 分配 / 混合集 | 24 / 24 |
| 空层 | `archive::zh`, `forum::en` |
| 检查点序号 | `1,2,3,4` |
| `keep_list` SHA-256 | `c2ab21b41e7404cb8773a3670c687184924101cbbfcc1edb1ebb069c9d473ee2` |
| `allocation_table` SHA-256 | `c8005ab68a9454653772c0758259aa171614047b5a34d28c1ef35d098ebba506` |
| `mixed_set` SHA-256 | `6c088c1dd41babff13845e5e1d875581af8262b19e05e6d1ba8dedf54ff1740c` |
| `normalize` artifact | `4ef81ee30996b7d5b76f766e007e9bf68e4f8b35fb064a4ec0587aa896c2cf52` |
| `allocate` artifact | `eeb1c299e972d4ee36a49f568a1883523a07ea9a7cd3aef67392f4933c3be0d2` |
| `assemble` artifact | `e9ff64dbef683e3ce756abfaaaa8e0a3925ebdc756ebfe34ed0b145f7ab3aa85` |
| `validate` artifact | `04a16550ede036716d05fff4a2a098b49ff71931c9dd8e576355eb801a0584e6` |

## 验证结果

实际执行：

```bash
python -m unittest discover -s tests -v
python scripts/verify_demo.py
```

结果：24 项测试全部通过，演示基准 8 项检查全部通过。覆盖：

- NFKC/空白折叠/小写化、4 个 16 位桶、海明距离 7 的近似重复、逐字节一致去重；
- 最大余数余数并列按 source/lang 仲裁、空层保留、确定性组装；
- 检查点字段、连续序号、损坏记录、重复序号、半写/不完整记录忽略、损坏产物隔离重写；
- 两个独立进程并发发布：一个成功，另一个稳定返回 `E_STALE_CHECKPOINT`，且失败方未写产物；
- 停止后无半成品阶段，恢复后的三类产物与全量构建逐字节一致，序号无跳跃；
- 分片替换、权重、海明阈值、token 估计方式变化的正确最早失效阶段与上游复用；
- 上游重建但产物摘要恰好相同时，下游仍通过阶段指纹传播失效；
- `E_INVALID_STAGE` 非法跳转、`E_INCREMENTAL_MISMATCH` 首个样本/层差异；
- CLI `demo`/`config-change` 与 HTTP 全链路。

实测的增量序号推进示例：

| 操作 | 发布序号 | 复用 |
|---|---|---|
| 初始全量 | 1,2,3,4 | 无 |
| 调整权重 | 5,6,7 | `normalize` |
| 调整海明阈值 | 8,9,10,11 | 无 |
| 调整 token 估计 | 12,13,14 | `normalize` |
| 停止后恢复 | 2,3,4 | `normalize` |

## 代码布局

```text
llm_pipeline/
  dedup.py          # 规范化、SimHash、四桶候选、去重
  allocation.py     # 精确最大余数、容量约束、空层
  assembly.py       # 确定性抽样与混合集
  validation.py     # 一致性检查与摘要报告
  checkpoints.py    # 原子检查点、哈希链、flock 发布
  engine.py         # 失效传播、复用、停止恢复、增量校验
  workspace.py      # 工作区、分片导入/替换、原子 I/O
  config.py         # 配置、阶段指纹组件
  generator.py      # 固定种子演示生成器
  server.py         # 本地 HTTP API
  cli.py            # CLI
pipeline.py         # CLI 入口
scripts/verify_demo.py
tests/test_pipeline.py
demo/               # 固定演示分片、配置、manifest、expected
```

## 设计边界

- 目标是本地确定性验证，不执行网络访问，不依赖外部数据集。
- 停止是协作式的；不处理无法捕获的 `SIGKILL` 后退出报告。
- HTTP 服务面向本机，不应直接暴露到不可信网络。
- 分片全量读入内存；演示和测试规模下简单、可审计。生产级超大语料可保持相同协议并替换为外部分块/索引存储。
