# 跨语言法律条款引证链冲突消解组件（legal-cite）

本地可运行、确定性、零外部服务依赖的库 + CLI + HTTP 服务。把多语种合同引证
（“第 3.2 条 / Section 3.2 / 第三条第二款”）归一到统一整数条款定位，对适用期限、
违约金比例、生效月份做要素归一与冲突消解，产出唯一可信引证并保留完整证据链。

## 1. 环境准备（Python 3.11，本地虚拟环境）

```bash
# 已在本机用 uv 安装托管 CPython 3.11.18（无需 Docker / 系统级改动）
uv python install 3.11
uv venv --python 3.11 .venv
.venv/bin/python -m pip install -r requirements.txt   # 或 uv pip install -r requirements.txt
```

固定版本见 `requirements.txt`：`numpy 2.1.3 / fastapi 0.115.6 / uvicorn 0.34.0 /
pytest 8.3.4`（另含 `httpx 0.28.1` 仅供 TestClient）。

- 无需 GPU、外部服务或真实账号；样本由固定种子 `20261123` 本地生成。
- 工作区需可读写，运行时目录：`data/`（分片）、`checkpoints/`（检查点）、
  `evidence/`（证据清单与摘要）。
- Windows 路径请用反斜杠或原始字符串，例如 `Path(r"C:\work\legal-cite\data")`；
  代码内部全部使用 `pathlib.Path`，不写死 POSIX 分隔符。文件锁使用 POSIX
  `fcntl.flock`（Linux/macOS）。

## 2. 一分钟端到端示例

```bash
.venv/bin/python -m legal_cite generate --output-dir data
.venv/bin/python -m legal_cite resolve data/citations_shard_1.jsonl \
    data/citations_shard_2.jsonl data/citations_shard_3.jsonl \
    --checkpoint-dir checkpoints
.venv/bin/python -m legal_cite batch data/citations_shard_1.jsonl \
    data/citations_shard_2.jsonl data/citations_shard_3.jsonl \
    --gold data/gold_annotations.jsonl --consistency \
    --output evidence/recompute_summary.json
.venv/bin/python -m legal_cite stop-resume data/citations_shard_1.jsonl \
    data/citations_shard_2.jsonl data/citations_shard_3.jsonl \
    --checkpoint-dir checkpoints
.venv/bin/python -m legal_cite evidence evidence/evidence_manifest.json --citation-id CITE-0001
.venv/bin/python -m pytest -q
```

启动仅绑定回环的 HTTP 服务（不监听外部网络）：

```bash
.venv/bin/python -m uvicorn legal_cite.api:app --host 127.0.0.1 --port 8765
# GET /health；POST /resolve；POST /evidence/verify
```

## 3. 目录结构与模块职责

- `legal_cite/errors.py`：7 个稳定业务错误码与结构化异常。
- `legal_cite/serde.py`：规范化 JSON（排序键、固定分隔符）、SHA-256 指纹、原子写。
- `legal_cite/config.py`：版本化配置（换算字典、语言权重、渠道先验、新鲜度、精度、
  复核阈值）及其配置指纹。
- `legal_cite/normalize.py`：阶段一，NFKC + 全半角统一 + 空白折叠，保留到原文的逐字符
  偏移映射；解析中文“第X条/第X款”、英文“Section X.Y / Article X”，输出统一
  定位 `A{article}.S{section}`（款缺省为 `S0`）与句式来源。
- `legal_cite/elements.py`：阶段二，期限归一为整月区间、违约金定点百分比、生效月份
  ISO 校验，记录每次换算因子，并按 (合同, 定位) 分组判定冲突。
- `legal_cite/resolution.py`：阶段三，对数域可信度
  `logit(渠道先验)+log(语言权重)+新鲜度项`，跨分片重复折叠，确定性仲裁
  （分数降序、再按 `citation_id` 升序），全组低于阈值则人工复核。
- `legal_cite/evidence.py`：阶段四，五环证据链（raw_anchor → normalization →
  locator_parse → element_normalization → resolution），链式哈希；v2 导出/导入/
  往返校验；v1 旧清单（自由文本定位、千分位比例、缺换算因子）迁移到 v2。
- `legal_cite/checkpoints.py`：原子检查点、`fcntl` 真实文件锁、单调序号、阶段拓扑。
- `legal_cite/pipeline.py`：四阶段编排、阶段指纹链与增量失效、优雅停止与恢复。
- `legal_cite/batch.py`：批量复算指标、两次运行逐字节一致、批量 vs 逐合同汇总比对。
- `legal_cite/sample_generator.py` / `legacy_v1_generator.py`：确定性样本与 v1 夹具。
- `legal_cite/cli.py` / `api.py`：命令行与 FastAPI 接口。
- `tests/`：39 个 pytest 用例，覆盖全部错误码、并发锁、崩溃原子性、迁移与恢复。

## 4. 确定性与版本指纹

- 所有产物用统一规范化 JSON 序列化后再 SHA-256；相同输入 + 相同配置两次运行的证据
  清单与复算摘要逐字节一致（测试 `test_pipeline_determinism_and_single_adoption`、
  `test_metrics_and_per_contract_consistency`）。
- 阶段指纹链式构造：`fp(stage)=sha256(prev_fp, stage, 阶段产物)`，起点包含分片摘要与
  配置指纹。分片内容、换算字典、语言权重、渠道先验或精度任一变化，都会定位到最早失效
  阶段并令其全部下游重建，未变上游继续复用（见 `test_incremental_invalidation_*`）。

## 5. 检查点、停止恢复与并发发布

- 每个阶段产物经 `temp 文件 + fsync + os.replace` 原子发布，崩溃不留半写记录
  （`test_crash_during_atomic_write_leaves_no_half_record`）。
- 停止请求只在不可拆分单元（单条引证 / 单个分组）边界生效；恢复后结论、降级与证据链
  与一次完整运行逐字节一致，检查点序号在每次全新构建中单调连续
  （`test_graceful_stop_then_resume_is_byte_identical`）。
- 多进程发布用真实 `flock` 串行化并比较序号：落后/重复/跨阶段错位报
  `E_STALE_CHECKPOINT`，缺前置阶段报 `E_INVALID_STAGE`
  （`tests/test_filelock.py`、`tests/test_checkpoints.py`）。

## 6. 错误码与触发点

| 错误码 | 触发场景 |
| --- | --- |
| `E_CITATION_UNPARSED` | 无定位句式、片段与原文不匹配、定位为负/越界，携带 citation_id 与偏移 |
| `E_ELEMENT_INVALID` | 期限倒置、比例越界、生效月份非法、未知单位/渠道/语言，携带要素名 |
| `E_EVIDENCE_BROKEN` | 证据链缺环、哈希断裂、结论与裁决不一致，指出断裂环节 |
| `E_VERSION_SHAPE_MISMATCH` | 清单版本号与内容形态不符、不支持的版本 |
| `E_STALE_CHECKPOINT` | 落后/重复/跨阶段错位的检查点发布 |
| `E_INVALID_STAGE` | 缺失前置阶段或未知阶段名 |
| `E_INCREMENTAL_MISMATCH` | 批量与逐合同汇总或两次复算不一致，附首个差异合同 |

异常引证不会被静默丢弃：解析/要素错误进入 `diagnostics.jsonl`，仍参与诊断统计。

## 7. 本地确定性验证结果

`pytest -q`：**39 passed**。样本集 3 个分片共 31 条引证、7 个真值冲突组，复算指标：

- 冲突识别率 1.0、消解正确率 1.0、证据链重建成功率 1.0；
- 5 条异常样本分别触发 2 个 `E_CITATION_UNPARSED` 与 3 个 `E_ELEMENT_INVALID`；
- 1 个 OCR 低可信组进入人工复核（无随机选择）；6 条跨分片重复被折叠并保留溯源；
- v1→v2 迁移与往返校验通过；停止-恢复后 `pipeline_fingerprint` 与完整运行一致；
- uvicorn 经 `/proc/net/tcp` 确认仅监听 `0100007F:8765`（127.0.0.1）。

## 8. 作为库集成

```python
from legal_cite import run_pipeline, default_config
state = run_pipeline(records, cfg=default_config(), ckpt_dir="checkpoints")
for decision in state["manifest"]["decisions"]:
    print(decision["locator"], decision["adopted_citation_id"],
          [(r["citation_id"], r["status"], r["reason"])
           for r in decision["resolutions"]])
```
