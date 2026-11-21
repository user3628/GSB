# FactMerge

本地可运行的跨语言多源事实陈述核验与证据链冲突消解组件。它面向中英文知识库中的同一实体，将人口、面积、成立年份、任期年月区间等声明统一归一，使用十进制精确比较完成冲突裁决，并输出可重导入、可追溯的版本化证据清单。

运行时不访问网络、不使用 GPU、不依赖容器；所有样例由固定随机种子的本地脚本生成。

## 快速开始（Windows）

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip install -e .

# 生成固定种子样本（已随仓库提供，可复现）
.\.venv\Scripts\python.exe .\scripts\generate_samples.py --seed 20261122 --out .\samples

# 运行测试
.\.venv\Scripts\python.exe -m pytest

# 批量复算并导出证据清单与摘要
.\.venv\Scripts\factmerge.exe --config .\configs\default.json --corpus .\samples\corpus --state-dir .\.factmerge-state --expected .\samples\expected.json batch --out-dir .\artifacts

# 一键验收并生成 artifacts\verification_report.json
.\.venv\Scripts\python.exe .\scripts\demo.py
```

Linux/macOS 将 `.\.venv\Scripts\python.exe` 替换为 `.venv/bin/python` 即可。未安装控制台脚本时，所有命令都可以改用 `python -m factmerge ...`。

Python 要求为 3.10 及以上。`requirements.txt` 对不同 Python 版本固定了 `numpy`/`pytest` 版本：3.10-3.13 使用 `numpy==2.2.6`、`pytest==8.4.2`，3.14 及以上使用 `numpy==2.5.5`、`pytest==9.2.0`。

## 目录

```text
src/factmerge/          可集成库与 CLI
  parser.py             阶段 1：Unicode、数值与年月片段解析
  units.py              阶段 2：单位换算与日历归一
  resolver.py           阶段 3：精确冲突消解与对数域可信度
  evidence.py           阶段 4：证据链、清单校验与重建
  migration.py          版本 1 证据清单迁移
  state.py              原子检查点、单调序号与停止文件
  pipeline.py           四阶段编排、批量指标、恢复与自检
  api.py                库级便捷接口
configs/default.json    默认单位字典、权重、精度与仲裁规则
samples/                固定种子语料分片、期望值、v1 清单与边界样本
scripts/generate_samples.py
scripts/demo.py
tests/                  40 项自动化测试
docs/architecture.md    设计与不变量
docs/verification.md    实际验收记录
artifacts/              示例输出与验证报告
```

## 输入格式

语料是 UTF-8 JSONL 分片。必需字段为 `claim_id`、`entity_id`、`lang`、`raw_value`、`unit`、表述句式、来源渠道；可选字段包括类别、观察时间和可信度标注。表述句式默认字段名为 `sentence`，同时兼容 `statement` 与 `sentence_template`。

```json
{
  "claim_id": "C-POP-ATLAS-ZH",
  "entity_id": "ENT-ATLAS",
  "lang": "zh",
  "category": "population",
  "raw_value": "1.38亿",
  "unit": "人",
  "sentence": "阿特拉斯市人口约为1.38亿人。",
  "source": "official_stats",
  "observed_at": "2025-01-01",
  "annotated_confidence": "0.98"
}
```

支持类别：`population`、`area`、`founded_year`、`tenure`。类别可省略，由单位、日期区间和句式关键词推断。

## 四个阶段

- 阶段 1 做 NFKC、空白折叠和全半角统一，保留规范化文本与原始文本之间的逐字符偏移映射；解析中文数字/量词、英文 `K/M/B/million` 缩写、千分位和小数，提取数值、嵌入比例、单位片段和解析来源。
- 阶段 2 将人口映射到 `person`、面积映射到 `km2`、成立年份映射到 `year`、任期映射到零基月序号；每个换算记录系数、系数精度、量化单位、舍入模式和可逐项复算的 `recompute` 诊断。单位缺失、未知、负值、区间倒置和开闭冲突均标记 `review` 并保留证据，不静默丢弃。
- 阶段 3 在实体和要素类别内按归一值分组。数值比较使用 `Decimal` 和配置的比较精度，不使用浮点容差。可信度在对数域由来源先验、语言权重、新鲜度衰减和可选标注合成；并列时按配置仲裁规则，默认最终以 `claim_id` 升序确定性裁决。
- 阶段 4 为采纳与降级声明构建 `source_claim -> stage1 -> stage2 -> stage3` 证据链，记录摘要、偏移、换算系数、置信度分解和裁决理由。导出的 v2 清单可重新校验并重建同一结论与同一证据链；中间阶段缺失时返回 `E_EVIDENCE_BROKEN` 和断点声明。

## 常用 CLI

```powershell
# 单实体核验
factmerge verify ENT-ATLAS

# 批量复算并写出 evidence_manifest.json / batch_summary.json
factmerge batch --out-dir artifacts

# 查询降级原因与完整证据链
factmerge query --manifest artifacts\evidence_manifest.json --claim-id C-TIE-B
factmerge query --manifest artifacts\evidence_manifest.json --entity-id ENT-DELTA

# 版本指纹、失效原因、检查点序号和复用计数
factmerge version

# 请求停止；当前不可拆分实体完成后退出
factmerge stop
factmerge resume --out-dir artifacts

# 证据清单校验与确定性重建
factmerge evidence validate artifacts\evidence_manifest.json
factmerge evidence rebuild artifacts\evidence_manifest.json --out artifacts\rebuilt_manifest.json

# 版本 1 证据清单迁移与等价回导
factmerge migrate-v1 samples\legacy_evidence_v1.jsonl --out-manifest artifacts\legacy_v2.json --out-v1 artifacts\legacy_roundtrip.jsonl

# 相同配置重复运行、批量与逐实体一致性自检
factmerge selfcheck
```

全局选项应放在子命令之前，例如 `--config`、`--corpus`、`--state-dir`、`--expected`。默认值已指向仓库内样例。

## 库接口

```python
from factmerge import FactMergeEngine, run_batch, verify_entity, query_claim

result = run_batch(
    "configs/default.json",
    "samples/corpus",
    state_dir=".factmerge-state",
    expected="samples/expected.json",
)
print(result["summary"]["decision_accuracy"])

single = verify_entity("configs/default.json", "samples/corpus", "ENT-ATLAS")
evidence = query_claim("artifacts/evidence_manifest.json", "C-TIE-B")
```

`FactMergeEngine` 还提供 `request_stop()`、`version_status()`、`query_entity()`、`query_claim()` 与 `consistency_check()`。

## 指纹与增量失效

版本指纹由语料摘要和四个阶段的配置内容摘要逐级串联：

| 变更 | 最早失效阶段 | 失效范围 |
| --- | --- | --- |
| 语料分片内容 | 1 | 1-4 |
| 解析、规范化、类别提示、数字比例 | 1 | 1-4 |
| 单位字典、量化精度、日历、兜底策略 | 2 | 2-4 |
| 语言/来源权重、新鲜度、比较精度、仲裁规则 | 3 | 3-4 |
| 证据 schema 配置 | 4 | 4 |

未失效的上游检查点直接复用。`version` 命令和 `version_status()` 返回当前/历史指纹、最早受影响阶段、原因代码、失效阶段、有效/失效检查点计数和复用计数。

## 检查点、停止与恢复

每个实体的每个阶段完成后，以“临时文件 + `fsync` + 原子替换”写入检查点。检查点带全局单调 `seq`、阶段名、实体、指纹和内容摘要；状态清单也原子更新。`stop` 仅写入停止请求文件，执行器在当前实体四阶段全部完成后才于下一个实体边界退出；`resume` 清除停止请求并复用有效检查点。恢复后的最终清单与完整运行逐字节一致。

## 版本 1 迁移

v1 导入支持带千分位字符串、自由文本单位、缺少换算因子和单值年月区间。可配置的单位文本映射将自由文本映射到单位字典项，并补全因子和精度；单月按闭区间点展开并标记 `V1_SINGLE_MONTH_EXPANDED`。不可映射单位返回 `E_INVALID_FORMAT`。导出回 v1 时保留原记录，样例回导逐字节一致。

## 错误码

- `E_PARSE_AMBIGUOUS`：空数值、无法解析或千分位/小数点冲突；包含 `claim_id` 与偏移。
- `E_EVIDENCE_BROKEN`：证据链缺环或摘要不一致；包含 `claim_id`、`entity_id` 和 `breakpoint`。
- `E_INVALID_FORMAT`：输入、配置或 v1 单位映射非法。
- `E_NOT_FOUND`：查询的实体或声明不存在。

CLI 将结构化错误写到 stderr，并以稳定退出码区分错误类型。

## 已执行验证

详细记录见 `docs/verification.md`，机器可读报告见 `artifacts/verification_report.json`。当前样例结果：

- 29 条声明、7 个实体；冲突识别率、裁决正确率、证据链重建成功率均为 `1.000000`。
- 两次相同配置运行及批量/逐实体汇总逐字节一致。
- 停止后恢复与一次完整运行的证据清单逐字节一致。
- 修改语言权重后最早失效阶段为 3，阶段 1/2 的 7 个实体检查点全部复用。
- v1 清单迁移校验通过，回导与原清单逐字节一致；不可映射单位返回 `E_INVALID_FORMAT`。
- 删除任一证据链的 `stage2` 后返回 `E_EVIDENCE_BROKEN`，并准确指出声明和断点。

测试命令与结果：`.venv/bin/python -m pytest`，`40 passed in 4.34s`。

## 约束与边界

组件是确定性规则引擎，不调用外部模型或网络。当前解析器覆盖任务要求中的中文数字/量词、英文缩写、常见英制/公制单位和年月区间语法；其他语言或自由文本句式应通过配置别名和类别提示扩展。可信度是来源、语言、新鲜度和标注的可解释组合，不替代领域事实审核；低置信和异常输入会保留为待复核声明。
