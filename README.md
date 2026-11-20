# 本地查询理解与意图解析引擎

一个完全离线、确定性可复现的查询理解引擎，覆盖规范化/分词、朴素贝叶斯与特征权重混合的意图分类、词典最长匹配槽位抽取、重叠消解、多级回退、异常注入、阶段版本指纹、缓存失效、批量评测、CLI 与本地 HTTP 接口。不访问搜索引擎、线上模型或外部账号。

## 快速开始

```bash
# 当前工作区已准备好 .venv311；从零准备也可执行：
./scripts/setup_env.sh
source .venv311/bin/activate

# 重新生成确定性数据（固定 seed=17）
python scripts/generate_data.py --seed 17

# 一键验收：测试 + 演示 + 两次同种子实验比对 + 不同种子区分
./scripts/run_acceptance.sh
```

也可以直接使用系统 Python（需 Python 3.11+、NumPy、pytest）：

```bash
python3 -m qiu version
python3 -m qiu parse --query "北京咖啡店" --query-id demo-1 --locale zh-CN --channel map
python3 -m qiu experiment --seed 17 --output artifacts/experiment.json
python3 -m qiu serve --host 127.0.0.1 --port 8765
```

## 交付结构

```text
qiu/                         核心实现与 CLI/HTTP 入口
  normalization.py           NFKC、空白折叠、小写化、带偏移分词
  classifier.py              朴素贝叶斯 + token 特征权重 + softmax
  slots.py                   三类词典最长匹配、优先级重叠消解、诊断
  fallback.py                历史先验 -> channel -> 最终兜底
  engine.py                  阶段缓存、版本指纹、异常注入、结果组装
  experiment.py              指标与带种子的 bootstrap 实验摘要
  server.py                  标准库 HTTPServer API
  demo.py                    可重复的端到端演示
scripts/generate_data.py     确定性数据生成器（seed 参数）
scripts/run_acceptance.sh    一键验收脚本
data/                        已生成的配置、训练集、评测集、词典
tests/                       16 项 pytest 测试
docs/architecture.md         架构、指纹和错误契约
artifacts/                   实际验收摘要、实验输出与演示报告
```

数据规模：48 条标注训练小样本（四类各 12 条）和 52 条评测查询（覆盖四类意图、中英混排、重叠候选及低置信度回退场景）。所有数据由 `scripts/generate_data.py` 按固定种子生成并随工作区提供。

`examples/requests.jsonl` 与批量输入使用如下包装格式；也可在输入行中直接给出查询字段并附加 `faults`：

```json
{"faults":{},"query":{"channel":"map","history_categories":[],"locale":"zh-CN","query_id":"demo-1","raw_query":"北京咖啡店"}}
```

## 单条解析

```bash
python3 -m qiu parse \
  --query "买苹果手机" \
  --query-id q-1001 \
  --locale zh-CN \
  --channel shop \
  --faults-json '{"overlap_slot":{"type":"brand","text":"苹果手机","start":1,"end":5}}'
```

结果包含：

- `normalization.normalized_query` 与 `tokens[*].start/end`（相对规范化字符串的偏移）
- `intent.candidates[*].components`：`prior_log`、`likelihood_log`、`feature_raw`、类别权重、加权分量、总分
- `intent.candidates[*].probability`：softmax 置信度
- `intent.confidence_kind`：`softmax`、`history_share` 或 `rule`，避免回退后混淆置信度语义
- `intent.fallback_trace`：四级回退的 selected/rejected/skipped 状态与原因
- `slots`：类型、起止偏移、来源词典版本
- `slot_diagnostics.suppressed`：被抑制候选、优先级、抑制者与原因
- `cache.stage_status`、`cache.invalidated_stages`、`cache.reused_stages`
- `versions` 与 `version_fingerprints`：四阶段版本快照及指纹

JSON 输出统一使用 UTF-8、键排序、紧凑分隔符与 12 位稳定浮点，适合逐字节比对。

## HTTP API

```bash
python3 -m qiu serve --port 8765
```

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `GET` | `/v1/version` | 阶段指纹、资产摘要、模型统计、缓存状态 |
| `POST` | `/v1/parse` | 单条解析；支持 `query` + `faults` 包装或直接查询对象 |
| `POST` | `/v1/experiment` | 批量实验，参数 `seed`、`bootstrap_samples` |
| `POST` | `/v1/reload` | 检查磁盘资产变化并按指纹重建 |

```bash
curl -s http://127.0.0.1:8765/v1/parse \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "query_id": "http-1",
      "raw_query": "北京咖啡店",
      "locale": "zh-CN",
      "channel": "map",
      "history_categories": []
    },
    "faults": {}
  }'
```

业务错误返回 HTTP 400，响应体仍为结构化 JSON，例如：

```json
{"error":{"code":"E_INVALID_LOCALE","details":{"allowed_locales":["en-US","zh-CN","zh-TW"],"locale":"xx-XX"},"message":"locale is not in the configured allow-list","query_id":"http-1","stage":"validation"},"ok":false}
```

## 稳定错误契约

| 业务码 | 阶段 | 触发方式 |
| --- | --- | --- |
| `E_INVALID_LOCALE` | `validation` | locale 不在允许列表，或注入 `invalid_locale` |
| `E_EMPTY_QUERY` | `normalization` | 空字符串/空白/仅标点，不进入后续阶段 |
| `E_QUERY_TOO_LONG` | `normalization` | 原始字符数超过 `max_query_chars`，或注入 `force_query_length` |
| `E_NUMERIC` | `intent` | 分数/置信度为 NaN 或 Infinity，或注入 `nan_confidence` |
| `E_DICT_VERSION` | `slots` | 词典版本不等于配置期望值，或注入 `dict_version` |
| `E_DICT_MISSING` | `slots` | 词典文件缺失/不可读，或注入 `missing_dictionary` |

`faults` 还支持 `overlap_slot`：注入候选进入正常重叠消解流程，被抑制时保留在诊断中。错误对象始终带 `query_id`、`stage`、`code`、`message`、`details`，不会返回部分成功结果。

## 回退链

主分类置信度低于 `policy.confidence_threshold` 后，固定依次尝试：

1. `history_prior`：历史点击类目经 `category_intent_map` 映射后的多数意图。
2. `channel_default`：`channel_defaults` 中的渠道默认意图。
3. `final_default`：`policy.final_default` 最终兜底。

`fallback_trace` 对每一级记录 `selected`、`rejected` 或 `skipped`；最终结果给出 `source` 与 `used_fallback_level`。主分类已可信时，后续级别显式标为 skipped。

## 版本指纹与失效

| 阶段 | 指纹依赖 |
| --- | --- |
| `normalization` | 规范化配置 |
| `intent` | 规范化指纹、分类配置（含类别/token 权重）、训练 JSONL 内容 |
| `slots` | 规范化指纹、槽位配置（含优先级）、词典内容与版本 |
| `decision` | intent 指纹、slots 指纹、阈值/回退策略配置 |

每次请求先读资产并计算当前指纹。缓存条目保存构建时各阶段指纹；只有当前指纹相同的阶段才复用。因此：

- 阈值变化：复用 `normalization/intent/slots`，重建 `decision`。
- 词典变化：复用 `normalization/intent`，重建 `slots/decision`。
- 训练样本变化：复用 `normalization/slots`，重建 `intent/decision`。
- 规范化配置变化：四阶段全部重建。

响应中的 `cache.stage_status` 会给出复用的旧指纹、变更组件或冷缓存原因；旧版本缓存不会被返回。

## 可复现实验

```bash
python3 -m qiu experiment --seed 17 --bootstrap-samples 200 --output artifacts/exp17-a.json
python3 -m qiu experiment --seed 17 --bootstrap-samples 200 --output artifacts/exp17-b.json
cmp artifacts/exp17-a.json artifacts/exp17-b.json   # 无输出即逐字节一致
python3 -m qiu experiment --seed 18 --bootstrap-samples 200 --output artifacts/exp18.json
```

摘要包含意图准确率、槽位 micro precision/recall/F1、回退触发率、意图混淆矩阵、回退级别计数、seed、配置/数据集/阶段指纹、run fingerprint 以及固定种子的 bootstrap 95% 区间。

## 实际验证结果

2026-11-20 在本工作区使用 Python 3.11.18、NumPy 2.4.6、pytest 9.2.0 执行 `./scripts/run_acceptance.sh`：

- pytest：`16 passed`
- 意图准确率：`0.903846153846`（47/52）
- 槽位精确率/召回率/F1：`1.0 / 1.0 / 1.0`（TP=66，FP=0，FN=0）
- 回退触发率：`0.134615384615`（7/52）
- 同 seed=17 的两次实验摘要 SHA-256 均为
  `f52a0532fb0e29fc21767397b66749423f0c477639f2595dfe58d34813cfa988`
- seed=18 摘要 SHA-256 为
  `ab25601875ce43297b0081fa33a280c80b2904a5e5b7e94104da7ea9851bcfa3`，与 seed=17 可区分
- 异常复现：`E_INVALID_LOCALE`、`E_EMPTY_QUERY`、`E_QUERY_TOO_LONG`、`E_NUMERIC`、`E_DICT_VERSION`、`E_DICT_MISSING` 均连续两次产生逐字节一致的错误 JSON
- 回退链演示实际命中 level 1/2/3；阶段失效演示实际观察到上述三类选择性重建

完整机器可读结果见 `artifacts/acceptance_summary.json`、`artifacts/experiment_seed17_run1.json`、`artifacts/experiment_seed18.json` 与 `artifacts/demo_report.json`。

更细的契约与设计说明见 `docs/architecture.md`。
