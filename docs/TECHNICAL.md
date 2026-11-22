# 合同扫描件条款区域检测与引证链冲突消解 — 技术说明

## 1. 设计目标与约束

- 纯 Python 3.10+ 与 NumPy、标准库实现；无 GPU、无网络、无容器、无外部数据集。
- 全部产物以规范化 JSON（键排序、无空白、UTF-8、`ensure_ascii=False`）落盘，同一输入与配置逐字节一致。
- 四阶段单向证据链：`detect -> parse -> normalize -> resolve`，证据清单可回导重建同一裁决。
- 定点整数作为业务比较域；浮点仅出现在固定化之前的候选扰动与对数求值中，落盘值均为整数/字符串分数。

## 2. 模块结构

| 模块 | 职责 |
| --- | --- |
| `contract_chain/codec.py` | 规范化序列化、SHA-256 摘要、半入整数定点化、原子 `os.replace` 写 |
| `contract_chain/config.py` | 四阶段不可变配置、阶段配置摘要、变更影响阶段映射 |
| `contract_chain/samplegen.py` | 固定种子 NumPy RNG 的多语种版面样本、配置与证据模板 |
| `contract_chain/detect.py` | 候选框扰动生成、像素面积 IoU、同类别贪心一对一匹配、背景诊断 |
| `contract_chain/parse.py` | NFKC/全半角/空白折叠、原始偏移映射、中英文句式多候选定位 |
| `contract_chain/normalize.py` | 月区间、百分比、生效月序号的定点归一与单位/区间错误 |
| `contract_chain/resolve.py` | 同合同同定位冲突分组、定点对数可信度、确定性仲裁 |
| `contract_chain/evidence.py` | 五环证据链、摘要校验、缺失环 `E_EVIDENCE_BROKEN`、回导重建 |
| `contract_chain/checkpoints.py` | OS 文件锁、原子检查点、单调序号、停止标记、失效截断 |
| `contract_chain/pipeline.py` | 指纹链、增量失效传播、全量逐字节校验、停止恢复 |
| `contract_chain/cli.py` / `httpapi.py` | 本地命令行与 HTTP 演示接口 |

## 3. 确定性策略

1. 候选框 RNG 使用 `(seed, page_index, region_index)` 派生的独立 `np.random.default_rng`，集合按 `candidate_id` 排序。
2. IoU 匹配候选对按 `(-iou, candidate_id, region_id)` 稳定排序，先到先得，已分配真值/候选均不可复用。
3. 所有集合输出在落盘前按稳定键排序；分组、评分、降级均不依赖输入分片到达顺序。
4. 定点换算通过 `Decimal.ROUND_HALF_UP` 实现，避免 Python 浮点与 banker rounding。
5. 可信度评分采用 `log(prior)+log(lang)+log(conf)+log(freshness)`，再量化到 `1e9` 整数刻度；新鲜度为精确分数 `2**(-age/half_life)`。
6. 写入均为临时文件 + `fsync` + `os.replace`，中断只可能留下旧完整文件或新完整文件。

## 4. 阶段产物与错误码

- `detect.json`：候选框、匹配、IoU/置信度定点值、未匹配背景诊断与统计。
- `parse.json`：区域绑定、归一化文本、`span_norm/span_raw` 偏移、多候选定位、首选标注、`E_REF_PARSE`。
- `normalize.json`：三类定点要素、换算因子与精度、保留区域坐标、`E_UNIT_UNRESOLVED`。
- `resolve.json`：冲突组、唯一胜出引证、组内评分分量、降级引证与理由。
- `evidence_manifest.json`：每条引证的五环证据链与清单摘要。
- 运行保护错误：`E_STALE_CHECKPOINT`、`E_INVALID_STAGE`、`E_INCREMENTAL_MISMATCH`。

## 5. 证据链五环与回导规则

每条记录必须包含：`region_coordinate`、`raw_text_span`、`location_parse`、`term_normalization`、`resolution`。
导入时先检查五环非空与区域坐标，再复算清单摘要；任一缺失直接返回 `E_EVIDENCE_BROKEN` 及缺失环名称，
禁止用默认值或推断补全。回导仅依据清单内容重建采纳/降级/解析排除结论，并与 `resolve.json` 对照。

## 6. 检查点、停止与增量

- 检查点记录字段：`seq`、`stage`、`input_digest`、`config_digest`、`product_digest`、`fingerprint`。
- 发布在 `fcntl.flock`（Windows 回退 `msvcrt`）锁内比较序号，只允许 `next_seq` 连续推进。
- 停止标记为工作区 `STOP` 文件；解析/归一循环在每个不可拆分引证单元边界检查，完成当前单元后抛 `StopRequested`。
- 指纹为链式摘要：每阶段输入摘要 + 该阶段配置摘要 + 上游指纹。最早变化阶段及其下游全部截断重建，上游复用。
- 增量后在临时工作区执行一次从零全量运行，逐字节比较四个阶段产物；首个差异携带阶段与 `ref_id`，序号不连续同样报错。

## 7. 本地接口

### CLI

```bash
python -m contract_chain.cli --workspace ./cc_ws init
python -m contract_chain.cli --workspace ./cc_ws run|full|resume|stop|status
python -m contract_chain.cli --workspace ./cc_ws demotions|evidence|reconstruct
python -m contract_chain.cli --workspace ./cc_ws incremental --sample new.json --config new_config.json
python -m contract_chain.cli --workspace ./cc_ws export exported.json
python -m contract_chain.cli --workspace ./cc_ws http --host 127.0.0.1 --port 8765
```

### HTTP

`GET /status /fingerprints /demotions /evidence /reconstruct /stats/conflicts`；
`POST /sample/import /evidence/import /config /run /run/full /run/incremental /resume /stop /evidence/export`。

## 8. 验证命令

```bash
python -c "import numpy"
python -m unittest discover -s tests -v
python demo.py
```

18 个单元/集成测试覆盖：IoU 一对一匹配与背景保留、字节确定性、多句式定位与解析错误、
定点归一与单位错误、唯一胜出与顺序无关、证据回导与断链、原子检查点/停止恢复/真实文件锁、
增量失效与逐字节 parity、HTTP 接口。
