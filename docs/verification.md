# 实际验证记录

验证日期：2026-11-22  
Python：3.14.4  
NumPy：2.5.5  
pytest：9.2.0  
运行方式：本地虚拟环境，无网络请求、无 GPU、无容器。

## 自动化测试

```text
$ .venv/bin/python -m pytest
....................................                                     [100%]
40 passed in 4.34s
```

覆盖范围：

- NFKC、空白折叠、全半角、原始偏移映射；
- 中文数字/量词、英文数字缩写、千分位/小数点歧义；
- 万/亿、平方公里、平方英里、单位缺失/未知、负值；
- 年月区间、月序号、开合区间和倒置区间；
- 跨单位精确相等、并发冲突、可信度并列仲裁、顺序无关；
- 证据清单重导入、逐字节重建、缺环断点；
- v1 千分位、自由单位、因子补全、单月展开、v1 回导和非法单位；
- 配置变更的最早失效阶段和上游检查点复用；
- 停止/恢复、外部停止请求、检查点原子包与单调序号；
- 检查点内容损坏时当前及下游阶段级联失效重建；
- 两次运行、批量与逐实体汇总一致性；
- CLI 批处理、查询、清单校验和结构化错误退出码；
- 固定种子生成器逐字节复现。

## 样例规模

```text
claims: 29
entities: 7
corpus shards: 3
generator seed: 20261122
```

样本包含跨语言人口/面积/年份/任期冲突与等价声明、`万`/`亿`/`M`/`million`/`sq mi`、全角数字、缺失/未知单位、负人口、倒置任期、歧义分隔符、空数值、并列可信度和单月 v1 清单。

## 批量指标

本次证据清单 SHA-256：`8bae0e5727cb7e70211932ee237ff5d09235fe25613e50e286d648b2acf36074`。

```json
{
  "conflict_identification_rate": "1.000000",
  "decision_accuracy": "1.000000",
  "evidence_rebuild_rate": "1.000000",
  "expected_conflict_group_count": 5,
  "identified_conflict_group_count": 5,
  "expected_decision_count": 12,
  "correct_decision_count": 12,
  "parse_error_count": 2,
  "accepted_claim_count": 12,
  "demoted_claim_count": 17
}
```

## 停止与恢复

在 7 个实体中停止于前 2 个：

```text
stopped status: stopped
next_entity_id: ENT-CEDAR
resumed status: complete
manifest byte equal: true
entity results equal: true
summary equal: true
checkpoint count: 28
checkpoint seq: [1, 2, ..., 28] (strictly monotonic)
resume reuse: stage1=2, stage2=2, stage3=2, stage4=2
```

## 增量失效

将 `language_weights.en` 从 `0.95` 改为 `0.51` 后：

```text
earliest_affected_stage: 3
invalidated_stages: [3, 4]
reason_codes: [CONFIDENCE_OR_ARBITRATION_CONFIG_CHANGED]
reuse_counts: stage1=7, stage2=7, stage3=0, stage4=0
```

阶段 1/2 指纹保持不变，阶段 3 指纹改变；上游检查点全部复用。

## 证据链校验

```text
manifest validation: ok=true
rebuilt manifest bytes equal: true
removed stage2:
  code=E_EVIDENCE_BROKEN
  claim_id=<actual claim>
  breakpoint=stage2
tampered link target digest:
  code=E_EVIDENCE_BROKEN
  breakpoint=links
```

## v1 迁移

```text
records: 5
validation: ok=true
v1 export byte equal to source: true
downgraded records: 5
L002 factor: 100000000
L003 factor: 2.589988110336, precision: 12
L005 value: 2021-04..2021-04[closed,closed]
L005 downgrade: V1_SINGLE_MONTH_EXPANDED
unmappable unit `furlongs`: E_INVALID_FORMAT, claim_id=L900
configured mapping `furlongs -> sq_mi`: migration succeeds
```

## 复算与一致性

```text
repeat manifest byte equal: true
repeat summary byte equal: true
batch vs per-entity aggregate equal: true
```

机器可读报告：`artifacts/verification_report.json`。示例批量输出：`artifacts/evidence_manifest.json` 与 `artifacts/batch_summary.json`。
