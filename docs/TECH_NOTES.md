# 技术说明与验证记录

## 1. 确定性与逐字节一致

- 序列化统一走 `crwbench.util.canonical_dumps`：`json.dumps(ensure_ascii=False, sort_keys=True,
  separators=(",",":"))` 加单个 LF，所有排序键、集合在落盘前转为有序列表。
- 数据生成器 `generator.py` 使用固定 `random.Random(seed)`，模板与权重表均为模块常量，
  分片按确定性轮转分配；同种子重复生成的 JSONL 与参考摘要完全一致。
- 分句在 NFKC 后的文本上进行，子句 `start/end` 为规范化文本字符偏移，断言
  `text[start:end] == segment.text`。
- 关键词命中按 `(位置, 长度降序, 词项)` 稳定排序并去重，否定/程度词在前置窗口内取最靠近命中的
  一个，保证同输入同配置输出逐字节一致。

## 2. 对数域打分

对类别 `c` 的命中，贡献为 `log(weight) + log(multiplier)`；按极性分桶：

```
score(c) = logsumexp(positive_contribs) - logsumexp(negative_contribs)
```

空桶以 `-1e12` 表示负无穷，纯否定或无命中分数为 0 并记录 `no_keyword_hit` /
`UNKNOWN_KEYWORD_TABLE` 依据。条款分数取五类最大值，按
`level_thresholds=[0.50, 1.30, 1.90]` 映射 0–3。测试断言“分数排序与等级排序一致”，
从机制上杜绝等级与分数方向矛盾。

## 3. 预算策略

判定单位是“条款级风险判定”；候选明细仍保留 (条款, 类别) 粒度的完整打分账本。

- 精确全量：判定所有有效条款；超全局预算或任一单类配额即淘汰。
- 关键词优先截断：仅判定有关键词命中的条款，按条款分数降序、ID 升序取前 `global_max_judgments`。
- 类别分档预算：按主导类别应用单类配额，再以全局预算兜底截断，是保证可行的回退策略。

选择优先级为精确 → 关键词 → 分档，淘汰原因写入 `strategy_decision.rejected_reasons`，
三方案判定数与配额合规性同时落盘，便于各档摘要比对。被判定条款保留精确阶段等级，
未判定条款为默认 0 级且依据为 `not_committed_under_<strategy>`，因此截断不会反转分数方向。

默认预算 `global_max_judgments=120`、五类配额各 34，配合演示数据规模：

| 档位 | 条款数（含边界样本） | 命中策略 | 判定数 |
| --- | --- | --- | --- |
| small | 27 | exact_full | 25 |
| medium | 123 | keyword_priority_truncated | 101 |
| large | 323 | category_tiered_budget | 120 |

## 4. 版本指纹与增量重算

- 输入指纹：分片文件 SHA256 与排序后 clause_id 列表的组合摘要。
- 配置指纹：YAML 原始结构的规范化 JSON 摘要；配置版本号 `config_version` 单独入检查点。
- 阶段指纹：每阶段产物的规范化摘要，管线启动时先在内存计算四阶段期望摘要，
  与检查点产物摘要比较，保留最长匹配前缀，`rollback_to` 删除其后的检查点与产物，
  仅重算失效阶段及下游。
- 否定窗口/程度权重变更最早影响 `features`；仅阈值变更最早影响 `scoring`；
  关键词权重变更同样从 `features` 失效。增量产物与干净工作目录全量产物按文件 SHA256
  逐字节比对，失败抛 `E_INCREMENTAL_MISMATCH` 并列出差异文件。

## 5. 检查点、停止恢复与并发

- 写入：临时文件 + `fsync` + `os.replace`，检查点 JSONL 采用“读全量→追加→整文件原子替换”，
  任意位置中断都不会留下半写记录；加载时跳过无法解析或缺摘要字段/摘要不匹配的损坏行。
- 停止：工作目录下 `STOP` 文件为信号，循环仅在一个不可拆分条款单元完成后检查，
  当前阶段不产生半检查点；`resume` 清除信号后从最近完整检查点续跑，最终产物与一次完整运行
  逐字节一致（测试 `TestStopResume` 覆盖）。
- 并发：所有发布经 `fcntl.flock` 排他锁保护，校验单调序号与阶段顺序；重复、回退、
  跨阶段错位分别返回 `E_STALE_CHECKPOINT` / `E_INVALID_STAGE`；
  序号回退或产物摘要不匹配返回 `E_STALE_SNAPSHOT` 且不覆盖原产物。
  多进程竞争测试 `tests/test_concurrency.py` 验证 4 进程并发只有 1 个成功连续发布。

## 6. 实际验证结果（本机 Python 3.11.18）

- `python -m unittest discover -s tests`：20 个用例全部通过（含并发与错误码）。
- `scripts/run_demo.py`：三档策略切换、重复运行摘要稳定、停止恢复逐字节一致、
  配置变更增量重算逐字节一致、HTTP 九个端点冒烟全部成功。
- 参考诊断 `config/reference_diagnostics.json` 与 `work/demo_fixed_summary.json`
  均可由本地脚本重建，无网络、GPU 或真实账号依赖。

## 7. 关键设计取舍

- 判定预算以条款为单位：若按 (条款,类别) 截断，会出现同一条款部分类别被评估而导致等级与
  分数方向矛盾；条款级单位保证“已判定=精确等级，未判定=显式默认低风险”。
- HTTP 仅绑定 `127.0.0.1`，`port=0` 时由操作系统分配空闲端口，避免环境端口冲突。
- YAML 单字标量（如 `不`、`no`）在 YAML 1.1 会被解析为布尔，配置加载器对否定词与关键词
  做字符串类型校验，默认配置全部加引号，错误时返回 `E_CONFIG`。
