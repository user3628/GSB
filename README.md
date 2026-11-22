# ShotCheck：本地确定性文生视频分镜语义核查管线

ShotCheck 是一个从零实现的本地文生视频分镜脚本语义核查项目。它只读取固定随机种子生成的本地 JSONL、JSON 主体表和同源参考标注，不调用生成模型、GPU、外部数据集或网络服务。

项目按四个阶段产出内容寻址产物：

1. `normalize`：提示词规范化、语义单元切分与单元类型标注。
2. `duration`：时长短语解析、毫秒归一、来源和精度记录。
3. `coverage`：主体/场景/否定窗口匹配、覆盖率、冲突扣减和三级结论。
4. `strategy`：依据规模与检查深度预算选择解析策略，并审计三种策略的一致性。

每阶段产物均通过原子检查点发布；支持内容指纹失效、优雅停止恢复、真实文件锁和稳定业务错误码。

## 快速开始

环境要求：Python 3.10+、可导入的 NumPy、POSIX `fcntl.flock`（Linux、macOS 或 WSL）。

```bash
python3 -m shotcheck.cli doctor
python3 -m shotcheck.cli generate --root runtime --seed 20261123
python3 -m shotcheck.cli run --workspace runtime/small
python3 -m shotcheck.cli run --workspace runtime/large
python3 -m shotcheck.cli verify --root runtime --seed 20261123
```

也可以直接使用：

```bash
make demo       # doctor + generate + 两档 run + verify
make test       # 25 个单元/集成测试
make verify
make serve      # http://127.0.0.1:18765
```

所有输入、检查点、对象产物、导出结果和验证报告都写到指定工作区目录内。默认交付工作区为 `runtime/`。

## 当前实测结果

本次执行环境：Python `3.14.4`、NumPy `2.2.1`、`fcntl.flock` 文件锁。

测试套件：

```text
Ran 25 tests ... OK
```

完整验证报告：`runtime/verification/report.json` 与 `runtime/verification/report.md`，总体结果为 `PASS`。

| 档位 | Shot 数 | 结论分布 | 错误码分布 | 单元类型分布 | 选中策略 |
| --- | ---: | --- | --- | --- | --- |
| small | 14 | pass 3 / review 2 / rejected 9 | `E_DURATION_PARSE` 3, `E_EMPTY_PROMPT` 2, `E_FORBIDDEN_SUBJECT` 1, `E_SEMANTIC_CONFLICT` 2 | subject 13 / scene 14 / constraint 3 / duration 12 | `exhaustive` |
| large | 51 | pass 39 / review 3 / rejected 9 | `E_DURATION_PARSE` 3, `E_EMPTY_PROMPT` 2, `E_FORBIDDEN_SUBJECT` 1, `E_SEMANTIC_CONFLICT` 2 | subject 50 / scene 87 / constraint 20 / duration 49 | `chunk_incremental` |

增量与恢复核对：

- 把 `large-001` 的时长从 `3500ms` 改为 `3200ms` 后，最早失效阶段为 `duration`，`normalize` 复用；其余 50 个 shot 的 outcome 逐字节一致。
- 增量重算的完整结果与“修改后从零全量运行”SHA-256 相同：`ec4aab1417319207767e5d4f307d6a700194c4fbb28726f7dc47af5afcef0678`。
- 在 `coverage` 阶段处理 2 个 shot 后停止，未发布第 3/4 阶段检查点或最终结果；恢复复用 `normalize/duration`，最终结果与基线逐字节一致。
- 两个真实进程同时发布同一阶段时只有一个成功，另一个返回 `E_STALE_CHECKPOINT`；重复发布、跨阶段错位和非法跳阶也均被拒绝。
- 三种策略在同一输入上给出相同 decision digest。

## 目录结构

```text
shotcheck/
  text.py             # NFKC、空白折叠、切分、单元分类
  duration.py         # 中文量词、阿拉伯单位、帧数、错误检测
  matching.py         # 最长匹配、否定窗口、冲突、覆盖率与结论
  strategies.py       # 三策略选择、候选打分、执行与一致性
  checkpoints.py      # 原子写入、fcntl 锁、序号与上游血缘
  pipeline.py         # 四阶段编排、失效识别、停止恢复、导出
  generator.py        # 固定种子两档样本与同源参考标注
  verification.py     # 全量、增量、恢复、锁与错误码验证
  server.py           # 本地 JSON HTTP 接口
  cli.py              # generate/run/verify/status/serve/doctor
tests/                # 25 个单元与集成测试
runtime/              # 实测样本、检查点、产物、报告
```

## 输入契约

每个档位工作区包含：

```text
runtime/<tier>/
  inputs/
    scripts.jsonl
    subjects.json
    reference.json
    config.json
    manifest.json
  checkpoints/
  objects/
  exports/
  locks/
```

`scripts.jsonl` 每条记录：

```json
{
  "shot_id": "large-001",
  "prompt": "黄色雨衣女孩站在月台边，雨夜中霓虹灯闪烁 and ...",
  "duration_phrase": "约三秒半",
  "must_subjects": ["雨衣女孩"],
  "must_scenes": ["雨夜", "霓虹灯", "wet asphalt"],
  "negative_terms": ["不要"]
}
```

`subjects.json`：

```json
{
  "schema_version": 1,
  "shots": {
    "large-001": {
      "required_subjects": ["雨衣女孩"],
      "forbidden_subjects": []
    }
  }
}
```

`reference.json` 是唯一真值来源，逐 shot 声明：

- 归一化提示词与语义单元切分：`text/start/end/type`。
- 时长目标：`target_ms/source/precision_digits`；预期失败时为目标 `null`。
- 覆盖率目标区间：主体、场景、时长一致率和综合得分的 `min/max`。
- 预期覆盖率、预期错误码和预期三级结论。

## 阶段一：规范化与切分

处理顺序：

1. Unicode `NFKC`。
2. 全角/半角统一。
3. 所有制表、换行、全角空格和连续空白折叠为单个半角空格。
4. 去除首尾空白。

切分边界包括中文句读、逗号/顿号/分号、`。！？`，以及显式连接词 `并且/同时/但是/然而/而且/此外/以及/接着/然后/随后` 和英文 `and/then/while/but/also`。

所有 `start/end` 都是相对于归一化提示词的半开区间 `[start,end)`，且满足：

```text
normalized_prompt[start:end] == unit.text
```

单元类型固定为：

- `subject_description`
- `scene_description`
- `constraint`
- `duration`

分类只使用输入中显式声明的主体、场景、否定词和时长表达式提示；无法进一步识别的描述性单元归为 `scene_description`，不会凭空制造关键词覆盖。空提示词或没有 `subject_description` 时输出 `E_EMPTY_PROMPT`，带 `shot_id`、`split_offset` 和 `split_position`。该 shot 不会进入阶段一 `shots` 映射，不产生半成品 shot 记录。

## 阶段二：时长归一

支持三类表达式：

| 类型 | 示例 | 结果 |
| --- | --- | --- |
| 中文数词量词 | `约三秒半`、`十二秒`、`一点五秒` | `3500/12000/1500ms`，`cn_quantifier` |
| 阿拉伯数字加单位 | `2.5s`、`1500ms`、`2分钟` | 毫秒整数，`arabic_unit` |
| 直接帧数 | `八帧`、`10 frames` | 按配置帧率换算，默认 25fps；`八帧 = 320ms`，`frames` |

同时记录源表达式区间、解析来源、原文小数/`半` 精度位数和全部短语。以下情况返回 `E_DURATION_PARSE`：

- 未知单位。
- 无可用时长表达式。
- 时长为零或负数。
- 同一 shot 的两个短语归一值不同。

冲突时不会静默采用第一个值，错误携带 `shot_id`、`phrase_offset`、所有 `phrase_offsets`、原因和已解析值。

## 阶段三：匹配、冲突与结论

### 最长匹配

主体、场景和否定词均在语义单元内做 leftmost-longest、非重叠匹配。命中的主体记录全局半开字符区间、`unit_id` 和单元类型。

### 否定窗口

对每个否定词命中区间 `[term_start,term_end)`，配置窗口为：

```text
[term_end, min(prompt_length, term_end + negative_window_chars))
```

窗口默认 18 字符。落在窗口内且被主体表要求出现的主体会被标记为冲突：

- 输出冲突主体。
- 输出主体区间与否定词区间两处偏移。
- 输出窗口和原因。
- 该主体不计入覆盖率；冲突 shot 强制 `rejected`，不会判为通过。

同时检查主体表中 `forbidden_subjects`。禁入主体出现时输出 `E_FORBIDDEN_SUBJECT`；同一主体同时被声明为必须和禁入时输出 `E_SEMANTIC_CONFLICT`。

### 覆盖率

覆盖率只统计能落入参考标注对应类型单元内的命中：

```text
must_subject_coverage = 未被冲突的主体数 / 主体表声明主体数
scene_coverage        = 参考 scene 单元内的场景命中数 / 场景关键词数
duration_consistency  = 解析时长毫秒值等于参考 target_ms ? 1 : 0
```

默认权重：

```json
{
  "must_subject_coverage": 0.45,
  "scene_coverage": 0.25,
  "duration_consistency": 0.30
}
```

综合分只对适用的组件重新归一化，量化到 6 位小数。分母为零的组件显式标记 `applicable=false`、`value=null`，并统计到 `na_component_distribution`。缺失参考目标不会推断为 0 或通过，而是记录 `E_REFERENCE_MISSING` 并进入复核。

三级结论：

- `pass`：所有适用指标和综合分都位于参考目标区间内。
- `review`：目标区间未满足但仍达到复核阈值，或证据不足/参考缺失。
- `rejected`：语义冲突、禁入主体、阶段错误，或综合分低于复核阈值。

## 阶段四：规模自适应策略

策略指标来自输入显式数据：

- shot 数。
- 提示词总字符数。
- 否定约束词数。
- 关键词数。
- 可配置 `check_depth_budget`。

候选策略：

1. `exhaustive`：逐单元全量匹配。
2. `keyword_priority_truncated`：关键词优先、截断无关单元；仅面向较大规模，小规模显式不可选。
3. `chunk_incremental`：按 shot 分块增量执行。

每个候选记录 `estimated_cost`、`budget_fit`、`quality_score`、`scale_fit`、总分、是否合格、淘汰原因。最终记录选择理由、候选打分明细和被淘汰原因。

三种策略只改变扫描方式和执行开销，不改变判定语义。阶段四会执行全部三种策略，并比较 canonical JSON decision digest；任何差异都会使 `all_strategies_equal=false` 并使验证失败。

## 检查点、失效与并发

每阶段检查点包含：

```json
{
  "stage": "coverage",
  "stage_index": 3,
  "sequence": 1,
  "input_fingerprint": "...",
  "config_version": "shotcheck-1.0.0:shotcheck-config-1",
  "config_fingerprint": "...",
  "artifact_digest": "...",
  "artifact_ref": "03-coverage-....json",
  "upstream": [
    {
      "stage": "duration",
      "sequence": 1,
      "input_fingerprint": "...",
      "artifact_digest": "..."
    }
  ]
}
```

写入保证：

- 产物先写内容寻址对象，再原子替换检查点。
- 临时文件位于同一目录，`fsync` 后 `os.replace`。
- 中断最多留下完整临时文件，不会留下半写 JSON 目标文件。
- `fcntl.flock` 保护检查点发布，真实跨进程互斥。
- 阶段内序号必须从 0 连续推进到 1、2、3……；落后、重复、跨阶段错位返回 `E_STALE_CHECKPOINT`。
- 缺失前置阶段或非法跳阶返回 `E_INVALID_STAGE`。
- 发布失败不修改已存在检查点。

失效矩阵：

| 输入变化 | 最早失效阶段 |
| --- | --- |
| 提示词 | `normalize` |
| 时长短语 | `duration` |
| 主体表 | `normalize`（同时影响切分类词与覆盖） |
| 否定词表 | `normalize`（同时影响单元分类与覆盖） |
| 权重/窗口/复核阈值 | `coverage` |
| 检查深度预算 | `strategy` |
| 切分配置 | `normalize` |

收到停止请求时，管线完成当前不可拆分的 shot 后抛出优雅停止，不发布当前阶段检查点。恢复时从最早缺失/失效阶段继续，最终 `exports/result.json` 与一次从零完整运行逐字节一致。

## 稳定错误码

| 错误码 | 定位信息 | 场景 |
| --- | --- | --- |
| `E_EMPTY_PROMPT` | `shot_id`, `split_offset`, `split_position` | 空提示词或无主体描述单元 |
| `E_DURATION_PARSE` | `shot_id`, `phrase_offset`, `phrase_offsets` | 未知单位、零/负时长、冲突短语、无表达式 |
| `E_SEMANTIC_CONFLICT` | `shot_id`, `subject`, `required_span`, `negative_span`, `window` | 必须主体落在否定窗口，或必须/禁入声明冲突 |
| `E_FORBIDDEN_SUBJECT` | `shot_id`, `subject`, `span` | 禁入主体出现在提示词 |
| `E_REFERENCE_MISSING` | `shot_id`, `metric`, `stage` | 适用指标缺失真值目标 |
| `E_STALE_CHECKPOINT` | `stage`, `expected_sequence`, `current_sequence`, `reason` | 落后、重复、错位、损坏或产物摘要不匹配 |
| `E_INVALID_STAGE` | `stage`, `reason` | 缺失前置阶段、非法跳阶、未知阶段 |

## 本地 HTTP 接口

启动：

```bash
python3 -m shotcheck.cli serve --workspace runtime/large --host 127.0.0.1 --port 18765
```

端口可通过 `--port` 改为任意可用本地端口；`--port 0` 会由操作系统分配临时端口。接口只监听本机默认地址，不依赖外部网络。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/status` | 输入摘要、四阶段检查点状态 |
| GET | `/result` | 读取最终确定性结果 |
| POST | `/run` | 启动管线；可传 `force`、`stop_after_stage`、`stop_after_shots` |
| POST | `/stop` | 请求在当前 shot 完成后停止 |
| POST | `/generate` | 重新生成 `small` 或 `large` 输入 |

示例：

```bash
curl -s http://127.0.0.1:18765/health
curl -s -X POST http://127.0.0.1:18765/run -H 'Content-Type: application/json' -d '{}'
curl -s http://127.0.0.1:18765/result
curl -s -X POST http://127.0.0.1:18765/stop -H 'Content-Type: application/json' -d '{}'
```

## 验证工具覆盖项

`shotcheck verify` 自动执行：

1. 按固定种子重新生成两档样本。
2. 对每个 shot 比对参考切分文本、类型、字符偏移。
3. 比对时长毫秒值、来源与精度。
4. 比对主体覆盖率、场景覆盖率、时长一致率、综合分和 N/A 组件。
5. 比对三级结论和完整错误码集合。
6. 检查错误码定位字段和错误码数量分布。
7. 检查单元类型数量分布、时长来源分布和结论分布。
8. 运行三种策略并比较 canonical decision digest。
9. 两个真实进程竞争发布，核验 `E_STALE_CHECKPOINT`。
10. 核验重复发布、非法跳阶、上游血缘错位。
11. 修改单个 shot 时长并增量重算，比较未受影响 shot 与全量结果。
12. 中途停止后恢复，与从零运行结果逐字节比较。
13. 对提示词、时长、主体表、否定词、权重、预算和切分配置执行指纹矩阵检查。

报告是确定性 JSON + Markdown：

```text
runtime/verification/report.json
runtime/verification/report.md
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖 Unicode 归一化、连接词切分、中文/阿拉伯/帧数时长、未知单位、零/负值、时长冲突、最长匹配、否定窗口冲突、禁入主体、N/A、目标区间复核/驳回、真实文件锁、并发发布、增量失效、停止恢复、两档参考真值和 HTTP 接口。

## 执行限制

- 不使用 Docker 或其他容器。
- 不访问外部数据集、模型或未授权服务。
- 所有样本和结果均由本地固定种子生成。
- 依赖仅使用 Python 标准库；NumPy 作为环境可导入性检查依赖。
