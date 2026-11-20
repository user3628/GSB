# 技术说明与验收记录

## 1. 架构

```text
sampling_engine/
  models.py       统一请求、清单、四阶段常量
  manifest.py     v1/v2 双向编解码、温度换算、降级账本
  stages.py       postprocess → normalize → decode → summarize 数值内核
  metrics.py      float64 统计与 Decimal(60) 高精度审计
  engine.py       工作区注册表、指纹、失效、停止恢复、查询
  checkpoints.py  每请求检查点、连续序号、锁、归档与恢复
  storage.py      同目录原子替换、跨平台文件锁、快照校验
  service.py      仅标准库的 127.0.0.1 HTTP 服务
  data.py         固定种子演示数据生成
  cli.py          CLI 与完整/停止恢复演示
```

运行时只依赖 NumPy；HTTP 服务、JSON、SHA-256、文件锁、Decimal 高精度计算均来自
Python 标准库。没有模型权重、GPU、外部 API 或容器依赖。

## 2. 两版清单互操作

### v2

请求必须包含 `request_id`、`logits_path`、`temperature`、`top_k`、`top_p`、
`repetition_penalty`、`seed`、`steps`。`top_k` 可为 `null`；`prompt_tokens`
可选。清单必须声明全局 `vocab`，且 token 非空、唯一。

### v1

请求用 `temp_percent` 表示温度百分比，不包含顶层 `top_p`。导入规则：

```text
temperature = temp_percent / v1_temperature_scale
top_p       = v1_default_top_p            # 仅纯旧版缺省时
```

`v1_temperature_scale` 与 `v1_default_top_p` 来自 `config/config.json`。兼容旧字段
别名：`version`、`id`、`logits`、`temperature_percent`。

导出到 v1 时每个请求写入：

- `x_downgrade.omitted_fields = ["top_p"]`；
- 温度百分比量化前后值、scale 和 `exact`；
- `x_preserved_sampling = {temperature, top_p}`。

再次导入带扩展的 v1 时，会校验 `temp_percent` 与保留温度一致，并恢复精确温度
和 top-p。因此 v2 → v1 → v2 的 `temperature`、`top_k`、`top_p`、`seed` 保持
不变。纯旧版 v1 则明确记录 `percent_scaled` 与 `v1_default_inference` 换算明细，
通过 `/temperature` 查询可观测。

请求标识必须由清单提供；缺 `request_id`（以及兼容 `id`）返回
`E_INVALID_MANIFEST`。`stable_request_id()` 只提供给构造新清单的工具使用，不会
在导入时掩盖缺失字段。

## 3. 四阶段算法

### 3.1 postprocess

1. 校验温度有限、`0 < temperature <= max_temperature`；否则
   `E_INVALID_PARAM`，上下文含 `request_id`、`parameter=temperature`、上限。
2. 读取二维 `[steps, vocab]` float64 张量并校验宽度、步数。
3. NaN、`+Inf`、全 `-Inf` 返回 `E_NUMERIC`，附请求标识、step 索引、阶段和内容摘要。
   单独的 `-Inf` 允许作为 mask；全负无穷没有可定义分布，因此报错。
4. 对 `prompt_tokens` 去重并升序应用重复惩罚：正 logit 除以 penalty，非正
   logit 乘以 penalty。该阶段是不可拆分请求单元的一部分，因此重复上下文来自
   清单中静态的 `prompt_tokens`，不依赖尚未产生的采样历史。
5. 应用温度缩放。并列最大值的 token 列表按 token_id 升序记录。
6. 产物为 `.npy`；逻辑摘要包含 little-endian float64 规范字节、形状和并列元数据。

### 3.2 normalize

每一步执行：

```text
m = max(logits)
logZ = m + log(sum(exp(logits - m)))
log_probs = logits - logZ
```

全过程不直接对原始 logits 求 `exp`，避免上溢。候选按 `(-log_prob, token_id)` 的
`lexsort` 顺序排序，不使用哈希迭代顺序。先做 top-k，再做 top-p：

- top-p 累积在完整分布概率上进行；
- 第一个使累积值 `>= top_p` 的候选被包含；
- 多个完全相等概率跨越阈值时，token_id 较小的先入选，因此集合唯一；
- 截断集合随后在对数域重新 log-sum-exp 归一化。

截断集合为空、概率和为零/非有限时返回 `E_NUMERIC`，含 step 索引和诊断。

### 3.3 decode

固定执行 `numpy.random.Generator(PCG64(seed))`。每一步对完整词表生成固定数量
Gumbel 噪声，在归一化 log probability 上做 Gumbel-max。被排除 token 的
log probability 为 `-Inf`，因此不可能被选中；精确并列由最低 token_id 获胜。
产物保存 token、token 文本、逐步 log probability、熵、候选数量和 Gumbel 分数。

### 3.4 summarize

计算总/平均 NLL、perplexity、平均熵和候选数量统计。另以
`Decimal(precision=60)` 对已存储的 float64 后处理 logits 和截断候选重新执行
log-sum-exp，得到本地高精度参考，并输出绝对/相对误差和有效位数。语料级指标先
累加 token 级 float64 值，再与 Decimal 参考总和比较。

## 4. 版本指纹与增量失效

请求指纹由 logits 文件内容 SHA-256、温度、top-k、top-p、重复惩罚、种子、步数、
结构版本、词表摘要和 prompt 构成。阶段输入指纹为链式结构：

| 变化项 | 最早受影响阶段 |
| --- | --- |
| logits 内容、温度、重复惩罚、结构版本、词表、prompt | `postprocess` |
| top-k、top-p | `normalize` |
| seed、steps | `decode` |
| summarize 算法版本 | `summarize` |

每阶段检查点保存 `input_fingerprint`、`config_version`、`artifact_digest` 和
算法版本。运行时逐阶段比较：

- 上游未变且检查点完整：直接复用；
- 某阶段指纹变化：从该阶段起归档并重建全部下游；
- 未受影响请求不运行，已有检查点与序号保持；
- 失效记录包含 `earliest_stage`、`changed_fields`、旧/新阶段指纹、归档序号。

`/status`、`/fingerprints` 和 CLI `status` 均可观察这些数据。旧产物移到
`requests/<id>/history/`，不会再次作为当前产物复用。

## 5. 检查点、锁与恢复

当前检查点位于 `requests/<id>/checkpoints/<10位序号>.json`，包含：

```json
{
  "request_id": "r2_small",
  "stage": "postprocess",
  "sequence": 1,
  "input_fingerprint": "...",
  "config_version": "...",
  "artifact_digest": "...",
  "artifact_file_digest": "...",
  "algorithm_version": "postprocess-v1"
}
```

发布顺序：在请求锁内校验请求、阶段、前置阶段和序号 → 将 staging 产物
`os.replace` 到最终路径 → 同目录写临时检查点、fsync → `os.replace` 检查点 →
fsync 目录。崩溃最多留下未被检查点引用的孤立 staging/artifact，不会留下半写
记录。读取时同时校验文件 SHA-256 与逻辑摘要。

规则：

- 新序号必须等于该请求历史/当前最大序号 + 1；
- 重复阶段、落后或跳跃序号返回 `E_STALE_CHECKPOINT`；
- 目标请求与记录 `request_id` 不一致返回 `E_STALE_CHECKPOINT`；
- 缺少前置阶段返回 `E_INVALID_STAGE` 并附 `missing_stage`；
- 文件锁在 POSIX 使用 `flock`，Windows 使用 `msvcrt.locking`，并叠加进程内线程锁。

失效时先把状态中的 pending 失效标记原子落盘，再归档检查点与产物，最后清标记。
恢复会完成 pending 归档，因此不会只失效一部分。历史与当前序号联合检查保持连续。

停止信号只在请求之间检查；当前请求视为不可拆分单元并完成全部四阶段。恢复时从
首个缺失阶段继续；decode 和 summarize 不会保存半条 token 序列。完整运行与停止
恢复均按规范化产物和业务字段重新计算摘要，不包含时间戳或执行轨迹。

## 6. 快照一致性

- 每个检查点校验产物文件摘要与逻辑摘要，不匹配返回 `E_SNAPSHOT_CONFLICT`。
- 工作区 `workspace_meta.json` 含单调 generation；工作区外 guard 记录同
  `workspace_id` 的最高 generation，检测整体快照版本回退。
- `snapshot-export` 输出文件摘要、状态文件和 `snapshot_digest`；
  `snapshot-validate` 检测 payload 摘要不匹配以及 generation 回退。

## 7. 本地 HTTP

配置示例为 `config/config.json`。服务拒绝非 loopback host；端口从配置读取，测试
可用端口 0 由系统分配。所有业务错误返回稳定 JSON：

```json
{"ok": false, "code": "E_NUMERIC", "message": "...", "context": {}}
```

## 8. Windows 原子替换

- 临时文件与目标文件始终位于同一目录，保证同卷重命名。
- 使用 `os.replace`，Windows 上覆盖已有目标也是原子替换语义。
- 写入后先 `fsync` 文件；目录 `fsync` 在 Windows 自动跳过。
- 锁文件使用 `msvcrt.locking`；解析失败或超时返回 `E_CONFLICT`，不会静默继续。
- 旧检查点先归档到同请求目录树内，再发布新版本，避免跨卷移动。

## 9. 演示数据与覆盖

固定种子 `20261121`，NumPy `default_rng` 生成：

| 文件 | 形状 | 特点 |
| --- | --- | --- |
| `r2_small.npy` | 7 × 24 | 小词表、中英混合 |
| `r2_tie.npy` | 6 × 24 | 每步 token 3/12 并列最大 |
| `r2_large.npy` | 10 × 512 | 大词表 |
| `r2_nan.npy` | 5 × 24 | step 2 token 7 为 NaN |
| `r2_inf.npy` | 5 × 24 | step 0 token 4 为 `+Inf` |
| `r1_small.npy` | 8 × 24 | v1 旧清单请求 |
| `r1_large.npy` | 6 × 512 | v1 大词表请求 |

完整 SHA-256、形状和异常标记见 `data/generator_metadata.json`。固定数据已保存在
`data/`，无需联网重新生成。

## 10. 实际验收结果

执行环境：Python 3.14.4，NumPy 2.5.5，pytest 9.2.0，Linux 本地文件系统。

```text
$ pytest -q
43 passed
```

完整演示：

```text
result_digest = 164077104f4d8ce13657bb1ba4c8e937c3759f4db0614e7e8f24a5d1dc17ca0d
successful_requests = 5
failed_requests = 2
token_count = 37
stop_resume_equal = true
checkpoint_sequences_contiguous = true
```

成功请求 token：

| 请求 | token |
| --- | --- |
| `r2_small` | `[13, 21, 14, 16, 13, 9, 17]` |
| `r2_tie` | `[12, 0, 12, 3, 3, 3]` |
| `r1_small` | `[2, 9, 11, 4, 16, 5, 2, 1]` |
| `r2_large` | `[22, 392, 156, 510, 280, 360, 117, 14, 486, 276]` |
| `r1_large` | `[433, 328, 451, 62, 243, 142]` |

异常请求：

| 请求 | 错误 | step | 原因 |
| --- | --- | --- | --- |
| `r2_nan` | `E_NUMERIC` | 2 | `nan` |
| `r2_inf` | `E_NUMERIC` | 0 | `positive_infinity` |

语料级指标：mean NLL `2.1326367605299152`，perplexity `8.437084078046725`，
mean entropy `2.207886800591538`，对 Decimal(60) 参考均达到 15 位有效数字。

测试还覆盖：

- v2 → v1 → v2 精确恢复温度/top-k/top-p/seed；
- 纯旧版 v1 默认 top-p 推导与换算明细；
- 并列最大值、等值概率 top-p 跨越阈值、log-sum-exp 上溢规避；
- NaN/`+Inf`/全负无穷、非法温度、缺字段/缺请求标识的错误码；
- 序号缺口、重复发布、跨请求发布、缺前置阶段；
- top-p、seed、logits 内容、结构版本四类最早失效阶段；
- 完整运行与停止恢复摘要一致、序号连续；
- 产物摘要篡改与快照版本回退检测；
- HTTP 导入、运行、停止、恢复、查询、转换和错误响应。

## 11. 明确的设计边界

- 重复惩罚使用清单中的静态 `prompt_tokens`，因为 postprocess 必须在 decode 前
  独立完成并可复用；不把采样历史反馈到已完成的 postprocess 张量。
- 单独 `-Inf` 视为合法 mask；只有 NaN、`+Inf` 和整行全 `-Inf` 是数值错误。
- Gumbel 随机流和浮点结果以固定 NumPy 版本为准；`requirements.txt` 对 Python
  3.10–3.13 与 3.14+ 分别固定兼容版本。
- HTTP 服务面向本地单机演示；跨进程发布保护由工作区文件锁和检查点序号完成，
  不提供公网认证层。
