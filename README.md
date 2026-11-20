# DetDiff 本地确定性扩散管线工作台

DetDiff 是一个完全本地、可复现、可诊断的“文生图扩散管线”工作台。它保留了真实扩散管线常见的四阶段结构，但用固定哈希嵌入、解析调度、SHAKE 伪噪声和线性插值代替模型与 GPU，因此不需要账号、网络、在线服务或预训练权重。

核心能力：

- 16 维单位文本哈希嵌入与逐步无条件分支权重调度。
- 确定性伪噪声采样、每步均值/方差轨迹摘要、显式钳制量化。
- 参考图融合的图生图路径；缺失、损坏或尺寸不符时不发布 PNG。
- 四阶段内容寻址产物、原子检查点、序号连续发布和文件锁。
- 基于输入版本指纹的最早失效阶段定位与下游传播。
- 单分片原子更新；增量结果先与从零全量构建逐字节比较，再发布。
- 只读 CLI 诊断和自包含静态 HTML 工作台。

## 快速开始

要求 Python 3.10+。本仓库已在 Python 3.14.4 上验证。

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install -e .

# 生成 4 个固定种子分片（含中英文提示词和 1 个图生图分片），并执行完整演示
.venv/bin/detdiff demo --workspace demo_workspace --reset
```

也可以使用：

```bash
./scripts/setup.sh
./scripts/run_demo.sh
make test
# 也可直接使用仓库根目录的离线包装器
./detdiff status --workspace demo_workspace
```

`demo_data/` 已包含一组可直接复制的固定种子输入与 YAML：

```bash
cp -a demo_data workspace
.venv/bin/detdiff build --workspace workspace
.venv/bin/detdiff status --workspace workspace
.venv/bin/detdiff verify --workspace workspace
```

## CLI

```bash
# 初始化固定样本；不访问网络
detdiff init-demo --workspace workspace --reset

# 全量构建；只计算失效阶段
detdiff build --workspace workspace --json

# 在 encode 阶段后注入停止，之后直接执行 build 即可恢复
detdiff build --workspace workspace --stop-after-stage encode
detdiff build --workspace workspace

# 输入注入：暂停文件存在时，在当前阶段完成后停止；退出码 130
touch pause.flag
detdiff build --workspace workspace --pause-file pause.flag
rm pause.flag
detdiff build --workspace workspace

# Ctrl-C / SIGTERM 也会在阶段边界安全停止，不破坏已发布检查点

# 单分片配置更新，并只重建失效链路
detdiff update --workspace workspace --shard mountain-zh \
  --prompt '新的中文提示词' --guidance-scale 7.25

# 替换参考图：先验证尺寸，再写为内容寻址文件
detdiff update --workspace workspace --shard harbor-zh-en \
  --reference-file new_reference.png

# 只读状态、静态 HTML 诊断
detdiff status --workspace workspace --json
detdiff diagnostics --workspace workspace --html workspace/diagnostics.html

# 在临时目录真正从零构建，并逐文件摘要比对
detdiff verify --workspace workspace --json
```

所有命令传 `--json` 都会输出结构化结果。业务错误同时写入 `code`、`message`、`shard`、`field` 和可选 `details`。

## 确定性管线

### 1. 文本编码

提示词先做 Unicode NFKC 规范化、空白折叠和 casefold，再用固定 `SHAKE-256` 材料派生 16 个 float64，并归一化为单位向量：

```text
material = canonical_json(domain, encoder_name, encoder_version, dimension, normalized_prompt)
embedding = normalize(float64(SHAKE-256(material)))
```

不依赖 Python 进程随机化的 `hash()`。相同提示词与编码参数生成逐字节相同的嵌入。

### 2. 条件调度

对 `n = steps`，第 `i` 步（从 0 开始）的无条件分支权重为：

```text
progress_i = i / (n - 1)                 # n == 1 时取 0
w_i = 1 / (1 + guidance * (0.25 + 0.75 * progress_i))
```

权重长度严格等于 `steps`，存为 little-endian float64。负值、NaN、Inf 或非法步数返回 `E_INVALID_CONFIG`，并包含分片和字段。

### 3. 确定性采样

- 从 `seed + shape + sampler version` 派生 SHAKE-256 伪噪声张量。
- 无条件目标为初始伪噪声的确定性平滑场。
- 条件目标由 16 维嵌入和像素坐标的正弦/余弦场生成。
- 每步执行 `target = (1-w_i) * conditional + w_i * unconditional`，再向 `target` 做线性插值。
- 每步记录 `{step, mean, variance}`，步数上限 512，避免无界轨迹。

图生图时，参考 PNG 被解码为同尺寸 RGB float64 张量，并与初始噪声按 `blend_strength` 混合。参考分支不引入额外随机性；融合强度为 0 时与无参考路径逐字节一致。

### 4. 量化

量化前显式将 float64 钳制到 `[-1, 1]`，并统计越界/非有限值数量；映射到 8 位后用 `np.rint` 取整并再次钳制到 `[0, 255]`，记录第二次钳制数量。PNG 由内置 writer 写出固定 IHDR、无时间戳、单 IDAT 和固定压缩级别，因此重复构建的 PNG 字节一致。

## 阶段指纹与失效传播

每阶段输入使用 canonical JSON 计算 SHA-256 指纹：

| 阶段 | 指纹关键输入 | 最早失效示例 |
|---|---|---|
| `encode` | 分片 ID、规范化提示词、全局编码器参数 | 提示词、encoder domain/version/dimension |
| `schedule` | encode 产物摘要、guidance、steps | 引导系数、采样步数 |
| `sample` | schedule 产物摘要、seed、参考图摘要、融合强度、输出形状 | seed、参考图内容、融合强度、尺寸 |
| `quantize` | sample 产物摘要、量化参数 | PNG 压缩级别、量化版本 |

一旦某阶段失效，其全部下游阶段也失效。上游阶段只有在检查点输入指纹与当前配置重新计算出的指纹完全一致、且内容寻址产物摘要校验通过时才复用。

`status --json` 为每个分片返回：

- `earliest_invalid`
- `rebuild_plan` / `reuse_plan`
- 每阶段 `status`、`invalid`、`reason`、`changed_fields`、`invalidated_by`
- 检查点序号、已记录指纹、期望指纹、当前 PNG 摘要
- 采样轨迹和量化前后钳制次数

查询只读文件，不创建目录、检查点或产物。

## 检查点与并发发布

工作区状态位于 `.state/`：

```text
.state/
  locks/workspace.lock
  locks/shard-<id>.lock
  shards/<id>/checkpoints/<stage>.json
  shards/<id>/artifacts/<stage>-<sha256>.bin
outputs/<id>.png
```

检查点包含分片标识、阶段名、输入指纹、配置版本、产物摘要、产物路径、产物大小、PNG 摘要，以及分片内单调序号。

- 产物按内容寻址，写入不会覆盖另一个合法版本。
- 写入使用同目录临时文件、`fsync`、原子 `os.replace` 和目录 `fsync`。
- 读取端只接受正式检查点文件名，忽略 `.tmp-*` 临时文件；中断后从最近一致阶段恢复。
- 发布在分片文件锁内进行，要求 `expected_sequence == max(existing_sequence) + 1`。
- 重复或落后序号返回 `E_STALE_CHECKPOINT`。
- 缺少紧邻前置阶段或前置产物摘要不合法时，发布后置阶段返回 `E_INVALID_STAGE`。
- 只有检查点原子落地后才发布最新可见 PNG；若中断发生在两者之间，状态查询会将量化阶段判为失效并触发重建。

## 增量发布的一致性保护

`update` 先在内存中计算所有失效阶段，同时复用已验证的上游产物。只要存在重建，构建器就会独立执行一次从零全量计算，并把每个分片的四个阶段产物和最终 PNG 与候选结果逐字节比较。

只有完全一致才发布任何新检查点或输出。若发现差异，返回 `E_INCONSISTENT_STATE`，并保留原有合法检查点和 PNG。`verify` 命令进一步在临时工作区真实写入一次全量构建，再对两个工作区的已发布阶段摘要和 PNG 摘要做逐文件比较。

## 业务错误码

| 业务码 | CLI 退出码 | 含义 |
|---|---:|---|
| `E_INVALID_CONFIG` | 2 | YAML/字段非法、guide 非法、blend 越界等 |
| `E_MISSING_REFERENCE` | 3 | 参考图缺失、无法解码或尺寸不符 |
| `E_STALE_CHECKPOINT` | 4 | 检查点序号落后、重复或并发发布失败 |
| `E_INVALID_STAGE` | 5 | 阶段名非法或缺少前置阶段产物 |
| `E_INCONSISTENT_STATE` | 6 | 增量结果与从零全量构建字节不一致 |

阶段边界主动停止返回退出码 130，已发布检查点保持有效。

## 演示数据

`demo_data/` 与 `detdiff init-demo` 生成相同的固定样本：

| 分片 | 提示词 | guidance | steps | seed | 参考图 |
|---|---|---:|---:|---:|---|
| `aurora-en` | Aurora over a deterministic mineral ridge | 3.5 | 14 | 1101 | 无 |
| `mountain-zh` | 中文提示词：层叠山脊与低温矿物纹理 | 6.0 | 18 | 2202 | 无 |
| `harbor-zh-en` | 港口地层与暖色沉积带 / harbor strata | 4.0 | 16 | 3303 | `references/harbor.png`，strength 0.38 |
| `microstructure-en` | Low-guidance crystalline microstructure | 1.25 | 9 | 4404 | 无 |

4 张输入 PNG 和 1 张参考 PNG 均由固定种子生成器产生。`demo` 命令自动完成：

1. 首次构建在 `aurora-en/encode` 后注入停止。
2. 查询失效链并从 `schedule` 恢复。
3. 完成全量构建并从零复核。
4. 更新 `mountain-zh` 的提示词和 guidance，只重建其失效链路。
5. 再次从零复核。
6. 注入非法 blend、非法 guidance 和缺失参考图，输出对应业务码。
7. 生成 `diagnostics.html` 和 `demo_report.json`。

## 实际验证结果

本机环境：Python 3.14.4、NumPy 2.5.5、Pillow 13.0.0、PyYAML 6.0.3。

测试命令：

```bash
.venv/bin/python -m unittest discover -s tests -v
```

结果：`Ran 26 tests ... OK`。覆盖嵌入/调度、钳制量化、缺失参考图、原子临时文件恢复、停止恢复、四类失效起点、参考图替换、增量/全量一致性、不一致保护、两进程并发序号竞争、只读查询、CLI 退出码，以及 `OMP_NUM_THREADS=1` 与 `4` 的跨进程逐字节一致。

固定演示实测：

- 初次完整构建：20 个已发布文件摘要，manifest SHA-256 `e3988f73455a9f72335d80774b35a9355825d82006f3bb6750491d3a911de26f`。
- 更新 `mountain-zh` 后再做从零全量比对：20 个文件全部一致，manifest SHA-256 `e180353bc96024cac67e39fc89f120d90c3efacfe41c14a8ac5731801bbb6979`。
- 注入结果：非法 blend / guidance 为 `E_INVALID_CONFIG`，缺失参考图为 `E_MISSING_REFERENCE`。

详细记录位于：

- `reports/test-output.txt`
- `reports/verification-summary.json`
- `reports/demo-result.json`
- `reports/verify-result.json`
- `reports/status.txt`

## 代码结构

```text
src/detdiff/
  config.py          YAML 解析、字段校验
  encoding.py        规范化、哈希嵌入、调度权重
  sampling.py        伪噪声、条件场、参考融合、轨迹摘要
  quantization.py    钳制、8 位量化、确定性 PNG writer
  artifacts.py       版本化阶段二进制容器
  versions.py        阶段输入元数据与指纹
  checkpoints.py     原子检查点、文件锁、序号协议
  pipeline.py        失效规划、增量构建、全量比对、诊断
  workspace.py       单分片原子更新
  diagnostics.py     CLI/静态 HTML 渲染
  demo.py            固定样本和端到端演示
  cli.py             命令行入口
tests/               26 项 unittest
demo_data/           固定种子 PNG 与 YAML
reports/             本次实际验证输出
```

这是一个用于工作流、失效传播、诊断和持久化语义验证的本地确定性系统；它有意不模拟真实扩散模型的视觉质量。
