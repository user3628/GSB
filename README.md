# 特征编码与选择工作台（feature workbench）

面向表格型样本的本地交互式建模准备工作台：对确定性 CSV 样本依次执行
**列类型推断 → 编码 → 特征选择 → 结果汇总**，支持会话中修改配置并增量重算，
结果与从零全量重建逐字节一致；进程中断后可从持久化状态恢复。
纯本地运行，不依赖任何线上数据集、账号或外部服务，不使用容器。

## 环境准备

- Python 3.10+（本仓库在 Python 3.14.4 + NumPy 2.3.5 下验证）
- 安装 NumPy（建议虚拟环境）：

```bash
python3 -m venv .venv --system-site-packages   # 或普通 venv 后 pip install -r requirements.txt
.venv/bin/python -c "import numpy"             # 确认可导入
```

## 快速开始

```bash
PY=.venv/bin/python
# 1. 生成确定性样本（固定种子）与默认配置
$PY -m workbench --state .workbench_state gen-sample --out data/sample.csv
$PY -m workbench --state .workbench_state gen-config --out data/config.json
# 2. 初始化配置并导入样本
$PY -m workbench --state .workbench_state config-init --from data/config.json
$PY -m workbench --state .workbench_state import-sample --path data/sample.csv
# 3. 运行流水线（infer -> encode -> select -> summarize）
$PY -m workbench --state .workbench_state run
# 4. 查看状态 / 阶段产物 / 选择结果 / 一致性校验
$PY -m workbench --state .workbench_state status
$PY -m workbench --state .workbench_state stage encode
$PY -m workbench --state .workbench_state selection
$PY -m workbench --state .workbench_state verify
# 5. 修改配置（局部重算）与停止/恢复
$PY -m workbench --state .workbench_state config-set --set selection.method=variance
$PY -m workbench --state .workbench_state run --budget-ms 5000
$PY -m workbench --state .workbench_state stop        # 请求优雅停止
$PY -m workbench --state .workbench_state resume      # 从最近一致记录恢复
# 6. 本地 HTTP 接口
$PY -m workbench --state .workbench_state serve --host 127.0.0.1 --port 8765
```

一键确定性演示（覆盖全链路并输出固定摘要）：

```bash
.venv/bin/python scripts/demo.py
```

## 样本与配置

- 样本由 `workbench/sample.py` 以固定种子生成（默认 seed=20261120、2000 行）：
  数值列 `age/income/score`，类别列 `city/segment/channel/device/loyalty`（5 个，
  ≥4 个要求），`income/city/channel` 含缺失值，`label` 为二分类标签。
- 配置为 JSON（`workbench/config.py` 中有默认值与严格校验），划分为
  `label_column / inference / missing / encoding / selection / runtime` 六个区段：
  - `encoding`：类别列默认编码器 + 逐列覆盖（`onehot` / `target` / `hash`）、
    哈希桶数、目标编码折数/平滑强度/种子、独热类别上限；
  - `missing`：数值（mean/median/constant）与类别（constant/mode）填充；
  - `selection`：`variance`（阈值）/ `mutual_info`（阈值、top_k、分箱数）/
    `rfe`（保留数、步长、岭正则）；
  - `runtime`：默认延迟预算 `latency_budget_ms`。

## 设计要点

### 阶段流水线与指纹

- 四个阶段：`infer → encode → select → summarize`。
- `stage_fp = sha256(stage | 样本指纹 | 该阶段相关配置区段哈希 | 上游 stage_fp)`，
  配置按区段求哈希，因此配置子项变更只使其影响链上最早的阶段及其下游失效。
- `cache_key = sha256(样本指纹 | 配置有效版本 | stage_fp)`；配置有效版本 =
  该阶段相关区段版本的最大值（每个区段记录最后变更版本）。
  缓存命中与重算结果逐字节一致（计算本身确定性），配置变更必然产生新键，
  不会以陈旧缓存冒充新配置结果。

### 目标编码防泄漏

- 折划分：`RandomState(seed).permutation(n) % n_folds`，种子固定后完全确定。
- 每行编码只使用**其他折**的标签统计（绝不使用当前行标签）：
  `enc = (类别标签和 + smoothing × 折外全局均值) / (类别计数 + smoothing)`。
- 训练折中不可见的类别回退到该折外全局均值。
- 诊断输出（`stage encode` 产物的 `target_encoding` 字段）包含每个类别在
  各折的编码值、折间极差 `spread` 与不可见折计数 `unseen_folds`。

### 特征选择

- 作用在编码后的特征空间，与编码列名映射对齐；被淘汰特征不进入
  `summarize` 的最终矩阵描述与下游输出。
- 方差过滤（ddof=0）、互信息（分位数分箱 + 列联表精确计算，纯算术无随机性）、
  递归消除（标准化特征 + 岭回归权重，逐步淘汰最小 |w|，重名按列名决胜），
  三种方法均为确定性实现；方法或阈值变化经指纹机制触发重算。

### 延迟预算

- 每次查询在可配置预算内返回；`Deadline` 在阶段边界与列/特征循环内协作式检查，
  超预算立即抛出并返回结构化错误 `E_LATENCY_BUDGET`（含预算与已耗时），不阻塞等待。

### 持久化、原子写与恢复

- 每个阶段产物先原子写入 `artifacts/<cache_key>.json|.f64`（临时文件 + fsync +
  `os.replace` + 目录 fsync），再原子追加日志记录 `journal/<seq>_<stage>.json`，
  记录含阶段名、样本指纹、配置版本、产物摘要与单调序号；任意位置中断只会留下
  被忽略的临时文件，不会产生半写记录。
- 恢复时扫描日志并取**最长一致前缀**（序号连续、JSON 可解析、产物摘要匹配），
  从最近一致记录继续；已发布阶段（按 cache_key 命中日志）不重复计算。
- 优雅停止（SIGTERM/SIGINT 或 `stop` 命令写停止标记）：完成当前不可拆分单元
  （一个阶段）后退出，退出码 0；强制终止（SIGKILL）后仅承诺恢复到最近一致记录。

### 并发发布保护

- 发布经 `publish.lock` 文件锁（fcntl.flock）串行化，锁内重读日志头并比较序号：
  仅允许 `seq = head + 1` 连续推进，落后或重复发布返回 `E_STALE_CHECKPOINT`；
  前置阶段产物（按上游指纹匹配）缺失时返回 `E_INVALID_STAGE`，两码稳定且互不混淆。

### 增量与全量一致性

- `verify` 在临时目录用同一配置存储从零全量重建，逐字节比对四个阶段的产物
  （含矩阵字节），不一致时返回 `E_INCONSISTENT` 并给出阶段、首个差异字节偏移
  与双方摘要。产物内容只依赖样本与配置内容（不嵌入版本号），因此不同编辑路径
  到达同一配置内容时摘要相同。

### 退出码与错误码

- 退出码：`0` 正常结束（含优雅停止）；`2` 业务错误（stderr 输出含 `E_xxx`
  的结构化 JSON）；`1` 未预期错误。该语义与 Windows 进程退出码习惯一致
  （正常结束返回整数 0，业务错误以 JSON/stderr 中的 E_xxx 表达）。
- 错误码：`E_LATENCY_BUDGET` / `E_INCONSISTENT` / `E_STALE_CHECKPOINT` /
  `E_INVALID_STAGE` / `E_CONFIG` / `E_SAMPLE` / `E_STATE` / `E_INTERNAL`。

## HTTP 接口（127.0.0.1）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 存活检查 |
| GET | `/status` | 日志头、配置版本、各阶段指纹 |
| GET | `/config` | 当前配置 |
| PUT/PATCH | `/config` | 整体替换 / 局部修改（`{"set": {"a.b": v}}`） |
| POST | `/sample` | 导入样本 `{"path": ...}` |
| POST | `/run` | 运行流水线 `{"budget_ms": ..., "stage_sleep_ms": ...}` |
| POST | `/stop` `/resume` | 请求停止 / 恢复 |
| POST | `/verify` | 全量重建一致性校验 |
| GET | `/stages/<name>` | 阶段产物（含目标编码诊断） |
| GET | `/selection` | 保留/淘汰特征清单 |

## 测试与验证结果

```bash
.venv/bin/python -m unittest discover -s tests -v   # 58 项测试
.venv/bin/python scripts/demo.py                    # 端到端确定性演示
```

本机实测（Python 3.14.4 + NumPy 2.3.5，Linux）：

- `unittest`：**58 项全部通过**（编码 14、选择 5、流水线 11、恢复 5、并发 9、
  CLI 7、HTTP 7，含防泄漏、平滑公式、折确定性、缓存逐字节一致、撕裂写恢复、
  SIGTERM/SIGKILL 恢复、并发发布保护、退出码与 E_xxx 错误）。
- `scripts/demo.py`：**全部检查 PASS**。关键记录：
  - 默认配置全量运行固定摘要
    `b371b890f5f25b094913c62cc488d4f7aa356b0aece93f77eeec85d1aa5da351`
    （2000 行 → 26 个编码特征 → 选择 12 个），全新状态目录复算摘要逐字节一致；
  - 目标编码诊断示例（loyalty/t00）：各折值
    `[0.448951, 0.409046, 0.424770, 0.413984, 0.415461]`，spread=0.039905；
  - 修改 `selection.*` 仅重算 select/summarize，修改 `encoding.*` 仅保留 infer，
    `verify` 均报告 consistent；
  - `--budget-ms 0` 返回 `E_LATENCY_BUDGET`、退出码 2；
  - SIGTERM 优雅停止（exit 0，已完成 infer/encode），resume 后摘要与不间断
    参考运行一致；SIGKILL 后日志截断到最近一致记录，resume 补齐剩余阶段且
    摘要一致；
  - 重复发布返回 `E_STALE_CHECKPOINT`，缺前置阶段返回 `E_INVALID_STAGE`；
  - HTTP 接口完成导入、运行、增量重算、状态查询与优雅关停（exit 0）。

## 目录结构

```
workbench/            # 核心包
  sample.py           # 确定性样本生成器
  config.py           # 配置默认值、校验、版本与区段指纹
  infer.py            # 阶段1：列类型推断
  encode.py           # 阶段2：缺失填充 + 独热/目标/哈希编码
  select.py           # 阶段3：方差/互信息/递归消除
  summarize.py        # 阶段4：结果汇总与固定摘要
  engine.py           # 编排：指纹、缓存、增量运行、停止、校验
  store.py            # 状态目录：日志恢复、原子发布、文件锁
  cli.py / server.py  # 命令行与本地 HTTP 接口
tests/                # 58 项 unittest 测试
scripts/demo.py       # 端到端确定性演示
TASK.md               # 原始任务书
```

## 说明与限制

- 文件锁基于 `fcntl`（Linux/macOS）；Windows 需替换为 `msvcrt.locking`，
  退出码语义不变。
- RFE 使用 LAPACK 求解器，跨 BLAS 实现的末位浮点可能略有差异；默认演示
  方法（互信息）为纯算术，跨平台逐字节稳定。
- 阶段内计算为单进程协作式取消，延迟预算检查位于阶段与列/特征循环边界。
