# LLM 预训练数据配比与去重流水线

本地可运行的多阶段流水线：对 JSONL 数据分片做规范化、SimHash 近似去重、
按 `(source, lang)` 分层的目标配比混合，输出训练混合集。支持持久化检查点、
中断恢复、版本失效传播与并发安全发布。

## 环境

- Python 3.11+（Windows 可用 `py -3.11`），仅依赖标准库 + NumPy + SQLite3
- 安装依赖：`pip install -r requirements.txt`

## 目录结构

```
pipeline/            # 流水线包
  textnorm.py        # NFKC 规范化 / 空白折叠 / 小写化 / 64 位 SimHash
  dedup.py           # 4x16 位分桶索引 + 海明阈值确认（首次出现优先）
  mixer.py           # 分层最大余数法分配 + 固定种子层内抽样（NumPy）
  store.py           # SQLite 检查点存储（序号、版本指纹、产物摘要、事件）
  stages.py          # 阶段实现与输入版本指纹计算
  runner.py          # 失效计划、停止/恢复、原子发布
  locking.py         # 跨平台文件锁（msvcrt / fcntl）
  app.py             # 交互式 CLI（python -m pipeline.app）
data/shards/         # 3 个演示分片（含跨分片重复与中英混合层）
data/mix_config.json # 去重参数与配比配置样例
scripts/make_demo_data.py  # 重新生成确定性演示数据
demo.py              # 端到端演示（停止/恢复/失效/错误码）
tests/test_pipeline.py     # 29 个单元与集成测试
```

运行工作区（默认当前目录，`--workspace` 可指定）在运行时生成：

```
artifacts/           # normalize.jsonl / dedup.jsonl(+report) / mix.jsonl(+stats)
state/checkpoints.db # 检查点、阶段状态、事件日志（SQLite）
state/publish.lock   # 发布文件锁
state/STOP           # 停止请求标记文件
```

## 快速开始

```bash
python scripts/make_demo_data.py        # 生成/重置演示数据
python -m pipeline.app run              # 运行全部阶段
python -m pipeline.app status           # 查看阶段状态与版本指纹
python -m pipeline.app stats            # 查看混合集统计
python -m pipeline.app                  # 进入交互式控制台（help 查看命令）
```

交互命令：`import <file>`、`submit <json>`、`run [--target S]`、`resume`、
`run-stage S`、`status [--json]`、`stats`、`stop`、`clearstop`、`reset`、`quit`。

## 阶段与语义

1. **normalize**：逐分片读取，text 做 NFKC + 小写 + 空白折叠，按空白切分估计
   token，blake2b 词频加权计算 64 位 SimHash；记录全局首次出现顺序 `order`。
2. **dedup**：4 个 16 位桶建立候选索引，与已保留记录做海明距离确认
   （阈值默认 3，可配置）；首次出现保留，保证可重复顺序。
3. **mix**：按 `source|lang` 分层统计样本数与 token 估计，按配置权重做最大
   余数法分配（受可用量约束并确定性再分配），层内用 `seed|layer` 固定种子
   抽样，输出按原顺序排列的混合集与统计文件。

## 检查点、恢复与失效

- 每阶段完成时，在文件锁保护下以单个 SQLite 事务写入检查点：阶段名、
  输入版本指纹（分片 / 去重参数 / 配比配置 / 上游产物摘要）、产物摘要、
  连续序号；产物文件经 tmp+fsync+`os.replace` 原子发布，任意位置中断
  不会产生半写记录。
- 停止：SIGINT/SIGTERM、`state/STOP` 文件或 `PIPELINE_STOP_AFTER=<stage>`
  注入都会让进程在完成当前不可拆分单元（一个阶段）后以退出码 75 退出；
  `resume` 从最近一致检查点继续，已发布阶段不重算，未完成阶段从起点重算，
  最终混合集与一次完整运行逐字节一致。
- 失效传播：分片内容、去重参数、配比配置任一变化都会被版本指纹识别，
  受影响阶段及其下游在 `status` 中标记为 `invalidated`（含原因），未受影响
  的上游产物继续复用；重建后序号连续推进。

## 并发发布与错误码

- 多进程发布由 `state/publish.lock` 文件锁 + 检查点序号比较保证：仅允许
  序号连续推进，落后/重复发布被拒绝并返回 `E_STALE_CHECKPOINT`，不覆盖新状态。
- 非法阶段跳转、缺失前置产物、未知阶段返回 `E_INVALID_STAGE` 与可读消息。
- 其他业务码：`E_INPUT`（样本/分片非法）、`E_CONFIG`（配置缺失/非法）。

## 确定性

同一输入（分片 + 配置）下所有产物逐字节一致：分片按文件名排序处理、JSON
输出键排序、抽样种子固定、分配次序按 `(-余数, 层名)` 稳定排序。

## 演示与测试

```bash
python demo.py                          # S1 完整运行 / S2 停止后恢复字节一致 /
                                        # S3 版本变更失效重建 / S4 错误码
python -m unittest discover -s tests -v # 29 个测试
```

Windows 下运行相同（`py -3.11 demo.py`）；停止验证可用 Ctrl+C（SIGINT）、
`stop` 命令写入 STOP 文件，或设置 `PIPELINE_STOP_AFTER=normalize` 注入。

## 实际验证结果（本环境）

- `python demo.py`：S1–S4 全部 PASS；停止注入后恢复生成的
  normalize/dedup/mix 与完整运行逐字节一致（摘要 `7e2cb2e6615f` /
  `e6a2de78fc95` / `730ebc697c2c`）。
- `python -m unittest discover -s tests`：29 个测试全部通过。
- 演示数据 17 条样本去重后保留 10 条（识别 5 个规范化/跨分片精确重复 +
  2 个海明距离 1 的近重复），混合集 10 条覆盖 5 个 `source|lang` 层。
