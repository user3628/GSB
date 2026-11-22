# QRWB — 跨语言查询改写并行编排工作台

面向中英混排搜索请求的**本地、确定性、无外部服务**改写工作台。为每条查询并行生成
三路改写候选（同义词替换、字形近似、跨语言桥接翻译），在全局评估预算 / 单通道配额 /
并发通道上限 / 每请求延迟预算的多重约束下编排裁决，并保证**并行执行与串行执行逐字节一致**、
**优雅停止后可从检查点恢复且结果与全新运行一致**、**配置或输入版本变化时按阶段增量失效重建**。

仅依赖 Python 3.10+ 标准库、NumPy（对数域数值）与 PyYAML；不使用 Docker、GPU 或任何网络服务。

## 目录结构

```
qrwb/                  主包
  errors.py            稳定业务错误码与结构化错误对象
  util.py              确定性 JSON、内容指纹、原子写、flock 文件锁
  config.py            YAML 配置加载/校验/版本指纹
  sampling.py          固定种子样本生成（同义词表/字形桶/桥接词典/JSONL 分片/清单）
  normalize.py         阶段一：规范化 + 切分偏移 + 三路派发
  candidates.py        阶段二：同义/字形/桥接候选生成（含 UNK 显式回退）
  scheduler.py         阶段三：预算账本、接纳、波次并发、延迟预算
  engine.py            单查询四阶段引擎与版本指纹束
  pipeline.py          全量流水线、停止/恢复、确定性归约、查询接口
  adjudication.py      阶段四：对数域裁决分、确定性仲裁、分层汇总
  checkpoints.py       检查点原子发布、序号连续性、失效传播
  workspace.py         输入装载与分片清单一致性校验
  cli.py               命令行接口
  server.py            标准库 http.server 本地接口
scripts/
  demo.py              一键验收演示（含停止恢复与并行/串行比对）
  mp_lock_demo.py      多进程文件锁发布保护演示
  run_server.py        HTTP 服务启动脚本
tests/                 30 个 unittest 用例
config/demo.yaml       演示配置（与默认配置一致）
docs/                  技术说明与验证结果
```

## 快速开始

```bash
pip install -r requirements.txt          # numpy>=1.24, PyYAML（离线可用本地 wheel）

# 一键完整验收（多路改写/预算/停止恢复/并行归约/多进程锁）
python3 scripts/demo.py

# 或手动逐步操作
python3 -m qrwb init ./work --shards 2 --rows 10
python3 -m qrwb run  ./work --config ./work/config.yaml            # 线程池并行
python3 -m qrwb run  ./work --config ./work/config.yaml --serial   # 单线程串行
python3 -m qrwb stop ./work --config ./work/config.yaml            # 请求优雅停止
python3 -m qrwb resume ./work --config ./work/config.yaml          # 从检查点恢复
python3 -m qrwb status ./work --config ./work/config.yaml --query-id q01-0000
python3 -m qrwb budget ./work --config ./work/config.yaml
python3 -m qrwb trace  ./work --config ./work/config.yaml --query-id q01-0000
python3 -m qrwb compare work_a/state/run_report.json work_b/state/run_report.json
```

## 四阶段流水线

1. **派发（dispatch）**：Unicode NFKC → 全半角统一 → 小写化 → 空白折叠；按中文单字与
   拉丁连续串切分并记录字符偏移；据此决定三路启用状态与候选上限。空查询/仅标点产出
   `E_EMPTY_QUERY` 并保留诊断上下文。
2. **候选生成（candidate）**：
   - 同义路：本地同义词表替换，记录被替换偏移与词典条目版本；
   - 字形路：固定 sha256 哈希近似码检索同码词项；
   - 桥接路：桥接词典映射目标语言候选；缺词项时输出 `[UNK:token]` 显式回退（`E_UNKNOWN_TOKEN`），绝不静默丢弃。
3. **编排（orchestration）**：以查询为调度单元，按 query_id 确定序接纳；在
   全局预算、单通道配额、并发通道上限三重约束下预占评估次数，按波次线程池并行；
   超延迟预算返回 `E_LATENCY_BUDGET`（带 query_id 与已耗时）而非阻塞。
4. **验证汇总（validation）**：NumPy 对数域合成裁决分
   `w_c·log(confidence) + w_r·log(route_weight) + w_o·overlap`，确定性同分仲裁，
   剔除空改写/与原查询重复/候选互重，按 `lang|channel` 分层输出采纳率与路命中分布。

## 确定性与并行归约

- 所有 JSON 落盘使用键排序、固定分隔符的规范化序列化（`util.canonical_dumps`）。
- 随机数全部来自显式种子的 `random.Random`；延迟模拟按 query_id 派生独立 RNG，
  与线程调度顺序无关。
- 并行与串行使用完全相同的接纳顺序、波次划分与归约排序（query_id → 路径 → 候选指纹），
  仅执行载体不同，因此 `state/*.json` 逐字节一致、`run_fingerprint` 相同。

## 检查点、恢复与版本失效

- 每查询一条追加式 `checkpoints/<query_id>.jsonl`，记录含 query_id、阶段名、输入指纹、
  配置版本、产物摘要与单调序号；通过「临时文件 + fsync + 原子 rename」写入，崩溃不留半写记录。
- 序号在查询内单调连续推进；重复/落后发布返回 `E_STALE_CHECKPOINT`，阶段回退/非法跳转
  返回 `E_INVALID_STAGE`；多进程发布经 `fcntl.flock` 真实文件锁串行化。
- 各阶段只依赖对应内容指纹（输入、规范化、三路词典、路线上限、预算、裁决权重）。
  版本变化时定位**最早受影响阶段**，删除该阶段及下游并重建，未受影响上游继续复用；
  失效事件记录在 `state/invalidations.jsonl`，可经 `status` 接口观测版本指纹与失效原因。
- 优雅停止以「不可拆分查询单元」为界：完成当前单元后退出；恢复后最终结果与全新运行
  逐字节一致，检查点序号连续。

## 稳定错误码

| 错误码 | 触发场景 |
| --- | --- |
| `E_EMPTY_QUERY` | 规范化后无有效词项 |
| `E_BUDGET_EXHAUSTED` | 全局评估预算不足 |
| `E_CHANNEL_QUOTA` | 单通道配额不足 |
| `E_LATENCY_BUDGET` | 超过每请求延迟预算 |
| `E_STALE_CHECKPOINT` | 序号落后/重复/错位发布 |
| `E_INVALID_STAGE` | 前置阶段缺失或非法阶段跳转 |
| `E_UNKNOWN_TOKEN` | 桥接词典缺词项（回退标记，非致命） |
| `E_WORKER_EXCEPTION` | 工作线程异常的结构化记录（保留已完成产物） |
| `E_FINGERPRINT_MISMATCH` | 旧指纹产物不可冒充新结果 |

## 本地 HTTP 接口

```bash
python3 scripts/run_server.py ./work --config ./work/config.yaml --port 8765
```

`POST /import`、`POST /run?parallel=0|1`、`GET /status?query_id=`、`GET /budget`、
`GET /trace?query_id=`、`POST /stop`、`POST /resume`、`GET /health`。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：规范化/派发确定性、三路候选与 UNK 回退、预算与延迟约束、对数域裁决与剔除规则、
并行/串行逐字节一致、停止恢复一致、版本失效传播、原子写与多进程文件锁、HTTP 冒烟、
分片清单篡改检测与跨分片重复声明。验证结果见 `docs/verification.md`。
