# QoS 流量整形策略管理与诊断工作台

面向本地回环环境的纯 Python/SQLite 工作台，提供：

- 紧凑二进制与分片 JSONL 两种策略集持久化格式，以及双向导入导出。
- 格式/版本协商、旧版本降级补齐、高版本与未知必填字段整批拒绝。
- bps/bytes/µs 归一化、整数溢出检查、亚毫秒时间戳截断与误差报告。
- 父子整形器依赖、环检测、父子速率约束和重复队列确定性裁决。
- 整数定点双令牌桶仿真，输出逐包令牌余量、发送/丢弃、绿黄红标记。
- 带相对容差的整形效果与 CIR 偏差诊断。
- 不可变快照整体切换、单写者提交、多读者一致读取。
- 基于 SQLite 意图记录的强杀恢复，不伪造未持久化的仿真结果。

所有样本均来自 `samples/`，不访问真实网络、外部站点或付费服务。

## 快速开始

```bash
python -m pip install -r requirements.txt
python scripts/generate_samples.py
python -m qos_workbench
```

默认监听 `127.0.0.1:18080`，可用 `QOS_HOST`、`QOS_PORT`、`QOS_DB_PATH` 覆盖：

```bash
QOS_DB_PATH=./data/qos.db QOS_PORT=18080 python -m qos_workbench
curl http://127.0.0.1:18080/api/health
```

OpenAPI 页面：`http://127.0.0.1:18080/docs`。服务必须使用单 Uvicorn worker；进程内写者锁与 SQLite WAL 共同保证提交串行、读取一致。

## 目录

```text
qos_workbench/            核心实现与 FastAPI 应用
  codecs.py               双格式编解码、兼容矩阵、单位与时间归一化
  validation.py           依赖、环、速率、父子与快照规则
  simulation.py           整数定点双令牌桶仿真
  diagnostics.py          相对容差偏差诊断与精度契约
  store.py                SQLite 快照、意图、恢复和写者串行
  api.py                  本地 HTTP API
samples/                  确定性策略与 255 包流量样本
scripts/                  样本生成与完整验收脚本
tests/                    43 项 pytest 测试
reports/                  最近一次可复现验收结果
docs/                     格式、API、架构与运行语义
```

## 主要 API

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/api/health` | 健康状态与当前快照 |
| `GET` | `/api/formats/compatibility` | 显式兼容矩阵 |
| `POST` | `/api/formats/negotiate` | 检测格式/版本并给出接受、降级或拒绝结论 |
| `POST` | `/api/policies/import` | 导入并提交为不可变快照 |
| `POST` | `/api/policies/validate` | 只解码/校验，不提交 |
| `GET/POST` | `/api/policies/export` | 导出二进制或 JSONL |
| `POST` | `/api/snapshots/load` | 直接加载归一化策略快照 |
| `GET` | `/api/snapshots/current` | 当前完整快照 |
| `POST` | `/api/simulations` | 内联流量或 `sample_name` 仿真 |
| `POST` | `/api/simulations/from-sample/{name}` | 使用 `samples/` 文件仿真 |
| `GET` | `/api/diagnostics/latest` | 最近已提交批次的诊断 |
| `GET` | `/api/recovery/report` | 启动恢复报告，只含持久化事实 |

导入示例：

```bash
B64=$(base64 -w0 samples/policies_v3.jsonl)
curl -sS http://127.0.0.1:18080/api/policies/import \
  -H 'content-type: application/json' \
  -d "{\"format\":\"auto\",\"content_base64\":\"$B64\",\"apply_snapshot\":true}"

curl -sS -X POST http://127.0.0.1:18080/api/simulations/from-sample/traffic.jsonl
```

Windows PowerShell 可用以下方式构造同样的请求体：

```powershell
$b64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes("samples/policies_v3.jsonl"))
$body = @{format="auto"; content_base64=$b64; apply_snapshot=$true} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:18080/api/policies/import `
  -ContentType 'application/json' -Body $body
```

业务错误统一为：

```json
{"ok": false, "error": {"code": "E_DEPENDENCY_CYCLE", "message": "...", "details": {}}}
```

完整请求/响应示例见 `docs/API.md`。

## 格式与语义

- 内部规范表示：速率 `bps`，突发 `bytes`，时间戳 `µs`，整数存储。
- 二进制 v3：`bps`、`bytes`、`µs`；魔数 `QOSB`，版本字节，大小端标记，头部 CRC32，逐记录长度前缀、存在位图和 CRC32。
- JSONL v3：`kbps`、`KB(1024 bytes)`、`ms`；每行均含队列、速率、突发、优先级、匹配规则及快照元数据。
- v1 缺少 PIR/PBS 时降级接受，规则固定为 `PIR=CIR`、`PBS=CBS`，逐记录写入降级明细；v4+ 返回 `E_FORMAT_UNSUPPORTED` 且不提交任何策略。
- JSON 数值使用 Decimal 解析；单位换算必须得到整数内部值，否则返回 `E_UNIT_PRECISION`，超出无符号 64 位返回 `E_UNIT_OVERFLOW`。
- 时间戳截断规则支持 `preserve_us`、`floor_ms`、`ceil_ms`、`nearest_ms`、`toward_zero_ms`，逐记录输出余数和累计误差。
- 重复队列采用“后出现者获胜”，导入报告列出所有位置、胜出索引和规则。

字节布局、位图、CRC 范围和版本矩阵见 `docs/FORMAT_SPEC.md`。

## 仿真与诊断契约

每个队列维护 CIR/CBS 与 PIR/PBS 两个桶，初次见到该队列的首包时两桶均为满。令牌以 `1e-6 bit` 整数定点累计：

```text
令牌亚比特增量 = rate_bps × elapsed_us
桶容量亚比特 = burst_bytes × 8 × 1_000_000
```

绿色到达包优先消耗承诺桶和峰值桶；承诺桶不足但峰值桶可用时标记黄色；峰值桶也不足时红色丢弃。黄色到达包不能重新变为绿色，红色到达包直接红色丢弃。输出包含每包前后令牌余量、动作、发送/丢弃/标记字节以及各队列累计统计。

诊断使用 Decimal 相对容差比较实际发送速率与 CIR，不采用裸浮点比较。比例固定输出 6 位小数，舍入方式为 `ROUND_HALF_UP`，并输出累计速率舍入误差。无流量队列单列为 `no_traffic`，不会误报欠发；清单均按队列标识稳定排序。

## 快照、并发与强杀恢复

- 快照策略、序号、生成时间和校验和以 `snapshot_seq` 为不可变版本边界；读者只看到一个完整版本，不会混合旧/新策略。
- 导入、快照切换与仿真批次进入同一进程级写者锁；SQLite 使用 WAL，允许并发读者。
- 每次写操作先以独立事务持久化 `intents.state=started`，再执行 `BEGIN IMMEDIATE` 业务事务，并在同一事务内把意图改为 `committed`。
- 快照序号必须递增；相同序号且内容校验和一致时幂等返回，相同序号不同内容或更低序号返回 `E_SNAPSHOT_INVALID`，旧快照继续可用。
- 进程被杀后，未提交 SQLite 事务由数据库回滚。启动扫描 `started` 意图并将其标记 `aborted`；未完成的仿真批次只记录为中止，`result_json`/`diagnostics_json` 保持 `NULL`。
- 恢复报告只陈述已持久化事实，不承诺强杀后生成完整退出报告或完整仿真结果。

## 验证结果

运行：

```bash
python scripts/run_validation.py
```

最近一次结果（`reports/validation_report.json`）：

- 环境：Python 3.14.4、FastAPI 0.115.6、Uvicorn 0.34.0、NumPy 2.2.1、SQLite 3.46.1。
- pytest：`43 passed`。
- 双格式互操作：v3 JSONL、little-endian、big-endian 解码快照完全相同；快照校验和 `bd2b0542...002b51`；三种格式策略内容 SHA-256 一致。
- v1：JSONL 与二进制降级结果一致，共记录 4 条策略的 PIR/PBS 补齐明细。
- 仿真：255 包，重复运行逐包及逐队列统计完全一致；绿/黄/红包数 `109/79/67`。
- 强杀：子进程以退出码 `91` 在持有未提交事务时被直接终止；重启后仅保留快照 1，崩溃意图为 `aborted`，崩溃批次结果和诊断为 `NULL`。
- 真实回环 HTTP：验收脚本选择空闲端口启动 Uvicorn 子进程，健康、导入、255 包仿真、big-endian 导出和诊断请求均成功。
- 五项验收检查：双格式互操作、确定性仿真、强杀恢复、pytest、真实回环 HTTP 全部为 `true`。

详细事实见 `reports/validation_report.json`，pytest 输出见 `reports/pytest.txt`。

## 已知边界

- 仅提供回环地址 HTTP API，不含鉴权、TLS 或远程多租户能力。
- 运行模型为单进程单 Uvicorn worker；数据库仍启用 WAL 和事务保护。
- `snapshot_no` 受 SQLite `INTEGER` 上限约束为 `2^63-1`；速率/突发/时间字段在策略表内以十进制文本保存，可覆盖无符号 64 位范围。
- 仿真只处理离线确定性样本，不连接真实网卡、NetFilter、eBPF 或 QoS 设备。
- 强制终止恢复基于意图记录和事务原子性，不试图恢复进程内未持久化的计算结果。
