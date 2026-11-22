# Loopback Margin Lab

面向本地回环环境的丢包与网络故障诊断工作台。项目从空工作区构建，包含真实 TCP 探测、确定性样本、两种可互换持久化格式、格式协商、归一化、SQLite 意图日志/崩溃恢复、统计与根因分析，以及无外部 CDN 依赖的可交互页面。

全部探测与指标采集目标限定为 `localhost`、`127.0.0.0/8` 和 `::1`。项目不访问真实站点、外部 DNS 或网络设备。

## 快速开始（Windows PowerShell）

```powershell
.\scripts\setup.ps1
.\scripts\start.ps1
```

打开 `http://127.0.0.1:8000/`。默认数据库为 `data\diaglab.sqlite3`。

手动准备命令等价于：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts\make_samples.py
.\.venv\Scripts\python.exe -m uvicorn diaglab.app:app --host 127.0.0.1 --port 8000
```

Linux / WSL：

```bash
./scripts/setup.sh
./scripts/start.sh
```

运行测试和端到端演示：

```powershell
.\scripts\test.ps1
.\scripts\demo.ps1
```

```bash
./scripts/test.sh
./scripts/demo.sh
```

演示会启动真实回环测试服务与真实 HTTP 工作台，使用固定样本执行两次导入/导出、成功/拒绝/超时探测、指标采集、统计与根因查询，并强制终止一个持有未提交事务的写进程，随后重启工作台读取恢复差异。结果写入 `artifacts/demo/e2e_report.json`。

## 目录

```text
diaglab/
  formats.py          二进制/JSONL 编解码、格式检测、兼容矩阵
  normalize.py        时间、单位与字段归一化审计
  stats.py            最近秩分位、丢包/抖动、补偿求和、根因排序
  probe.py            真实 TCP 回环探测
  test_service.py     echo/hang TCP 测试服务与吞吐/错误指标
  service_metrics.py  回环指标 HTTP 采集
  store.py            SQLite 单写者、幂等批次、快照读、意图恢复
  app.py              FastAPI HTTP API 与页面服务
  web/                本地可视化页面（HTML/CSS/JS）
scripts/
  make_samples.py     生成确定性样本
  test_server.py      echo/hang 本地测试服务
  crash_worker.py     强制终止演示用的未提交导入进程
  e2e_demo.py         端到端演示
  setup/start/test/demo 脚本（PowerShell + Bash）
samples/              固定二进制、JSONL、损坏与不支持版本样本
tests/                格式、归一化、统计、探测、存储、API 测试
```

## 内部记录模型

归一化记录的核心八字段：

| 字段 | 类型 | 含义 |
|---|---|---|
| `timestamp_ms` | `uint64` | 内部毫秒时间戳 |
| `probe_id` | UTF-8 string | 探测标识 |
| `path_node` | UTF-8 string | 路径节点 |
| `protocol` | `tcp \| udp \| icmp` | 探测协议；真实采集当前为 TCP |
| `success` | bool | 成功标志 |
| `rtt_us` | `uint32` | 往返时延，微秒 |
| `payload_bytes` | `uint32` | 载荷字节数 |
| `failure_type` | `none \| refused \| timeout \| reset \| error` | 失败分类 |

归一化审计还保留 `rounding_bit`、`sub_ms_remainder_us`、`rounding_error_us`、`rtt_missing`、`payload_missing`、来源格式与版本。统计只把 `success=true`、`failure_type=none` 且 `rtt_us>0` 的记录视作有效 RTT 样本；旧版缺省补出的 `0` 不会被当作有效时延。

## 紧凑二进制格式：DXPK

固定头 16 字节，所有多字节整数按头内字节序标记解释：

| 偏移 | 长度 | 字段 |
|---:|---:|---|
| 0 | 4 | 魔数 `DXPK` |
| 4 | 1 | 格式版本：`1` 旧版降级，`2` 当前完整版 |
| 5 | 1 | 大小端标记：`0x4C` little-endian，`0x42` big-endian |
| 6 | 1 | RTT 单位代码：`0=us`，`1=ms` |
| 7 | 1 | 载荷单位代码：`0=B`，`1=KiB` |
| 8 | 2 | 头标志；bit0 表示未知必填特性，遇到即 `E_FORMAT_UNSUPPORTED` |
| 10 | 4 | 记录数 |
| 14 | 2 | 头长度，当前为 16 |

每条记录重复以下结构：

```text
uint32 payload_length
byte   payload[payload_length]
uint32 crc32(payload)
```

`payload` 内部：

```text
uint8  presence_bitmap      bit0=RTT 存在，bit1=载荷存在
uint64 timestamp_ms
uint8  protocol_code
uint8  success
uint8  failure_code
uint16 probe_id_utf8_length + bytes
uint16 path_node_utf8_length + bytes
[uint32 rtt_value]          仅 bit0 存在时
[uint32 payload_value]      仅 bit1 存在时
```

- v2 要求 RTT 与载荷位均存在；v1 允许缺失。
- 任一 CRC 不符、长度前缀越界/过大、字段实际长度与长度前缀不一致，均返回 `E_FRAME_CORRUPT`。
- 头部声明未知必填标志或版本高于 2，返回 `E_FORMAT_UNSUPPORTED`。

## 分片 JSONL 格式

每行一个 JSON 对象，不带单独头行：

```json
{"failure_type":"none","format":"jsonl","path_node":"local-edge-a","payload_unit":"B","payload_value":64,"probe_id":"probe-a-001","protocol":"tcp","rtt_unit":"us","rtt_value":1200,"success":true,"timestamp_us":1704067200000250,"version":2}
```

必需字段为 `format`、`version`、`timestamp_us`、`probe_id`、`path_node`、`protocol`、`success`、`failure_type`。v2 还要求 `rtt_value/rtt_unit` 与 `payload_value/payload_unit`；v1 可省略。所有记录的 `version` 必须一致，混用版本直接拒绝。`required_features` 中出现当前工具未知的必填特性时返回 `E_FORMAT_UNSUPPORTED`。

## 格式协商矩阵

| 格式 | 版本 | 结论 | 行为 |
|---|---:|---|---|
| binary / JSONL | 1 | `degrade_accept` | 缺失 RTT 补 `rtt_us=0`，缺失载荷补 `payload_bytes=0`，写降级明细 |
| binary / JSONL | 2 | `accept` | 完整接受 |
| binary / JSONL | ≥3 | `reject` | `E_FORMAT_UNSUPPORTED`，不创建提交版本 |

兼容矩阵可由 `GET /api/formats/compatibility` 查询。成功/降级导入的裁定与明细写入 `negotiations` 表，可通过 `GET /api/negotiations` 或 `GET /api/imports/{batch_id}` 查询。

## 时间与量纲归一化

- 二进制源时间戳单位为毫秒，直接成为内部 `timestamp_ms`，无舍入。
- JSONL 源时间戳单位为微秒。调用方通过 `timestamp_policy` 声明 `truncate`（别名 `trunc`）、`floor`、`ceil` 或 `half_up`（别名 `round`）。
- 每条记录输出 `sub_ms_remainder_us`、`rounding_bit` 和 `rounding_error_us = timestamp_ms×1000 - source_timestamp_us`。
- 累计舍入误差是逐记录整数误差之和，不使用浮点累加。
- 二进制头与 JSONL 行分别声明 RTT/载荷单位。支持 `us/ms/s` 与 `B/KiB/MiB/KB/MB`，统一换算为整数微秒与字节。
- 负数、未知单位或换算后超过 `uint32` 的最大值会整批拒绝并返回 `E_UNIT_INVALID`，不会写入任何记录。

## 真实 TCP 探测

`POST /api/probes` 只接受回环目标。探测流程为：

1. 先持久化意图记录。
2. 创建真实 TCP 套接字，按声明超时连接。
3. 发送确定性载荷（ASCII `D` 重复），半关闭写端，等待完整回显。
4. 连接成功且完整回显：`success=true`，记录真实往返微秒。
5. 明确拒绝：`failure_type=refused`，业务码 `E_CONNECTION_REFUSED`，HTTP 502。
6. 连接或读取超时：`failure_type=timeout`，业务码 `E_PROBE_TIMEOUT`，HTTP 504。
7. 所有路径均在 `finally` 中关闭套接字，并返回 `socket_closed=true`。

失败探测结果同样会被完整提交并可查询、导出。响应分别使用 HTTP 502/504，但诊断记录已经持久化，可通过 `batch_id` 查询。

本地测试服务：

```bash
python scripts/test_server.py --mode echo  --probe-port 38081 --admin-port 38082
python scripts/test_server.py --mode hang  --probe-port 38083 --admin-port 38084
python scripts/test_server.py --mode refuse --probe-port 38085 --admin-port 38086
```

`echo` 模式完整回显，`hang` 模式接受连接后不回显，`refuse` 模式只保留一个未监听的回环端口以触发真实 `ECONNREFUSED`。`/metrics` 返回 `accepted_connections`、`completed_probes`、`hung_connections`、`bytes_in`、`bytes_out`、`errors` 和 `throughput_bytes_per_sec`；工作台通过 `POST /api/service-metrics/poll` 采集并持久化。

## 统计数值契约

窗口统一为 `[start_ms, end_ms)`，按 `path_node` 分组。

- 丢包率：`failed / total`。百分比固定 2 位小数，比例固定 6 位小数，均使用 `Decimal` `ROUND_HALF_UP`；响应注明舍入方式。
- RTT 分位：确定性最近秩法。对排序后的 `n` 个样本，`rank = ceil(p·n)`，取第 `rank` 个样本（1 基），不插值。输出 `p50/p90/p95/p99`、`sample_count` 与方法说明。
- 抖动：按 `(timestamp_ms, ordinal)` 排序，计算相邻有效 RTT 的绝对差，输出 `mean(|RTT[i]-RTT[i-1]|)` 与 P95。均值固定 2 位小数并使用 `ROUND_HALF_UP`。
- 累计时延：使用 Neumaier 补偿求和，同时报告精确整数和、补偿结果、与精确和的绝对误差，以及保守上界 `(n+4)·2^-53·Σ|xi| + 4·2^-53·|Σxi| + 1e-9 µs`。
- 有效 RTT 样本少于 `min_samples`（默认 5）时，`GET /api/stats` 返回 `E_INSUFFICIENT_SAMPLES`，并包含 `required_samples`、`actual_samples`、总记录数；不输出零值伪分位。根因候选会保留可核验的丢包/失败证据，但把分位与抖动置为 `null` 并给出同码警告。
- 每次计算输出 `calculation_digest`，相同快照、窗口和样本重复计算应逐位一致。
- `GET /api/compare` 使用调用方声明的相对容差 `rel_tol` 与 `math.isclose` 比较两路径；不可比较项返回 `null`。

故障等级：

| 等级 | 判定 |
|---|---|
| `critical` | 丢包率 ≥50%，或 timeout 失败 ≥3，或抖动均值 ≥5000µs |
| `degraded` | 丢包率 ≥10%，或存在任一失败，或抖动均值 ≥2000µs |
| `healthy` | 其他 |

根因排序分数：

```text
score = 0.55 × loss_fraction
      + 0.25 × min(jitter_mean_us / 5000, 1)
      + 0.20 × failure_weight
```

`failure_weight` 按 timeout=1.0、refused=0.8、reset=0.6、error=0.5 加权平均。排序稳定性规则为分数降序、丢包率降序、路径名升序。

## SQLite、并发与恢复

核心表：`meta`、`intents`、`batches`、`commits`、`records`、`negotiations`、`stats_snapshots`、`service_metrics`、`recovery_events`。

- 进程内单一写者 `RLock` 串行化导入、探测、指标采集和统计重算；SQLite 事务使用 `BEGIN IMMEDIATE`。
- 每个只读查询使用独立连接与显式读事务，并采用 WAL 快照隔离；因此不会看到“部分记录已写入、统计仍旧版本”的中间状态。
- 每次提交原子地写入 `commits`、`records`、`negotiations`、物化 `stats_snapshots`，并推进 `meta.current_version`。
- 导入/采集/重算前先单独提交 `intents` 行；数据事务同时把意图标为 `complete`。
- 批次标识幂等：同批次、同内容和同操作复用已提交结果；内容或操作不一致返回 `E_CONFLICTING_BATCH`。
- 启动时扫描 `pending/running` 意图。SQLite 自动回滚未提交事务；恢复器把意图标为 `aborted`，若批次行未提交则补写一个 `aborted` 批次，并记录 `recovery_events`。
- `GET /api/recovery/report` 只读取已提交的 SQLite 行，报告最后完整版本、记录数、意图/批次状态、恢复事件以及 `planned/committed/difference` 差异。

强制终止演示使用独立写进程：它先提交意图，再在 `BEGIN IMMEDIATE` 中写入 3 条未提交记录并发布 marker；演示父进程执行 `kill` 后重启工作台。恢复报告应显示版本不变、当前记录不变、计划 3 / 已提交 0 / 差值 -3，批次状态为 `aborted`。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 可交互诊断页面 |
| GET | `/health`, `/api/health` | SQLite 健康与版本 |
| GET | `/api/formats/compatibility` | 显式兼容矩阵 |
| POST | `/api/preflight` | Base64 内容预检，不写数据库 |
| POST | `/api/imports` | 校验、归一化、幂等导入 |
| GET | `/api/imports/{batch_id}` | 批次、裁定、降级与意图明细 |
| GET | `/api/negotiations` | 最近格式裁定记录 |
| GET | `/api/overview` | 版本、计数、路径、最近批次与指标 |
| GET | `/api/paths` | 路径汇总 |
| GET | `/api/records` | 归一化记录与 RTT 时序数据 |
| GET | `/api/stats` | 单路径丢包、分位、抖动、累计与摘要 |
| GET | `/api/stats/snapshot` | 当前版本物化统计快照 |
| POST | `/api/stats/recompute` | 先写意图后重算统计版本 |
| GET | `/api/root-causes` | 故障等级与候选排序 |
| GET | `/api/compare` | 带相对容差的双路径比较 |
| POST | `/api/probes` | 真实回环 TCP 探测并提交 |
| POST | `/api/service-metrics/poll` | 采集回环测试服务指标 |
| GET | `/api/service-metrics` | 指标历史 |
| GET | `/api/exports/binary` | 导出 v2 二进制，可选大小端 |
| GET | `/api/exports/jsonl` | 导出 v2 JSONL |
| GET | `/api/recovery/report` | 已落盘事实恢复报告 |

失败响应统一为：

```json
{"ok": false, "code": "E_...", "message": "...", "details": {}}
```

主要业务码：`E_FRAME_CORRUPT`、`E_FORMAT_UNSUPPORTED`、`E_UNIT_INVALID`、`E_PROBE_TIMEOUT`、`E_CONNECTION_REFUSED`、`E_INSUFFICIENT_SAMPLES`、`E_CONFLICTING_BATCH`、`E_BATCH_ABORTED`、`E_BAD_REQUEST`。非回环目标由参数校验返回 `E_BAD_REQUEST`，并注明只允许本地目标。

## 固定样本

`scripts/make_samples.py` 生成并记录 SHA-256 到 `samples/manifest.json`：

| 样本 | 预期 |
|---|---|
| `binary_v2.bin` | v2 接受，21 条，三路径，包含 timeout/refused 事件 |
| `binary_big_endian_v2.bin` | 大端 v2，可无损解码 |
| `binary_v1_degraded.bin` | v1 降级接受，2 条缺 RTT，2 条缺载荷 |
| `binary_bad_crc.bin` | `E_FRAME_CORRUPT` |
| `binary_bad_length.bin` | 非法长度前缀，`E_FRAME_CORRUPT` |
| `binary_v3_unsupported.bin` | `E_FORMAT_UNSUPPORTED` |
| `jsonl_v2.jsonl` | 微秒时间戳、`ms` RTT、`KiB` 载荷单位换算 |
| `jsonl_v1_degraded.jsonl` | v1 降级与缺省审计 |
| `jsonl_unit_invalid.jsonl` | 负载荷，`E_UNIT_INVALID` |
| `jsonl_v3_unsupported.jsonl` | 高版本拒绝 |
| `jsonl_unknown_required.jsonl` | 未知必填特性拒绝 |

## 已验证结果

本仓库当前实测：`33 passed`，完整演示 20/20 检查通过。详细结果见 `VERIFICATION.md` 与 `artifacts/demo/e2e_report.json`。

页面数据全部来自本地 SQLite 与回环 HTTP/套接字；没有前端 CDN、外部字体或第三方运行时服务。
