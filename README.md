# DNS Workbench

面向本地回环环境的 DNS 递归解析缓存中间件与诊断工作台。项目只访问 `127.0.0.1` 上的确定性权威测试服务，不依赖真实域名、外部 DNS 或线上站点；同时提供 UDP/TCP DNS 入口、带 TTL 的正负缓存、并行上游调度、单飞合并、预算裁决、SQLite 持久化恢复和 HTTP 诊断接口。

## 交付内容

- `src/dns_workbench/protocol.py`：严格 DNS 编解码，支持 A、AAAA、CNAME、MX、TXT、NS、SOA、域名压缩指针、标签长度/名称总长校验和截断处理。
- `src/dns_workbench/resolver.py`：逐跳向配置的本地权威服务器查询，追踪 CNAME 链，执行最大跳数、环检测、SOA 负缓存 TTL 和每跳超时。
- `src/dns_workbench/scheduler.py`：全局并发、每权威并发、轮次预算、优先级、有界队列、单飞登记和合并/拆分裁决闸门。
- `src/dns_workbench/service.py`：缓存、会话、幂等、合并、缓存提交、调度轨迹和预算跳过编排。
- `src/dns_workbench/storage.py`：SQLite WAL 单写者事务、缓存/会话/意图/轮次/恢复报告。
- `src/dns_workbench/listener.py`：真实 UDP 与 TCP DNS 侦听器及原始 DNS 应答。
- `src/dns_workbench/api.py`：FastAPI 诊断 API 和统一 JSON 错误。
- `src/dns_workbench/authority.py`：确定性本地权威测试服务，支持 UDP/TCP、延迟、丢弃和截断行为。
- `samples/`：固定权威区数据和查询样本。
- `tests/`：协议、缓存 TTL、调度、合并、超时、监听器、API、幂等和恢复测试。
- `config.example.json`、`scripts/setup_windows.ps1`、`scripts/run_demo.ps1`：Windows 本地启动入口。
- `docs/TECHNICAL.md`、`docs/VERIFICATION.md`：设计说明和本次实测记录。

## Windows 快速开始

要求 Python 3.11 或更高。所有端口均为 `127.0.0.1` 高位端口，无需管理员权限。

```powershell
cd <project-root>
.\scripts\setup_windows.ps1
```

脚本默认通过 Windows Python Launcher 的 `py -3` 选择最新的 Python 3；如需指定解释器，可传入例如 `-Python "py -3.12"`。

脚本会：

1. 创建 `.venv`，使用 `--system-site-packages` 以便优先复用本机已缓存/已安装依赖。
2. 若存在 `wheelhouse/`，执行 `pip install --no-index --find-links wheelhouse -r requirements.txt`。
3. 无 wheelhouse 且未指定 `-AllowNetwork` 时，只验证系统可用包，不访问外网。
4. 以 `--no-build-isolation --no-deps` 安装本项目并运行测试。

启动完整本地环境（HTTP + DNS UDP/TCP + 两个权威测试服务）：

```powershell
.\.venv\Scripts\python.exe -m dns_workbench serve --config config.example.json
```

默认端点：

- HTTP 诊断：`http://127.0.0.1:18080`
- DNS UDP/TCP：`127.0.0.1:15353`
- 权威测试服务：`127.0.0.1:15361`、`127.0.0.1:15362`

可用标准 DNS 工具或 Python 客户端直接查询，例如：

```powershell
Resolve-DnsName -Name a.lab.test -Type A -Server 127.0.0.1 -DnsOnly
```

## 可重复演示

```powershell
.\scripts\run_demo.ps1
```

演示脚本会使用固定权威区数据和固定查询样本完成：

1. 冷缓存解析 A 记录，并按大小写混写查询验证同一缓存键。
2. 12 个热点并发查询触发单飞合并，确认只有一个上游请求且所有等待者 TTL 相同。
3. 70-80ms 轮次预算下产生确定性跳过，输出派发、等待和跳过清单。
4. 真实上游丢弃响应，返回 `E_UPSTREAM_TIMEOUT`，并确认活动套接字归零。
5. 查询进行中强制终止服务进程，重启后扫描 SQLite 事实，报告已中止会话并验证已完成缓存仍命中。

输出和机器可读报告默认写入 `artifacts/demo/`。本次实测结果见 `docs/VERIFICATION.md`。

## DNS 行为

### 严格报文处理

- 入站 UDP 和 TCP 都按 DNS 帧格式解析；TCP 使用两字节长度前缀。
- 压缩指针必须向后，必须指向已解析过的标签起点，并禁止越界、前向和循环。
- 单个标签最多 63 字节，展开域名最多 255 字节；计数、RDLENGTH、TXT 长度和已知 RR 的 RDATA 必须完全一致。
- 畸形报文、截断报文、非法压缩指针或不支持的结构返回业务码 `E_MESSAGE_INVALID`，监听器发送 FORMERR，且不会创建解析会话。
- UDP 应答超过配置大小（默认 1232 字节）时设置 TC；上游收到 TC 时会关闭 UDP 套接字并改用 TCP 重新查询。

### 解析与缓存

- 查询名统一为小写、保留末尾点、保留转义标签；缓存键为 `规范化域名|类型|类别`。
- 正向缓存保存完整 CNAME 链和最终记录；条目寿命取所有已缓存记录 TTL 的最小值。
- NXDOMAIN 与 NODATA 使用 SOA 的 `min(SOA TTL, SOA.MINIMUM)` 作为负缓存寿命。
- 缓存条目保存原始 TTL 和 `written_at_ms`；查询时以毫秒计算剩余寿命，DNS 线格式按秒向上取整。
- 过期条目不命中，并在查询或缓存 API 访问时清理。
- CNAME 环返回 `E_CNAME_LOOP`；超过 `max_cname_hops` 返回 `E_CHAIN_TOO_LONG`。会话记录最终 RCODE、来源记录类型、查询链和上游跳数。

## 调度与合并语义

每轮从缓存命中和幂等复用开始，仅将未命中且互不相同的上游任务送入调度：

- 任务优先按显式 `priority`、热点标记/热点名和到达顺序排序。
- 准入容量为 `global_upstream_limit + queue_limit`，按唯一上游任务计数；同一域名等待者不重复占用准入名额。
- 超过准入容量的任务返回 `E_QUEUE_FULL`，并在轮次 `skipped` 清单中记录原因和数值。
- 跨轮共用同一个有界并发闸门；已有活动任务占满全局名额时，超过 `queue_limit` 的跨轮等待者同样返回 `E_QUEUE_FULL`。
- 真正派发在 `ConcurrencyGate` 内原子获取全局名额和权威服务器名额；CNAME 跨权威时逐跳释放和重新获取。
- 同一缓存键使用全局单飞登记。首个任务成为 leader，其余请求等待同一 future；缓存事务提交后才完成 future，因此不会看到半写状态。
- 所有正常合并等待者复用同一份应答快照和同一个 TTL 起点。

### 预算冲突裁决

等待者周期性检查 leader 的保守完成时间：

```text
projected_shared_finish = leader_started_at + configured_upstream_timeout
eligible_to_split =
    merge_split_enabled
    and leader_already_started
    and leader_age >= merge_split_after_ms
    and remaining_round_budget >= min_split_budget_ms
    and projected_shared_finish > round_deadline - merge_split_reserve_ms
```

每个 flight 默认最多拆出一个长尾请求。裁决输出 `decision`、`reason`、`leader_age_ms`、`remaining_budget_ms`、`projected_shared_finish_ms`、`projected_shared_wait_ms` 和拆分次数等可核对数值。拆分请求不写缓存，规范合并 leader 仍是唯一缓存写入者。

### 时间预算

- 轮次预算覆盖准入、并发名额等待、合并等待和活动上游查询剩余时间。
- 活动上游查询的有效超时为 `min(configured_upstream_timeout, remaining_round_budget)`。
- 因轮次预算取消活动查询时返回 `E_BUDGET_EXHAUSTED`，上下文明确 `socket_closed=true`，不会伪装成 `E_UPSTREAM_TIMEOUT`。
- 只有配置的每跳上游超时真正发生时，才返回 `E_UPSTREAM_TIMEOUT`。

## 持久化与恢复

SQLite 使用 WAL、`synchronous=FULL` 和单写者串行事务。主要表：

- `cache_entries`：正/负缓存、原始 TTL、写入时刻、过期时刻、记录和 SOA 上下文。
- `sessions`：幂等指纹、查询状态、最终 RCODE、来源类型、应答和错误。
- `intent_log`：缓存写入意图，状态为 `pending`、`committed` 或 `rolled_back`。
- `rounds`：调度轨迹、派发/等待/跳过清单和完成状态。
- `recovery_reports`：每次启动基于已提交行生成的恢复报告。
- `runtime_meta`：首次启动和干净退出标记。

缓存写入顺序固定为：

1. 独立事务写入并提交 `pending` 意图。
2. 单事务写入/更新缓存条目、完成 leader 会话、把意图标记为 `committed`。

进程被强制终止后，下次启动会：

- 删除与未完成意图关联的缓存行；
- 把 `pending` 意图标记为 `rolled_back`；
- 把 `pending/resolving` 会话标记为 `aborted`，业务码 `E_PROCESS_INTERRUPTED`；
- 把仍在 `running` 的轮次标记为 `interrupted`；
- 仅依据 SQLite 已落盘事实生成报告，不伪造完整退出报告；
- 已完成缓存条目和会话保持可用。恢复后的同域名查询复用已完成结果（TTL 继续递减）。

## HTTP API

所有失败响应统一为：

```json
{
  "error": {
    "code": "E_CNAME_LOOP",
    "message": "CNAME chain contains a loop",
    "context": {"chain": ["loop1.lab.test.", "loop2.lab.test.", "loop1.lab.test."]}
  }
}
```

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/v1/queries` | 单查询；支持 `name/type/qclass/query_id/priority/hot/round_budget_ms` 或 `wire_hex` |
| `POST` | `/api/v1/queries/batch` | 批量查询；`requests` 或 `queries`；可设置轮次预算和 `fail_fast=true` |
| `POST` | `/api/v1/wire/query` | 提交原始 DNS 十六进制报文，返回 JSON 与 DNS 线格式应答十六进制 |
| `GET` | `/api/v1/cache?name=...&type=...` | 按规范化域名和类型查询缓存及剩余 TTL |
| `GET` | `/api/v1/cache/by-name/{name}` | 路径形式查询缓存 |
| `GET` | `/api/v1/cache/key/{cache_key}` | 精确缓存键查询；不存在/过期返回 404 |
| `GET` | `/api/v1/sessions/{query_id}` | 会话状态、RCODE、来源类型、应答和所属轮次轨迹 |
| `GET` | `/api/v1/rounds/{round_id}` | 派发、等待、跳过、合并裁决及轮次会话 |
| `GET` | `/api/v1/recovery` | 最近一次启动恢复报告 |
| `GET` | `/api/v1/recovery/reports?limit=20` | 历史恢复报告 |
| `GET` | `/api/v1/health` | 并发闸门、单飞、上游套接字和监听器状态 |

单查询示例：

```powershell
curl.exe -s http://127.0.0.1:18080/api/v1/queries `
  -H "Content-Type: application/json" `
  -d '{"name":"cname.lab.test.","type":"A"}'
```

批量查询示例：

```powershell
curl.exe -s http://127.0.0.1:18080/api/v1/queries/batch `
  -H "Content-Type: application/json" `
  -d '{"requests":[{"name":"hot.lab.test.","type":"A"},{"name":"hot.lab.test.","type":"A"}],"round_budget_ms":1000}'
```

## 配置

`config.example.json` 是完整示例。关键字段：

- `db_path`、`log_dir`：SQLite 与日志位置。
- `http_host/http_port`、`dns_host/dns_port`：仅绑定回环地址。
- `enable_dns_listener`、`enable_authorities`：是否启动 DNS 入口和随进程权威测试服务。
- `max_cname_hops`、`max_udp_response_bytes`：链长与 UDP 截断限制。
- `scheduler.global_upstream_limit`：全局上游并发上限。
- `scheduler.per_authority_limit`：单权威并发上限。
- `scheduler.queue_limit`：有界等待队列长度。
- `scheduler.round_budget_ms`：默认每轮预算。
- `scheduler.upstream_timeout_ms`：每跳上游真实超时。
- `merge_split_*`：合并/拆分裁决阈值。
- `authorities[]`：权威 ID、回环地址、端口、负责区和固定区文件。

## 验证

```bash
python -m pytest -q
python -m dns_workbench demo --workdir artifacts/demo
```

本次实际验证结果见 `docs/VERIFICATION.md`：37 项测试通过，演示报告为 `PASS`。

## 边界与约束

- 仅面向本地回环测试环境；权威地址必须显式配置。
- 未实现 DNSSEC、递归根提示、EDNS Client Subnet 或任意公网递归。
- 域名比较按 DNS ASCII 大小写不敏感规则；未做 IDNA 转换。
- 合并在轮次预算冲突时允许每个 flight 有限次拆分；拆分结果不写缓存。
- 进程内单飞登记是权威的并发协调器，SQLite 是唯一持久化事实源。

