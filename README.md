# dnsmw：本地回环递归 DNS 中间件

`dnsmw` 是一个只面向 `127.0.0.1` 的递归 DNS 中间件，用于上层网络诊断工具。它同时侦听 UDP 与 TCP，从本地确定性权威测试服务器逐跳递归，维护带独立 TTL 的正缓存与负缓存，并通过 SQLite 保存缓存提交意图、解析会话和恢复报告。

本项目不使用 Docker，不访问外部 DNS，也不依赖持久化文件格式互操作。核心实现位于 DNS 报文层、递归链路、缓存状态机和并发提交边界。

## 快速开始（Windows）

要求：Windows 10/11、Python 3.11、可绑定 `127.0.0.1` 高位端口。

```powershell
cd <project-root>
.\scripts\setup_windows.ps1
.\scripts\run_demo.ps1
```

等价的 PowerShell 手工步骤：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install dnslib==0.9.26 pytest==9.2.0
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts\demo.py
```

固定演示端口：

| 角色 | 地址 | 说明 |
| --- | --- | --- |
| 本地权威测试服务器 | `127.0.0.1:15353` | UDP + TCP 同端口 |
| 递归中间件 DNS | `127.0.0.1:15354` | UDP + TCP 同端口 |
| HTTP 管理接口 | `127.0.0.1:18080` | 只读 JSON |

手工启动服务：

```powershell
# 终端 1：确定性权威服务器
.\.venv\Scripts\python.exe -m dnsmw.authserver --port 15353

# 终端 2：递归中间件
.\.venv\Scripts\python.exe -m dnsmw.server `
  --udp-port 15354 --tcp-port 15354 --upstream-port 15353 `
  --upstream-timeout 0.8 --max-cname-hops 4 `
  --db runtime\middleware.sqlite3 --admin-port 18080
```

## 实际验证结果

为满足任务指定的 Python 版本，本轮在 WSL 中从源码安装 CPython 3.11.9 并重建 `.venv`；同时在宿主机 Windows 使用独立 CPython 3.11.9 运行时、独立 venv 和用户级临时目录完成同一套验证，不修改 Windows 系统 Python。

```text
Python 3.11.9
dnslib 0.9.26
pytest 9.2.0
```

实测命令与结果：

```text
.venv/bin/python -m pytest -q
32 passed in 34.27s

.venv/bin/python scripts/demo.py
PASS: 25/25 checks
results=artifacts/demo-results.json

# Windows 11 + CPython 3.11.9（独立 venv）
.venv\Scripts\python.exe -m pytest -q
32 passed in 34.81s

.venv\Scripts\python.exe scripts\demo.py
PASS: 25/25 checks
results=artifacts/windows-demo-results.json
```

演示中的关键实测事实：

- 无 EDNS 的 `big.example.test. TXT` 在 UDP 上返回 `TC=1`，应答为空 RRset，报文 34 字节；TCP 重传返回 14 条 TXT，2736 字节且 `TC=0`。
- 同一域名用 EDNS(0) 4096 查询时返回完整 14 条记录，响应 OPT 的 UDP 载荷回显为 4096；UDP、TCP、EDNS 三次读取只触发一次上游解析。
- `chain.example.test.` 返回 `CNAME(300) -> CNAME(120) -> A(30)`；1 秒后大小写混写命中缓存，TTL 分别变为 `299/119/29`，没有新增上游查询。
- NXDOMAIN 的 SOA 负缓存 TTL 为 45，NODATA 的 SOA TTL 为 15；两者缓存命中均保持 RCODE 并递减 TTL。
- 10 个大小写混写的并发重复请求只产生 1 次上游解析，`E_CNAME_LOOP`、`E_CHAIN_TOO_LONG`、`E_UPSTREAM_REFUSED`、`E_UPSTREAM_TIMEOUT` 均通过自定义 EDNS 选项回传。
- 每跳超时实测约 0.80 秒返回 SERVFAIL（Windows 为 0.812 秒），`active_sockets=0` 且 `sockets_opened=sockets_closed=15`。
- 强制 `kill` 后进程返回码为 `-9`；SQLite 中保留 `crash0.example.test.` 的 `running` 会话与 `pending` commit intent。重启后报告将其分别标记为 `interrupted/incomplete` 与 `abandoned`，未提交缓存行被清理；此前已提交的 CNAME 链缓存仍可命中且不访问上游。

机器可读明细见 `artifacts/demo-results.json` 与 `artifacts/windows-demo-results.json`。

## 目录结构

```text
dnsmw/
  codec.py      严格线格式校验、压缩指针、EDNS、按协议重编码与截断
  model.py      无压缩指针的语义记录、解析结果
  upstream.py   每跳独立超时、UDP->TCP 回退、套接字强制关闭
  resolver.py   CNAME 链、负缓存规则、并发单飞
  cache.py      SQLite 会话/意图/缓存状态机与恢复
  server.py     UDP/TCP 接入层、CLI
  admin.py      只读 HTTP 管理接口
  authserver.py 固定 zone 的本地权威测试服务器
  client.py     演示与测试用 DNS 客户端
scripts/
  demo.py       固定端口端到端演示，含真实强制终止恢复
  setup_windows.ps1 / run_demo.ps1
tests/          32 个协议、缓存、解析器与端到端测试
artifacts/demo-results.json
```

## 协议层行为

### 严格报文校验

解析前会遍历整个报文的 question、answer、authority、additional 区，校验：

- 头长度、各 section 数量、RDATA 长度和尾随字节；
- 标签长度 `0..63`、保留的 `01/10` 标签类型、完整域名展开后不超过 255 字节；
- 压缩指针不得指向头部、不得前向/自指、不得形成循环；
- CNAME/NS/MX/SOA 的压缩域名必须恰好落在 RDATA 边界内；
- TXT 字符串和 EDNS option 长度必须严格闭合；
- OPT 只能出现一次，owner 必须是根，version 必须为 0。

畸形请求返回 DNS `FORMERR`，且 `ANCOUNT=0`，不会生成伪造答案。中间件同时在结构化日志、HTTP `/stats` 的 `errors` 中记录业务码 `E_MALFORMED_MESSAGE`。

DNS 线协议没有通用“业务错误码”字段。因此，可安全解析且携带 EDNS 的请求会在响应 OPT 的自定义 option `65001` 中收到 ASCII 业务码；无 EDNS 或结构性畸形请求通过 DNS RCODE、HTTP 管理接口和日志暴露业务码。这是为了不破坏标准 DNS 报文语义。

### EDNS(0)、截断与协议一致性

- 无 OPT 的 UDP 客户端按传统 512 字节上限编码；
- 有 OPT 时读取客户端 UDP 载荷，响应回显 OPT、UDP 载荷和 DO 位；
- UDP 完整编码超限时返回 `TC=1`，只保留 question 与 OPT，不返回半截 RRset；
- TCP 不受 UDP 载荷限制，始终返回完整语义结果且 `TC=0`；
- 同一次缓存命中会针对当前请求重新编码，绝不回放旧 UDP 报文；
- 中间件响应设置 `AA=0`、`RA=1`；测试权威服务器设置 `AA=1`、`RA=0`。

### 缓存键

缓存键为：

```text
(规范化查询名, QTYPE, QCLASS)
```

规范化规则：ASCII 大小写折叠、始终补末尾点、逐标签转义 `.` 与 `\`，其他不可打印字节使用 `\DDD`。例如两个标签 `a.b`、`example` 会规范为 `a\.b.example.`。缓存不保存压缩指针或完整响应报文，而保存语义 RR。

## 递归与缓存语义

### CNAME 链

解析器从当前 owner 查找 CNAME；若权威应答一次带回多跳链，则在同一应答内继续消耗。否则逐跳再次查询。每一跳都：

- 使用独立上游超时；
- 记录会话进度；
- 单独保留原 TTL；
- 检测目标是否已出现，出现重复即 `E_CNAME_LOOP`；
- 链跳数超过 `--max-cname-hops` 即 `E_CHAIN_TOO_LONG`；
- 任何失败都终止并返回 SERVFAIL 业务结果，不写正缓存。

### 正/负缓存 TTL

正缓存保存每条 RR 的原始 TTL，命中时按同一已流逝时间分别递减；缓存整体在最短 RR TTL 到期时失效，因此不会返回“链首仍有效、链尾已过期”的不完整链。

负缓存按本任务要求采用不同来源：

| 类型 | 负缓存 TTL 来源 | 固定样本结果 |
| --- | --- | --- |
| NXDOMAIN | `SOA.MINIMUM` | SOA 最小值为 45，缓存 TTL 45 |
| NODATA | `min(SOA.TTL, SOA.MINIMUM)` | SOA TTL 15、最小 45，缓存 TTL 15 |

负缓存保存 RCODE、CNAME 链（如有）和 SOA。命中时返回原 RCODE，SOA TTL 按负缓存剩余时间递减。失败、超时、REFUSED、畸形上游应答永不进入缓存。

## 并发、原子提交与恢复

### 并发单飞

`RecursiveResolver` 以规范化缓存键维护 in-flight `Future`：

1. 首个请求成为 leader，建立会话并访问上游；
2. 相同键的其余请求成为 waiter，只等待 leader 的完整结果；
3. leader 完成后所有 waiter 用自己的协议、EDNS 载荷和查询名大小写重新编码；
4. 失败结果也只解析一次，不重复打上游。

### 单写者提交

SQLite 写入串行化。一次成功解析的提交过程为：

```text
resolution_sessions(status=running)
  -> cache_commit_intents(state=pending)         [先提交意图]
  -> BEGIN IMMEDIATE
       cache_entries + intent(state=committed)
       session(state=committed)
     COMMIT                                      [一条完整版本]
  -> 原子替换内存中的已提交视图
```

读取线程只看到提交前或提交后的完整版本，不会看到一条 CNAME 链只写入一部分的中间状态。

### 强制终止恢复

启动时扫描持久化事实：

- `resolution_sessions.status='running'`：标记为 `interrupted`，业务码 `E_PROCESS_INTERRUPTED`；
- `cache_commit_intents.state='pending'`：标记为 `abandoned`；
- 引用了非 committed intent 或非 committed session 的缓存行：删除；
- 已过期缓存行：删除；
- 已 committed 的会话与缓存：保留并重新载入内存。

恢复报告写入 `recovery_reports`，仅汇总 SQLite 中实际存在的记录，不猜测曾经发生过的解析内容。

## HTTP 管理接口

所有接口只绑定回环地址：

| 路径 | 内容 |
| --- | --- |
| `GET /health` | 端口、数据库、上游、运行时长 |
| `GET /stats` | 请求、缓存、上游套接字、错误计数、恢复摘要 |
| `GET /cache` | 当前已提交缓存及其剩余 TTL、来源记录类型 |
| `GET /sessions` | 最近解析会话状态 |
| `GET /intents` | 最近缓存提交意图 |
| `GET /recovery` | 最近一次启动恢复报告 |

## 固定 zone 样本

| 名称 | 记录/行为 |
| --- | --- |
| `www.example.test.` | A `192.0.2.10`、AAAA `2001:db8::10` |
| `chain.example.test.` | CNAME 300 -> `mid1` CNAME 120 -> `mid2` A 30 |
| `mail.example.test.` | MX 10 `mx1.example.test.` |
| `txt.example.test.` | TXT 两条字符串 |
| `big.example.test.` | 14 条 TXT，用于 512 截断/TCP 重传/EDNS |
| `example.test.` | NS、SOA（TTL 15，MINIMUM 45） |
| `missing.example.test.` | NXDOMAIN + SOA |
| `www.example.test. MX` | NODATA + SOA |
| `loop1/loop2` | 相互 CNAME，触发 `E_CNAME_LOOP` |
| `long0..long6` | 6 跳 CNAME，触发 `E_CHAIN_TOO_LONG` |
| `refused.example.test.` | 上游 REFUSED |
| `timeout.example.test.` | 上游延迟 3 秒，触发每跳超时 |
| `malformed.example.test.` | 上游响应带非法自指压缩指针 |
| `crash0 -> crash1 -> crash2` | 在 `crash2` 延迟，演示链中途强制终止 |
| `a\.b.example.test.` | 带点转义标签，用于规范化缓存键 |

权威服务器使用 `dnslib` 正常压包，因此应答真实包含压缩指针；测试还单独构造非法前向/自指/头部指针验证拒绝逻辑。

## 测试覆盖

```text
tests/test_codec.py     严格解析、压缩指针、标签上限、记录 roundtrip、EDNS、截断
tests/test_cache.py     TTL 递减/过期、单写者可见性、pending intent 清理与恢复
tests/test_resolver.py  CNAME 链、循环、超长、负缓存、超时、并发单飞
tests/test_e2e.py       UDP/TCP/EDNS、缓存复用、畸形请求、负缓存、并发、套接字释放
```

所有测试与演示只访问 `127.0.0.1`，没有外部权威服务器或未授权目标。

## 主要业务码

| 业务码 | DNS 结果 | 含义 |
| --- | --- | --- |
| `E_MALFORMED_MESSAGE` | FORMERR | 请求线格式非法/超长标签/非法压缩指针 |
| `E_CNAME_LOOP` | SERVFAIL | CNAME 目标回到链中已出现的名字 |
| `E_CHAIN_TOO_LONG` | SERVFAIL | 超过配置最大 CNAME 跳数 |
| `E_UPSTREAM_TIMEOUT` | SERVFAIL | 单跳上游超时，套接字已关闭 |
| `E_UPSTREAM_REFUSED` | SERVFAIL | 上游 RCODE=REFUSED |
| `E_UPSTREAM_MALFORMED_MESSAGE` | SERVFAIL | 上游报文/压缩指针非法 |
| `E_UPSTREAM_QUESTION_MISMATCH` | SERVFAIL | 上游 ID 对应但 question 不匹配 |
| `E_NEGATIVE_WITHOUT_SOA` | SERVFAIL | NXDOMAIN/NODATA 缺少负缓存 TTL 来源 |
| `E_PROCESS_INTERRUPTED` | 仅恢复报告 | 上次进程在提交前被强制终止 |

更多实现细节见 `docs/TECHNICAL.md`。
