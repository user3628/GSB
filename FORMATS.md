# 输入格式与稳定业务码

本文描述内置生成器与解析器共享的线格式。所有多字节整数在二进制日志中均为小端，文本长度前缀除外。TTL 在二进制中也是小端 `uint32` 秒；JSON 与文本使用十进制秒。

## 1. 二进制探针日志（DNSB v1/v2）

### 文件头

| 偏移 | 类型 | 字段 |
|---:|---|---|
| 0 | 4 bytes | 魔数 ASCII `DNSB` |
| 4 | `uint16` LE | 格式版本：1 或 2 |
| 6 | `uint32` LE | 事件记录数 |

v1 的第一条事件从偏移 10 开始。v2 在记录区之前增加：

| 类型 | 字段 |
|---|---|
| `uint16` LE + bytes | 应答来源地址 |
| `uint16` LE + bytes | 解析器标识 |

未知魔数返回 `E_BINARY_MAGIC`，未知版本返回 `E_UNKNOWN_VERSION`，任何越界读取返回 `E_BINARY_TRUNCATED`，错误均带文件字节偏移。

### 事件记录

| 类型 | 字段 |
|---|---|
| `int64` LE | 相对观察起点的纳秒偏移 |
| `uint32` LE | 事务标识 |
| `uint16` LE | DNS 标志位 |
| `uint16` LE | 问题类型 |
| `uint8` | 角色：0=query, 1=response, 2=cache |
| `uint16` LE + bytes | 问题名原始字节 |
| `uint16` LE | 资源记录数 |

### 资源记录

| 类型 | 字段 |
|---|---|
| `uint8` | 区段：0=answer, 1=authority, 2=additional, 3=cache |
| `uint16` LE | RR 类型 |
| `uint32` LE | TTL 秒（小端） |
| `uint16` LE + bytes | owner 名 |
| `uint16` LE + bytes | RDATA |

A 的 RDATA 必须为 4 字节，AAAA 必须为 16 字节；其他类型保留十六进制数据。角色/区段未知或 A/AAAA 长度错误返回 `E_BINARY_FIELD`。

## 2. 逐行 JSON 摘要

每行一个对象。空行忽略；字段示例：

```json
{"time":"2024-01-01T00:00:00.00001005Z","txid":"0x1001","flags":"0x8180","role":"response","qname":"Cross.Example.","qtype":1,"answers":[{"name":"cross.example.","type":"A","ttl":120,"address":"192.0.2.1"}],"additionals":[],"cache":[]}
```

- `time`：RFC3339/RFC3339Nano。
- `txid`：无符号 32 位 JSON number，或十进制/`0x` 十六进制字符串。
- `flags`/`qtype`/`ttl`：number 或字符串；标志支持 `0x`。
- `role`：`query`、`response`、`cache` 及少量兼容别名。
- 数组：`answers`、`authorities`、`additionals`、`cache`。
- RR 的 `name` 为空或 `@` 时继承问题名；A/AAAA 地址可放 `address` 或 `data`。

语法错误返回 `E_JSON_SYNTAX`，字段类型/地址/角色错误返回 `E_JSON_FIELD`，均定位到该行起点。

## 3. 旧版长度前缀文本块

文件由连续块组成。每块先有 4 字节大端 `uint32` 长度，然后是指定长度的 UTF-8 文本：

```text
txid=0x2004
flags=0x8180
role=cache
qname=ttl.example.
qtype=1
filetime=1334854080000145000
cache=ttl.example.|A|30|192.0.2.20
```

- `txid` 支持十进制、`0x` 十六进制和裸十六进制字母写法。
- `filetime`：Windows FILETIME，1601 纪元、100 ns 单位、十进制 `uint64`。
- `answer`/`authority`/`additional`/`cache`：`owner|type|ttl|address`。
- owner 为空或 `@` 时继承问题名；TTL 为十进制。

长度前缀超出剩余字节或超过配置块上限返回 `E_TEXT_LENGTH`，键值/角色/十进制字段非法返回 `E_TEXT_FIELD`。

## 4. 名称与事务标识规范化

- 问题名先做 ASCII 小写，去掉最后一个根点。
- 标签不能为空、不能超过 63 字节，完整 wire 形式不能超过 255 字节。
- 配对键采用 `{label_len}{lower_label}...0` 的标签长度序列，而不是普通字符串比较。
- 事务标识最终统一为 `uint32` 视图；文本与 JSON 十六进制写法同样进入该视图。
- 同窗口内同一事务标识若映射到多个不同问题名，严格模式返回 `E_TXID_QNAME_CONFLICT`。

## 5. 固定时间换算

令 `T=10_000_000` 为每秒刻度数：

- RFC3339：`ticks = unix_seconds*T + unix_nanoseconds/100`，截断 `unix_nanoseconds%100` ns。
- 相对纳秒：`ticks = observation_start_ticks + relative_ns/100`，截断 `abs(relative_ns%100)` ns。
- FILETIME：`ticks = int64(filetime) - 116_444_736_000_000_000`，无截断。

所有乘法、加法都做 int64 溢出检查；RFC3339 纳秒必须在 `[0,1e9)`；FILETIME 必须落在可表示 int64 Unix 刻度范围内。失败返回 `E_TIME_OVERFLOW`，不追加账本条目。

时间区间按“刻度 ± 1 tick（仅当存在亚刻度截断）”构造。只有候选区间完全处于查询关联边界内才直接裁决；区间跨越边界返回不确定关联。

## 6. 内置六类畸形样本

| 文件/对象 | 断言业务码 | 注入内容 |
|---|---|---|
| `malformed/truncated.dnb` | `E_BINARY_TRUNCATED` | 记录在问题名中途截断 |
| `malformed/unknown-version.dnb` | `E_UNKNOWN_VERSION` | 不支持的版本头 |
| `malformed/length-overrun.txt` | `E_TEXT_LENGTH` | 长度前缀大于剩余块字节 |
| `malformed/txid-qname-conflict.jsonl` | `E_TXID_QNAME_CONFLICT` | 同窗口同 txid 两个问题名 |
| `malformed/filetime-overflow.txt` | `E_TIME_OVERFLOW` | FILETIME `uint64` 最大值 |
| `malformed/snapshot-gap.db` | `E_SNAPSHOT_VERSION` | 删除版本 2，保留连续前缀 1，并存在版本 3 |

测试逐个验证错误码与偏移，并断言失败加载或裁决不会推进已有快照版本。

## 7. 业务码速查

| 码 | 含义 |
|---|---|
| `E_BINARY_MAGIC` | 魔数错误 |
| `E_BINARY_TRUNCATED` | 二进制越界/截断 |
| `E_BINARY_FIELD` | 二进制角色、区段或 RDATA 非法 |
| `E_UNKNOWN_VERSION` | 二进制版本头未知 |
| `E_JSON_SYNTAX` | JSON 行语法错误 |
| `E_JSON_FIELD` | JSON 字段/地址/角色非法 |
| `E_TEXT_LENGTH` | 文本长度前缀非法 |
| `E_TEXT_FIELD` | 文本键值或记录非法 |
| `E_TXID` | 事务标识不是 32 位整数 |
| `E_DNS_NAME` | DNS 名称非法 |
| `E_TIME_OVERFLOW` | 定点时间换算溢出 |
| `E_TIME_FORMAT` | 时间文本格式非法 |
| `E_EVENT_BUDGET` | 单会话事件数超限 |
| `E_WINDOW_BUDGET` | 单会话时间跨度超限 |
| `E_TXID_QNAME_CONFLICT` | 同窗口 txid/问题名冲突 |
| `E_SNAPSHOT_VERSION` | 快照版本链跳号或版本缺失 |
| `E_SNAPSHOT_REPLACED` | 内容摘要/前序链/信任锚不一致 |
| `E_SNAPSHOT_FORMAT` | 快照载荷格式错误 |
| `E_CONFIG` | 配置非法 |
| `E_SOURCE_KIND` | 来源类型不可识别 |
| `E_IO` | 本地文件/SQLite I/O 错误 |

