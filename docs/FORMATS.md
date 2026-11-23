# 三种观测源格式

所有解析器都把事件转换为同一个 `Event` 模型。事件保留源相对路径、原始文件字节偏移、源格式、解析器版本、32 位事务号、标志位、规范化问题名、采样标记、应答记录与附加记录。

## 二进制报文日志

文件由连续记录组成，每条记录由公共前缀、版本相关头部、问题名和资源记录组成。多字节长度字段始终是大端，便于未知版本按长度跳过。

### 公共前缀（从记录起点计算）

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 4 | magic | ASCII `DNSL` |
| 4 | 1 | version | `1` 或 `2` |
| 5 | 4 | record_length | 整条记录字节数，大端 `uint32` |

### v1 头部布局

v1 记录头部总长 26 字节，正文从记录起点偏移 26 开始。

| 偏移 | 长度 | 字段 | 编码 |
| --- | --- | --- | --- |
| 9 | 1 | event_kind | 0=query, 1=response, 2=cache |
| 10 | 2 | flags | 大端 `uint16` |
| 12 | 4 | txid | 大端 `uint32` |
| 16 | 4 | sample_tick | 大端 `uint32` |
| 20 | 2 | qname_length | 大端 `uint16` |
| 22 | 2 | answer_count | 大端 `uint16` |
| 24 | 2 | additional_count | 大端 `uint16` |

### v2 头部布局

v2 头部总长 32 字节，正文从记录起点偏移 32 开始。字段顺序和字节序都与 v1 不同。

| 偏移 | 长度 | 字段 | 编码 |
| --- | --- | --- | --- |
| 9 | 4 | txid | 小端 `uint32` |
| 13 | 1 | event_kind | 0=query, 1=response, 2=cache |
| 14 | 2 | flags | 小端 `uint16` |
| 16 | 8 | sample_tick | 小端 `uint64` |
| 24 | 2 | qname_length | 小端 `uint16` |
| 26 | 2 | answer_count | 小端 `uint16` |
| 28 | 2 | additional_count | 小端 `uint16` |
| 30 | 2 | parser_build | 小端 `uint16`，保留/构建号 |

### 问题名与资源记录

问题名使用 DNS 线格式：每个标签为 `[1 字节长度][标签正文]`，以 `0x00` 结尾。解析后统一转小写、去掉根尾点，并用标签长度序列作为内部 `name_wire` 身份。报告中的 `name_wire` 是规范化线格式的十六进制表示。

正文依次是问题名、`answer_count` 条应答记录、`additional_count` 条附加记录。每条记录：

1. 记录名（线格式）；
2. type、class、TTL、rdata_length；
3. rdata。

v1 的 type/class/TTL/rdata_length 全部使用大端；v2 全部使用小端。因此 v1 与 v2 的 TTL 字节序不兼容，不能按同一偏移直接读取。

| type | rdata 解释 |
| --- | --- |
| 1 (`A`) | 4 字节 IPv4 |
| 28 (`AAAA`) | 16 字节 IPv6 |
| 其它 | 保留十六进制 rdata |

未知 `version` 的记录使用公共 `record_length` 整体跳过，并记录 `E_VER_UNSUPPORTED`。声明长度超过剩余字节时记录 `E_TRUNCATED` 并停止该源。

## JSONL 摘要

每行一个完整 JSON 对象。空行跳过；每个非空行是一个事件。字节偏移为该行起点。字段：

```json
{
  "event": "query|response|cache",
  "txid": 4097,
  "flags": 33152,
  "parser_version": "v1|v2",
  "sample": "S000002",
  "name": "Cache.Example.Test.",
  "answers": [
    {"name": "cache.example.test", "type": "A", "class": 1, "ttl": 60, "address": "192.0.2.10"}
  ],
  "additionals": [
    {"name": "evil.additional.test", "type": "A", "class": 1, "ttl": 999, "address": "203.0.113.66"}
  ]
}
```

- `txid` 和 `flags` 接受 JSON 数字或字符串；字符串事务号支持十进制与 `0x` 十六进制。
- `parser_version` 缺省为 `v1`；观察到未知值时记录 `E_VER_UNSUPPORTED` 并跳过该行。
- `sample` 可写成 `S000001`、`sample-001` 或整数；缺少时为 `S000000`。
- `kind` 可替代 `event`；`qname` 可替代 `name`。
- `additional` 可作为 `additionals` 的兼容别名。
- 记录 `name` 为空时继承问题名。

## 长度前缀文本块

文件由连续文本块组成。块语法为：

```text
<decimal_length>:<payload>\n
```

`decimal_length` 是 payload 的 UTF-8 字节数，不含冒号与换行。块之间允许一个 `\r\n` 或 `\n`。长度前缀超过剩余字节时产生 `E_LENGTH_OVERFLOW`，并且此前成功块保留。

payload 使用 `key=value` 字段，以 `|` 分隔：

```text
txid=4901|kind=cache|flags=0x0100|sample=S000003|version=v1|name=cache.example.test|answers=cache.example.test,A,1,12,192.0.2.10
```

支持字段：

- `txid`、`transaction_id`、`id`：十进制或 `0x` 十六进制。
- `kind`/`event`：`query`、`response`、`cache` 及常用别名。
- `flags`：十进制或 `0x` 十六进制。
- `sample`/`time`：固定采样标记。
- `version`/`parser_version`：`v1` 或 `v2`。
- `name`/`qname`/`question`：问题名。
- `answers`/`answer`：分号分隔记录。
- `additionals`/`additional`：分号分隔附加记录。

记录字段为四列或五列：

```text
name,type,ttl,address
name,type,class,ttl,address
```

TTL 和 class 接受十进制或 `0x` 十六进制。地址统一为 `netip.Addr.String()` 表示，使 IPv4/IPv6 跨格式比较稳定。

## 归一与偏移

- 事务号统一为 32 位无符号整数视图；十进制与十六进制只影响输入表示。
- 问题名统一小写、去掉一个尾点，禁止空标签和超长标签。
- 采样时钟是虚拟观测 tick，不读取墙钟。报告时间统一写成 `S` 加至少 6 位数字。
- 二进制记录偏移为记录起点；问题/附加记录偏移为其记录名起点。
- JSON 偏移为行起点，资源记录偏移尽量定位到对应 `address` 字段。
- 文本偏移为块起点；资源记录偏移定位到 payload 中对应记录片段。
