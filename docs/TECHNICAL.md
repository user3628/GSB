# 技术说明

## 1. 处理管线

```text
原始字节
  -> Detect/Negotiate（格式与版本裁决）
  -> Codec Decode（二进制/JSONL -> Record）
  -> Validate（必填、长度、事件序号）
  -> Identity Normalize（连接 ID/SNI/证书序列号）
  -> Time Normalize（ns/RFC3339 -> ms + rounding bits）
  -> Policy Timeline（按事件时间选择当时版本）
  -> Replay State Machine + PKI Adjudication
  -> SQLite Intent + 单事务原子提交
  -> 完整 commit 快照读者
```

解码和全部连接重放先在内存完成；任何记录损坏、版本不支持、身份冲突或非法状态迁移都不会进入批次提交。

## 2. 兼容矩阵

Reader 当前规范版本为 v2，启动时将可读取的旧版本转换为规范 v2 模型。

| 格式 | 写入版本 | 裁决 | 处理 |
|---|---:|---|---|
| `MTLP` binary | 1 | `degraded_accepted` | 缺 `protocol_version` 时补 `TLS 1.2`；事件偏移按毫秒精度解释为纳秒字段 |
| `MTLP` binary | 2 | `accepted` | 纳秒偏移、协议版本、策略时间线、位图全部可用 |
| `MTLP` binary | >2 | `rejected` / `E_FORMAT_UNSUPPORTED` | 高版本必需位图未知时拒绝 |
| JSONL | 1 | `degraded_accepted` | 缺协议版本补 `TLS 1.2`；RFC3339 秒精度按零亚毫秒读入 |
| JSONL | 2 | `accepted` | RFC3339Nano 与显式协议版本 |
| JSONL | >2 | `rejected` / `E_FORMAT_UNSUPPORTED` | 原始 JSON 键检查发现无法解释的非 `x_` 必填字段 |

矩阵也可从 `GET /api/v1/compatibility` 获取。`x-`/`x_` 前缀字段视为本地扩展；未知普通字段在当前版本按损坏或不支持处理，避免静默丢弃必填语义。

## 3. 紧凑二进制布局

包头部：

```text
magic[4] = "MTLP"
format_version uint8
record_count   uint16 BE
```

每条记录：

```text
record_length uint32 BE        # body + CRC32 的总长度
body:
  bitmap uint16 BE             # bit0=protocol, bit1=policy_timeline, bit15=future_required
  connection_id uint16-length UTF-8
  sni uint16-length UTF-8
  [protocol_version uint16-length]
  observed_start_unix_ms int64 BE
  pem_chain_block uint32-length bytes
  policy:
    version uint16-string
    mode uint8                 # 1=SPKI, 2=root SHA-256
    spki_pin_count + uint16-strings
    trusted_root_count + uint16-strings
  timeline_block uint32-length:
    count uint16
    each effective_offset_ns int64 + policy
  events_block uint32-length:
    count uint16
    each seq uint32 + type uint8 + offset int64
crc32_ieee(body) uint32 BE
```

长度越界、声明长度与剩余字节不一致、CRC 不匹配或事件类型未知均返回 `E_RECORD_CORRUPT`，上下文带从 1 开始的 `record_index`。

## 4. 时间与身份归一化

二进制绝对时间为 `observed_start_unix_ms * 1e6 + offset_ns`；JSONL 使用 `time.RFC3339Nano` 解析。内部时间戳为 Unix 毫秒整数。

支持规则：

- `truncate`/`floor`：直接丢弃亚毫秒余数。
- `round`：余数 `>= 500000ns` 时进位。
- `ceil`：存在正余数即进位。

每个事件输出：

- `remainder_ns`
- `rounding_bit`：余数达到半毫秒阈值时为 1
- `rounded_up`
- `applied_delta_ns = normalized_ns - raw_ns`
- `cumulative_rounding_error_ns`：按事件序列累计 `applied_delta_ns`

身份规范：

- 连接 ID：去除空白并转大写。
- SNI：`TrimSpace`、去末尾 `.`、转小写。
- 证书序列号：去除 `:`、`-`、空格和 `0x`，通过大整数转为大写十六进制。

同一规范连接 ID 的 SNI 或叶子序列号与已落盘事实不一致时返回 `E_IDENTITY_CONFLICT`。仅大小写/根点/分隔符差异会归并。

## 5. 证书与固定判定

`internal/pki` 不依赖系统根池或网络：

1. 解析全部 PEM `CERTIFICATE` 块。
2. 检查签名算法白名单；SHA-1/DSA/Unknown 等返回 `E_CHAIN_INVALID`。
3. 在观测事件时间检查 `NotBefore`/`NotAfter`，输出 `validity_remaining_ms = NotAfter.UnixMilli() - event_ms`。
4. 以非 CA 证书为叶子；按 `RawIssuer == RawSubject`、AKI/SKI 兼容和 `CheckSignatureFrom` 密码学条件寻找签发者。
5. DFS 支持乱序和多张交叉证书；以证书 SHA-256 指纹为 visited 键防止路径成环。存在环且无可达自签名根时返回 `E_CHAIN_INVALID`。
6. 输出有序 serial、每张证书的 SPKI SHA-256、整证书 SHA-256、有效期余量和策略条目命中情况。

策略：

- `spki_sha256`：策略值可为 hex、`sha256:<base64>`、URL/raw base64；链上任意证书的 `RawSubjectPublicKeyInfo` SHA-256 命中即可。
- `trusted_root_sha256`：比较路径末端自签名根的整证书 DER SHA-256。
- SPKI 未命中但末端根不在 `TrustedRoots`：结论为 `suspected_mitm`。
- 有受信根但固定未命中、SNI 不匹配或事件不足：通常为 `insufficient_evidence`，避免把策略/轮换问题直接夸大为攻击。
- 缺根、成环、签名/有效期/算法错误：`E_CHAIN_INVALID`，结论为证据不足，而不是 MITM。

## 6. 策略热更新

`Record.Policy` 是初始版本，`PolicyTimeline` 是整体替换列表；HTTP `POST /api/v1/policy-versions/switch` 可追加连接级切换。每个事件按 `timestamp_ms` 选择满足 `effective_from_ms <= event_ms` 的最后一个版本。

因此：

- 早期 `pin_verification` 使用旧版本并可判定可信。
- 新版本生效后的事件使用新版本，可判定 MITM。
- 最终结论聚合所有校验：只要有强 MITM 判定，连接结论为 `suspected_mitm`。
- `policy_version_sequence` 按事件给出版本序列，`policy_versions` 给出去重序列。
- 旧版本在提交时标记 `retired`；调用 `/api/v1/policy-versions/{old}/conclusions` 返回 `E_VERSION_RETIRED`，上下文含 `replaced_by`、`current_available_versions` 和 `available_revision_range`。历史结论 JSON 仍保留当时策略文档与证据。

如果新策略请求缺少版本/mode 等必填项，服务在写入前返回 `E_RECORD_CORRUPT`，不会切换旧策略。

## 7. 状态机

```text
(empty) --handshake_start--> HANDSHAKE_STARTED
HANDSHAKE_STARTED --certificate_received--> CERTIFICATE_RECEIVED
CERTIFICATE_RECEIVED --pin_verification--> PIN_CHECKED
PIN_CHECKED --pin_verification--> PIN_CHECKED   # 热更新后再次校验
PIN_CHECKED --handshake_complete--> HANDSHAKE_COMPLETED

HANDSHAKE_STARTED|CERTIFICATE_RECEIVED|PIN_CHECKED
  --abnormal_termination--> ABNORMAL_TERMINATED
```

重复 start、证书先于 start、校验先于证书、完成先于校验、终止后继续事件等均返回 `E_INVALID_TRANSITION`。

结论规则：

- `trusted`：合法完成、无异常终止、链有效、所有固定校验命中且 SNI 匹配。
- `suspected_mitm`：存在强证据（固定未命中且末端根不受信，或伴随 SNI/身份冒用与不受信根）。
- `insufficient_evidence`：未完成、异常终止、链无效或证据/策略歧义；与 MITM 严格分离。
- `ABNORMAL_TERMINATED` 永不输出 `trusted`。

## 8. SQLite 单写者与恢复

数据库表包括 `commits`、`intents`、`connections`、`conclusion_versions`、`events`、`evidence`、`policy_versions`、`connection_policy_switches`、`records`、`batches`、`recovery_runs`。

提交流程：

1. 获取进程内单写者互斥锁。
2. 用独立事务插入 `commits(status=PENDING)` 和 `intents(status=started)`，并提交，确保意图持久化。
3. 开启 payload 事务，写入原始记录、连接当前态、结论版本、事件、证据和策略版本。
4. 在同一 payload 事务中把 intent 改为 `committed`、commit 改为 `COMPLETE`。
5. 提交 payload 事务并释放写锁。

读者不获取写锁；SQLite WAL 允许并发读。读者只能查询 `status=COMPLETE` 的 commit 以及 `commit_version <= N` 的最新结论版本，因此不会看到“MITM 结论已写但证据未写”的中间态。

强制终止演练在步骤 2 后、步骤 4/5 前调用 `SIGKILL`。重启 `Recover` 扫描 `started`/`committing` 意图：

- payload 事务未提交，SQLite 自动回滚；
- 将意图和 PENDING commit 标记为 `aborted`；
- 返回最后一个 `COMPLETE` commit、当前可见连接和仅来自数据库的 facts；
- 恢复后可重新导入同一批次，AUTOINCREMENT 会使用新的 commit version。

## 9. 确定性样本

`internal/sample` 使用固定种子的确定性 `io.Reader` 生成 Ed25519 根/中间 CA/叶子证书，固定时间为 `2025-11-24T08:00:00Z`。样本覆盖：

- 当前二进制可信连接；
- 当前 JSONL 可信连接；
- 大小写/SNI 根点差异的同一实体；
- JSONL v1 与 binary v1 降级；
- 高版本未知必填字段拒绝；
- CRC 损坏拒绝；
- 身份冲突；
- 热更新下 v1 trusted / v2 MITM；
- 不受信攻击者根；
- 异常终止；
- `SIGKILL` 恢复批次。
