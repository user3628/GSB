# 架构与一致性说明

## 包结构

根包 `cockpitaudio` 是可嵌入的领域组件；`cmd/cockpit-audio` 是本地演示 CLI。

- `generate.go` / `wav.go` / `rng.go`：固定种子生成 16 kHz、单声道、16-bit PCM WAV 分片和 JSON 会话清单；自带确定性 PRNG，不依赖系统时间。
- `vad.go` / `fft.go`：偏移切轨、毫秒到整数采样点换算、汉宁窗、radix-2 FFT、对数能量/频谱平坦度、双阈值滞回和最短语音/静音处理。
- `attribution.go`：起止毫秒时延、声速/几何距离差、词表对数证据、优先级项、候选依据和歧义判定。
- `edit.go`：统一毫秒时间轴、重叠标记、切分/合并/优先级/词表编辑、单调编辑版本与审计记录。
- `query.go`：`active_zones`、`delay_details`、`attribution_basis` 三类查询；版本化缓存键和 `E_LATENCY_BUDGET`。
- `incremental.go`：阶段指纹、最早受影响阶段、增量刷新和全量字节比对。
- `store.go` / `filelock*.go`：SQLite 事务快照、分片内单调序号、部分唯一有效索引、阶段头 CAS、哈希链、真实 `flock`/`LockFileEx` 文件锁。
- `engine.go`：逐分片逐阶段执行、检查点复用、优雅停止边界和恢复。
- `concurrency.go` / `verification.go`：并发编辑下的版本快照前端、结算侧重算和本地验收编排。

## 确定性边界

所有需要比较的产物均通过 `CanonicalJSON` 输出：结构体字段顺序固定，Go `encoding/json` 对 map key 排序，不写入墙钟时间、绝对路径或随机发布者。浮点指标统一 round 到 6 位，时延细节 round 到 3 位。

阶段指纹刻意分层：

- `vad_config_fingerprint`：采样率、帧长/帧移、滞回阈值、最短段、掩码轮数。
- `attribution_config_fingerprint`：声速、间隔阈值、TDOA 容差、几何、优先级权重、人数上限。
- `edit_fingerprint`：编辑版本、区间、优先级覆盖、词表覆盖和审计日志。
- `query_fingerprint`：查询预算等只影响查询阶段的配置。

因此修改归属阈值不会改变 VAD 阶段摘要；修改帧参数会从 VAD 开始重算，再重放归属、时间轴和查询。

## 并发与缓存

`QueryFront` 在 RLock 下取得不可变 `QueryService` 指针后立即释放锁。编辑者在 Lock 下生成新的 `SessionResult` 和新的 `QueryService`，再原子替换指针。查询要么看到完整旧版本，要么看到完整新版本。

缓存键结构为：

```text
type + at/start/end/command/shard + config_version + edit_version
```

并发审计会记录每个实际响应的 `edit_version`，然后在结算侧重放对应数量的编辑操作、从零构建该版本，并重新计算同一查询的规范 JSON 进行逐字节比较。

## SQLite 发布协议

每个分片维护独立的 `shard_counters.next_sequence`。发布者可以传 `sequence=0` 在真实文件锁内取号，也可以传显式序号执行 CAS：

1. 获取 `<db>.publish.lock` 的进程级文件锁。
2. 开启 SQLite 事务。
3. 校验 `requested_sequence == next_sequence`。
4. 作废旧的同分片同阶段有效记录。
5. 插入新检查点和 SHA-256 哈希链。
6. 对 `stage_heads` 执行带旧序号条件的更新。
7. 推进分片序号并提交。

落后、重复、未来跳号和跨分片预期不匹配均返回 `E_STALE_CHECKPOINT`，并在 `diagnostics.classification` 中给出 `duplicate_or_behind`、`future_gap` 或 `cross_shard`。直接改写库记录时，`VerifySnapshot` 会区分 `rollback` 与 `tamper`，正常链头为 `normal_advance`。

## Windows 注意事项

依赖已 vendor 到仓库，固定 `modernc.org/sqlite v1.29.0` 与 `golang.org/x/sys v0.16.0`。SQLite 为纯 Go 驱动，因此不需要 gcc；文件锁在 Windows 上使用 `LockFileEx`，非 Windows 使用 `flock`。已用以下命令交叉验证：

```powershell
$env:GOOS="windows"; $env:GOARCH="amd64"; $env:CGO_ENABLED="0"
go build -o artifacts\cockpit-audio.exe .\cmd\cockpit-audio
```
