# mitmpin 技术说明：中间人检测与证书固定校验管线

## 1. 总体架构

纯本地、确定性、无网络握手的数据处理管线。证书与密钥由标准库 `crypto/x509`
在内存中自建，校验全部在内存完成；持久化使用纯 Go 驱动 `modernc.org/sqlite`
（`CGO_ENABLED=0` 可交叉编译到 Windows，无需 GCC）。

| 包 | 职责 |
| --- | --- |
| `internal/sample` | 观测/策略类型、错误码、解析校验（策略唯一性、算法、事件连续性、链结构） |
| `internal/certutil` | 确定性 CA/叶证书生成、PEM、SPKI 摘要、指纹、SAN 匹配、链结构与路径验证 |
| `internal/pin` | 固定校验与 MITM 判定（SPKI 集合 / 受信根路径），输出逐项 Finding |
| `internal/policy` | 快照规范化内容哈希，识别整体替换式版本变化 |
| `internal/store` | SQLite：快照事务、版本链、检查点、连接终态 |
| `internal/engine` | 单连接逐事件重放、检查点提交、中断恢复、跳号回退 |
| `internal/scheduler` | 加权排队、等待队列、单连接/全局预算裁决、有界 worker 池 |
| `internal/replay` | 交互侧独立重放，与结算侧逐条比对（不读写检查点与报告） |
| `internal/report` | 确定性 JSON 报告，稳定排序、固定 marker、字节级一致 |
| `internal/generator` | 五类确定性样本与证书素材生成 |
| `internal/pipeline` | 编排、热更新绑定、故障注入 |
| 根包 `mitmpin` | 宿主集成门面：加载/应用/校验/回放/恢复/导出 |
| `cmd/mitmpin` | 命令行：generate / run / inject |
| `examples/quickstart` | 一键运行示例 |

## 2. 业务码与需求映射

| 业务码 | 触发条件 | 对应需求 |
| --- | --- | --- |
| `E_POLICY_MISSING` | 策略引用缺失、快照内策略标识重复/为空 | 需求 1 |
| `E_HASH_ALGO_UNSUPPORTED` | SPKI 固定项算法非 sha256/sha384 | 需求 1 |
| `E_EVENT_SEQ_GAP` | 事件序号非 1..N 连续或事件类型未知 | 需求 1 |
| `E_CHAIN_BROKEN` | PEM 损坏、空链、签名关系不连续（中间 CA 缺失/异根拼接） | 需求 2、6 |
| `E_MITM_SUSPECTED` | SPKI 全不命中，或无法构建到受信根的有效路径 | 需求 2 |
| `E_SNI_MISMATCH` | SNI 不在叶证书 SAN（兼容 CN） | 需求 2 |
| `E_PROTOCOL_NOT_ALLOWED` | 协议版本不在策略允许集合 | 需求 2 |
| `E_SNAPSHOT_VERSION` | 检查点版本链跳号，回退到最近连续版本 | 需求 4、6 |
| `E_EVENT_BUDGET` | 单连接事件数超过上限，截断并保留已处理结论 | 需求 5 |
| `E_GLOBAL_BUDGET` | 全局内存预算不足，按事件数从少到多降级 | 需求 5 |
| `E_POLICY_UNCHANGED` | 新快照内容哈希与最新快照一致，拒绝空更新 | 需求 3 |
| `E_POLICY_VERSION_CONFLICT` | 快照版本号重复/未递增 | 需求 3 |

固定校验闸门顺序：协议版本 → SNI/SAN → 链结构完整性 → SPKI 固定或受信根路径。
各闸门在 `Findings` 中保留独立检查项，主业务码取首个致命失败；致命码产生后终止后续重放。

## 3. 关键设计决策

### 3.1 策略热更新与连接一致性
- 策略以带递增版本号的快照存储，`CommitSnapshot` 在单 SQLite 事务内写入
  `policy_snapshots` 与 `snapshot_chain`，失败整体回滚。
- 连接绑定在“握手开始”时按全局调度序确定：`UpdateStep.AfterSeq=N` 表示第 N 个
  连接开始前生效；已进入握手阶段的连接始终沿用开始时版本，新连接用最新版本。
- 每条事件轨迹都冗余记录 `policy_version`，逐事件可追溯。
- `policy.ContentHash` 对策略集合做规范化哈希（排序策略、固定项、允许版本，
  根证书取 PEM 哈希），与版本号/marker 无关；只升版本号但内容相同判定为
  `E_POLICY_UNCHANGED`，真正的整体替换才产生新哈希。

### 3.2 检查点与中断恢复
- 每个事件处理后提交一条检查点（checkpoint_version 随事件 1..N 递增），
  记录事件序号、类型、策略版本绑定、最近固定结论与累计轨迹 JSON。
- 重启时 `recoverTrace` 校验版本链连续性；跳号返回 `E_SNAPSHOT_VERSION`，
  删除不连续检查点回退到最近连续版本，已提交结论不回退，未提交事件整体重做。
- 恢复只读检查点与观测表，不依赖报告是否生成。

### 3.3 并发与资源预算
- 规划与执行分离：`NewPlan` 仅依据事件数与连接标识做确定性裁决，与并发度无关，
  因此并发度变化时业务码序列保持稳定（有专门测试断言）。
- 加权排队按事件数升序、同数按连接标识排序；超并发连接进入带缓冲等待队列，
  worker 以 FIFO 拉取，绝不直接拒绝。
- 单连接超限：截断到 `MaxEventPerConn` 并标 `E_EVENT_BUDGET`。
- 全局预算：按排队顺序分配事件成本单位，不足者按“事件数从少到多”首先被降级，
  分配剩余预算后标 `E_GLOBAL_BUDGET`，保留已能处理的事件结论。

### 3.4 确定性与可回放
- 报告只使用样本自带 `marker`，不写任何墙上时间；map 全部转排序切片；
  JSON 经泛型解码归一化后固定缩进输出，HTML 转义关闭。
- 交互侧 `replay` 用相同输入独立重算每个 `pin_check` 的业务码，与结算侧轨迹
  逐条比对，报告中 `replay.all_consistent` 与每个事件的 `consistent` 可复核。
- 相同输入与参数重复运行，报告字节级一致（SHA-256 相同）。

### 3.5 故障注入（不改源码）
五类注入均通过批次 `faults` 配置或 CLI 参数构造，运行在批次深拷贝上，
正常样本同批隔离：
- `pin_mismatch`：为目标连接构造独立坏固定项策略副本，其他连接不受影响。
- `hot_update_race`：在目标连接开始后插入高版本整体替换快照，验证其仍用旧版本。
- `chain_broken`：移除链中中间 CA，触发链结构闸门。
- `event_seq_gap`：改写事件序号制造跳变。
- `snapshot_version_gap`：先正常落检查点，再在 SQLite 层把末检查点版本 +2，
  重启恢复断言 `E_SNAPSHOT_VERSION` 与回退行为。

## 4. 验证结果

执行环境：Go 1.27.4（`GOTOOLCHAIN=local`），Linux amd64；`go.mod` 声明 `go 1.22`，
驱动锁定 `modernc.org/sqlite v1.34.5`（其 go.mod 声明 go 1.21）。

- `CGO_ENABLED=0 go test -count=1 ./...`：9 个包全部 `ok`。
- `go test -race`（scheduler/pipeline/engine）：无 data race。
- `GOOS=windows GOARCH=amd64 go build`：产出 PE32+ 可执行文件，无 CGO。
- CLI 端到端：五类样本业务码符合预期；两次运行报告 SHA-256 完全相同。
- 五类故障注入：目标连接业务码正确、同批其他连接终态未污染、策略快照版本未被异常推进。

## 5. 运行方式

```bash
export GOTOOLCHAIN=local
go build -o bin/mitmpin ./cmd/mitmpin

# 生成确定性样本（五类场景 + 热更新）
./bin/mitmpin generate -out data/sample.json

# 一键运行并导出报告
./bin/mitmpin run -in data/sample.json -db data/run.db -report data/report.json \
  -concurrency 4 -global-mem 100000 -max-events 100

# 五类故障注入
./bin/mitmpin inject -kind pin_mismatch        -target conn-normal-1 -in data/sample.json -db data/f.db
./bin/mitmpin inject -kind hot_update_race     -target conn-normal-2 -in data/sample.json -db data/f.db
./bin/mitmpin inject -kind chain_broken        -target conn-normal-3 -in data/sample.json -db data/f.db
./bin/mitmpin inject -kind event_seq_gap       -target conn-normal-1 -in data/sample.json -db data/f.db
./bin/mitmpin inject -kind snapshot_version_gap -target conn-normal-1 -in data/sample.json -db data/f.db

# 库接入示例
go run ./examples/quickstart
```

## 6. 宿主集成接口（根包 mitmpin）

- `LoadBatch / LoadBatchFile`：观测与策略加载校验
- `OpenStore / ApplyPolicy`：存储打开、策略快照事务应用
- `VerifyPinning`：单连接固定校验与 MITM 判定
- `ReplayObservation / Recover`：交互侧重放、检查点中断恢复
- `Run / ExportReport`：整批管线运行与确定性报告导出
- `InjectSnapshotVersionGap / PolicyContentHash / GenerateSampleBatch`：
  异常注入、内容哈希、确定性样本入口
