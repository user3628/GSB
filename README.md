# Key Lifecycle Diagnostic Workbench

面向本地授权样本的密钥生命周期可视化诊断工作台。项目完全由确定性样本驱动，协议交互仅使用进程内内存通道，不访问网络；持久化使用纯 Go SQLite，不依赖 CGO。

## 当前状态

- 模块：`keylife`，`go 1.22.0`，依赖 `modernc.org/sqlite v1.34.5`。
- 已在本机 `go1.27.4 linux/amd64` 上完成测试、竞态检测和端到端验收。
- 已使用 `CGO_ENABLED=0 GOOS=windows GOARCH=amd64` 成功生成 Windows x86-64 可执行文件。
- 最新验收结果：13/13 项通过；快照链连续至版本 19；报告重复运行字节一致。
- 验收产物见 `artifacts/verification.json`、`artifacts/demo-report.json`、`artifacts/go-test.txt`、`artifacts/go-test-race.txt`。

## 快速开始

Linux/macOS：

```bash
export TMP=/tmp
go mod download
go test ./...
go run ./cmd/keylife demo
go run ./cmd/keylife verify --config configs/verify.example.json --out artifacts/verification.json
```

Windows amd64（PowerShell / cmd）：

```bat
set TMP=%TEMP%
go mod download
go test ./...
go run ./cmd/keylife demo
go run ./cmd/keylife verify --config configs\verify.example.json --out artifacts\verification.json
go build -o keylife.exe .\cmd\keylife
```

如默认 Go 代理不可达，仅首次下载依赖时可设置 `GOPROXY=https://goproxy.cn,direct`；运行、测试和演示阶段不需要网络。

## 工作台命令

```text
keylife demo      [--dir DIR] [--threshold N] [--now-tick T] [--scale N]
                  [--max-concurrency N] [--subject-limit N] [--json] [--report FILE]
keylife verify    [--root DIR] [--config FILE] [--threshold N] [--now-tick T] [--scale N]
                  [--max-concurrency N] [--subject-limit N] [--anomalies a,b]
                  [--skip-crash] [--out FILE]
keylife report    --db FILE [--subject S] [--threshold N] [--now-tick T] [--out FILE]
keylife timeline  --db FILE [--subject S] [--key K] [--now-tick T] [--json]
keylife replay    --db FILE [--version N] [--json]
keylife paths     --db FILE [--subject S] [--threshold N] [--now-tick T]
                  [--max-concurrency N] [--subject-limit N] [--compare] [--json]
keylife alerts    --db FILE [--now-tick T] [--threshold N] [--json]
keylife inject    --db FILE --type snapshot_gap|snapshot_replacement [--version N]
```

- `demo`：在临时目录或 `--dir` 中生成全生命周期样本，展示时间轴、路径选择和告警。
- `verify`：执行双路径一致性、异常注入、报告确定性和真实进程崩溃恢复验收。
- `timeline`：按主体/密钥查看阶段轨道、存活时长和轮换窗口。
- `replay`：按版本回放完整快照；请求跳号版本会被拒绝。
- `paths`：展示当前阈值、待裁决密钥数、选中路径、业务码和稳定性摘要。
- `alerts`：查看版本链、过期、轮换超期和弱算法告警。
- `report`：导出稳定排序的 JSON 诊断报告。
- `inject`：通过外部参数直接注入版本跳号或同版本整体替换样本。

## 核心设计

### 1. 生命周期与业务码

密钥状态严格按 `generated -> active -> rotated -> archived -> destroyed` 迁移。生成成功后处于 `generated`；轮换会保留旧密钥的 `rotated` 终态，并在同一事务中生成一个 `generated` 后继密钥。

稳定业务码：

- `E_SUBJECT_SCOPE`：操作者与密钥所属主体不一致。
- `E_KEY_EXPIRED`：使用、激活或轮换已过期密钥。
- `E_WEAK_ALGO`：密钥或本次请求的有效算法强度低于主体策略。
- `E_PHASE_TRANSITION`：非法阶段迁移或当前阶段不能执行该操作。
- `E_ROTATION_CONTENTION`：同密钥并发轮换竞争。
- `E_KEY_BUDGET`：单主体待处理请求达到上限。
- `E_SNAPSHOT_VERSION`：版本链跳号或不可连续加载。
- `E_SNAPSHOT_REPLACED`：同版本快照内容、父哈希或链头被整体替换。
- `E_KEY_NOT_FOUND`、`E_SNAPSHOT_INTEGRITY`、`E_POLICY_NOT_FOUND`、`E_CONFIG` 等用于补充诊断。

失效操作在事务前或事务内校验失败，不写入密钥、审计或快照。

### 2. 单调时钟与确定性输出

所有时间都是 `mono-local-sample/v1` 整数 tick，比较只使用整数差，不比较墙钟时间。JSON 中的时间字段统一输出固定采样标记，例如：

```json
{
  "created_at": "mono-local-sample/v1:tick=100",
  "rotation_remaining": "mono-local-sample/v1:delta=20"
}
```

`KeyMetrics` 输出已存活时长、剩余有效期、下次轮换时间、剩余轮换窗口、轮换超期和过期标记。默认样本使用虚拟单调时钟；也提供基于 `time.Since` 的单调时钟适配器。

### 3. 规模自适应裁决

按主体统计不同待裁决密钥数，阈值规则为：

- `pending_keys < threshold`：逐密钥精算路径 `per-key-accrual`。
- `pending_keys >= threshold`：批量流式路径 `batch-streaming`。

流式路径通过进程内 channel 逐条送达，不依赖网络。两条路径共用同一裁决语义，但独立执行，并在 `verify` 中比较完整 canonical bytes（裁决结果 + 审计轨迹），同时输出摘要 SHA-256。阈值可通过 `--threshold` 或配置文件调整。

### 4. 版本化快照与完整性

每次成功的生成、激活、轮换、归档或销毁，在单个 SQLite 事务内完成：

1. 写入密钥状态；
2. 写入审计事件；
3. 读取事务内完整策略、密钥、审计状态；
4. 生成 canonical JSON 快照；
5. 写入递增版本、`parent_hash`、`content_hash`、`chain_hash` 和链头锚点；
6. 提交事务。

加载时按版本顺序校验连续性、payload 内部版本、内容哈希、父链哈希和链头锚点。版本跳号返回 `E_SNAPSHOT_VERSION` 并保留最近连续前缀；同版本内容替换返回 `E_SNAPSHOT_REPLACED` 并回退到前一个连续版本。旧快照只追加、不覆盖。

### 5. 并发与资源控制

- 全局并发上限通过 `--max-concurrency` / JSON `max_concurrency` 配置，超额请求进入等待队列。
- 单主体待处理请求上限通过 `--subject-limit` / JSON `subject_limit` 和主体策略配置，超限立即返回 `E_KEY_BUDGET`。
- 同密钥轮换使用进程内 key-level gate；只有一个请求可进入事务，其余并发请求返回 `E_ROTATION_CONTENTION`。
- SQLite 写入由进程内写锁串行化，并使用 WAL、`synchronous=FULL` 和 busy timeout。

### 6. 中断恢复

`verify` 会启动真实子进程，在轮换事务写入后、快照提交前调用 `os.Exit(91)`：

- 未提交操作由 SQLite 回滚；重启后密钥仍为 `active`，版本不变，可再次正常轮换。
- 在提交后强制退出时，已提交状态和快照保留，重启后密钥为 `rotated`，不回退。

### 7. 异常注入

`verify --anomalies ...` 或配置文件可选择以下样本。注入只使用输入、参数和临时 SQLite 文件，不修改源码。

| 类型 | 注入方式 | 期望业务码 |
| --- | --- | --- |
| `subject_scope` | 外部请求使用其他主体 actor | `E_SUBJECT_SCOPE` |
| `expired_use` | 单调时钟推进到过期 tick 后使用 | `E_KEY_EXPIRED` |
| `weak_algo` | 对 256 位策略主体请求 `aes-128-gcm` | `E_WEAK_ALGO` |
| `phase_transition` | 对 `generated` 密钥请求归档 | `E_PHASE_TRANSITION` |
| `snapshot_gap` | 外部插入 `version + 2` 快照行 | `E_SNAPSHOT_VERSION` |
| `snapshot_replacement` | 保持版本号不变，整体替换快照 payload | `E_SNAPSHOT_REPLACED` |
| `rotation_contention` | 8 个并发轮换请求，提交门闩阻塞首个请求 | `E_ROTATION_CONTENTION` |

每个异常都断言密钥终态、快照版本变化和连续前缀。并发竞争样本额外断言仅 1 个成功、7 个竞争失败、其他密钥状态不变。

## 实际验收结果

运行命令：

```bash
go run ./cmd/keylife verify --config configs/verify.example.json \
  --out artifacts/verification.json
```

结果摘要（`artifacts/verification.json`）：

- 总状态：`passed=true`，13/13 检查通过。
- 生命周期链：版本 1..19 连续，最终链头 `9e0866ad4cfd5ddc8903903de29bd89566f4e72b58b379fff00f0ed0a03b7fe0`。
- 双路径：
  - 3 个密钥、阈值 4：选择 `per-key-accrual`，`byte_identical=true`，摘要 `e009f580...`。
  - 5 个密钥、阈值 4：选择 `batch-streaming`，`byte_identical=true`，摘要 `3d4ea481...`。
- 报告确定性：两次独立构建均为 37,343 字节，SHA-256 均为 `ed51e37da963ad698bde2e84e528b4fd7d1d9bf8cae0a2f8f5ef57e9e406f5ff`。
- 崩溃恢复：
  - `before_commit`：重启版本 15、`active`；重试成功至版本 16、`rotated`。
  - `after_commit`：重启版本 16、`rotated`，已提交状态未回退。
- 异常业务码：`E_SUBJECT_SCOPE`、`E_KEY_EXPIRED`、`E_WEAK_ALGO`、`E_PHASE_TRANSITION`、`E_SNAPSHOT_VERSION`、`E_SNAPSHOT_REPLACED`、`E_ROTATION_CONTENTION` 全部符合预期。
- `go test ./...`：全部通过。
- `go test -race ./...`：全部通过。

> 最终报告哈希会随源码版本和报告字段变化；以当前 `artifacts/verification.json` 中记录的值为准。

## 诊断报告字段

`report` 输出包含：

- `summary`：主体、密钥、活跃密钥、审计、快照和告警计数。
- `subjects`：主体策略、密钥数、阶段分布和算法族。
- `keys`：按 `subject,id` 稳定排序的密钥摘要与单调时间指标。
- `transitions`：按主体、密钥、审计序号稳定排序的阶段迁移轨迹。
- `adjudication`：阈值、路径选择、业务码序列、裁决审计轨迹和 SHA-256 摘要。
- `snapshot_chain`：物理版本列表、连续版本、内容哈希、链哈希和恢复位置。
- `alerts`：版本链、过期、轮换超期、弱算法等告警。

报告不含绝对临时路径、进程 ID、墙钟时间或随机 UUID；相同参数和相同数据库状态重复导出时字节级一致。

## 代码结构

```text
cmd/keylife/           CLI 工作台与崩溃子进程入口
internal/model/        阶段、算法、业务码、单调时间与快照模型
internal/store/        SQLite schema、事务、快照链校验与外部注入
internal/service/      生命周期事务、轮换门闩、裁决与恢复钩子
internal/adjudicator/  自适应双路径裁决与字节比较
internal/resource/     全局队列与主体预算控制
internal/sample/       本地确定性样本生成器
internal/report/       JSON 诊断报告
internal/verify/       端到端验收与异常断言
configs/               外部验证配置示例
artifacts/             实际运行产物
```

## 约束说明

- 样本密钥材料为协议状态模拟，不生成或导出真实私钥。
- 所有协议消息仅通过进程内 channel 传递；没有网络监听器或外部目标。
- SQLite 文件、WAL 和报告均写入 `TMP` 或显式指定目录。
- 过期密钥允许执行归档/销毁清理；过期后的使用、激活和轮换返回 `E_KEY_EXPIRED`。
- 阈值等于待裁决密钥数时选择流式路径，规则已在输出中显式记录。
