# MotionLab · 多镜头视频预测与运动时序滚动推演引擎

MotionLab 是一个完全本地、确定性的多分支运动推演应用。它从固定种子生成低分辨率帧序列与运动清单，在全局并发、分支配额和轮次预算约束下并行滚动多个预测分支，逐步渲染、评估、剪枝，并将每个已提交事实写入 SQLite。应用提供浏览器控制台与 CLI，可在强制终止后仅依赖已提交事务恢复。

- Go 1.22+，纯 Go SQLite 驱动 `modernc.org/sqlite`
- 无容器、无外部数据集、无 GPU、无浏览器 CDN 依赖
- 仅监听回环地址，所有演示与验证均在单机完成
- 每条样本包含匀速平移方块、缓慢形变色块和一次方向突变
- 内置 3 条序列；默认策略会产生多分支剪枝与配额封顶

## 快速开始

```powershell
# Windows PowerShell / CMD
go mod download
go run ./cmd/motionlab init --data .\runtime
go run ./cmd/motionlab serve --data .\runtime --addr 127.0.0.1:8080
```

打开 `http://127.0.0.1:8080`。服务若收到非回环地址会在绑定前拒绝启动。

Linux/macOS：

```bash
go run ./cmd/motionlab init --data ./runtime
go run ./cmd/motionlab serve --data ./runtime --addr 127.0.0.1:8080
```

构建单文件程序：

```bash
go build -o motionlab ./cmd/motionlab
# Windows: go build -o motionlab.exe .\cmd\motionlab
```

## 可重复演示与验收

一条命令运行 3 条序列、确定性重放、剪枝预算等价、真实子进程强杀恢复、时间线溢出、共享帧写保护、跨进程检查点、预算延后和策略三态验证：

```bash
go run ./cmd/motionlab demo --data ./demo-runtime
```

报告写入 `./demo-runtime/demo-report.json`。自动验收器会启动当前可执行文件自身，在提交第 7 步后以退出码 137 强制终止，再启动恢复流程，并将恢复结果与连续执行做确定性投影比较。

测试与静态检查：

```bash
go test ./...
go test -race ./internal/...
go vet ./...
GOOS=windows GOARCH=amd64 go build ./...
```

实际验证结果见 [VALIDATION.md](VALIDATION.md)，设计细节见 [docs/DESIGN.md](docs/DESIGN.md)。

## 应用能力

### 1. 确定性状态与帧渲染

- 自实现 SplitMix64 PRNG，避免 `math/rand` 跨 Go 版本行为差异。
- 状态包含绝对位移、速度、形变尺度、旋转、突变事件标记。
- 每步先平移与形变，再在清单指定的 `direction_change_step` 执行一次方向突变，最后按衰减系数更新速度。
- 渲染器从不可变共享初始帧提取方块与色块纹理；方块做亚像素平移，色块做缩放/旋转逆映射。
- 采样使用双线性插值，合成与输出统一做 8 位量化；预测帧的 SHA-256 基于原始 RGB 字节，而非 PNG 容器。
- 分支 0 与真值使用相同运动方程，在默认样本上逐步 MAE 恒为 0，可作为渲染一致性的自检基线。

### 2. 三预算并行调度

- 执行以分支为调度单元，预计算确定性波次；每波实际并发 goroutine 数不超过全局并发上限。
- 每分支目标步数为 `min(requested_steps, branch_quota)`。
- 单轮预算采用**确定性虚拟毫秒（vms）**：每步计 1 vms，分支接纳另有 2 vms 固定成本。
- 每波最多接纳 `min(global_concurrency, floor(round_budget / branch_cost))` 个分支。
- 未获接纳的分支以 `not_started` 持久化，原因形如 `E_TIME_BUDGET: branch needs 14 virtual-ms, round budget is 1 ms`，不会静默丢失。
- 虚拟预算是有意设计：墙钟并发会使接纳和剪枝顺序不可复现，与“中断恢复/剪枝后逐字节一致”的更强要求冲突。界面与报告仍展示真实提交结果和真实吞吐。

### 3. 共享状态隔离

- 每个序列只有一个 `SharedFrame`，持有初始帧、背景帧与纹理提取区；构造后固定为只读。
- 所有分支共享同一初始运动状态快照，进入分支后立即按值复制。
- 任何写共享帧的尝试都会返回结构化 `E_SHARED_FRAME_WRITE`，包含 `sequence_id`、`branch_id`、偏移和字节数。
- 分支的 motion state、误差曲线、渲染中间量与帧均为分支局部数据。

### 4. 剪枝与预算回收

- 每提交一步后计算预测帧与真值帧的逐像素平均绝对误差（MAE）。
- `MAE > prune_threshold` 时立即裁决为 `pruned`，记录触发步、阈值、实际 MAE 和剩余配额。
- 被剪枝分支的剩余配额记录在 `reclaimed_quota` 中；未派发分支不会改变保留分支的配额与推演轨迹。
- 验收器以相同阈值运行“仅派发幸存分支”的基线，并逐分支比较状态/帧摘要、错误曲线、步记录和配额使用。

### 5. 误差与裁决汇总

- 每步记录状态摘要、预测帧摘要、真值帧摘要、MAE、取值范围检查、裁决与全局检查点序号。
- 报告包含各分支完整误差曲线、平均/最终/最好/最差 MAE、最终状态、最终帧摘要和裁决理由。
- 横向裁决仅考虑已执行且未剪枝的分支，以平均 MAE 最低者为胜；同分时分支号小者优先。
- 请求步数超过真值时间线时返回 `E_TIMELINE_OVERFLOW`，携带序列标识、真值步数和超出步数。

### 6. SQLite 事务恢复

每个预测步骤在一个事务内原子写入：

1. 连续检查点记录；
2. 步骤记录（状态、帧 PNG、MAE、真值摘要、裁决）；
3. 分支进度与最终裁决；
4. 剪枝/完成审计事件；
5. `runs.checkpoint_seq` 条件更新。

`steps` 上存在部分唯一索引，保证同一 `run_id + branch_id + step_no` 只有一条 `valid=1` 记录。恢复只读取分支最新已提交状态、下一次步号和已提交误差曲线，不重放未提交内存。

强制终止后不写退出报告；下一次 `run/resume` 从最后一个提交点继续。`demo` 通过真实子进程强杀验证连续执行与恢复执行的规范化报告字节和确定性指纹完全一致。

### 7. 并发发布保护

- 进程内使用互斥序列化写入；跨进程使用操作系统文件锁：Unix `flock`、Windows `LockFileEx`。
- 文件锁内开启 SQLite 事务，读取当前检查点并执行条件更新：只允许 `stored_seq + 1`。
- 落后、重复或跳跃发布均返回 `E_STALE_CHECKPOINT`，错误中带 scope、期望序号和已存序号。
- 测试同时覆盖同进程竞争与两个独立测试进程竞争，保证恰好一个发布者成功。

### 8. 策略推进、替换与回退

策略快照含 `version`、`parent_version`、`parent_hash` 与运行参数。分类规则：

| 条件 | 分类 | 行为 |
|---|---|---|
| 内容指纹完全相同 | `advance` | 正常推进，不失效分支 |
| 版本递增、父链匹配、运行参数相同 | `advance` | 仅推进元数据，分支事实保留 |
| 版本相同但内容改变 | `replacement` | 全部分支状态失效重建 |
| 版本递增但父链不匹配或运行参数改变 | `replacement` | 全部分支状态失效重建 |
| 候选版本低于当前版本 | `rollback` | 拒绝，返回 `E_POLICY_ROLLBACK` |

替换在原 run 内递增 `policy_epoch`、失效旧步骤、清空分支、重置检查点并重建计划；新检查点使用新的 epoch scope。回退在写事务前拒绝。

## CLI

```bash
./motionlab init  --data ./runtime [--force]
./motionlab serve --data ./runtime --addr 127.0.0.1:8080
./motionlab run   --data ./runtime --seq harbor_slide [--fresh] [--json]
./motionlab demo  --data ./demo-runtime [--out ./demo-report.json]
./motionlab import --data ./runtime --spec sequence.json --frames ./frames \
  [--initial initial.png] [--background background.png]
```

`run` 还可设置 `--steps`、`--concurrency`、`--quota`、`--budget`、`--prune`、`--disable-prune`。`--crash-after N` 用于验收器模拟提交后强杀。

## HTTP API

所有 JSON 成功响应为 `{"ok":true,"data":...}`；失败为 `{"ok":false,"error":{...}}`。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/health` | 服务与 SQLite 状态 |
| GET | `/api/bootstrap` | 序列、策略、运行摘要 |
| GET | `/api/sequences` | 序列清单 |
| GET | `/api/sequences/{id}` | 序列规格与真值摘要 |
| GET | `/api/sequences/{id}/initial.png` | 共享初始帧 |
| GET | `/api/sequences/{id}/background.png` | 无物体背景帧 |
| GET | `/api/sequences/{id}/truth/{n}.png` | 真值帧 |
| POST | `/api/import` | 导入运动清单 + PNG 帧 |
| GET | `/api/policy` | 当前策略快照与指纹 |
| GET | `/api/runs` | 运行档案 |
| POST | `/api/runs` | 创建/重置并可选启动运行 |
| GET | `/api/runs/{id}` | 运行、分支、步骤、事件快照 |
| POST | `/api/runs/{id}/start` | 启动/继续 |
| POST | `/api/runs/{id}/stop` | 优雅停止并保留已提交前缀 |
| POST | `/api/runs/{id}/resume` | 从已提交事实恢复 |
| GET | `/api/runs/{id}/report` | 规范化推演报告 |
| GET | `/api/runs/{id}/events` | 剪枝/完成与策略事件 |
| GET | `/api/runs/{id}/branches/{b}/steps` | 单分支步骤明细 |
| GET | `/api/runs/{id}/branches/{b}/frames/{n}.png` | 已提交预测帧 |
| PUT | `/api/runs/{id}/policy` | 推进/替换/回退策略 |

创建运行示例：

```json
{
  "sequence_id": "harbor_slide",
  "requested_steps": 12,
  "concurrency": 2,
  "branch_quota": 12,
  "round_time_budget_ms": 80,
  "prune_threshold": 2.5,
  "stage_mode": false,
  "fresh": true,
  "auto_start": true
}
```

`stage_mode=true` 时每次启动/恢复只推进一个调度波次，完成后将运行置为 `paused`，可反复调用 `resume` 逐阶段观察；控制台提供“单阶段启动”按钮。

## 导入格式

HTTP `POST /api/import` 接受：

```json
{
  "spec": { "sequence_id": "my_sequence", "total_frames": 8, "...": "..." },
  "frames": [
    {"index": 0, "png_base64": "..."},
    {"index": 1, "png_base64": "..."}
  ],
  "initial_png_base64": "可选",
  "background_png_base64": "可选"
}
```

`spec` 即运动清单中的单个 `SequenceSpec`。CLI 导入则从目录读取 `0000.png` 起的连续真值帧。导入会校验尺寸、帧数、分支数、场景矩形并原子写入清单/帧索引。

## 数据目录

首次运行自动生成：

```text
runtime/
  manifest.json              # 3 条运动清单
  policy.json                # 版本化策略快照
  frames.json                # 真值帧摘要索引
  sequences/<id>/
    initial.png
    background.png
    truth/0000.png ...
  motionlab.db               # SQLite WAL
  .locks/                    # 操作系统文件锁
```

## 主要错误码

- `E_TIMELINE_OVERFLOW`：预测步数越过真值帧，带序列与超出步数
- `E_SHARED_FRAME_WRITE`：写共享初始帧，带序列与分支号
- `E_STALE_CHECKPOINT`：检查点落后、重复或跳跃
- `E_POLICY_ROLLBACK`：策略版本回退
- `E_POLICY_CONFLICT`：运行中策略变化或重复确定性键
- `E_RUN_BUSY`：同一运行重复执行
- `E_TIME_BUDGET`：分支未启动原因（记录在分支状态，不作为 HTTP 顶层错误）
- `E_BRANCH_QUOTA`：有效配额为零
