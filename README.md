# 主机文件权限策略治理工作台

这是一个从零实现的本地权限治理服务。它把 JSON 策略快照/增量、真实文件系统落地、安全描述符/POSIX ACL 回读、真实文件打开验证、有效的级联视图、SQLite 意图日志和崩溃恢复放在同一个可运行工作台中。

代码不依赖 Docker，不访问外部目标，也不要求预建真实用户账户。样本、摘要和增量由程序内置生成器确定性产出。

## 交付内容

- `cmd/workbench`：HTTP 服务、恢复、清理、样本、摘要和端到端验收命令。
- `internal/model`：策略校验、规范化摘要、组成员闭包、路径继承、有效权限、级联差异。
- `internal/store`：SQLite 版本/视图/意图/验证/恢复报告持久化。
- `internal/fsx`：平台文件落地与 ACL 回读；Linux POSIX ACL、Windows 安全描述符。
- `internal/engine`：提交、原子级联、版本绑定读取、并发预算、意图事务与恢复。
- `internal/httpapi`：本地 HTTP 接口和结构化错误。
- `internal/acceptance`：真实文件、真实内核打开、强制终止和恢复验收。
- `artifacts/acceptance-report.json`：本次实际验收记录。
- `artifacts/recovery-report.json`：本次强制终止后的实际恢复报告。
- `docs/DESIGN.md`：事务、依赖失效、平台 ACL 和并发裁决的技术说明。

## Windows 运行

环境：Windows 10+、Go 1.22+、具备创建文件和设置安全描述符权限的本地账户。项目使用纯 Go SQLite 驱动，不需要 CGO 或 MSVC。

```powershell
go version
go test ./...
go build -o workbench.exe .\cmd\workbench

# --root 必须是空目录；--db 必须在 root 之外
New-Item -ItemType Directory C:\workbench-data\root -Force
.\workbench.exe serve `
  --root C:\workbench-data\root `
  --db C:\workbench-data\state.db `
  --addr 127.0.0.1:8080
```

Windows 落地路径：

- 每个策略版本构建独立的 `root\trees\vNNNNNN-*` 物理树。
- 每个受管目录/文件建立受保护 DACL，先写真实安全描述符，再通过 `GetNamedSecurityInfo` 回读 ACE、掩码和继承标志比较。
- 主体使用确定性合成 SID，不创建真实账户。DACL 以 Everyone 为放行基线，并为每个主体写入针对其缺失权限的拒绝掩码。
- 验证时创建包含 Everyone、RESTRICTED 与目标主体 SID 的受限令牌，在线程上 `ImpersonateLoggedOnUser`，再调用真实 `CreateFileW` 请求 `GENERIC_READ` 或 `GENERIC_WRITE`。拒绝来自 Windows 内核访问检查，不是 Go 侧预判。
- 每个主体 × 每个受管路径 × `read/write` 各执行一次真实打开；目录写验证使用带 `FILE_FLAG_BACKUP_SEMANTICS` 的目录句柄。

构建已验证可交叉编译：

```bash
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
```

当前交付环境是 WSL/Linux，无法在本机执行 Windows 运行时验收；Windows 代码已编译，Linux 上的同等真实内核验收记录见下文。Windows 本机可运行：

```powershell
.\workbench.exe acceptance `
  --report artifacts\acceptance-report.json `
  --recovery-report artifacts\recovery-report.json
```

## Linux 验收适配层

为了让当前 WSL 工作区也能执行真实文件访问，而不是只做内存模拟，Linux 后端使用：

- 每个主体映射为确定性 UID/GID；
- 每个受管节点写入真实 `system.posix_acl_access` 并 `getxattr` 回读比较；
- 通过 `setuid/setgid` 子进程执行真实 `dd` 打开；
- 普通文件的写测试打开现有文件，不截断；
- 目录写测试在目录内创建并打开探针文件，这是 Linux 上检验目录写+执行权限的真实打开方式；
- 拒绝仅当内核返回 `Permission denied` 时记录 `kernel_denied=true`。

Linux 验收需 root，以便切换到确定性主体 UID：

```bash
go build -o /tmp/hostacl-workbench ./cmd/workbench
sudo /tmp/hostacl-workbench acceptance \
  --report artifacts/acceptance-report.json \
  --recovery-report artifacts/recovery-report.json
```

## HTTP API

服务默认监听 `127.0.0.1:8080`。所有失败响应均为：

```json
{
  "code": "E_STALE_BASE",
  "message": "increment base version does not match the committed head",
  "context": {"base_version": 3, "current_version": 4, "target_version": 4}
}
```

主要端点：

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/healthz` | 健康检查 |
| `GET` | `/v1/state` | 当前 head、摘要、树路径、主体/路径/视图数量 |
| `GET` | `/v1/versions` | 所有已提交版本 |
| `GET` | `/v1/sample` | 确定性快照 v1 与增量 v2/v3/v4 |
| `POST` | `/v1/snapshots` | 整体快照替换 |
| `POST` | `/v1/increments` | 带基础版本的局部增量 |
| `POST` | `/v1/apply` | 对当前版本重新落地 ACL 并回读 |
| `POST` | `/v1/cascade` | 从当前策略原子重建全部有效视图 |
| `POST` | `/v1/verify` | 主体/路径/读写方式真实打开验证 |
| `GET` | `/v1/verifications?run_id=` | 验证运行状态；策略推进后旧运行标记 `superseded` |
| `POST` | `/v1/reads` | 携带 `observed_version` 的版本绑定读取 |
| `GET` | `/v1/views` | 当前有效视图，可按 `subject_id`、`path` 查询 |
| `GET` | `/v1/recovery` | 最近一次启动恢复报告 |

### 提交快照

```bash
curl -s http://127.0.0.1:8080/v1/sample > sample.json
python - <<'PY' > snapshot.json
import json
print(json.dumps(json.load(open('sample.json'))['SnapshotV1']))
PY
curl -s -X POST http://127.0.0.1:8080/v1/snapshots \
  -H 'Content-Type: application/json' --data-binary @snapshot.json
```

快照 `digest` 覆盖规范化后的 `version/generated_at/subjects/paths`。自定义输入可先写不带摘要的 JSON，再用：

```bash
./workbench digest --kind snapshot --in input.json --out snapshot.json
./workbench digest --kind increment --in input.json --out increment.json
```

### 增量

```json
{
  "base_version": 1,
  "version": 2,
  "generated_at": "2026-11-25T00:00:02Z",
  "digest": "sha256:...",
  "subject_changes": [
    {"op": "upsert", "subject": {"id": "g-readers", "type": "group", "priority": 50, "members": ["u-api"]}}
  ],
  "path_changes": [
    {"op": "delete", "path_id": "obsolete/file.txt"}
  ]
}
```

同一基础版本的并发增量由写锁与 SQLite head 比较交换共同保护，只有一个成功；其余返回 `E_STALE_BASE`。

### 版本绑定读取

```json
{"subject_id": "u-api", "path": "data", "observed_version": 3}
```

当已提交视图版本 `view_version >= observed_version` 时返回结论并标记 `used_version`；若视图落后则返回 HTTP 409 与：

```json
{
  "code": "E_STALE_READ",
  "message": "effective view has not reached the reader's observed policy version",
  "context": {"view_version": 2, "observed_version": 3, "subject_id": "u-api", "path": "data"}
}
```

### 验证与预算

```json
{
  "max_concurrency": 6,
  "queue_size": 128,
  "budget_ms": 60000,
  "subject_ids": ["u-api", "svc-indexer"],
  "paths": ["data", "apps/api/config.json"]
}
```

- `tasks > max_concurrency + queue_size` 时立即返回 `E_VERIFY_QUEUE_FULL`，不写验证结论。
- 每轮按主体优先级降序、主体 ID、路径、打开方式稳定排序。
- 策略版本提交会在同一事务中把旧版本验证运行标记为 `superseded`；旧结论只可审计，不可冒充当前结论。
- 预算耗尽后，未开始或执行中被取消的任务进入 `skipped`，`status=degraded`。
- 每条结果记录 `open_mode`、`requested`、`actual`、`kernel_denied`、`expected`、平台身份和实际引擎。

## 业务码

| 业务码 | HTTP | 语义 |
|---|---:|---|
| `E_SPEC_INVALID` | 400 | 整批规格无效，不产生任何提交 |
| `E_STALE_BASE` | 409 | 增量基础版本不是当前 head，或并发中落败 |
| `E_APPLY_MISMATCH` | 409 | ACL 回读不一致，或真实打开与有效视图不一致 |
| `E_STALE_READ` | 409 | 有效视图版本落后于读者观察版本 |
| `E_VERIFY_QUEUE_FULL` | 429 | 有界验证队列容量不足 |
| `E_IDENTITY_UNAVAILABLE` | 503 | 平台不具备真实身份切换能力 |
| `E_RECOVERY_REQUIRED` | 503 | 启动恢复无法重建最后完整版本 |

## 实际验证结果

本次在 WSL/Linux（Go 1.27.4）执行：

```text
go test -race ./...                     PASS
GOOS=windows CGO_ENABLED=0 go build     PASS
sudo workbench acceptance               PASS
```

验收报告结果：

- 21/21 项检查通过。
- 确定性快照 v1、组嵌套增量 v2、继承变化增量 v3、恢复后 v4、并发增量 v5 全部落地。
- 6 个主体 × 6 个受管路径 × 读/写 = 72 次真实内核打开。
- 29 次放行，43 次内核拒绝，0 次视图与内核不一致。
- 观察到并通过 `E_SPEC_INVALID`、`E_STALE_BASE`、`E_APPLY_MISMATCH`、`E_STALE_READ`、`E_VERIFY_QUEUE_FULL`。
- v5 提交后，v4 的真实打开验证运行在同一提交事务中变为 `superseded`，历史结论不再具有当前效力。
- 在“ACL 已落地、数据库尚未提交”位置强制终止，进程退出码 137；重启后扫描到 pending increment，回滚到最后完整版本 3，清理孤儿树，再成功重新提交原 v4。
- 最终 head 为版本 5，`view_digest` 为 `sha256:5a1dd2...4688a`。

详细机器可读证据见 `artifacts/acceptance-report.json` 和 `artifacts/recovery-report.json`。

## 恢复与运维

```bash
# 只扫描并恢复
workbench recover --root /path/root --db /path/state.db --out recovery.json

# 正常结束后清理工作台创建的树与 marker
workbench cleanup --root /path/root
```

恢复逻辑只报告已落盘事实：

- pending 意图是否仍存在；
- 最后完整版本与树路径；
- 是否发现缺失/未引用树；
- 是否重新落地最后完整版本；
- 哪些孤儿树被删除。

不会把未提交的意图宣称为成功。

## 约束与说明

- 目录写权限在 Linux 上不能以 `O_WRONLY` 打开目录本身，因此使用目录内真实创建并打开探针文件来验证写+执行权限；Windows 直接以目录句柄请求写权限。
- Windows 主体是确定性合成 SID，不依赖真实账户；真实访问由受限令牌 `CreateFileW` 完成。生产环境若需要绑定真实账户，可将 SID 映射层替换为账户 SID/登录令牌，策略与事务层无需变化。
- Linux 验收适配层使用确定性 UID 模拟命名主体，不能替代 Windows 本机运行时验收；本次已提供 Windows 编译验证和完整 Windows API 实现。
- 工作台不提供网络远程访问控制；HTTP 默认只监听环回地址。

## 测试

```bash
go test ./...
go test -race ./...
go vet ./...
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
```

单元测试覆盖规格/摘要、组环、组嵌套、继承、拒绝优先、级联差异、陈旧读、并发基础版本、恢复中止、队列上限和 HTTP 结构化错误。
