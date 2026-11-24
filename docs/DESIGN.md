# 技术说明

## 1. 状态与边界

一次完整提交包含四类持久状态：

1. `policy_versions`：不可变策略版本、摘要、树路径、级联摘要。
2. `effective_views`：每个主体对每个路径的读/写结论、主体闭包、ACL 来源链和视图摘要。
3. `policy_head`：当前完整版本和当前物理树。
4. `intents`：在副作用发生前写入的意图记录。

文件系统不存“半版本”。每个提交构建独立的 `trees/vNNNNNN-<intent>` 树，ACL 回读通过后，SQLite 事务才同时写版本、全部视图、head 和意图完成状态。任何时刻读者看到的 head 要么是旧完整版本，要么是新完整版本。

## 2. 规格校验与摘要

提交前完成全部校验，任何问题都返回 `E_SPEC_INVALID`，不创建意图、不触碰文件系统。检查包括：

- 主体 ID 唯一，类型合法；
- 组成员引用存在，仅组参与嵌套边与环检测；
- 路径为清洁相对路径，无盘符、绝对根、`.`、`..`、反斜杠或空段；
- 路径不重复，文件不能成为其它受管路径的父节点；
- ACL 主体存在，权限仅为 `read/write`，效果仅为 `allow/deny`。

主体、路径、成员、ACL 和权限排序后计算 SHA-256。快照摘要覆盖版本、生成时间、完整主体和完整路径；增量摘要覆盖基础版本、目标版本、生成时间和全部局部变更。

快照版本必须高于当前 head。增量目标版本必须等于 `base_version + 1` 且基础版本必须等于当前 head。

## 3. 有效权限

### 组闭包

如果组 `G` 的 `members` 包含 `P`，则 `P` 属于 `G`。对目标主体，沿“成员 → 包含它的组”方向 BFS，得到它自身与所有祖先组。组自身也参与 ACL 判定，但不会自动获得其成员的权利。

### 路径继承

对于祖先受管节点 `A` 和目标 `T`，`A` 的 ACE 到达 `T` 的条件是：`T.Inherit=true`，且 `A` 与 `T` 之间每个受管路径也继承父级。目标节点自己的直接 ACE 始终生效。

### 拒绝优先

对每个主体闭包匹配 ACL。允许位和拒绝位独立累计，最终：

```text
read  = anyAllow(read)  && !anyDeny(read)
write = anyAllow(write) && !anyDeny(write)
```

视图同时保存主体闭包、路径链和命中 ACE，便于解释结论和比较语义变化。视图摘要包含版本，因此版本推进本身会更新摘要。

## 4. 依赖失效与级联

提交事务中先计算新旧全量视图，再比较：

- 主体闭包是否变化；
- 路径链是否变化；
- 命中 ACE 集合是否变化；
- 最终读写结论是否变化。

`CascadeDiff` 产出受影响主体/路径清单。随后在同一个 SQLite 事务中插入新版本、全部新视图、更新 head 并完成意图。读取事务只能看到旧 head+旧视图或新 head+新视图，不会出现部分视图更新。

同一事务还会把旧版本的 `verification_runs` 标记为 `superseded`。历史运行记录保留用于审计，但不再具有当前版本结论效力；新版本结论必须重新执行真实打开后生成。查询接口为 `GET /v1/verifications?run_id=...`。

显式 `/v1/cascade` 同样先写意图，再删除/重建当前版本的全部视图，并更新 head 的 `view_digest`；同一版本已有的真实验证运行会被标记为 `superseded`。重新落地 ACL 的 `/v1/apply` 同样使该版本旧验证结论失效。

## 5. 物理落地

### 版本树

新版本在 `trees/.stage-<intent>` 构建。受管文件和目录创建完成后，平台 ACL 写入并回读；成功后原子改名为版本树。数据库未提交时该树不被 head 引用，属于可清理孤儿。

### Linux

- 每个主体有稳定哈希 UID/GID，范围避开普通系统账户常用区。
- 以 `system.posix_acl_access` 二进制格式写入命名用户条目。
- 管理拥有者保留目录 `rwx`、文件 `rw` 维护权；实际策略主体只通过命名 UID 条目获得权限。
- 目录内所有主体至少获得执行位以支持路径遍历；读/写仍受目录自身读/写位控制。
- ACL 回读按 tag/perm/id 精确比较。

### Windows

- 每个主体生成确定性合成 SID。
- 每个受管节点设置 `PROTECTED_DACL`，阻止父目录自动继承干扰。
- DACL 由 Everyone 基线允许项、逐主体拒绝掩码和 SYSTEM 维护项组成。验证令牌同时把 Everyone、RESTRICTED 和目标主体 SID 放入限制列表，使普通与受限两轮访问检查都具备可放行基线；逐主体拒绝仍只匹配目标合成 SID。
- 写后使用 `GetNamedSecurityInfo` + `GetSecurityDescriptorDacl` + `GetAce` 回读类型、SID、掩码和标志。
- 主体验证使用 `CreateRestrictedToken` 创建含 Everyone、RESTRICTED 和目标 SID 的受限令牌，在线程上模拟后调用真实 `CreateFileW`。两次系统访问检查的交集实现对不同合成主体的可区分裁决。

## 6. 真实打开

每个任务记录：

```json
{
  "subject_id": "u-api",
  "path": "data",
  "open_mode": "write",
  "requested": "write",
  "actual": "denied",
  "kernel_denied": true,
  "expected": "denied",
  "match": true,
  "identity": "uid=690739,gid=799548",
  "engine": "linux-setuid-open"
}
```

Linux 用子进程切换 UID/GID 后执行真实打开；Windows 用受限令牌和 `CreateFileW`。任何 ACL 被篡改、文件缺失或身份后端异常都会表现为内核结果与视图不一致，验证 API 返回 `E_APPLY_MISMATCH`，不会把组件预判冒充成用户态结论。

## 7. 验证预算

任务稳定排序：主体优先级降序、主体 ID、路径、打开方式。容量为：

```text
max_concurrency + queue_size
```

超出容量时不启动任何打开，直接返回 `E_VERIFY_QUEUE_FULL` 和容量上下文。每轮建立截止时间；截止后已开始但被取消的任务和未开始任务都进入 `skipped`，状态为 `degraded`。`skipped` 按同一稳定顺序输出。

验证意图在任务执行前提交；只有 `verification_runs`、`verification_results` 和意图完成状态同一事务写入后，结果才对外可见。

## 8. 崩溃恢复

正常提交顺序：

```text
校验 → 写 pending intent → 建树/写 ACL/回读 → DB 事务提交版本+视图+head+intent完成
```

若在任意中间位置收到 `SIGKILL`，SQLite 中只可能留下 pending intent，head 仍指向最后完整版本。启动恢复：

1. 读取 head 和所有 pending intent；
2. 对无已提交证据的 pending intent 标记 `aborted`；
3. 若存在未提交的 policy/apply 副作用，或 head 树缺失，则从最后完整策略重新落地新树并回读；
4. 更新 head 树路径；
5. 删除未被任何已提交版本引用的树；
6. 将恢复报告写入 SQLite。

恢复报告中的每条事实都来自数据库记录、目录存在性或本轮重新落地结果；不会把未提交动作写成成功。

## 9. 并发模型

- 策略提交与物理落地持有写锁。
- 验证与读取持有读锁；验证期间 head 不切换。
- 同一基础版本的请求在写锁内读取 head，第二个请求看到已推进版本并返回 `E_STALE_BASE`。
- SQLite `policy_head` 更新包含版本比较条件，形成第二道 CAS 防线。
- 级联视图更新与 head 更新在同一 SQLite 事务中，禁止部分重建。
- 旧版本验证运行在同一事务中失效，避免把历史内核结论当作当前权限结论。

## 10. 已知平台差异

- Linux 不能以写方式 `open` 目录本身，目录写测试采用“在目录内创建并打开探针文件”。这仍是内核对目录写+执行权限的真实判定。
- Windows 合成 SID 不要求创建本地账户；真实账户映射可在 SID 解析/令牌构建层替换。
- 当前运行环境为 WSL，因此 Windows 后端完成了交叉编译，但运行时证据来自 Linux 适配层。Windows 本机验收命令已写入 README。
