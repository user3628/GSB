# JoinCost Lab

面向本地确定性样本的连接顺序优化、代价评估与诊断工作台。项目只用 Python
标准库（Python 3.10+），使用 SQLite 内置 `sqlite3`，HTTP 服务只监听回环地址。
不访问外部网络，不依赖真实数据库，也不需要 Docker。

## 已交付

- 两种可互换统计格式：紧凑二进制 `JCSB` v1/v2 与自描述 JSON Lines v1/v2。
- 显式兼容矩阵：`ACCEPT`、`DEGRADE_ACCEPT`、`REJECT`，导入报告持久化。
- 定点数值归一化、行数溢出检查、逐条舍入位与累计舍入误差。
- 等值、范围、范围连接、自连接选择率估算及相关系数修正。
- 仅生成有谓词边的左深连接，禁止笛卡尔积分支，搜索超限附剪枝数。
- 对数域中间基数、有限上界、扫描/连接/中间写出三项定点代价分解。
- 相对容差定点比较、关系名稳定字典序平局裁决、快照版本绑定。
- SQLite 原子快照替换、请求幂等/冲突检测、意图日志、强制终止恢复。
- 仅回环 HTTP API 与内置诊断页面。
- 固定种子样本生成器、41 项回归测试、真实 `kill` 恢复演示。

详细格式见 [docs/FORMATS.md](docs/FORMATS.md)，设计与业务规则见
[docs/DESIGN.md](docs/DESIGN.md)，实际验证记录见 [VALIDATION.md](VALIDATION.md)。

## Windows 快速开始

PowerShell：

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python samples\generate_samples.py
python -m unittest discover -s tests -v
python scripts\demo.py
python scripts\joincost.py serve --db artifacts\joincost.db --port 8765
```

运行时不依赖任何第三方包。若希望获得 `joincost` 控制台入口，可在有 `setuptools`
的虚拟环境中执行 `python -m pip install -e .`；否则直接使用上面的源码启动器。

也可显式设置源码路径后使用模块入口：

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
python -m joincost.cli serve --db artifacts\joincost.db --port 8765
```

Linux/macOS 验证命令：

```bash
python3 samples/generate_samples.py
python3 -m unittest discover -s tests -v
python3 scripts/demo.py
python3 scripts/joincost.py serve --db artifacts/joincost.db --port 8765
```

随后访问 `http://127.0.0.1:8765/`。诊断页面可导入两种样本、构建计划、查看快照和恢复报告。

## HTTP API

所有失败响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_FORMAT_CORRUPT",
    "message": "...",
    "details": {}
  }
}
```

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 健康状态与当前快照 |
| `GET` | `/` | 本地诊断工作台 |
| `POST` | `/v1/snapshots/import` | 按 `path` 或 `content_base64` 导入快照 |
| `GET` | `/v1/snapshot` | 当前完整快照与历史版本列表 |
| `GET` | `/v1/snapshots/{version}` | 指定历史版本 |
| `GET` | `/v1/imports` | 导入报告，包括 `REJECT` 报告 |
| `POST` | `/v1/plans` | 提交幂等计划构建请求 |
| `GET` | `/v1/plans/{request_id}` | 读取计划或已存错误 |
| `GET` | `/v1/requests/{request_id}` | 请求执行状态 |
| `GET` | `/v1/recovery` | 启动恢复报告与最近一次恢复报告 |
| `POST` | `/v1/recovery` | 主动执行一次恢复扫描 |

导入二进制样本：

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8765/v1/snapshots/import `
  -ContentType "application/json" `
  -Body '{"path":"samples/stats_v2.bin"}'
```

提交计划：

```powershell
$body = @{
  request_id = "demo-1"
  snapshot_version = 1001
  predicates = Get-Content samples\predicates.json -Raw | ConvertFrom-Json
} | ConvertTo-Json -Depth 20
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8765/v1/plans `
  -ContentType "application/json" `
  -Body $body
```

计划响应包含：最优关系顺序、逐步骤选择率与中间基数、每步有限上界、扫描/连接/
中间写出三项定点代价、舍入说明、搜索统计、容差契约和版本绑定信息。

## 样本

`samples/generate_samples.py` 使用固定种子 `20261123` 生成：

- `stats_v2.bin` 与 `stats_v2.jsonl`：语义摘要完全相同（同一版本和 digest）。
- `stats_v1_legacy.bin` / `.jsonl`：旧版本降级接受样本。
- `stats_v3_unsupported.jsonl`：未知必填字段/高版本拒绝样本。
- `stats_replacement.bin` / `.jsonl`：下一单调版本，用于整体替换。
- `predicates.json`：等值、范围、范围连接、自连接谓词。

样本说明见 [samples/README.md](samples/README.md)。

## 固定规模与确定性

- 默认最多搜索 8 个关系，硬上限 9；超过时返回 `E_SEARCH_SPACE_TOO_LARGE` 和剪枝数。
- 内部比率定点比例为 `1e9`，基数与代价定点比例为 `1e6`。
- 基数先在对数域累加，再半偶舍入；代价分项只做整数定点加总。
- 每个中间结果给出 `upper_bound_rows`，上限为 `2^63-1`，并标记是否封顶。
- 同一快照、同一请求标识和同一输入返回完全相同的已持久化计划。

## 测试与演示

```bash
python3 -m unittest discover -s tests -v
python3 scripts/demo.py
```

演示脚本会真实启动多个回环 HTTP 子进程，完成两种格式互操作、异常注入、版本替换，
并以 `kill` 强制终止正在构建计划的进程；重启后从 SQLite 意图记录恢复并输出差异。
输出同时写入 `artifacts/demo_report.json`。
