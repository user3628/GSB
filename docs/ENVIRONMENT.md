# 环境与依赖记录

本次实际验证环境：

```text
Linux 6.18.58.1-microsoft-standard-WSL2 x86_64
Ubuntu/WSL
go version go1.27.4 linux/amd64
gcc (Ubuntu 15.2.0-16ubuntu1) 15.2.0
libsqlite3-dev: 3.46.1-9ubuntu0.3
sqlite3 CLI: 3.46.1
```

`go.mod` 声明 `go 1.22`，代码使用 Go 1.22 可用特性；实际本机工具链为 Go 1.27.4。

## SQLite 驱动

选择 cgo 常见驱动：

```text
github.com/mattn/go-sqlite3 v1.14.24
```

该驱动 vendored 在 `vendor/github.com/mattn/go-sqlite3`，其捆绑的 SQLite amalgamation 版本为：

```c
#define SQLITE_VERSION "3.46.1"
```

系统同时安装了 `libsqlite3-dev 3.46.1`，满足 cgo/LDFLAGS 环境准备要求。构建时需要：

```bash
sudo apt-get update
sudo apt-get install -y gcc libsqlite3-dev
go build -o bin/csrouter ./cmd/csrouter
```

当前环境 `CGO_ENABLED=1`。项目已提交 `vendor/`，在依赖缓存或网络不可用时，Go 会以 vendor 模式构建，不需要联网下载依赖。

## NFKC 依赖

真实 Unicode NFKC 使用：

```text
golang.org/x/text v0.21.0/unicode/norm
```

同样已经 vendored。没有使用外部数据集、Python 服务、Docker 或其他容器。

## 网络与端口

- 生成器和验证完全本地、确定性。
- HTTP 默认监听 `127.0.0.1:18099`。
- `internal/api.Server.ListenAndServe` 会拒绝非 loopback host，避免误暴露到局域网。
- 如端口占用，可传其他空闲回环端口，例如 `--addr 127.0.0.1:18100`。
