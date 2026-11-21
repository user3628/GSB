"""支持 ``python -m recon3d`` 启动本地 HTTP 服务。

端口取环境变量 RECON3D_PORT（默认 8077），始终绑定 127.0.0.1。
"""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    port = int(os.environ.get("RECON3D_PORT", "8077"))
    uvicorn.run("recon3d.api:app", host="127.0.0.1", port=port, log_level="info")


if __name__ == "__main__":
    main()
