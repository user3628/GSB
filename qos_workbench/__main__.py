from __future__ import annotations

import uvicorn

from .config import API_HOST, API_PORT


def main() -> None:
    uvicorn.run(
        "qos_workbench.api:app",
        host=API_HOST,
        port=API_PORT,
        workers=1,
        log_level="info",
    )


if __name__ == "__main__":
    main()

