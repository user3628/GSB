from __future__ import annotations

import argparse
import asyncio

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse


app = FastAPI(title="Loopback upstream fixture")


@app.get("/ok")
def ok() -> dict[str, object]:
    return {"ok": True, "mode": "success"}


@app.get("/slow")
async def slow(delay_ms: int = Query(default=500, ge=0, le=5000)) -> dict[str, object]:
    await asyncio.sleep(delay_ms / 1000)
    return {"ok": True, "mode": "slow", "delay_ms": delay_ms}


@app.get("/fail")
def fail() -> JSONResponse:
    return JSONResponse(status_code=503, content={"ok": False, "mode": "upstream_failure"})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Loopback-only upstream fixture endpoint")
    parser.add_argument("--port", type=int, default=8766)
    return parser.parse_args()


def main() -> None:
    import uvicorn

    args = parse_args()
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
