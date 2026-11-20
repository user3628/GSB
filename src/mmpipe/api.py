"""本地 HTTP 交互接口（FastAPI），仅监听 127.0.0.1，不访问外部服务。"""

import threading

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from . import STAGES
from .errors import PipeError
from .importer import import_samples, load_workspace_inputs
from .pipeline import StopController, expected_inputs, run_pipeline
from .storage import Workspace

_app_state: dict = {}


def create_app(workspace_root: str) -> FastAPI:
    app = FastAPI(title="mmpipe", version="1.0.0")
    state = {
        "root": workspace_root,
        "lock": threading.Lock(),
        "run_thread": None,
        "stop": None,
        "last_report": None,
        "last_error": None,
    }
    _app_state["default"] = state

    def get_state() -> dict:
        return _app_state["default"]

    def open_ws() -> Workspace:
        return Workspace(state["root"])

    @app.exception_handler(PipeError)
    async def pipe_error_handler(_request, exc: PipeError):
        return JSONResponse(status_code=409,
                            content={"ok": False, "error": exc.to_dict()})

    @app.get("/health")
    def health():
        return {"ok": True, "service": "mmpipe", "workspace": state["root"]}

    @app.post("/workspace/init")
    def workspace_init():
        ws = open_ws()
        ws.cleanup_stale_tmp()
        ws.log_event("workspace_init_http")
        return {"ok": True, "workspace": ws.root,
                "cleaned_tmp_files": ws.cleaned_tmp}

    @app.post("/import")
    def import_data(body: dict):
        ws = open_ws()
        manifest = import_samples(
            ws,
            body.get("texts", "samples/texts.jsonl"),
            body.get("images", "samples/images.json"),
            body.get("config", "samples/pipeline_config.json"))
        return {"ok": True, "manifest": manifest}

    @app.post("/run")
    def run(body: dict | None = None):
        body = body or {}
        with state["lock"]:
            if state["run_thread"] and state["run_thread"].is_alive():
                return JSONResponse(
                    status_code=409,
                    content={"ok": False, "error": {
                        "code": "E_RUN_IN_PROGRESS",
                        "message": "pipeline run already in progress"}})
            ws = open_ws()
            stop = StopController(ws)
            stop.clear()
            state["stop"] = stop
            state["last_error"] = None

            def target():
                local_ws = Workspace(state["root"])
                try:
                    config, texts, images = load_workspace_inputs(local_ws)
                    state["last_report"] = run_pipeline(
                        local_ws, config, texts, images,
                        stop=state["stop"],
                        max_workers=body.get("workers"),
                        unit_delay=float(body.get("unit_delay", 0.0)))
                except PipeError as exc:
                    state["last_error"] = exc.to_dict()
                except Exception as exc:  # pragma: no cover
                    state["last_error"] = {"code": "E_INTERNAL",
                                           "message": str(exc)}

            thread = threading.Thread(target=target, daemon=True)
            state["run_thread"] = thread
            thread.start()
        return {"ok": True, "started": True}

    @app.post("/stop")
    def stop():
        ws = open_ws()
        if state["stop"] is not None:
            state["stop"].request()
        else:
            ws.request_stop()
        ws.log_event("stop_requested_http")
        return {"ok": True, "stop_requested": True}

    @app.get("/run/status")
    def run_status():
        thread = state["run_thread"]
        running = bool(thread and thread.is_alive())
        return {
            "ok": True,
            "running": running,
            "stop_requested": open_ws().stop_requested(),
            "last_report": state["last_report"],
            "last_error": state["last_error"],
        }

    @app.post("/resume")
    def resume(body: dict | None = None):
        body = body or {}
        ws = open_ws()
        stop = StopController(ws)
        stop.clear()
        config, texts, images = load_workspace_inputs(ws)
        report = run_pipeline(ws, config, texts, images, stop=stop,
                              max_workers=body.get("workers"),
                              unit_delay=float(body.get("unit_delay", 0.0)))
        state["last_report"] = report
        return {"ok": True, "report": report}

    @app.get("/status")
    def status():
        ws = open_ws()
        stage_state = ws.state()["current"]
        expected = None
        if ws.config_exists():
            try:
                config, texts, images = load_workspace_inputs(ws)
                expected = expected_inputs(config, texts, images)
            except (FileNotFoundError, OSError):
                expected = None
        total, valid = ws.corrupt_checkpoint_count()
        stages = {}
        for stage in STAGES:
            cur = stage_state.get(stage)
            entry = {"status": cur.get("status") if cur else "absent"}
            if cur:
                entry.update({
                    "seq": cur["seq"],
                    "input_fingerprint": cur["input_fingerprint"],
                    "config_version": cur["config_version"],
                    "product_digest": cur["product_digest"],
                })
                if cur.get("status") == "invalidated":
                    entry["invalidation"] = cur.get("invalidation")
            if expected:
                entry["expected_input_fingerprint"] = expected[stage]["input"]
                entry["expected_config_version"] = expected[stage]["config"]
                entry["matches_expected"] = bool(
                    cur and cur.get("status") == "published"
                    and cur["input_fingerprint"] == expected[stage]["input"]
                    and cur["config_version"] == expected[stage]["config"])
            stages[stage] = entry
        return {"ok": True, "workspace": ws.root, "head_seq": ws.head_seq(),
                "checkpoint_files": total, "valid_checkpoints": valid,
                "ignored_corrupt": total - valid,
                "stop_requested": ws.stop_requested(),
                "stages": stages,
                "invalidations": ws.invalidations()}

    @app.get("/stages/{stage}")
    def stage_detail(stage: str):
        if stage not in STAGES:
            raise PipeError("E_INVALID_STAGE", f"unknown stage {stage!r}",
                            stage=stage)
        ws = open_ws()
        cur = ws.stage_state(stage)
        product = ws.load_current_product(stage)
        return {"ok": True, "stage_state": cur, "product": product}

    @app.get("/aggregate")
    def aggregate():
        ws = open_ws()
        product = ws.load_current_product("aggregation")
        if product is None:
            raise PipeError("E_INVALID_STAGE",
                            "aggregation product is not published",
                            stage="aggregation")
        return {"ok": True, "aggregate": product}

    @app.get("/events")
    def events():
        return {"ok": True, "events": open_ws().events()}

    return app


def main(argv: list[str] | None = None) -> int:
    import uvicorn
    import argparse
    parser = argparse.ArgumentParser(prog="mmpipe-serve")
    parser.add_argument("-w", "--workspace", default="mmpipe_workspace")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    app = create_app(args.workspace)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
