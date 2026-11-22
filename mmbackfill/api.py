from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, Request
from fastapi.responses import JSONResponse

from .canonical import read_json, read_jsonl
from .engine import Engine
from .errors import HTTP_STATUS, EngineError
from .workspace import Workspace


def create_app(workspace_root: Path | str) -> FastAPI:
    workspace = Workspace(workspace_root).ensure()
    engine = Engine(workspace)
    app = FastAPI(title="mmbackfill local API", version="0.1.0")

    @app.exception_handler(EngineError)
    async def engine_error_handler(_request: Request, exc: EngineError) -> JSONResponse:
        return JSONResponse(
            status_code=HTTP_STATUS.get(exc.code, 400),
            content={"ok": False, "error": exc.to_dict()},
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "workspace": str(workspace.root)}

    @app.get("/doctor")
    def doctor() -> dict[str, Any]:
        return workspace.doctor()

    @app.get("/status")
    def status() -> dict[str, Any]:
        return engine.status()

    @app.put("/layout")
    @app.post("/layout")
    def put_layout(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        return engine.install_layout(payload)

    @app.post("/layout/import")
    def import_layout(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        path = payload.get("path")
        if not isinstance(path, str) or not path:
            raise EngineError("E_INVALID_INPUT", "path must be a non-empty string", "request_validation")
        return engine.install_layout(read_json(Path(path)))

    @app.put("/images")
    def put_images(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        entries = payload.get("entries")
        if not isinstance(entries, list):
            raise EngineError("E_INVALID_INPUT", "entries must be an array", "request_validation")
        copy_from = payload.get("copy_from")
        source_dir = Path(copy_from) if isinstance(copy_from, str) and copy_from else None
        return engine.import_images(entries, source_dir)

    @app.post("/images/import")
    def import_images(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        path = payload.get("path")
        if not isinstance(path, str) or not path:
            raise EngineError("E_INVALID_INPUT", "path must be a non-empty string", "request_validation")
        copy_from = payload.get("copy_from")
        return engine.import_images(
            read_jsonl(Path(path)),
            Path(copy_from) if isinstance(copy_from, str) and copy_from else None,
        )

    @app.put("/texts")
    def put_texts(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        entries = payload.get("entries")
        if not isinstance(entries, list):
            raise EngineError("E_INVALID_INPUT", "entries must be an array", "request_validation")
        return engine.import_texts(entries)

    @app.post("/texts/import")
    def import_texts(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        path = payload.get("path")
        if not isinstance(path, str) or not path:
            raise EngineError("E_INVALID_INPUT", "path must be a non-empty string", "request_validation")
        return engine.import_texts(read_jsonl(Path(path)))

    @app.post("/inputs/import")
    def import_inputs(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        images_path = payload.get("images_jsonl")
        texts_path = payload.get("texts_jsonl")
        layout_path = payload.get("layout")
        if not all(isinstance(value, str) and value for value in (images_path, texts_path, layout_path)):
            raise EngineError(
                "E_INVALID_INPUT",
                "images_jsonl, texts_jsonl and layout must be paths",
                "request_validation",
            )
        engine.install_layout(read_json(Path(layout_path)))
        copy_from = payload.get("copy_from")
        result = engine.import_inputs(
            read_jsonl(Path(images_path)),
            read_jsonl(Path(texts_path)),
            Path(copy_from) if isinstance(copy_from, str) and copy_from else None,
        )
        return result

    @app.get("/catalog")
    def catalog() -> dict[str, Any]:
        return {
            "ok": True,
            "images": workspace.load_images(),
            "texts": workspace.load_texts(),
        }

    @app.post("/backfill")
    def backfill(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        required = ("batch_id", "text_ids", "expected_sequence")
        missing = [field for field in required if field not in payload]
        if missing:
            raise EngineError(
                "E_INVALID_INPUT",
                "missing required backfill fields",
                "request_validation",
                details={"missing_fields": missing},
            )
        overrides = payload.get("vector_overrides")
        if overrides is not None and not isinstance(overrides, dict):
            raise EngineError("E_INVALID_INPUT", "vector_overrides must be an object", "request_validation")
        return engine.submit_batch(
            batch_id=payload["batch_id"],
            text_ids=payload["text_ids"],
            expected_sequence=payload["expected_sequence"],
            order=payload.get("order", "id"),
            vector_overrides=overrides,
        )

    @app.post("/backfill/all")
    def backfill_all(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        text_ids = payload.get("text_ids")
        if not isinstance(text_ids, list):
            raise EngineError("E_INVALID_INPUT", "text_ids must be an array", "request_validation")
        return engine.backfill_all(
            text_ids=text_ids,
            expected_sequence=payload.get("expected_sequence", 0),
            batch_size=payload.get("batch_size", 2),
            batch_prefix=payload.get("batch_prefix", "batch"),
            order=payload.get("order", "id"),
        )

    @app.get("/index")
    @app.get("/shards")
    def index() -> dict[str, Any]:
        return engine.index()

    @app.get("/batches")
    @app.get("/checkpoints")
    def batches() -> dict[str, Any]:
        return engine.batches()

    @app.get("/invalidations")
    def invalidations() -> dict[str, Any]:
        return engine.invalidations()

    @app.get("/vectors")
    def vectors() -> dict[str, Any]:
        return engine.vectors()

    @app.get("/diagnostics")
    def diagnostics() -> dict[str, Any]:
        return engine.diagnostics()

    @app.post("/stop")
    def stop(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
        reason = "operator request"
        if isinstance(payload, dict) and isinstance(payload.get("reason"), str):
            reason = payload["reason"]
        return {"ok": True, "stop": workspace.request_stop(reason)}

    @app.post("/recover")
    @app.post("/resume")
    def recover(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
        clear_stop = True if not isinstance(payload, dict) else bool(payload.get("clear_stop", True))
        return engine.recover(clear_stop=clear_stop)

    return app
