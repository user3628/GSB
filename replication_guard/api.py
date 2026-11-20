from __future__ import annotations

import base64
import binascii
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Mapping

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import AppError
from .formats import COMPATIBILITY_MATRIX
from .service import ReplicationService


logger = logging.getLogger("replication_guard")


async def _json_body(request: Request) -> Any:
    try:
        return await request.json()
    except Exception as exc:
        raise AppError(
            "E_BAD_REQUEST",
            "请求体不是合法 JSON",
            status_code=400,
        ) from exc


def _int_or_none(value: Any, field: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise AppError("E_BAD_REQUEST", f"{field} 必须为整数", status_code=400)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise AppError("E_BAD_REQUEST", f"{field} 必须为整数", status_code=400) from exc


def _policy_payload(body: Mapping[str, Any]) -> tuple[dict[str, Any], int | None, str | None]:
    if "policy" in body:
        policy = body["policy"]
        if not isinstance(policy, Mapping):
            raise AppError("E_POLICY_INVALID", "policy 必须是对象", status_code=422)
        raw = dict(policy)
    else:
        metadata_keys = {"batchId", "batch_id", "baseDatasetVersion", "base_dataset_version"}
        raw = {key: value for key, value in body.items() if key not in metadata_keys}
    base = body.get("baseDatasetVersion", body.get("base_dataset_version"))
    batch_id = body.get("batchId", body.get("batch_id"))
    return raw, _int_or_none(base, "baseDatasetVersion"), str(batch_id) if batch_id else None


def create_app(db_path: str | Path | None = None) -> FastAPI:
    service = ReplicationService(db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.startup()
        yield

    app = FastAPI(
        title="Replication Lag Governance",
        version="1.0.0",
        description="主从复制延迟治理策略编辑、格式互操作与确定性裁决本地服务",
        lifespan=lifespan,
    )
    app.state.service = service

    @app.exception_handler(AppError)
    async def app_error_handler(_request: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.payload())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "code": "E_BAD_REQUEST",
                "message": "请求参数校验失败",
                "details": {"errors": exc.errors()},
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "ok": False,
                "code": code,
                "message": str(exc.detail),
                "details": {},
            },
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled error for %s", request.url.path, exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": "E_INTERNAL",
                "message": "服务内部错误",
                "details": {"type": type(exc).__name__},
            },
        )

    @app.get("/")
    def index() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "replication-lag-governance",
            "version": "1.0.0",
            "docs": "/docs",
            "health": "/healthz",
        }

    @app.get("/healthz")
    def health() -> dict[str, Any]:
        return {"ok": True, "status": "ready"}

    @app.get("/api/compatibility")
    def compatibility() -> dict[str, Any]:
        return {
            "ok": True,
            "matrix": COMPATIBILITY_MATRIX,
            "rules": {
                "binary-v1": "DEGRADED_ACCEPT: forwardCompatible=false, policyVersion=1, 秒级时间乘 1000",
                "binary-v2": "ACCEPT",
                "jsonl-v1": "ACCEPT",
                "higher-with-unknown-required": "REJECT: E_FORMAT_INCOMPATIBLE, 整批不导入",
            },
        }

    @app.get("/api/state-machine")
    def state_machine() -> dict[str, Any]:
        return service.state_machine_description()

    @app.get("/api/policies")
    def list_policies() -> dict[str, Any]:
        return service.list_policies()

    @app.get("/api/policies/export")
    def export_policies(
        format: str = "jsonl",
        version: int | None = None,
        shards: int = 1,
        bundle: bool = False,
    ) -> Response:
        content, metadata = service.export_policies(format, version=version, shards=shards)
        headers = {
            "X-RG-Format": str(metadata["format"]),
            "X-RG-Version": str(metadata["version"]),
            "X-RG-Dataset-Version": str(metadata["datasetVersion"]),
            "X-RG-Truncated-Records": str(len(metadata.get("truncations", []))),
        }
        if isinstance(content, dict):
            body = {"ok": True, "metadata": metadata, "shards": content}
            return JSONResponse(content=body, headers=headers)
        if bundle:
            return JSONResponse(content={"ok": True, "metadata": metadata, "content": content}, headers=headers)
        if metadata["format"] == "policy-binary":
            return Response(content=content, media_type="application/octet-stream", headers=headers)
        return Response(content=content, media_type="application/x-ndjson; charset=utf-8", headers=headers)

    @app.post("/api/policies/export")
    async def export_policies_post(request: Request) -> Response:
        body = await _json_body(request)
        if not isinstance(body, Mapping):
            raise AppError("E_BAD_REQUEST", "请求体必须是对象", status_code=400)
        return export_policies(
            format=str(body.get("format", "jsonl")),
            version=_int_or_none(body.get("version"), "version"),
            shards=int(body.get("shards", 1)),
            bundle=bool(body.get("bundle", False)),
        )

    @app.post("/api/policies/import")
    async def import_policies(request: Request) -> Response:
        query = request.query_params
        content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
        format_hint = query.get("format", "auto")
        batch_id = query.get("batch_id") or query.get("batchId")
        base_version = _int_or_none(query.get("base_dataset_version") or query.get("baseDatasetVersion"), "baseDatasetVersion")
        source_name = query.get("source_name") or query.get("sourceName")

        if content_type in {"application/octet-stream", "application/x-binary"}:
            data: bytes | str | list[str] = await request.body()
        elif content_type == "application/json":
            body = await _json_body(request)
            if isinstance(body, list):
                lines = [json.dumps({"_meta": {"format": "policy-jsonl", "version": 1}}, separators=(",", ":"))]
                lines.extend(json.dumps(item, ensure_ascii=False, separators=(",", ":")) for item in body)
                data = "\n".join(lines)
                format_hint = "jsonl" if format_hint == "auto" else format_hint
            elif isinstance(body, Mapping):
                format_hint = str(body.get("format", format_hint))
                batch_id = body.get("batchId", body.get("batch_id", batch_id))
                base_version = _int_or_none(
                    body.get("baseDatasetVersion", body.get("base_dataset_version", base_version)),
                    "baseDatasetVersion",
                )
                source_name = body.get("sourceName", body.get("source_name", source_name))
                if "shards" in body:
                    shards = body["shards"]
                    if isinstance(shards, Mapping):
                        data = [str(value) for value in shards.values()]
                    elif isinstance(shards, list):
                        data = [str(value) for value in shards]
                    else:
                        raise AppError("E_BAD_REQUEST", "shards 必须是数组或对象", status_code=400)
                else:
                    raw_data = body.get("data", body.get("content"))
                    if raw_data is None:
                        raise AppError("E_BAD_REQUEST", "导入请求缺少 data 或 shards", status_code=400)
                    encoding = str(body.get("encoding", "utf-8")).lower()
                    if isinstance(raw_data, list):
                        data = [str(item) for item in raw_data]
                    elif isinstance(raw_data, str) and (
                        format_hint.lower() in {"binary", "bin", "compact", "compact-binary", "policy-binary"}
                        or encoding == "base64"
                    ):
                        try:
                            data = base64.b64decode(raw_data, validate=True)
                        except (ValueError, binascii.Error) as exc:
                            raise AppError(
                                "E_BAD_REQUEST",
                                "binary 格式的 JSON 请求必须使用合法 base64 data",
                                status_code=400,
                            ) from exc
                    else:
                        data = raw_data if isinstance(raw_data, (str, bytes)) else str(raw_data)
            else:
                raise AppError("E_BAD_REQUEST", "JSON 导入体必须是对象或策略数组", status_code=400)
        else:
            raw = await request.body()
            if not raw:
                raise AppError("E_BAD_REQUEST", "导入请求体为空", status_code=400)
            data = raw.decode("utf-8-sig") if format_hint not in {"binary", "bin"} else raw

        result = service.import_policies(
            data,
            format_hint=format_hint,
            batch_id=str(batch_id) if batch_id else None,
            source_name=str(source_name) if source_name else None,
            expected_base_dataset_version=base_version,
        )
        headers = {"X-RG-Business-Code": "OK"}
        if result.get("businessCode"):
            headers["X-RG-Business-Code"] = str(result["businessCode"])
        return JSONResponse(content=result, headers=headers)

    @app.post("/api/policies")
    async def create_policy(request: Request) -> dict[str, Any]:
        body = await _json_body(request)
        if not isinstance(body, Mapping):
            raise AppError("E_BAD_REQUEST", "请求体必须是对象", status_code=400)
        raw, base, batch_id = _policy_payload(body)
        return service.upsert_policy(
            raw,
            base_dataset_version=base,
            batch_id=batch_id,
            operation="POLICY_CREATE",
        )

    @app.get("/api/policies/{slave_id}")
    def get_policy(slave_id: str) -> dict[str, Any]:
        return service.get_policy(slave_id)

    @app.put("/api/policies/{slave_id}")
    @app.patch("/api/policies/{slave_id}")
    async def update_policy(slave_id: str, request: Request) -> dict[str, Any]:
        body = await _json_body(request)
        if not isinstance(body, Mapping):
            raise AppError("E_BAD_REQUEST", "请求体必须是对象", status_code=400)
        raw, base, batch_id = _policy_payload(body)
        return service.upsert_policy(
            raw,
            slave_id=slave_id,
            base_dataset_version=base,
            batch_id=batch_id,
            operation="POLICY_UPDATE",
        )

    @app.delete("/api/policies/{slave_id}")
    def delete_policy(
        slave_id: str,
        base_dataset_version: int | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        return service.delete_policy(
            slave_id,
            base_dataset_version=base_dataset_version,
            batch_id=batch_id,
        )

    @app.post("/api/observations")
    @app.post("/api/observations/import")
    async def ingest_observations(request: Request) -> dict[str, Any]:
        body = await _json_body(request)
        if isinstance(body, list):
            observations = body
            batch_id = request.query_params.get("batch_id") or request.query_params.get("batchId")
            source_name = request.query_params.get("source_name") or request.query_params.get("sourceName")
        elif isinstance(body, Mapping):
            observations = body.get("observations")
            batch_id = body.get("batchId", body.get("batch_id", request.query_params.get("batch_id")))
            source_name = body.get("sourceName", body.get("source_name", request.query_params.get("source_name")))
        else:
            raise AppError("E_BAD_REQUEST", "请求体必须是观测数组或对象", status_code=400)
        if not isinstance(observations, list):
            raise AppError("E_BAD_REQUEST", "observations 必须是数组", status_code=400)
        return service.ingest_observations(
            observations,
            batch_id=str(batch_id) if batch_id else None,
            source_name=str(source_name) if source_name else None,
        )

    @app.get("/api/observations")
    def list_observations(batch_id: str | None = None) -> dict[str, Any]:
        return service.list_observations(batch_id=batch_id)

    @app.post("/api/recalculate")
    @app.post("/api/state/recalculate")
    async def recalculate(request: Request) -> Response:
        body = await _json_body(request)
        if not isinstance(body, Mapping):
            raise AppError("E_BAD_REQUEST", "请求体必须是对象", status_code=400)
        result = service.recalculate(
            batch_id=body.get("batchId", body.get("batch_id")),
            observation_batch_id=body.get(
                "observationBatchId", body.get("observation_batch_id")
            ),
            window_size=_int_or_none(body.get("windowSize", body.get("window_size", 2)), "windowSize") or 2,
            base_dataset_version=_int_or_none(
                body.get("baseDatasetVersion", body.get("base_dataset_version")), "baseDatasetVersion"
            ),
        )
        headers = {"X-RG-Business-Code": ",".join(result.get("businessCodes") or []) or "OK"}
        return JSONResponse(content=result, headers=headers)

    @app.get("/api/states")
    def list_states() -> dict[str, Any]:
        return service.list_states()

    @app.post("/api/states/{slave_id}/transition")
    async def transition_state(slave_id: str, request: Request) -> dict[str, Any]:
        body = await _json_body(request)
        if not isinstance(body, Mapping):
            raise AppError("E_BAD_REQUEST", "请求体必须是对象", status_code=400)
        target = body.get("targetState", body.get("target_state", body.get("target")))
        if target is None:
            raise AppError("E_BAD_REQUEST", "缺少 targetState", status_code=400)
        return service.transition_state(
            slave_id,
            str(target),
            batch_id=body.get("batchId", body.get("batch_id")),
            base_state_version=_int_or_none(
                body.get("baseStateVersion", body.get("base_state_version")), "baseStateVersion"
            ),
            reason=str(body.get("reason", "MANUAL_TRANSITION")),
        )

    @app.get("/api/verdicts")
    def list_verdicts(limit: int = 50) -> dict[str, Any]:
        return service.list_verdicts(limit=min(max(limit, 1), 500))

    @app.get("/api/verdicts/{recalc_id}")
    def get_verdict(recalc_id: str) -> dict[str, Any]:
        return service.get_verdict(recalc_id)

    @app.patch("/api/verdicts/{recalc_id}/rows/{slave_id}")
    async def edit_verdict(recalc_id: str, slave_id: str, request: Request) -> dict[str, Any]:
        body = await _json_body(request)
        if not isinstance(body, Mapping):
            raise AppError("E_BAD_REQUEST", "请求体必须是对象", status_code=400)
        action = body.get("effectiveAction", body.get("effective_action"))
        if action is None:
            raise AppError("E_BAD_REQUEST", "缺少 effectiveAction", status_code=400)
        return service.edit_adjudication(
            recalc_id,
            slave_id,
            effective_action_value=str(action),
            note=str(body.get("note", "")),
            batch_id=body.get("batchId", body.get("batch_id")),
            base_dataset_version=_int_or_none(
                body.get("baseDatasetVersion", body.get("base_dataset_version")), "baseDatasetVersion"
            ),
        )

    @app.post("/api/verdicts/{recalc_id}/replay")
    def replay_verdict(recalc_id: str) -> dict[str, Any]:
        return service.replay_verdict(recalc_id)

    @app.get("/api/recovery-report")
    def recovery_report() -> dict[str, Any]:
        return service.recovery_report()

    @app.post("/api/recovery/scan")
    def recovery_scan() -> dict[str, Any]:
        return service.recover()

    @app.get("/api/intents")
    def intents(limit: int = 100) -> dict[str, Any]:
        return service.list_intents(limit=min(max(limit, 1), 1000))

    @app.get("/api/batches/{batch_id}")
    def get_batch(batch_id: str) -> dict[str, Any]:
        return service.get_batch(batch_id)

    return app


app = create_app()

