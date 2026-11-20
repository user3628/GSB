from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from flask import Flask, Response, jsonify, request
from werkzeug.exceptions import HTTPException

from .errors import InvalidArgument, KVError
from .store import KVStore


def create_app(store: KVStore) -> Flask:
    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False
    app.extensions["kvstore"] = store

    @app.errorhandler(KVError)
    def handle_kv_error(error: KVError) -> Tuple[Response, int]:
        return jsonify(error.as_dict()), error.http_status

    @app.errorhandler(404)
    def handle_not_found(_error: Any) -> Tuple[Response, int]:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": {"code": "E_ROUTE_NOT_FOUND", "message": "HTTP route does not exist"},
                }
            ),
            404,
        )

    @app.errorhandler(HTTPException)
    def handle_http_error(error: HTTPException) -> Tuple[Response, int]:
        code_map = {
            400: "E_HTTP_BAD_REQUEST",
            405: "E_METHOD_NOT_ALLOWED",
            413: "E_PAYLOAD_TOO_LARGE",
        }
        status = int(error.code or 500)
        return (
            jsonify(
                {
                    "ok": False,
                    "error": {
                        "code": code_map.get(status, f"E_HTTP_{status}"),
                        "message": error.description or error.name,
                    },
                }
            ),
            status,
        )

    @app.errorhandler(Exception)
    def handle_unexpected(error: Exception) -> Tuple[Response, int]:
        app.logger.exception("unhandled API error")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": "unexpected server error",
                        "details": {"type": type(error).__name__, "reason": str(error)},
                    },
                }
            ),
            500,
        )

    @app.get("/health")
    def health() -> Response:
        summary = store.index_summary(limit=1)
        return jsonify(
            {
                "ok": True,
                "status": "healthy",
                "active_segment": summary["segment"],
                "snapshot_seq": summary["snapshot_seq"],
                "last_txn_seq": summary["last_txn_seq"],
            }
        )

    @app.post("/v1/keys")
    def write_key_body() -> Tuple[Response, int]:
        body = _json_body()
        key = body.pop("key", None)
        return jsonify({"ok": True, **store.write(key, **_write_arguments(body))}), 201

    @app.put("/v1/kv/<path:key>")
    def write_key_path(key: str) -> Tuple[Response, int]:
        body = _json_body()
        return jsonify({"ok": True, **store.write(key, **_write_arguments(body))}), 201

    @app.get("/v1/kv/<path:key>")
    def read_key(key: str) -> Response:
        timeout_ms = _read_timeout_ms()
        result = store.get(key, read_timeout_ms=timeout_ms)
        return jsonify({"ok": True, **result})

    @app.delete("/v1/kv/<path:key>")
    def delete_key(key: str) -> Response:
        return jsonify({"ok": True, **store.delete(key)})

    @app.get("/v1/index")
    def index_summary() -> Response:
        limit = _positive_int(request.args.get("limit", "1000"), "limit")
        return jsonify({"ok": True, **store.index_summary(limit=limit)})

    @app.get("/v1/export/binary")
    def export_binary() -> Response:
        endian = request.args.get("endian", "little")
        if endian not in {"little", "big"}:
            raise InvalidArgument("endian must be little or big", {"field": "endian"})
        data, record_count = store.export_binary_with_count(endian=endian)
        return Response(
            data,
            mimetype="application/octet-stream",
            headers={
                "Content-Disposition": 'attachment; filename="kvstore-export.kvsb"',
                "X-Record-Count": str(record_count),
                "X-Format": "compact_binary",
                "X-Endianness": endian,
            },
        )

    @app.get("/v1/export/jsonl")
    def export_jsonl() -> Response:
        data, record_count = store.export_jsonl_with_count()
        return Response(
            data,
            mimetype="application/x-ndjson",
            headers={
                "Content-Disposition": 'attachment; filename="kvstore-export.jsonl"',
                "X-Record-Count": str(record_count),
                "X-Format": "json_lines",
            },
        )

    @app.post("/v1/import/binary")
    def import_binary() -> Response:
        return _import(store, "compact_binary")

    @app.post("/v1/import/jsonl")
    def import_jsonl() -> Response:
        return _import(store, "json_lines")

    @app.post("/v1/import")
    def import_auto() -> Response:
        return _import(store, _requested_import_format())

    @app.post("/v1/import/negotiate")
    def negotiate_import_auto() -> Response:
        data, source_name = _request_payload()
        result = store.negotiate_import(
            data,
            expected_format=_requested_import_format(),
            source_name=source_name,
        )
        return jsonify({"ok": True, **result})

    def _requested_import_format() -> Optional[str]:
        requested = request.args.get("format")
        if requested in (None, "", "auto"):
            return None
        if requested in {"binary", "compact_binary"}:
            return "compact_binary"
        if requested in {"jsonl", "json_lines"}:
            return "json_lines"
        raise InvalidArgument("format must be binary, jsonl, or auto", {"field": "format"})

    @app.post("/v1/expire")
    def expire() -> Response:
        body = _json_body()
        limit = body.get("limit", 1000)
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise InvalidArgument("limit must be an integer", {"field": "limit"})
        return jsonify({"ok": True, **store.expire_due(limit=limit)})

    @app.post("/v1/snapshot/switch")
    def snapshot_switch() -> Response:
        body = _json_body()
        path = body.get("path")
        if path is not None and (not isinstance(path, str) or not path):
            raise InvalidArgument("snapshot path must be a non-empty string", {"field": "path"})
        return jsonify({"ok": True, **store.switch_snapshot(path)})

    @app.get("/v1/recovery/report")
    def recovery_report() -> Response:
        return jsonify({"ok": True, "report": store.current_recovery_report()})

    return app


def _json_body() -> Dict[str, Any]:
    if not request.data:
        return {}
    body = request.get_json(force=True, silent=False)
    if body is None:
        return {}
    if not isinstance(body, dict):
        raise InvalidArgument("request body must be a JSON object")
    return body


def _write_arguments(body: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {"value", "version", "written_at_us", "expires_at_us", "ttl_ms", "extras"}
    unknown = sorted(set(body) - allowed)
    if unknown:
        raise InvalidArgument("unknown write fields", {"fields": unknown})
    if "value" not in body:
        raise InvalidArgument("write body must contain value", {"field": "value"})
    return {
        "value": body["value"],
        "version": body.get("version"),
        "written_at_us": body.get("written_at_us"),
        "expires_at_us": body.get("expires_at_us"),
        "ttl_ms": body.get("ttl_ms"),
        "extras": body.get("extras"),
    }


def _read_timeout_ms() -> int:
    raw = request.args.get("read_timeout_ms")
    if raw is None:
        raw = request.headers.get("X-Read-Timeout-Ms", "1000")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidArgument("read_timeout_ms must be a positive integer") from exc
    if value <= 0:
        raise InvalidArgument("read_timeout_ms must be a positive integer")
    return value


def _positive_int(raw: Any, name: str) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidArgument(f"{name} must be a positive integer", {"field": name}) from exc
    if value <= 0:
        raise InvalidArgument(f"{name} must be a positive integer", {"field": name})
    return value


def _import(store: KVStore, expected_format: Optional[str]) -> Response:
    data, source_name = _request_payload()
    batch_id = request.headers.get("X-Batch-Id") or request.args.get("batch_id")
    if batch_id is None and request.form:
        batch_id = request.form.get("batch_id")
    result = store.import_data(
        data,
        expected_format=expected_format,
        batch_id=batch_id,
        source_name=source_name,
    )
    return jsonify({"ok": True, **result})


def _request_payload() -> Tuple[bytes, str]:
    uploaded = request.files.get("file")
    if uploaded is not None:
        data = uploaded.read()
        source_name = uploaded.filename or "upload"
    else:
        data = request.get_data(cache=False)
        source_name = "request-body"
    return data, source_name
