from __future__ import annotations

import base64
import json
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from .errors import StoreError
from .store import Store


def _parse_int(value: Any, field: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise StoreError("E_BAD_REQUEST", f"{field} must be an integer", {"field": field})
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise StoreError(
            "E_BAD_REQUEST", f"{field} must be an integer", {"field": field, "value": value}
        ) from exc


def make_handler(store: Store) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "LocalKVIndex/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            # The demo captures stdout; request logs are intentionally quiet.
            return

        def _send_json(
            self,
            status: int,
            payload: Any,
            headers: dict[str, str] | None = None,
        ) -> None:
            body = json.dumps(
                payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)

        def _send_bytes(
            self,
            status: int,
            body: bytes,
            content_type: str,
            headers: dict[str, str] | None = None,
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)

        def _send_error(self, error: StoreError) -> None:
            self._send_json(error.http_status, error.to_dict())

        def _read_body(self) -> bytes:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length < 0 or length > 128 * 1024 * 1024:
                raise StoreError(
                    "E_BAD_REQUEST",
                    "request body size is invalid or exceeds 128 MiB",
                    {"content_length": length},
                )
            return self.rfile.read(length)

        def _read_json_body(self, *, allow_empty: bool = False) -> Any:
            raw = self._read_body()
            if not raw:
                if allow_empty:
                    return {}
                raise StoreError("E_BAD_REQUEST", "request body is required")
            try:
                return json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise StoreError(
                    "E_BAD_REQUEST",
                    "request body is not valid JSON",
                    {"error": str(exc)},
                ) from exc

        def _query(self) -> dict[str, list[str]]:
            return parse_qs(urlsplit(self.path).query, keep_blank_values=True)

        def _query_one(self, name: str) -> str | None:
            values = self._query().get(name)
            return values[-1] if values else None

        def _query_int(self, name: str) -> int | None:
            return _parse_int(self._query_one(name), name)

        def _key_from_path(self, path: str) -> str | None:
            prefix = "/keys/"
            if not path.startswith(prefix):
                return None
            return unquote(path[len(prefix) :])

        def do_GET(self) -> None:  # noqa: N802
            try:
                parsed = urlsplit(self.path)
                path = parsed.path
                if path == "/health":
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "service": "local-kv-index",
                            "sqlite": "python-stdlib",
                        },
                    )
                    return
                if path == "/":
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "service": "local-kv-index",
                            "endpoints": [
                                "PUT /keys/{key}",
                                "GET /keys/{key}",
                                "DELETE /keys/{key}",
                                "GET /export/binary",
                                "GET /export/jsonl",
                                "POST /import",
                                "POST /snapshot/switch",
                                "GET /snapshot/export",
                                "GET /recovery/report",
                            ],
                        },
                    )
                    return
                if path == "/stats":
                    self._send_json(200, {"ok": True, **store.stats()})
                    return
                key = self._key_from_path(path)
                if key is not None:
                    record = store.get(key, now_ms=self._query_int("now_ms"))
                    self._send_json(200, {"ok": True, "record": record})
                    return
                if path == "/export/binary":
                    endian = self._query_one("endian") or "little"
                    body = store.export_binary(endian=endian)
                    self._send_bytes(
                        200,
                        body,
                        "application/octet-stream",
                        {
                            "Content-Disposition": "attachment; filename=kv-export.kvs",
                            "X-KV-Format": "KVSG",
                            "X-KV-Endian": endian,
                        },
                    )
                    return
                if path == "/export/jsonl":
                    shard_count = self._query_int("shards") or 1
                    if self._query_one("bundle") in ("1", "true", "yes"):
                        payload = store.export_jsonl_shards(shard_count=shard_count)
                        self._send_json(200, {"ok": True, **payload})
                    else:
                        body = store.export_jsonl(shard_count=shard_count)
                        self._send_bytes(
                            200,
                            body,
                            "application/x-ndjson; charset=utf-8",
                            {"X-KV-Shard-Count": str(shard_count)},
                        )
                    return
                if path in ("/snapshot/export", "/snapshot"):
                    doc = store.export_snapshot(snapshot_seq=self._query_int("snapshot_seq"))
                    self._send_json(200, {"ok": True, "snapshot": doc})
                    return
                if path == "/recovery/report":
                    self._send_json(200, {"ok": True, **store.recovery_report()})
                    return
                if path == "/segments":
                    self._send_json(200, {"ok": True, **store.segment_summary()})
                    return
                if path == "/debug/heap":
                    self._send_json(200, {"ok": True, **store.heap_check()})
                    return
                if path == "/maintenance/report":
                    self._send_json(
                        200,
                        {"ok": True, "report": store.maintenance_report()},
                    )
                    return
                if path.startswith("/imports/") and len(path) > len("/imports/"):
                    batch_id = unquote(path[len("/imports/") :])
                    self._send_json(
                        200, {"ok": True, **store.get_import_report(batch_id)}
                    )
                    return
                raise StoreError(
                    "E_NOT_FOUND", "route not found", {"method": "GET", "path": path}
                )
            except StoreError as exc:
                self._send_error(exc)
            except Exception as exc:  # pragma: no cover - defensive HTTP boundary
                traceback.print_exc()
                self._send_json(
                    500,
                    {
                        "ok": False,
                        "code": "E_INTERNAL",
                        "message": "unexpected internal error",
                        "context": {"error": str(exc)},
                    },
                )

        def do_PUT(self) -> None:  # noqa: N802
            try:
                path = urlsplit(self.path).path
                key = self._key_from_path(path)
                if key is None:
                    raise StoreError(
                        "E_NOT_FOUND", "route not found", {"method": "PUT", "path": path}
                    )
                record = self._write_key(key)
                self._send_json(200, {"ok": True, "record": record})
            except StoreError as exc:
                self._send_error(exc)
            except Exception as exc:  # pragma: no cover
                traceback.print_exc()
                self._send_json(
                    500,
                    {
                        "ok": False,
                        "code": "E_INTERNAL",
                        "message": "unexpected internal error",
                        "context": {"error": str(exc)},
                    },
                )

        def do_POST(self) -> None:  # noqa: N802
            try:
                parsed = urlsplit(self.path)
                path = parsed.path
                if path.startswith("/keys/"):
                    key = self._key_from_path(path)
                    assert key is not None
                    record = self._write_key(key)
                    self._send_json(200, {"ok": True, "record": record})
                    return
                if path in (
                    "/import",
                    "/imports",
                    "/import/binary",
                    "/import/jsonl",
                    "/import/auto",
                ):
                    forced_format = None
                    if path == "/import/binary":
                        forced_format = "binary"
                    elif path == "/import/jsonl":
                        forced_format = "jsonl"
                    report = self._import(forced_format=forced_format)
                    self._send_json(200, {"ok": True, "report": report})
                    return
                if path in ("/snapshot/switch", "/snapshot/load"):
                    report = self._snapshot_switch()
                    self._send_json(200, report)
                    return
                if path in ("/maintenance/expire", "/maintenance/cleanup-expired"):
                    body = self._read_json_body(allow_empty=True)
                    now = body.get("now_ms") if isinstance(body, dict) else None
                    result = store.expire(
                        now_ms=None if now is None else int(now)
                    )
                    self._send_json(200, result)
                    return
                if path == "/maintenance/verify-segments":
                    self._send_json(200, store.verify_segments())
                    return
                if path == "/maintenance/compact":
                    self._send_json(200, store.compact())
                    return
                raise StoreError(
                    "E_NOT_FOUND", "route not found", {"method": "POST", "path": path}
                )
            except StoreError as exc:
                self._send_error(exc)
            except Exception as exc:  # pragma: no cover
                traceback.print_exc()
                self._send_json(
                    500,
                    {
                        "ok": False,
                        "code": "E_INTERNAL",
                        "message": "unexpected internal error",
                        "context": {"error": str(exc)},
                    },
                )

        def do_DELETE(self) -> None:  # noqa: N802
            try:
                path = urlsplit(self.path).path
                key = self._key_from_path(path)
                if key is None:
                    raise StoreError(
                        "E_NOT_FOUND",
                        "route not found",
                        {"method": "DELETE", "path": path},
                    )
                body = self._read_json_body(allow_empty=True)
                if not isinstance(body, dict):
                    raise StoreError("E_BAD_REQUEST", "DELETE body must be an object")
                query = self._query()
                version = _parse_int(
                    body.get("version", (query.get("version") or [None])[-1]), "version"
                )
                expected = _parse_int(
                    body.get(
                        "expected_version", (query.get("expected_version") or [None])[-1]
                    ),
                    "expected_version",
                )
                base = _parse_int(
                    body.get(
                        "base_state_version",
                        (query.get("base_state_version") or [None])[-1],
                    ),
                    "base_state_version",
                )
                result = store.delete(
                    key,
                    version=version,
                    expected_version=expected,
                    base_state_version=base,
                )
                self._send_json(200, result)
            except StoreError as exc:
                self._send_error(exc)
            except Exception as exc:  # pragma: no cover
                traceback.print_exc()
                self._send_json(
                    500,
                    {
                        "ok": False,
                        "code": "E_INTERNAL",
                        "message": "unexpected internal error",
                        "context": {"error": str(exc)},
                    },
                )

        def _write_key(self, key: str) -> dict[str, Any]:
            content_type = (self.headers.get("Content-Type") or "").lower()
            query = self._query()
            if "application/json" in content_type:
                body = self._read_json_body()
                if not isinstance(body, dict):
                    raise StoreError("E_BAD_REQUEST", "write body must be a JSON object")
            else:
                raw_value = self._read_body()
                body = {"value": raw_value.decode("utf-8", errors="strict")}
            value = body.get("value", "")
            encoding = body.get("value_encoding", "utf8")
            if "value_base64" in body:
                value = body["value_base64"]
                encoding = "base64"
            if encoding == "base64":
                if not isinstance(value, str):
                    raise StoreError("E_BAD_REQUEST", "base64 value must be a string")
                try:
                    value_bytes = base64.b64decode(value, validate=True)
                except Exception as exc:
                    raise StoreError(
                        "E_BAD_REQUEST", "value_base64 is invalid", {"error": str(exc)}
                    ) from exc
            elif encoding in ("utf8", "text", None):
                if not isinstance(value, str):
                    raise StoreError("E_BAD_REQUEST", "value must be a string")
                value_bytes = value.encode("utf-8")
            else:
                raise StoreError(
                    "E_BAD_REQUEST",
                    "unsupported value_encoding",
                    {"value_encoding": encoding},
                )
            version = _parse_int(
                body.get("version", (query.get("version") or [None])[-1]), "version"
            )
            write_ts = _parse_int(
                body.get(
                    "write_timestamp_ms",
                    (query.get("write_timestamp_ms") or [None])[-1],
                ),
                "write_timestamp_ms",
            )
            write_ts_us = _parse_int(
                body.get(
                    "write_timestamp_us",
                    (query.get("write_timestamp_us") or [None])[-1],
                ),
                "write_timestamp_us",
            )
            if write_ts is None and write_ts_us is not None:
                write_ts = write_ts_us // 1000
            expires = _parse_int(
                body.get(
                    "expires_at_ms", (query.get("expires_at_ms") or [None])[-1]
                ),
                "expires_at_ms",
            )
            expires_us = _parse_int(
                body.get(
                    "expires_at_us", (query.get("expires_at_us") or [None])[-1]
                ),
                "expires_at_us",
            )
            if expires is None and expires_us is not None:
                expires = expires_us // 1000
            expires_in = _parse_int(body.get("expires_in_ms"), "expires_in_ms")
            if expires_in is not None:
                base_ts = write_ts if write_ts is not None else store._now_ms()
                expires = base_ts + expires_in
            expires_in_us = _parse_int(body.get("expires_in_us"), "expires_in_us")
            if expires_in_us is not None:
                base_ts = write_ts if write_ts is not None else store._now_ms()
                expires = base_ts + expires_in_us // 1000
            expected = _parse_int(body.get("expected_version"), "expected_version")
            base_state = _parse_int(
                body.get("base_state_version"), "base_state_version"
            )
            batch_id = body.get("batch_id")
            return store.put(
                key,
                value_bytes,
                version=version,
                write_ts_ms=write_ts,
                expires_at_ms=expires,
                expected_version=expected,
                base_state_version=base_state,
                batch_id=str(batch_id) if batch_id is not None else None,
            )

        def _import(self, *, forced_format: str | None = None) -> dict[str, Any]:
            raw = self._read_body()
            content_type = (self.headers.get("Content-Type") or "").lower()
            query = self._query()
            wrapper: dict[str, Any] = {}
            data = raw
            if "application/json" in content_type and raw:
                try:
                    parsed = json.loads(raw)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    parsed = None
                if isinstance(parsed, dict) and (
                    "data_base64" in parsed
                    or "data" in parsed
                    or "shards" in parsed
                    or "records" in parsed
                ):
                    wrapper = parsed
                    if "data_base64" in parsed:
                        try:
                            data = base64.b64decode(parsed["data_base64"], validate=True)
                        except Exception as exc:
                            raise StoreError(
                                "E_BAD_REQUEST",
                                "data_base64 is invalid",
                                {"error": str(exc)},
                            ) from exc
                    elif "data" in parsed:
                        raw_data = parsed["data"]
                        if not isinstance(raw_data, str):
                            raise StoreError(
                                "E_BAD_REQUEST", "data must be a string",
                                {"field": "data"},
                            )
                        data = raw_data.encode("utf-8")
                    elif "shards" in parsed:
                        data = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
                    elif "records" in parsed:
                        data = encode_records_wrapper(parsed["records"])
                elif isinstance(parsed, dict) and "key" in parsed:
                    data = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
            requested = (
                forced_format
                or wrapper.get("format")
                or (query.get("format") or [None])[-1]
                or "auto"
            )
            batch_id = (
                wrapper.get("batch_id")
                or (query.get("batch_id") or [None])[-1]
                or None
            )
            base_version = _parse_int(
                wrapper.get(
                    "base_state_version", (query.get("base_state_version") or [None])[-1]
                ),
                "base_state_version",
            )
            return store.import_data(
                data,
                requested_format=str(requested),
                batch_id=None if batch_id is None else str(batch_id),
                base_state_version=base_version,
            )

        def _snapshot_switch(self) -> dict[str, Any]:
            raw = self._read_body()
            if not raw:
                raise StoreError("E_BAD_REQUEST", "snapshot body is required")
            try:
                doc = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise StoreError(
                    "E_SNAPSHOT_INVALID",
                    "snapshot request body is not valid JSON",
                    {"stage": "http_json_decode", "error": str(exc)},
                ) from exc
            expected = None
            if isinstance(doc, dict) and "snapshot" in doc:
                expected = _parse_int(
                    doc.get("expected_base_snapshot_seq"),
                    "expected_base_snapshot_seq",
                )
                snapshot_doc = doc["snapshot"]
            else:
                snapshot_doc = doc
            query_expected = self._query_one("expected_base_snapshot_seq")
            if query_expected is not None:
                expected = int(query_expected)
            return store.load_snapshot(
                snapshot_doc, expected_base_snapshot_seq=expected
            )

    return Handler


def encode_records_wrapper(records: Any) -> bytes:
    if not isinstance(records, list):
        raise StoreError("E_BAD_REQUEST", "records must be an array")
    return (
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"
    ).encode("utf-8")


class LocalKVServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def make_server(store: Store, host: str = "127.0.0.1", port: int = 0) -> LocalKVServer:
    return LocalKVServer((host, int(port)), make_handler(store))


def serve(store: Store, host: str = "127.0.0.1", port: int = 0) -> None:
    server = make_server(store, host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()

