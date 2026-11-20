"""Tiny standard-library client used by demos and integration tests."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


class ApiError(Exception):
    def __init__(self, status: int, payload: dict[str, Any]) -> None:
        self.status = status
        self.code = payload.get("code", "E_INTERNAL")
        self.message = payload.get("message", "request failed")
        self.context = payload.get("context", {})
        super().__init__(f"HTTP {status} {self.code}: {self.message}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "http_status": self.status,
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


class MVCCClient:
    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                raw = response.read()
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as error:
            raw = error.read()
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                payload = {
                    "code": "E_INTERNAL",
                    "message": raw.decode("utf-8", errors="replace"),
                    "context": {},
                }
            raise ApiError(error.code, payload) from None

    def health(self) -> dict[str, Any]:
        return self.request("GET", "/health")

    def begin(self, isolation: str, txn_id: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"isolation": isolation}
        if txn_id:
            body["txn_id"] = txn_id
        return self.request("POST", "/txns", body)

    def read(self, txn_id: str, table: str, row_id: str) -> dict[str, Any]:
        return self.request("POST", f"/txns/{txn_id}/read", {"table": table, "row_id": row_id})

    def query(self, txn_id: str, table: str) -> dict[str, Any]:
        return self.request("POST", f"/txns/{txn_id}/query", {"table": table})

    def open_edit(self, txn_id: str, table: str, row_id: str) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/txns/{txn_id}/edit-sessions",
            {"table": table, "row_id": row_id},
        )

    def save(
        self,
        txn_id: str,
        session_id: str,
        row_version: int,
        data: dict[str, Any] | None = None,
        delete: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"row_version": row_version}
        if data is not None:
            body["data"] = data
        if delete:
            body["delete"] = True
        return self.request("POST", f"/txns/{txn_id}/edit-sessions/{session_id}/save", body)

    def savepoint(self, txn_id: str, name: str) -> dict[str, Any]:
        return self.request("POST", f"/txns/{txn_id}/savepoints", {"name": name})

    def rollback_to(self, txn_id: str, name: str) -> dict[str, Any]:
        return self.request("POST", f"/txns/{txn_id}/rollback-to", {"name": name})

    def commit(self, txn_id: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.request("POST", f"/txns/{txn_id}/commit", body or {})

    def rollback(self, txn_id: str, reason: str = "client_request") -> dict[str, Any]:
        return self.request("POST", f"/txns/{txn_id}/rollback", {"reason": reason})

    def update(self, txn_id: str, table: str, row_id: str, row_version: int, data: dict[str, Any]) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/txns/{txn_id}/rows/{table}/{row_id}/update",
            {"row_version": row_version, "data": data},
        )

    def delete(self, txn_id: str, table: str, row_id: str, row_version: int) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/txns/{txn_id}/rows/{table}/{row_id}/delete",
            {"row_version": row_version},
        )

    def recovery_report(self) -> dict[str, Any]:
        return self.request("GET", "/recovery/report")["report"]

