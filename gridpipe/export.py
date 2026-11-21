"""Deterministic result export files."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from .canonical import atomic_write_bytes, dump_bytes, sha256_bytes


def derived_exports(export_data: Dict[str, Any]) -> Dict[str, Any]:
    tables: List[Dict[str, Any]] = []
    charts: List[Dict[str, Any]] = []
    validations: List[Dict[str, Any]] = []
    for shard in export_data["shards"]:
        tables.append(
            {
                "shard_id": shard["shard_id"],
                "shard_index": shard["shard_index"],
                "status": shard["status"],
                "table": shard["table"],
                "errors": [error for error in shard["errors"] if error.get("stage") == "table"],
            }
        )
        charts.append(
            {
                "shard_id": shard["shard_id"],
                "shard_index": shard["shard_index"],
                "status": shard["status"],
                "chart": shard["chart"],
                "errors": [error for error in shard["errors"] if error.get("stage") == "chart"],
            }
        )
        if shard["validation"] is not None:
            validations.append(shard["validation"])
    return {
        "tables.json": {
            "schema_version": 1,
            "run_id": export_data["run_id"],
            "shards": tables,
        },
        "charts.json": {
            "schema_version": 1,
            "run_id": export_data["run_id"],
            "shards": charts,
        },
        "validation.json": {
            "schema_version": 1,
            "run_id": export_data["run_id"],
            "shards": validations,
            "summary": export_data["summary"],
        },
        "budget.json": {
            "schema_version": 1,
            "run_id": export_data["run_id"],
            "budget": export_data["budget"],
        },
    }


def write_exports(pipeline: Any, output_dir: str | Path | None = None) -> Dict[str, Any]:
    destination = Path(output_dir).resolve() if output_dir is not None else pipeline.output_dir
    destination.mkdir(parents=True, exist_ok=True)
    data = pipeline.export_data()
    documents = {"export.json": data}
    documents.update(derived_exports(data))
    files: List[Dict[str, Any]] = []
    for name, document in documents.items():
        payload = dump_bytes(document)
        atomic_write_bytes(destination / name, payload)
        files.append({"path": name, "bytes": len(payload), "sha256": sha256_bytes(payload)})
    manifest_digest = str(data["manifest"]["sha256"]) + "\n"
    atomic_write_bytes(destination / "manifest.sha256", manifest_digest.encode("ascii"))
    files.append(
        {
            "path": "manifest.sha256",
            "bytes": len(manifest_digest),
            "sha256": sha256_bytes(manifest_digest.encode("ascii")),
        }
    )
    files.sort(key=lambda item: item["path"])
    return {
        "output_dir": str(destination),
        "run_id": data["run_id"],
        "summary": data["summary"],
        "files": files,
        "content_sha256": sha256_bytes(dump_bytes({"files": files, "summary": data["summary"]})),
    }
