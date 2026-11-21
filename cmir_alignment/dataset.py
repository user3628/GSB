"""Dataset manifest loading, validation and content fingerprints."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .canonical import digest_file, digest_obj
from .descriptors import normalize_caption
from .errors import (
    CMIRError,
    E_INVALID_DATASET,
    E_MISSING_INPUT,
)


DATASET_SCHEMA = "cmir.dataset.v1"
REQUIRED_FIELDS = ("item_id", "image_file", "caption", "lang", "source")


@dataclass
class DatasetItem:
    shard_id: str
    item_id: str
    image_file: str
    caption: str
    lang: str
    source: str
    image_path: Path

    def public_dict(self) -> dict[str, str]:
        return {
            "item_id": self.item_id,
            "image_file": self.image_file,
            "caption": self.caption,
            "lang": self.lang,
            "source": self.source,
        }


@dataclass
class ShardData:
    shard_id: str
    manifest_path: Path
    items: list[DatasetItem] = field(default_factory=list)
    diagnostics: list[dict[str, Any]] = field(default_factory=list)

    def content_digest(self) -> str:
        rows = []
        for item in self.items:
            image_digest = digest_file(item.image_path) if item.image_path.is_file() else None
            rows.append(
                {
                    "item_id": item.item_id,
                    "image_file": item.image_file,
                    "image_digest": image_digest,
                    "caption": item.caption,
                    "lang": item.lang,
                    "source": item.source,
                }
            )
        return digest_obj({"schema": DATASET_SCHEMA, "shard_id": self.shard_id, "items": rows})


@dataclass
class Dataset:
    root: Path
    shards: list[ShardData]
    diagnostics: list[dict[str, Any]] = field(default_factory=list)
    index_path: Path | None = None

    @classmethod
    def load(cls, root: str | Path) -> "Dataset":
        dataset_root = Path(root).resolve()
        index_path = dataset_root / "dataset.json"
        if not index_path.is_file():
            raise CMIRError(
                E_INVALID_DATASET,
                "dataset.json is missing",
                stage="dataset_import",
                dataset_dir=str(dataset_root),
            )
        try:
            descriptor = json.loads(index_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CMIRError(
                E_INVALID_DATASET,
                "dataset.json is not valid JSON",
                stage="dataset_import",
                reason=str(exc),
            ) from exc
        if not isinstance(descriptor, dict) or not isinstance(descriptor.get("shards"), list):
            raise CMIRError(
                E_INVALID_DATASET,
                "dataset.json must contain a shards array",
                stage="dataset_import",
            )

        shards: list[ShardData] = []
        global_diagnostics: list[dict[str, Any]] = []
        seen_ids: dict[str, str] = {}
        seen_shards: set[str] = set()
        for raw_shard in descriptor["shards"]:
            if isinstance(raw_shard, str):
                shard_id = Path(raw_shard).name
                manifest = raw_shard
            elif isinstance(raw_shard, dict):
                shard_id = str(raw_shard.get("shard_id", ""))
                manifest = str(raw_shard.get("manifest", ""))
            else:
                raise CMIRError(
                    E_INVALID_DATASET,
                    "each shard entry must be a string or object",
                    stage="dataset_import",
                )
            if not shard_id:
                shard_id = Path(manifest).stem or "shard"
            if shard_id in seen_shards:
                raise CMIRError(
                    E_INVALID_DATASET,
                    "duplicate shard_id",
                    shard_id=shard_id,
                    stage="dataset_import",
                )
            seen_shards.add(shard_id)
            manifest_path = (dataset_root / manifest).resolve()
            if not manifest_path.is_file():
                raise CMIRError(
                    E_INVALID_DATASET,
                    "shard manifest is missing",
                    shard_id=shard_id,
                    manifest=str(manifest_path),
                    stage="dataset_import",
                )
            shard = ShardData(shard_id=shard_id, manifest_path=manifest_path)
            try:
                lines = manifest_path.read_text(encoding="utf-8").splitlines()
            except OSError as exc:
                raise CMIRError(
                    E_INVALID_DATASET,
                    "cannot read shard manifest",
                    shard_id=shard_id,
                    reason=str(exc),
                ) from exc
            for line_number, line in enumerate(lines, start=1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    shard.diagnostics.append(
                        {
                            "code": E_INVALID_DATASET,
                            "shard_id": shard_id,
                            "line": line_number,
                            "field": "manifest_line",
                            "message": f"invalid JSONL: {exc}",
                        }
                    )
                    continue
                missing_fields = [name for name in REQUIRED_FIELDS if name not in row]
                if missing_fields:
                    item_id = str(row.get("item_id", f"line-{line_number}"))
                    shard.diagnostics.append(
                        {
                            "code": E_MISSING_INPUT,
                            "item_id": item_id,
                            "shard_id": shard_id,
                            "field": missing_fields[0],
                            "missing_fields": missing_fields,
                            "message": "manifest record is missing required fields",
                        }
                    )
                    continue
                item_id = str(row["item_id"])
                if item_id in seen_ids:
                    shard.diagnostics.append(
                        {
                            "code": E_INVALID_DATASET,
                            "item_id": item_id,
                            "shard_id": shard_id,
                            "field": "item_id",
                            "message": f"duplicate item_id also used by {seen_ids[item_id]}",
                        }
                    )
                    continue
                seen_ids[item_id] = shard_id
                image_file = str(row["image_file"])
                image_path = (dataset_root / image_file).resolve()
                if not image_path.is_file():
                    shard.diagnostics.append(
                        {
                            "code": E_MISSING_INPUT,
                            "item_id": item_id,
                            "shard_id": shard_id,
                            "field": "image_file",
                            "image_file": image_file,
                            "message": "image file does not exist",
                        }
                    )
                caption = str(row["caption"])
                if not normalize_caption(caption):
                    shard.diagnostics.append(
                        {
                            "code": E_MISSING_INPUT,
                            "item_id": item_id,
                            "shard_id": shard_id,
                            "field": "caption",
                            "message": "caption is empty after normalization",
                        }
                    )
                shard.items.append(
                    DatasetItem(
                        shard_id=shard_id,
                        item_id=item_id,
                        image_file=image_file,
                        caption=caption,
                        lang=str(row["lang"]),
                        source=str(row["source"]),
                        image_path=image_path,
                    )
                )
            shard.items.sort(key=lambda item: item.item_id)
            if not shard.items:
                shard.diagnostics.append(
                    {
                        "code": E_INVALID_DATASET,
                        "shard_id": shard_id,
                        "field": "items",
                        "message": "shard contains no valid items",
                    }
                )
            global_diagnostics.extend(shard.diagnostics)
            shards.append(shard)

        shards.sort(key=lambda shard: shard.shard_id)
        if not shards:
            global_diagnostics.append(
                {
                    "code": E_INVALID_DATASET,
                    "shard_id": "__dataset__",
                    "field": "shards",
                    "message": "dataset contains no shards",
                }
            )
        return cls(
            root=dataset_root,
            shards=shards,
            diagnostics=global_diagnostics,
            index_path=index_path,
        )

    @property
    def items(self) -> list[DatasetItem]:
        return [item for shard in self.shards for item in shard.items]

    @property
    def item_count(self) -> int:
        return sum(len(shard.items) for shard in self.shards)

    def shard(self, shard_id: str) -> ShardData:
        for shard in self.shards:
            if shard.shard_id == shard_id:
                return shard
        raise CMIRError(
            E_INVALID_DATASET,
            "unknown shard_id",
            shard_id=shard_id,
            available_shards=[shard.shard_id for shard in self.shards],
        )

    def content_digests(self) -> dict[str, str]:
        return {shard.shard_id: shard.content_digest() for shard in self.shards}

    def dataset_digest(self) -> str:
        return digest_obj(
            {
                "schema": DATASET_SCHEMA,
                "content_digests": self.content_digests(),
            }
        )

    def assert_valid(self) -> None:
        if self.diagnostics:
            first = self.diagnostics[0]
            raise CMIRError(
                first["code"],
                first["message"],
                **{key: value for key, value in first.items() if key not in ("code", "message")},
                diagnostics=self.diagnostics,
            )
