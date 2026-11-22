from __future__ import annotations

import base64
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import atomic_write_bytes, canonical_bytes, digest_json, sha256_bytes
from .errors import business_error


TEXT_FIELDS = ("text_id", "text", "lang", "source_channel")
IMAGE_FIELDS = ("image_id", "png", "feature", "text_id", "source_operator")


@dataclass(frozen=True)
class Dataset:
    texts: tuple[dict[str, Any], ...]
    images: tuple[dict[str, Any], ...]
    text_duplicates: int = 0
    image_duplicates: int = 0
    duplicate_content_groups: tuple[dict[str, Any], ...] = ()

    def text_map(self) -> dict[str, dict[str, Any]]:
        return {record["text_id"]: record for record in self.texts}

    def image_map(self) -> dict[str, dict[str, Any]]:
        return {record["image_id"]: record for record in self.images}

    def content_fingerprint(self, data_dir: Path | None = None) -> str:
        payload = {
            "texts": list(self.texts),
            "images": list(self.images),
            "png_sha256": png_digests(self.images, data_dir) if data_dir is not None else {},
        }
        return digest_json(payload)


def png_digests(images: tuple[dict[str, Any], ...] | list[dict[str, Any]], data_dir: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for image in images:
        relative = str(image.get("png", ""))
        path = Path(relative)
        if not path.is_absolute():
            path = data_dir / path
        if path.exists() and path.is_file():
            result[str(image["image_id"])] = sha256_bytes(path.read_bytes())
        else:
            result[str(image["image_id"])] = "<missing>"
    return result


def normalize_text_record(record: dict[str, Any], *, source: str) -> dict[str, Any]:
    missing = [field for field in TEXT_FIELDS if field not in record]
    if missing:
        raise business_error(
            "E_INVALID_INPUT",
            "data_import",
            "text record is missing required fields",
            field=missing[0],
            diagnostics={"missing_fields": missing, "source": source},
            http_status=400,
        )
    text_id = record.get("text_id")
    if not isinstance(text_id, str) or not text_id.strip():
        raise business_error(
            "E_INVALID_INPUT", "data_import", "text_id must be a non-empty string", field="text_id", http_status=400
        )
    for field in ("text", "lang", "source_channel"):
        if not isinstance(record.get(field), str):
            raise business_error(
                "E_INVALID_INPUT",
                "data_import",
                f"{field} must be a string",
                sample_id=text_id,
                field=field,
                http_status=400,
            )
    return {
        "text_id": text_id,
        "text": record["text"],
        "lang": record["lang"],
        "source_channel": record["source_channel"],
    }


def normalize_image_record(record: dict[str, Any], *, source: str) -> dict[str, Any]:
    missing = [field for field in IMAGE_FIELDS if field not in record]
    if missing:
        raise business_error(
            "E_INVALID_INPUT",
            "data_import",
            "image record is missing required fields",
            field=missing[0],
            diagnostics={"missing_fields": missing, "source": source},
            http_status=400,
        )
    image_id = record.get("image_id")
    if not isinstance(image_id, str) or not image_id.strip():
        raise business_error(
            "E_INVALID_INPUT", "data_import", "image_id must be a non-empty string", field="image_id", http_status=400
        )
    if not isinstance(record.get("png"), str):
        raise business_error(
            "E_INVALID_INPUT",
            "data_import",
            "png must be a relative file name",
            sample_id=image_id,
            field="png",
            http_status=400,
        )
    if not isinstance(record.get("feature"), list):
        raise business_error(
            "E_INVALID_INPUT",
            "data_import",
            "feature must be an array",
            sample_id=image_id,
            field="feature",
            http_status=400,
        )
    if not isinstance(record.get("source_operator"), str):
        raise business_error(
            "E_INVALID_INPUT",
            "data_import",
            "source_operator must be a string",
            sample_id=image_id,
            field="source_operator",
            http_status=400,
        )
    if record.get("text_id") is not None and not isinstance(record.get("text_id"), str):
        raise business_error(
            "E_INVALID_INPUT",
            "data_import",
            "text_id must be a string or null",
            sample_id=image_id,
            field="text_id",
            http_status=400,
        )
    return {
        "image_id": image_id,
        "png": record["png"],
        "feature": list(record["feature"]),
        "text_id": record.get("text_id"),
        "source_operator": record["source_operator"],
    }


def _deduplicate(records: list[dict[str, Any]], id_field: str, kind: str) -> tuple[tuple[dict[str, Any], ...], int]:
    by_id: dict[str, dict[str, Any]] = {}
    duplicates = 0
    for record in records:
        sample_id = str(record[id_field])
        previous = by_id.get(sample_id)
        if previous is None:
            by_id[sample_id] = record
            continue
        if canonical_bytes(previous) == canonical_bytes(record):
            duplicates += 1
            continue
        raise business_error(
            "E_INVALID_INPUT",
            "data_import",
            f"conflicting duplicate {kind} id",
            sample_id=sample_id,
            field=id_field,
            diagnostics={"kind": kind, "id": sample_id},
            http_status=400,
        )
    ordered = tuple(by_id[key] for key in sorted(by_id))
    return ordered, duplicates


def _duplicate_text_groups(texts: tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    groups: dict[str, list[str]] = {}
    for record in texts:
        key = sha256_bytes(record["text"].encode("utf-8"))
        groups.setdefault(key, []).append(record["text_id"])
    return tuple(
        {"content_sha256": key, "text_ids": sorted(ids)}
        for key, ids in sorted(groups.items())
        if len(ids) > 1
    )


def load_dataset(data_dir: str | Path) -> Dataset:
    root = Path(data_dir)
    text_path = root / "texts.jsonl"
    image_path = root / "images.json"
    if not text_path.exists() or not image_path.exists():
        raise business_error(
            "E_INVALID_INPUT",
            "data_import",
            "dataset files are missing",
            diagnostics={"texts_path": str(text_path), "images_path": str(image_path)},
            http_status=404,
        )

    raw_texts: list[dict[str, Any]] = []
    for line_number, line in enumerate(text_path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise business_error(
                "E_INVALID_INPUT",
                "data_import",
                "invalid JSONL row",
                diagnostics={"line": line_number, "error": str(error)},
                http_status=400,
            ) from error
        if not isinstance(value, dict):
            raise business_error(
                "E_INVALID_INPUT", "data_import", "JSONL row must be an object", diagnostics={"line": line_number}, http_status=400
            )
        raw_texts.append(normalize_text_record(value, source=f"texts.jsonl:{line_number}"))

    try:
        raw_images_value = json.loads(image_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as error:
        raise business_error("E_INVALID_INPUT", "data_import", "invalid images JSON", diagnostics={"error": str(error)}, http_status=400) from error
    if not isinstance(raw_images_value, list):
        raise business_error("E_INVALID_INPUT", "data_import", "images JSON must be an array", http_status=400)
    raw_images = [normalize_image_record(value, source="images.json") for value in raw_images_value]

    texts, text_duplicates = _deduplicate(raw_texts, "text_id", "text")
    images, image_duplicates = _deduplicate(raw_images, "image_id", "image")
    return Dataset(
        texts=texts,
        images=images,
        text_duplicates=text_duplicates,
        image_duplicates=image_duplicates,
        duplicate_content_groups=_duplicate_text_groups(texts),
    )


def serialized_texts(texts: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> bytes:
    lines = [canonical_bytes(normalize_text_record(record, source="write")) for record in texts]
    return b"".join(line + b"\n" for line in lines)


def serialized_images(images: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> bytes:
    records = [normalize_image_record(record, source="write") for record in images]
    return canonical_bytes(sorted(records, key=lambda item: item["image_id"])) + b"\n"


def write_dataset(data_dir: str | Path, texts: list[dict[str, Any]], images: list[dict[str, Any]]) -> None:
    root = Path(data_dir)
    root.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(root / "texts.jsonl", serialized_texts(texts))
    atomic_write_bytes(root / "images.json", serialized_images(images))


def replace_text_record(data_dir: str | Path, text_id: str, changes: dict[str, Any]) -> dict[str, Any]:
    dataset = load_dataset(data_dir)
    records = [dict(record) for record in dataset.texts]
    target = next((record for record in records if record["text_id"] == text_id), None)
    if target is None:
        raise business_error("E_SAMPLE_NOT_FOUND", "data_edit", "text_id not found", sample_id=text_id, field="text_id", http_status=404)
    for field in ("text", "lang", "source_channel"):
        if field in changes and changes[field] is not None:
            if not isinstance(changes[field], str):
                raise business_error("E_INVALID_INPUT", "data_edit", f"{field} must be a string", sample_id=text_id, field=field, http_status=400)
            target[field] = changes[field]
    normalized = normalize_text_record(target, source="edit")
    write_dataset(data_dir, records, list(dataset.images))
    return normalized


def replace_image_record(data_dir: str | Path, image_id: str, changes: dict[str, Any]) -> dict[str, Any]:
    dataset = load_dataset(data_dir)
    records = [dict(record) for record in dataset.images]
    target = next((record for record in records if record["image_id"] == image_id), None)
    if target is None:
        raise business_error("E_SAMPLE_NOT_FOUND", "data_edit", "image_id not found", sample_id=image_id, field="image_id", http_status=404)
    if "feature" in changes and changes["feature"] is not None:
        if not isinstance(changes["feature"], list):
            raise business_error("E_INVALID_INPUT", "data_edit", "feature must be an array", sample_id=image_id, field="feature", http_status=400)
        target["feature"] = list(changes["feature"])
    for field in ("source_operator", "text_id", "png"):
        if field in changes:
            target[field] = changes[field]
    normalized = normalize_image_record(target, source="edit")
    write_dataset(data_dir, list(dataset.texts), records)
    return normalized


def import_from_paths(source_texts: str | Path, source_images: str | Path, data_dir: str | Path) -> Dataset:
    source_text_path = Path(source_texts).resolve()
    source_image_path = Path(source_images).resolve()
    texts: list[dict[str, Any]] = []
    for line_number, line in enumerate(source_text_path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise business_error(
                "E_INVALID_INPUT",
                "data_import",
                "invalid JSONL row",
                diagnostics={"line": line_number, "error": str(error)},
                http_status=400,
            ) from error
        if not isinstance(value, dict):
            raise business_error(
                "E_INVALID_INPUT",
                "data_import",
                "JSONL row must be an object",
                diagnostics={"line": line_number},
                http_status=400,
            )
        texts.append(normalize_text_record(value, source=str(source_text_path)))
    images_value = json.loads(source_image_path.read_text(encoding="utf-8-sig"))
    if not isinstance(images_value, list):
        raise business_error("E_INVALID_INPUT", "data_import", "images JSON must be an array", http_status=400)
    images = [normalize_image_record(value, source=str(source_image_path)) for value in images_value]
    texts, _ = _deduplicate(texts, "text_id", "text")
    images, _ = _deduplicate(images, "image_id", "image")
    source_dataset = Dataset(texts=texts, images=images)

    destination = Path(data_dir)
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "png").mkdir(parents=True, exist_ok=True)
    rewritten_images: list[dict[str, Any]] = []
    for image in source_dataset.images:
        source_png = Path(str(image["png"]))
        if not source_png.is_absolute():
            source_png = source_image_path.parent / source_png
        if not source_png.exists():
            raise business_error(
                "E_INVALID_INPUT",
                "data_import",
                "referenced PNG does not exist",
                sample_id=str(image["image_id"]),
                field="png",
                diagnostics={"png": str(source_png)},
                http_status=400,
            )
        destination_png = destination / "png" / source_png.name
        if source_png.resolve() != destination_png.resolve():
            shutil.copyfile(source_png, destination_png)
        rewritten = dict(image)
        rewritten["png"] = f"png/{source_png.name}"
        rewritten_images.append(rewritten)
    write_dataset(destination, list(source_dataset.texts), rewritten_images)
    return load_dataset(destination)


def import_records(
    data_dir: str | Path,
    texts: list[dict[str, Any]],
    images: list[dict[str, Any]],
) -> Dataset:
    destination = Path(data_dir)
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "png").mkdir(parents=True, exist_ok=True)
    rewritten_images: list[dict[str, Any]] = []
    for index, raw in enumerate(images):
        image = normalize_image_record(raw, source="http-import")
        png_base64 = raw.get("png_base64")
        png_path = Path(str(image["png"]))
        if png_base64:
            try:
                payload = base64.b64decode(str(png_base64), validate=True)
            except ValueError as error:
                raise business_error(
                    "E_INVALID_INPUT",
                    "data_import",
                    "png_base64 is not valid base64",
                    sample_id=str(image["image_id"]),
                    field="png_base64",
                    http_status=400,
                ) from error
            target_name = png_path.name if png_path.suffix.lower() == ".png" else f"import_{index:03d}.png"
            atomic_write_bytes(destination / "png" / target_name, payload)
            image["png"] = f"png/{target_name}"
        elif not (png_path.is_absolute() and png_path.exists()) and not (destination / png_path).exists():
            raise business_error(
                "E_INVALID_INPUT",
                "data_import",
                "PNG payload or a readable local png path is required",
                sample_id=str(image["image_id"]),
                field="png",
                http_status=400,
            )
        rewritten_images.append(image)
    write_dataset(destination, texts, rewritten_images)
    return load_dataset(destination)
