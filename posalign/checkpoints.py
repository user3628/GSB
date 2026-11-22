"""Content-addressed stage objects and monotonic per-shard checkpoints."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    read_json,
    sha256_bytes,
)
from .errors import BusinessError, E_INVALID_STAGE, E_STALE_CHECKPOINT
from .filelock import exclusive_file_lock


STAGE_RANK = {"parse": 0, "align": 1, "adjudicate": 2, "summarize": 3}


class ObjectStore:
    def __init__(self, workspace: str | Path) -> None:
        self.root = Path(workspace).resolve() / "state" / "objects"

    def put(self, payload: dict[str, Any]) -> str:
        data = canonical_json_bytes(payload)
        digest = sha256_bytes(data)
        path = self.root / f"{digest}.json"
        if path.exists():
            try:
                if path.read_bytes() == data:
                    return digest
            except OSError:
                pass
        atomic_write_bytes(path, data)
        return digest

    def get(self, digest: str) -> dict[str, Any]:
        path = self.root / f"{digest}.json"
        if not path.is_file():
            raise BusinessError(
                E_INVALID_STAGE,
                "referenced stage object is missing",
                {"payload_digest": digest, "path": str(path)},
            )
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise BusinessError(
                E_INVALID_STAGE,
                "referenced stage object cannot be read",
                {"payload_digest": digest, "cause": str(exc)},
            ) from exc
        if sha256_bytes(data) != digest:
            raise BusinessError(
                E_INVALID_STAGE,
                "stage object digest mismatch",
                {"payload_digest": digest, "path": str(path)},
            )
        return read_json(path)

    def exists(self, digest: str) -> bool:
        return (self.root / f"{digest}.json").is_file()


@dataclass
class PrepareResult:
    generation: int
    no_work: bool
    invalidation: dict[str, Any]
    products: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "generation": self.generation,
            "no_work": self.no_work,
            "invalidation": self.invalidation,
            "products": self.products,
        }


class CheckpointStore:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()
        self.root = self.workspace / "state"
        self.lock_path = self.root / "lock"
        self.head_path = self.root / "head.json"
        self.checkpoints_dir = self.root / "checkpoints"
        self.objects = ObjectStore(self.workspace)

    def _empty_head(self) -> dict[str, Any]:
        return {
            "schema": "posalign.checkpoint_head.v1",
            "generation": 0,
            "seq": 0,
            "generation_max_stage_rank": -1,
            "products": {},
            "active_run": None,
            "last_invalidation": None,
        }

    def _read_head_locked(self) -> dict[str, Any]:
        head = read_json(self.head_path, default=None)
        if head is None:
            return self._empty_head()
        if head.get("schema") != "posalign.checkpoint_head.v1":
            raise BusinessError(
                E_INVALID_STAGE,
                "checkpoint head schema is invalid",
                {"path": str(self.head_path)},
            )
        return head

    def _write_head_locked(self, head: dict[str, Any]) -> None:
        atomic_write_json(self.head_path, head)

    def read_head(self) -> dict[str, Any]:
        with exclusive_file_lock(self.lock_path):
            return self._read_head_locked()

    @staticmethod
    def _product_name(shard: str, stage: str) -> str:
        return f"{shard}/{stage}"

    @staticmethod
    def _parse_product_name(name: str) -> tuple[str, str]:
        shard, stage = name.rsplit("/", 1)
        return shard, stage

    def _reused_products(self, head: dict[str, Any]) -> list[dict[str, Any]]:
        reused: list[dict[str, Any]] = []
        for name, product in sorted(head["products"].items()):
            shard, stage = self._parse_product_name(name)
            reused.append(
                {
                    "stage": stage,
                    "shard": shard,
                    "generation": product.get("generation"),
                    "sequence": product.get("seq"),
                    "payload_digest": product.get("payload_digest"),
                    "fingerprint": product.get("fingerprint"),
                }
            )
        return reused

    def _component_reasons(
        self,
        old: dict[str, Any] | None,
        new: dict[str, Any],
        shards: list[str],
        *,
        initial: bool,
    ) -> list[dict[str, Any]]:
        reasons: list[dict[str, Any]] = []
        if initial or old is None:
            for shard in shards:
                reasons.append(
                    {
                        "stage": "parse",
                        "shard": shard,
                        "reason": "initial_run",
                    }
                )
            return reasons
        old_gov = old.get("governance", {})
        new_gov = new.get("governance", {})
        mappings = {
            "alias_fingerprint": ("parse", "old_label_alias_table_changed"),
            "candidate_rules_fingerprint": ("parse", "candidate_rules_changed"),
            "v1_fingerprint": ("parse", "structure_v1_rules_changed"),
            "alignment_fingerprint": ("align", "alignment_tolerance_changed"),
            "adjudication_fingerprint": ("adjudicate", "adjudication_priority_or_threshold_changed"),
        }
        for component, (stage, reason) in mappings.items():
            if old_gov.get(component) != new_gov.get(component):
                for shard in shards:
                    reasons.append({"stage": stage, "shard": shard, "reason": reason})
        old_shards = old.get("shards", {})
        new_shards = new.get("shards", {})
        old_names = set(old_shards)
        new_names = set(new_shards)
        for shard in sorted(new_names - old_names):
            reasons.append({"stage": "parse", "shard": shard, "reason": "shard_added"})
        for shard in sorted(old_names - new_names):
            reasons.append({"stage": "parse", "shard": shard, "reason": "shard_removed"})
            reasons.append(
                {"stage": "summarize", "shard": shard, "reason": "shard_removed"}
            )
        for shard in sorted(old_names & new_names):
            if old_shards.get(shard) != new_shards.get(shard):
                reasons.append(
                    {"stage": "parse", "shard": shard, "reason": "shard_content_changed"}
                )
        if old.get("engine") != new.get("engine"):
            for shard in shards:
                reasons.append(
                    {"stage": "parse", "shard": shard, "reason": "engine_schema_changed"}
                )
        return reasons

    def prepare_run(
        self,
        *,
        components: dict[str, Any],
        shard_ids: list[str],
        desired_parse: dict[str, str],
        desired_align: dict[str, str],
        desired_adjudicate: str,
        signature: str,
        force_adjudicate: bool = False,
        force_reasons: list[dict[str, Any]] | None = None,
    ) -> PrepareResult:
        with exclusive_file_lock(self.lock_path):
            head = self._read_head_locked()
            active = head.get("active_run")
            if (
                active is not None
                and active.get("signature") == signature
                and active.get("status") != "complete"
            ):
                return PrepareResult(
                    generation=int(head["generation"]),
                    no_work=False,
                    invalidation=head.get("last_invalidation") or {},
                    products=head["products"],
                )

            old_components = active.get("components") if active else None
            initial = old_components is None
            reasons = self._component_reasons(
                old_components, components, shard_ids, initial=initial
            )
            if force_reasons:
                reasons.extend(force_reasons)

            invalid: dict[str, set[str]] = {stage: set() for stage in STAGE_RANK}
            for shard in shard_ids:
                product = head["products"].get(self._product_name(shard, "parse"))
                if product is None or product.get("fingerprint") != desired_parse[shard]:
                    invalid["parse"].add(shard)
            for shard in shard_ids:
                product = head["products"].get(self._product_name(shard, "align"))
                if (
                    shard in invalid["parse"]
                    or product is None
                    or product.get("fingerprint") != desired_align[shard]
                ):
                    invalid["align"].add(shard)
            adjudicate_product_names = [
                name for name in head["products"] if name.endswith("/adjudicate")
            ]
            if (
                invalid["align"]
                or force_adjudicate
                or not adjudicate_product_names
                or any(
                    head["products"].get(self._product_name(shard, "adjudicate"), {}).get(
                        "fingerprint"
                    )
                    != desired_adjudicate
                    for shard in shard_ids
                )
            ):
                invalid["adjudicate"].update(shard_ids)
            if invalid["adjudicate"]:
                invalid["summarize"].update(shard_ids)

            removed_shards = {
                self._parse_product_name(name)[0]
                for name in head["products"]
                if self._parse_product_name(name)[0] not in set(shard_ids)
            }
            invalid["summarize"].update(removed_shards)

            no_work = not any(invalid.values())
            if no_work:
                if active is not None:
                    active["status"] = "complete"
                    active["components"] = components
                    head["active_run"] = active
                    self._write_head_locked(head)
                return PrepareResult(
                    generation=int(head["generation"]),
                    no_work=True,
                    invalidation=head.get("last_invalidation") or {},
                    products=head["products"],
                )

            generation = int(head["generation"]) + 1
            cleared: list[dict[str, Any]] = []
            for stage in STAGE_RANK:
                for shard in sorted(invalid[stage]):
                    name = self._product_name(shard, stage)
                    product = head["products"].pop(name, None)
                    if product is not None:
                        cleared.append(
                            {
                                "stage": stage,
                                "shard": shard,
                                "generation": product.get("generation"),
                                "sequence": product.get("seq"),
                            }
                        )
            for name in list(head["products"]):
                shard, _stage = self._parse_product_name(name)
                if shard not in set(shard_ids):
                    head["products"].pop(name)
            deduped_reasons: list[dict[str, Any]] = []
            seen: set[tuple[Any, ...]] = set()
            for reason in reasons:
                key = (reason.get("stage"), reason.get("shard"), reason.get("reason"))
                if key not in seen:
                    seen.add(key)
                    deduped_reasons.append(reason)
            invalidation = {
                "generation": generation,
                "reasons": deduped_reasons,
                "invalidated_products": cleared,
                "reused_products": self._reused_products(head),
            }
            active = {
                "signature": signature,
                "components": components,
                "desired_parse": desired_parse,
                "desired_align": desired_align,
                "desired_adjudicate": desired_adjudicate,
                "status": "running",
            }
            head.update(
                {
                    "generation": generation,
                    "generation_max_stage_rank": -1,
                    "active_run": active,
                    "last_invalidation": invalidation,
                }
            )
            self._write_head_locked(head)
            return PrepareResult(
                generation=generation,
                no_work=False,
                invalidation=invalidation,
                products=head["products"],
            )

    def invalidate_stages(
        self,
        stages: list[str],
        *,
        reasons: list[dict[str, Any]],
    ) -> dict[str, Any]:
        with exclusive_file_lock(self.lock_path):
            head = self._read_head_locked()
            cleared: list[dict[str, Any]] = []
            for stage in stages:
                if stage not in STAGE_RANK:
                    raise BusinessError(
                        E_INVALID_STAGE,
                        f"unknown stage {stage!r}",
                    )
                for name in sorted(head["products"]):
                    shard, product_stage = self._parse_product_name(name)
                    if product_stage == stage:
                        product = head["products"].pop(name)
                        cleared.append(
                            {
                                "stage": stage,
                                "shard": shard,
                                "generation": product.get("generation"),
                                "sequence": product.get("seq"),
                            }
                        )
            invalidation = head.get("last_invalidation") or {}
            invalidation.setdefault("generation", head["generation"])
            invalidation.setdefault("invalidated_products", []).extend(cleared)
            existing = invalidation.get("reasons", [])
            existing.extend(reasons)
            invalidation["reasons"] = existing
            head["last_invalidation"] = invalidation
            if head.get("active_run") is not None:
                head["active_run"]["status"] = "running"
            self._write_head_locked(head)
            return invalidation

    def _validate_prerequisites_locked(
        self,
        *,
        stage: str,
        shard: str,
        head: dict[str, Any],
        required_shards: list[str],
        mapping_version: int,
    ) -> list[str]:
        missing: list[str] = []
        if stage == "align":
            if self._product_name(shard, "parse") not in head["products"]:
                missing.append(f"{shard}/parse")
        elif stage == "adjudicate":
            for required in required_shards:
                if self._product_name(required, "align") not in head["products"]:
                    missing.append(f"{required}/align")
            if mapping_version <= 0 or not (
                self.workspace / "mapping" / "versions" / f"{mapping_version:08d}.json"
            ).is_file():
                missing.append(f"mapping/version/{mapping_version}")
        elif stage == "summarize":
            for required in required_shards:
                if self._product_name(required, "adjudicate") not in head["products"]:
                    missing.append(f"{required}/adjudicate")
            if mapping_version <= 0 or not (
                self.workspace / "mapping" / "versions" / f"{mapping_version:08d}.json"
            ).is_file():
                missing.append(f"mapping/version/{mapping_version}")
        return missing

    @staticmethod
    def _prerequisite_names(
        *,
        stage: str,
        shard: str,
        required_shards: list[str],
        mapping_version: int,
    ) -> list[str]:
        if stage == "parse":
            return []
        if stage == "align":
            return [f"{shard}/parse"]
        if stage == "adjudicate":
            return [f"{required}/align" for required in required_shards] + [
                f"mapping/{mapping_version:08d}"
            ]
        if stage == "summarize":
            return [f"{required}/adjudicate" for required in required_shards] + [
                f"mapping/{mapping_version:08d}"
            ]
        return []

    def publish(
        self,
        *,
        stage: str,
        shard: str,
        payload: dict[str, Any],
        fingerprint: str,
        generation: int | None = None,
        governance_version: str,
        governance_fingerprints: dict[str, str],
        shard_content_fp: str,
        mapping_version: int = 0,
        required_shards: list[str] | None = None,
        sequence: int | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGE_RANK:
            raise BusinessError(E_INVALID_STAGE, f"unknown stage {stage!r}")
        payload_digest = self.objects.put(payload)
        required = sorted(required_shards or [shard])
        with exclusive_file_lock(self.lock_path):
            head = self._read_head_locked()
            expected_generation = int(head["generation"])
            if generation is None:
                generation = expected_generation
            if generation != expected_generation:
                raise BusinessError(
                    E_STALE_CHECKPOINT,
                    "checkpoint generation is stale",
                    {
                        "requested_generation": generation,
                        "current_generation": expected_generation,
                    },
                )
            expected_seq = int(head["seq"]) + 1
            if sequence is None:
                sequence = expected_seq
            if sequence != expected_seq:
                raise BusinessError(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence must advance contiguously",
                    {
                        "requested_sequence": sequence,
                        "expected_sequence": expected_seq,
                        "current_sequence": head["seq"],
                    },
                )
            name = self._product_name(shard, stage)
            existing = head["products"].get(name)
            if existing is not None and existing.get("generation") == generation:
                raise BusinessError(
                    E_STALE_CHECKPOINT,
                    "duplicate stage publication in the same generation",
                    {"stage": stage, "shard": shard, "generation": generation},
                )
            rank = STAGE_RANK[stage]
            max_rank = int(head.get("generation_max_stage_rank", -1))
            if rank < max_rank:
                raise BusinessError(
                    E_STALE_CHECKPOINT,
                    "cross-stage backward publication is not allowed",
                    {
                        "stage": stage,
                        "stage_rank": rank,
                        "generation_max_stage_rank": max_rank,
                    },
                )
            missing = self._validate_prerequisites_locked(
                stage=stage,
                shard=shard,
                head=head,
                required_shards=required,
                mapping_version=mapping_version,
            )
            if missing:
                raise BusinessError(
                    E_INVALID_STAGE,
                    "required upstream stage products are missing",
                    {"stage": stage, "shard": shard, "missing": missing},
                )
            if not self.objects.exists(payload_digest):
                raise BusinessError(
                    E_INVALID_STAGE,
                    "payload object disappeared before publication",
                    {"payload_digest": payload_digest},
                )
            checkpoint = {
                "schema": "posalign.checkpoint.v1",
                "seq": sequence,
                "previous_seq": head["seq"],
                "generation": generation,
                "stage": stage,
                "stage_rank": rank,
                "shard": shard,
                "shard_content_fingerprint": shard_content_fp,
                "governance_version": governance_version,
                "governance_fingerprints": governance_fingerprints,
                "mapping_table_version": mapping_version,
                "payload_digest": payload_digest,
                "fingerprint": fingerprint,
                "prerequisites": self._prerequisite_names(
                    stage=stage,
                    shard=shard,
                    required_shards=required,
                    mapping_version=mapping_version,
                ),
            }
            checkpoint_path = self.checkpoints_dir / f"{sequence:08d}.json"
            atomic_write_json(checkpoint_path, checkpoint)
            head["seq"] = sequence
            head["generation_max_stage_rank"] = max(max_rank, rank)
            head["products"][name] = {
                "stage": stage,
                "shard": shard,
                "generation": generation,
                "seq": sequence,
                "payload_digest": payload_digest,
                "fingerprint": fingerprint,
                "mapping_version": mapping_version,
                "shard_content_fingerprint": shard_content_fp,
            }
            if head.get("active_run") is not None:
                head["active_run"]["status"] = "running"
            self._write_head_locked(head)
            return checkpoint

    def payload_for(self, product: dict[str, Any]) -> dict[str, Any]:
        return self.objects.get(product["payload_digest"])

    def mark_status(self, status: str, *, extra: dict[str, Any] | None = None) -> None:
        with exclusive_file_lock(self.lock_path):
            head = self._read_head_locked()
            if head.get("active_run") is not None:
                head["active_run"]["status"] = status
                if extra:
                    head["active_run"]["status_detail"] = extra
            self._write_head_locked(head)

    def status(self) -> dict[str, Any]:
        head = self.read_head()
        active = head.get("active_run") or {}
        return {
            "generation": head["generation"],
            "seq": head["seq"],
            "sequence": head["seq"],
            "active_run": active,
            "invalidation": head.get("last_invalidation"),
            "products": head["products"],
        }

