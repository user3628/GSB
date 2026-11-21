"""High-level integration surface for block attention and incremental updates."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from .checkpoint import CheckpointStore, RunHeader
from .config import AttentionConfig
from .core import (
    CoreAttentionResult,
    InputFingerprints,
    arrays_byte_equal,
    assemble_outputs,
    build_fingerprints,
    compute_row_block,
    full_attention,
    max_abs_deviation,
    needed_column_blocks,
    validate_and_prepare,
)
from .digest import array_digest, sha256_arrays, sha256_json
from .errors import fail
from .numeric import NumericStats, validate_attention_inputs
from .scheduler import RowBlockPlan, plan_row_block
from .state import RowBlockState


@dataclass
class AttentionResult:
    shard_id: str
    status: str
    run_id: str
    output: np.ndarray | None
    row_max: np.ndarray | None
    row_norm: np.ndarray | None
    committed_row_blocks: int
    expected_row_blocks: int
    available_sequence: int
    reused_row_blocks: list[int] = field(default_factory=list)
    recomputed_row_blocks: list[int] = field(default_factory=list)
    numeric_stats: NumericStats = field(default_factory=NumericStats)
    plans: list[RowBlockPlan] = field(default_factory=list)
    verified: bool = False
    anomalies: list[dict[str, Any]] = field(default_factory=list)
    invalidation: dict[str, Any] | None = None
    served_from_checkpoint: bool = False

    @property
    def complete(self) -> bool:
        return self.status == "complete"

    def digest(self) -> str | None:
        if self.output is None or self.row_max is None or self.row_norm is None:
            return None
        return sha256_arrays(
            {
                "output": self.output,
                "row_max": self.row_max,
                "row_norm": self.row_norm,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "shard_id": self.shard_id,
            "status": self.status,
            "run_id": self.run_id,
            "committed_row_blocks": self.committed_row_blocks,
            "expected_row_blocks": self.expected_row_blocks,
            "available_sequence": self.available_sequence,
            "reused_row_blocks": self.reused_row_blocks,
            "recomputed_row_blocks": self.recomputed_row_blocks,
            "verified": self.verified,
            "served_from_checkpoint": self.served_from_checkpoint,
            "numeric_stats": self.numeric_stats.to_dict(),
            "result_sha256": self.digest(),
            "anomalies": self.anomalies,
            "plans": [plan.to_dict() for plan in self.plans],
        }
        if self.output is not None:
            value["output_sha256"] = array_digest(self.output)
            value["row_max_sha256"] = array_digest(self.row_max)
            value["row_norm_sha256"] = array_digest(self.row_norm)
        if self.invalidation is not None:
            value["invalidation"] = self.invalidation
        return value


@dataclass(frozen=True)
class PrefixUpdateRequest:
    """Explicit prefix-update protocol request keyed by shard identity."""

    shard_id: str
    key_start: int
    key_end: int
    k_values: np.ndarray
    v_values: np.ndarray


def _merge_numeric_stats(states: list[Any]) -> NumericStats:
    result = NumericStats()
    for state in states:
        result = result.merge(state.numeric_stats)
    return result


class AttentionEngine:
    """Owns one shard's inputs, configuration and checkpoint namespace."""

    def __init__(
        self,
        workspace: str | Path,
        shard_id: str,
        q: np.ndarray,
        k: np.ndarray,
        v: np.ndarray,
        config: AttentionConfig,
    ) -> None:
        self.shard_id = shard_id
        self.q, self.k, self.v = validate_and_prepare(q, k, v)
        validate_attention_inputs(self.q, self.k, self.v, config, shard_id)
        self.config = config
        self.fingerprints = build_fingerprints(self.q, self.k, self.v, config)
        self.store = CheckpointStore(workspace, shard_id)

    @classmethod
    def from_npz(
        cls,
        workspace: str | Path,
        shard_id: str,
        input_path: str | Path,
        config: AttentionConfig,
    ) -> "AttentionEngine":
        with np.load(input_path, allow_pickle=False) as payload:
            try:
                q = payload["q"]
                k = payload["k"]
                v = payload["v"]
            except KeyError as exc:
                fail("E_SHAPE", f"Input shard {input_path} is missing {exc}")
        return cls(workspace, shard_id, q, k, v, config)

    def _fingerprint_dict(self, fingerprints: InputFingerprints | None = None) -> dict[str, str]:
        fingerprints = fingerprints or self.fingerprints
        return {
            "q_fingerprint": fingerprints.q,
            "kv_fingerprint": fingerprints.kv,
            "input_fingerprint": fingerprints.input,
            "config_fingerprint": fingerprints.config,
        }

    def _header_matches(self, header: RunHeader) -> bool:
        return (
            header.config_fingerprint == self.fingerprints.config
            and header.input_fingerprint == self.fingerprints.input
            and header.q_shape == list(self.q.shape)
            and header.k_shape == list(self.k.shape)
            and header.v_shape == list(self.v.shape)
        )

    def _require_matching_latest(self, *, require_complete: bool) -> tuple[RunHeader, list[RowBlockState], Any, list[dict[str, Any]]]:
        latest = self.store.latest_header()
        if latest is None:
            fail(
                "E_INVALID_STAGE",
                "No checkpoint state exists for this shard",
                shard_id=self.shard_id,
                available_sequence=0,
            )
        if not self._header_matches(latest):
            state = self.store.query_state(latest.run_id)
            fail(
                "E_INVALID_STAGE",
                "Latest checkpoint does not match the supplied Q/K/V and config",
                shard_id=self.shard_id,
                latest_run_id=latest.run_id,
                expected_input_fingerprint=self.fingerprints.input,
                checkpoint_input_fingerprint=latest.input_fingerprint,
                available_sequence=state["available_sequence"],
                hint="use apply_prefix_update or apply_config_change from the original inputs",
            )
        states, scan, anomalies = self.store.recover_prefix(latest.run_id)
        if require_complete and not scan.complete:
            fail(
                "E_INVALID_STAGE",
                "A complete checkpoint is required for this operation",
                shard_id=self.shard_id,
                run_id=latest.run_id,
                committed_row_blocks=scan.committed_row_blocks,
                expected_row_blocks=scan.header.expected_row_blocks,
                available_sequence=scan.available_sequence,
            )
        return latest, states, scan, anomalies

    def _build_result(
        self,
        *,
        header: RunHeader,
        states: list[Any],
        status: str,
        reused_row_blocks: list[int],
        recomputed_row_blocks: list[int],
        plans: list[RowBlockPlan],
        verified: bool,
        anomalies: list[dict[str, Any]],
        invalidation: dict[str, Any] | None = None,
        served_from_checkpoint: bool = False,
    ) -> AttentionResult:
        numeric = _merge_numeric_stats(states)
        if status == "complete":
            output, row_max, row_norm = assemble_outputs(states, self.q.shape)
            available_sequence = header.base_seq + len(states) - 1 if states else header.base_seq - 1
        else:
            output = None
            row_max = None
            row_norm = None
            available_sequence = (
                states[-1].sequence if states else header.base_seq - 1
            )
        return AttentionResult(
            shard_id=self.shard_id,
            status=status,
            run_id=header.run_id,
            output=output,
            row_max=row_max,
            row_norm=row_norm,
            committed_row_blocks=len(states),
            expected_row_blocks=header.expected_row_blocks,
            available_sequence=available_sequence,
            reused_row_blocks=reused_row_blocks,
            recomputed_row_blocks=recomputed_row_blocks,
            numeric_stats=numeric,
            plans=plans,
            verified=verified,
            anomalies=anomalies,
            invalidation=invalidation,
            served_from_checkpoint=served_from_checkpoint,
        )

    def run(
        self,
        *,
        max_new_row_blocks: int | None = None,
        start_row_block: int | None = None,
    ) -> AttentionResult:
        """Run, resume, or return a cached complete result.

        ``max_new_row_blocks`` is the graceful-stop control used by the demo.
        A paused result does not expose a partial attention matrix.
        """

        if max_new_row_blocks is not None and max_new_row_blocks < 0:
            fail("E_CONFIG", "max_new_row_blocks must be non-negative")
        latest = self.store.latest_header()
        if latest is None:
            header = self.store.create_run(
                config=self.config,
                q_shape=self.q.shape,
                k_shape=self.k.shape,
                v_shape=self.v.shape,
                fingerprints=self._fingerprint_dict(),
                mode="full",
                expected_latest_run_id=None,
            )
            states: list[Any] = []
            anomalies: list[dict[str, Any]] = []
            scan = self.store.scan_run(header.run_id)
            reused: list[int] = []
        else:
            if not self._header_matches(latest):
                state = self.store.query_state(latest.run_id)
                fail(
                    "E_INVALID_STAGE",
                    "Latest run belongs to different attention inputs or semantic config",
                    shard_id=self.shard_id,
                    latest_run_id=latest.run_id,
                    available_sequence=state["available_sequence"],
                    committed_row_blocks=state["committed_row_blocks"],
                    hint="use the incremental update APIs instead of silently replacing state",
                )
            header, loaded_states, scan, anomalies = self._require_matching_latest(require_complete=False)
            states = [state.to_summary() for state in loaded_states]
            del loaded_states
            reused = list(range(len(states)))

        expected = header.expected_row_blocks
        if start_row_block is not None and start_row_block != len(states):
            fail(
                "E_INVALID_STAGE",
                "Requested resume point does not match available contiguous state",
                shard_id=self.shard_id,
                requested_start_row_block=start_row_block,
                available_row_blocks=len(states),
                available_sequence=scan.available_sequence,
                expected_row_blocks=expected,
            )

        if scan.complete:
            return self._build_result(
                header=header,
                states=states,
                status="complete",
                reused_row_blocks=reused,
                recomputed_row_blocks=[],
                plans=[],
                verified=True,
                anomalies=anomalies,
                served_from_checkpoint=True,
            )

        plans: list[RowBlockPlan] = []
        recomputed: list[int] = []
        remaining = max_new_row_blocks
        batch, heads, q_length, head_dim = self.q.shape
        key_length = self.k.shape[2]
        row_block = len(states)
        while row_block < expected:
            if remaining is not None and remaining <= 0:
                break
            q_start = row_block * self.config.row_block_size
            q_end = min(q_start + self.config.row_block_size, q_length)
            columns = needed_column_blocks(self.config, key_length, q_end)
            plan = plan_row_block(
                self.config,
                shard_id=self.shard_id,
                batch=batch,
                heads=heads,
                head_dim=head_dim,
                key_length=key_length,
                q_start=q_start,
                q_end=q_end,
                row_block=row_block,
                column_blocks=columns,
            )
            state, diagnostics = compute_row_block(
                self.q,
                self.k,
                self.v,
                config=self.config,
                shard_id=self.shard_id,
                row_block=row_block,
                plan=plan,
                fingerprints=self.fingerprints,
            )
            published = self.store.publish_next(state, run_id=header.run_id)
            state.sequence = int(published["sequence"])
            state.run_id = header.run_id
            self.store.append_schedule(
                {
                    "run_id": header.run_id,
                    "row_block": row_block,
                    "sequence": state.sequence,
                    "decision": plan.to_dict(),
                    "recompute": diagnostics,
                },
                run_id=header.run_id,
            )
            states.append(state.to_summary())
            del state
            plans.append(plan)
            recomputed.append(row_block)
            row_block += 1
            if remaining is not None:
                remaining -= 1

        status = "complete" if len(states) == expected else "paused"
        return self._build_result(
            header=header,
            states=states,
            status=status,
            reused_row_blocks=reused,
            recomputed_row_blocks=recomputed,
            plans=plans,
            verified=status == "complete",
            anomalies=anomalies,
            served_from_checkpoint=bool(reused) and not recomputed,
        )

    def resume(
        self,
        *,
        max_new_row_blocks: int | None = None,
        start_row_block: int | None = None,
    ) -> AttentionResult:
        if self.store.latest_header() is None:
            fail(
                "E_INVALID_STAGE",
                "No checkpoint state exists to resume",
                shard_id=self.shard_id,
                available_sequence=0,
                available_row_blocks=0,
            )
        return self.run(max_new_row_blocks=max_new_row_blocks, start_row_block=start_row_block)

    @staticmethod
    def _changed_key_positions(old_k: np.ndarray, old_v: np.ndarray, new_k: np.ndarray, new_v: np.ndarray) -> tuple[int, int] | None:
        old_len = old_k.shape[2]
        new_len = new_k.shape[2]
        maximum = max(old_len, new_len)
        first: int | None = None
        last: int | None = None
        for key_index in range(maximum):
            changed = key_index >= old_len or key_index >= new_len
            if not changed:
                changed = not (
                    np.array_equal(old_k[:, :, key_index, :], new_k[:, :, key_index, :])
                    and np.array_equal(old_v[:, :, key_index, :], new_v[:, :, key_index, :])
                )
            if changed:
                if first is None:
                    first = key_index
                last = key_index
        if first is None or last is None:
            return None
        return first, last + 1

    def _changed_column_blocks(
        self,
        old_k: np.ndarray,
        old_v: np.ndarray,
        new_k: np.ndarray,
        new_v: np.ndarray,
    ) -> list[int]:
        old_len = old_k.shape[2]
        new_len = new_k.shape[2]
        maximum_blocks = (max(old_len, new_len) + self.config.col_block_size - 1) // self.config.col_block_size
        changed: list[int] = []
        for col_block in range(maximum_blocks):
            start = col_block * self.config.col_block_size
            end = min(start + self.config.col_block_size, max(old_len, new_len))
            old_start = min(start, old_len)
            old_end = min(end, old_len)
            new_start = min(start, new_len)
            new_end = min(end, new_len)
            old_k_block = old_k[:, :, old_start:old_end, :]
            old_v_block = old_v[:, :, old_start:old_end, :]
            new_k_block = new_k[:, :, new_start:new_end, :]
            new_v_block = new_v[:, :, new_start:new_end, :]
            if not (
                np.array_equal(old_k_block, new_k_block)
                and np.array_equal(old_v_block, new_v_block)
            ):
                changed.append(col_block)
        return changed

    @staticmethod
    def _check_byte_equality(
        *,
        shard_id: str,
        incremental: tuple[np.ndarray, np.ndarray, np.ndarray],
        fresh: CoreAttentionResult,
        row_block_start: int,
        row_block_end: int,
    ) -> None:
        names = ("output", "row_max", "row_norm")
        fresh_values = (fresh.output, fresh.row_max, fresh.row_norm)
        deviations: dict[str, float] = {}
        mismatched: list[str] = []
        for name, left, right in zip(names, incremental, fresh_values):
            if arrays_byte_equal(left, right):
                continue
            mismatched.append(name)
            deviations[name] = max_abs_deviation(left, right)
        if mismatched:
            fail(
                "E_INCREMENTAL_MISMATCH",
                "Reused and recomputed column blocks do not reproduce full attention byte-for-byte",
                shard_id=shard_id,
                row_block_start=row_block_start,
                row_block_end=row_block_end,
                mismatched_fields=mismatched,
                max_abs_deviation=max(deviations.values(), default=float("inf")),
                deviations=deviations,
            )

    def _verify_against_full(
        self,
        *,
        q: np.ndarray,
        k: np.ndarray,
        v: np.ndarray,
        config: AttentionConfig,
        states: list[RowBlockState],
        row_block_start: int,
        row_block_end: int,
    ) -> None:
        candidate = assemble_outputs(states, q.shape)
        fresh = full_attention(q, k, v, config, shard_id=self.shard_id)
        self._check_byte_equality(
            shard_id=self.shard_id,
            incremental=candidate,
            fresh=fresh,
            row_block_start=row_block_start,
            row_block_end=row_block_end,
        )

    def apply_prefix_update(
        self,
        *,
        shard_id: str | None = None,
        key_start: int,
        key_end: int,
        k_values: np.ndarray,
        v_values: np.ndarray,
        verify: bool | None = None,
    ) -> AttentionResult:
        """Replace or extend a K/V prefix and rebuild only dependent blocks."""

        if shard_id is not None and shard_id != self.shard_id:
            fail(
                "E_INVALID_STAGE",
                "Prefix update shard identifier does not match this engine",
                request_shard_id=shard_id,
                engine_shard_id=self.shard_id,
            )
        old_k, old_v = self.k, self.v
        old_length = old_k.shape[2]
        if key_start < 0 or key_end <= key_start:
            fail("E_SHAPE", "Invalid affected key interval", key_start=key_start, key_end=key_end)
        if key_start > old_length:
            fail(
                "E_SHAPE",
                "Prefix update would leave a gap after the current K/V length",
                shard_id=self.shard_id,
                key_start=key_start,
                current_key_length=old_length,
            )
        k_values = np.asarray(k_values)
        v_values = np.asarray(v_values)
        expected_shape = (old_k.shape[0], old_k.shape[1], key_end - key_start, old_k.shape[3])
        if k_values.shape != expected_shape or v_values.shape != expected_shape:
            fail(
                "E_SHAPE",
                "Prefix update K/V slices have the wrong shape",
                shard_id=self.shard_id,
                expected_shape=list(expected_shape),
                k_shape=list(k_values.shape),
                v_shape=list(v_values.shape),
            )
        new_length = max(old_length, key_end)
        new_k = np.zeros((old_k.shape[0], old_k.shape[1], new_length, old_k.shape[3]), dtype=np.float64)
        new_v = np.zeros_like(new_k)
        new_k[:, :, :old_length, :] = old_k
        new_v[:, :, :old_length, :] = old_v
        new_k[:, :, key_start:key_end, :] = np.asarray(k_values, dtype=np.float64)
        new_v[:, :, key_start:key_end, :] = np.asarray(v_values, dtype=np.float64)
        validate_attention_inputs(self.q, new_k, new_v, self.config, self.shard_id)

        latest, old_states, scan, anomalies = self._require_matching_latest(require_complete=True)
        changed_keys = self._changed_key_positions(old_k, old_v, new_k, new_v)
        if changed_keys is None:
            return self._build_result(
                header=latest,
                states=old_states,
                status="complete",
                reused_row_blocks=list(range(len(old_states))),
                recomputed_row_blocks=[],
                plans=[],
                verified=True,
                anomalies=anomalies,
                invalidation={
                    "invalidation_id": None,
                    "reasons": [],
                    "changed_key_interval": [key_start, key_end],
                    "reused_row_blocks": list(range(len(old_states))),
                    "recomputed_row_blocks": [],
                },
                served_from_checkpoint=True,
            )

        changed_cols = self._changed_column_blocks(old_k, old_v, new_k, new_v)
        if not changed_cols:
            fail(
                "E_INCREMENTAL_MISMATCH",
                "Key content changed but no dependent column block was identified",
                shard_id=self.shard_id,
                changed_key_interval=list(changed_keys),
            )
        first_changed_key, last_changed_key = changed_keys
        earliest_col = min(changed_cols)
        if self.config.causal:
            earliest_row = min(
                first_changed_key // self.config.row_block_size,
                scan.header.expected_row_blocks,
            )
        else:
            earliest_row = 0

        new_fingerprints = build_fingerprints(self.q, new_k, new_v, self.config)
        batch, heads, q_length, head_dim = self.q.shape
        new_states: list[RowBlockState] = []
        plans: list[RowBlockPlan] = []
        schedule: list[dict[str, Any]] = []
        reused_rows: list[int] = []
        recomputed_rows: list[int] = []
        reused_cols_by_row: dict[str, list[int]] = {}
        recomputed_cols_by_row: dict[str, list[int]] = {}
        force_cols = set(changed_cols)
        for row_block, old_state in enumerate(old_states):
            if row_block < earliest_row:
                state = old_state.copy_for_run(run_id="", sequence=0)
                state.input_fingerprint = new_fingerprints.input
                state.kv_fingerprint = new_fingerprints.kv
                new_states.append(state)
                reused_rows.append(row_block)
                reused_cols_by_row[str(row_block)] = [item.col_block for item in old_state.partials]
                continue
            q_start = row_block * self.config.row_block_size
            q_end = min(q_start + self.config.row_block_size, q_length)
            columns = needed_column_blocks(self.config, new_k.shape[2], q_end)
            plan = plan_row_block(
                self.config,
                shard_id=self.shard_id,
                batch=batch,
                heads=heads,
                head_dim=head_dim,
                key_length=new_k.shape[2],
                q_start=q_start,
                q_end=q_end,
                row_block=row_block,
                column_blocks=columns,
            )
            state, diagnostics = compute_row_block(
                self.q,
                new_k,
                new_v,
                config=self.config,
                shard_id=self.shard_id,
                row_block=row_block,
                plan=plan,
                reused_partials={item.col_block: item for item in old_state.partials},
                force_recompute_cols=force_cols,
                fingerprints=new_fingerprints,
            )
            new_states.append(state)
            plans.append(plan)
            recomputed_rows.append(row_block)
            reused_cols_by_row[str(row_block)] = [int(item) for item in diagnostics["reused_columns"]]
            recomputed_cols_by_row[str(row_block)] = [int(item) for item in diagnostics["recomputed_columns"]]
            schedule.append(
                {
                    "run_id": latest.run_id,
                    "row_block": row_block,
                    "decision": plan.to_dict(),
                    "recompute": diagnostics,
                }
            )

        should_verify = self.config.verify_incremental if verify is None else verify
        if should_verify:
            self._verify_against_full(
                q=self.q,
                k=new_k,
                v=new_v,
                config=self.config,
                states=new_states,
                row_block_start=earliest_row,
                row_block_end=scan.header.expected_row_blocks - 1,
            )

        invalidation_id = "inv-" + sha256_json(
            {
                "parent_run_id": latest.run_id,
                "new_input_fingerprint": new_fingerprints.input,
                "changed_key_interval": list(changed_keys),
                "changed_col_blocks": changed_cols,
            }
        )[:20]
        invalidation = {
            "invalidation_id": invalidation_id,
            "shard_id": self.shard_id,
            "parent_run_id": latest.run_id,
            "reasons": ["prefix_content_changed"],
            "changed_key_interval": [first_changed_key, last_changed_key],
            "affected_col_blocks": changed_cols,
            "earliest_affected_row_block": earliest_row,
            "earliest_affected_col_block": earliest_col,
            "affected_row_blocks": list(range(earliest_row, scan.header.expected_row_blocks)),
            "reused_row_blocks": reused_rows,
            "recomputed_row_blocks": recomputed_rows,
            "reused_col_blocks_by_row": reused_cols_by_row,
            "recomputed_col_blocks_by_row": recomputed_cols_by_row,
            "old_input_fingerprint": latest.input_fingerprint,
            "new_input_fingerprint": new_fingerprints.input,
            "verified_byte_identical": bool(should_verify),
        }
        header = self.store.publish_snapshot(
            config=self.config,
            q_shape=self.q.shape,
            k_shape=new_k.shape,
            v_shape=new_v.shape,
            fingerprints=self._fingerprint_dict(new_fingerprints),
            states=new_states,
            mode="prefix_incremental",
            parent_run_id=latest.run_id,
            invalidation=invalidation,
            schedule_decisions=schedule,
        )
        self.k, self.v = new_k, new_v
        self.fingerprints = new_fingerprints
        return self._build_result(
            header=header,
            states=new_states,
            status="complete",
            reused_row_blocks=reused_rows,
            recomputed_row_blocks=recomputed_rows,
            plans=plans,
            verified=bool(should_verify),
            anomalies=anomalies,
            invalidation=invalidation,
        )

    def apply_prefix_update_request(
        self,
        request: PrefixUpdateRequest,
        *,
        verify: bool | None = None,
    ) -> AttentionResult:
        return self.apply_prefix_update(
            shard_id=request.shard_id,
            key_start=request.key_start,
            key_end=request.key_end,
            k_values=request.k_values,
            v_values=request.v_values,
            verify=verify,
        )

    def apply_config_change(self, new_config: AttentionConfig) -> AttentionResult:
        old_config = self.config
        semantic_changes: list[str] = []
        if (
            old_config.row_block_size != new_config.row_block_size
            or old_config.col_block_size != new_config.col_block_size
        ):
            semantic_changes.append("block_parameters_changed")
        if old_config.causal != new_config.causal:
            semantic_changes.append("causal_mask_changed")
        if old_config.online_strategy != new_config.online_strategy:
            semantic_changes.append("online_strategy_changed")
        if (
            old_config.score_clamp != new_config.score_clamp
            or old_config.extreme_threshold != new_config.extreme_threshold
        ):
            semantic_changes.append("numeric_policy_changed")
        if not semantic_changes:
            self.config = new_config
            latest, states, _, anomalies = self._require_matching_latest(require_complete=True)
            return self._build_result(
                header=latest,
                states=states,
                status="complete",
                reused_row_blocks=list(range(len(states))),
                recomputed_row_blocks=[],
                plans=[],
                verified=True,
                anomalies=anomalies,
                served_from_checkpoint=True,
            )

        latest, _, _, anomalies = self._require_matching_latest(require_complete=True)
        fresh = full_attention(self.q, self.k, self.v, new_config, shard_id=self.shard_id)
        new_fingerprints = fresh.input_fingerprints
        invalidation_id = "inv-" + sha256_json(
            {
                "parent_run_id": latest.run_id,
                "old_config": old_config.semantic_fingerprint,
                "new_config": new_config.semantic_fingerprint,
                "reasons": semantic_changes,
            }
        )[:20]
        invalidation = {
            "invalidation_id": invalidation_id,
            "shard_id": self.shard_id,
            "parent_run_id": latest.run_id,
            "reasons": semantic_changes,
            "changed_key_interval": [0, self.k.shape[2]],
            "affected_col_blocks": list(
                range((self.k.shape[2] + new_config.col_block_size - 1) // new_config.col_block_size)
            ),
            "earliest_affected_row_block": 0,
            "earliest_affected_col_block": 0,
            "affected_row_blocks": list(range(len(fresh.states))),
            "reused_row_blocks": [],
            "recomputed_row_blocks": list(range(len(fresh.states))),
            "reused_col_blocks_by_row": {},
            "recomputed_col_blocks_by_row": {
                str(state.row_block): [item.col_block for item in state.partials]
                for state in fresh.states
            },
            "old_config_fingerprint": old_config.semantic_fingerprint,
            "new_config_fingerprint": new_config.semantic_fingerprint,
            "verified_byte_identical": True,
        }
        schedule = [
            {
                "run_id": latest.run_id,
                "row_block": index,
                "decision": plan.to_dict(),
                "recompute": {"reason": "config_invalidated_all_blocks"},
            }
            for index, plan in enumerate(fresh.plans)
        ]
        header = self.store.publish_snapshot(
            config=new_config,
            q_shape=self.q.shape,
            k_shape=self.k.shape,
            v_shape=self.v.shape,
            fingerprints=self._fingerprint_dict(new_fingerprints),
            states=fresh.states,
            mode="config_incremental",
            parent_run_id=latest.run_id,
            invalidation=invalidation,
            schedule_decisions=schedule,
        )
        self.config = new_config
        self.fingerprints = new_fingerprints
        return self._build_result(
            header=header,
            states=fresh.states,
            status="complete",
            reused_row_blocks=[],
            recomputed_row_blocks=list(range(len(fresh.states))),
            plans=fresh.plans,
            verified=True,
            anomalies=anomalies,
            invalidation=invalidation,
        )

    def query_state(self) -> dict[str, Any]:
        return self.store.query_state()

    def query_invalidations(self) -> dict[str, Any]:
        return self.store.query_invalidations()

    def budget_report(self) -> dict[str, Any]:
        header = self.store.latest_header()
        decisions = self.store.load_schedule()
        if header is None:
            return {
                "shard_id": self.shard_id,
                "budget_bytes": self.config.memory_budget_bytes,
                "has_state": False,
                "decisions": [],
            }
        if not decisions:
            batch, heads, q_length, head_dim = self.q.shape
            key_length = self.k.shape[2]
            decisions = []
            row_blocks = (q_length + self.config.row_block_size - 1) // self.config.row_block_size
            for row_block in range(row_blocks):
                q_start = row_block * self.config.row_block_size
                q_end = min(q_start + self.config.row_block_size, q_length)
                columns = needed_column_blocks(self.config, key_length, q_end)
                plan = plan_row_block(
                    self.config,
                    shard_id=self.shard_id,
                    batch=batch,
                    heads=heads,
                    head_dim=head_dim,
                    key_length=key_length,
                    q_start=q_start,
                    q_end=q_end,
                    row_block=row_block,
                    column_blocks=columns,
                )
                decisions.append({"run_id": header.run_id, "row_block": row_block, "decision": plan.to_dict()})
        split_rows = [
            item
            for item in decisions
            if item.get("decision", {}).get("decision") == "batch_split_required"
        ]
        estimates = [int(item["decision"]["estimated_peak_bytes"]) for item in decisions if "decision" in item]
        return {
            "shard_id": self.shard_id,
            "run_id": header.run_id,
            "budget_bytes": self.config.memory_budget_bytes,
            "workspace_factor": self.config.workspace_factor,
            "estimated_peak_bytes": max(estimates, default=0),
            "split_count": len(split_rows),
            "split_row_blocks": [item["row_block"] for item in split_rows],
            "decisions": decisions,
        }

