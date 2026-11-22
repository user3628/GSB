from __future__ import annotations

import threading
import time
from typing import Any

import httpx
from pydantic import BaseModel, Field

from .aggregation import aggregate_blocks, validate_and_collect_sequences
from .config import LabConfig, RoundPolicy
from .errors import AppError, ErrorCode
from .injection import validate_injection_against_config
from .models import (
    GradientBlock,
    InjectionSpec,
    classify_code,
    is_retryable,
    sha256_json,
)
from .samples import generate_round_blocks
from .state_machine import BatchState, StateTracker
from .store import Store


class BatchRequest(BaseModel):
    batch_id: str = Field(min_length=3, max_length=120, pattern=r"^[A-Za-z0-9_.:-]+$")
    round: int = Field(ge=0, le=1_000_000)
    injection: Any = Field(default_factory=dict)


class BatchOrchestrator:
    def __init__(self, config: LabConfig, store: Store) -> None:
        self.config = config
        self.store = store
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._injector_url = f"http://127.0.0.1:{config.injector_port}"

    def _batch_lock(self, batch_id: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(batch_id, threading.Lock())

    def execute(self, request: BatchRequest) -> dict[str, Any]:
        try:
            spec = InjectionSpec.from_payload(request.injection)
            validate_injection_against_config(self.config, spec, request.round)
        except AppError:
            raise
        except Exception as exc:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "Injection specification could not be validated",
                {"reason": str(exc)},
            ) from exc

        normalized_request = {
            "batch_id": request.batch_id,
            "round": request.round,
            "injection": spec.to_dict(),
        }
        request_sha = sha256_json(normalized_request)
        with self._batch_lock(request.batch_id):
            existing = self.store.peek_batch(request.batch_id)
            if existing is not None and existing["request_sha256"] != request_sha:
                raise AppError(
                    ErrorCode.BATCH_PAYLOAD_CONFLICT,
                    "This batch_id was already used with a different payload",
                    {
                        "batch_id": request.batch_id,
                        "existing_request_sha256": existing["request_sha256"],
                        "received_request_sha256": request_sha,
                    },
                )
            if existing is not None and bool(existing["is_final"]):
                return self.store.get_final_batch(request.batch_id)
            row = self.store.prepare_batch(
                batch_id=request.batch_id,
                request_sha256=request_sha,
                round_no=request.round,
                request_payload=normalized_request,
            )
            if bool(row["is_final"]):
                return self.store.get_final_batch(request.batch_id)
            if existing is not None and not bool(existing["is_final"]):
                raise AppError(
                    ErrorCode.BATCH_NOT_READY,
                    "An unfinished batch intent exists and is owned by another writer",
                    {"batch_id": request.batch_id, "state": existing["state"]},
                )
            try:
                return self._execute_new_batch(request, spec)
            except AppError as exc:
                committed = self.store.peek_batch(request.batch_id)
                if committed is not None and bool(committed["is_final"]):
                    return self.store.get_final_batch(request.batch_id)
                return self._abort_runtime_error(request.batch_id, exc)

    def _execute_new_batch(
        self, request: BatchRequest, spec: InjectionSpec
    ) -> dict[str, Any]:
        tracker = StateTracker()
        retry_attempts: list[dict[str, Any]] = []
        fault_events: list[dict[str, Any]] = []
        all_blocks = generate_round_blocks(self.config, request.round)
        policy = self.config.policy_for_round(request.round)

        non_target_sequences: dict[str, list[dict[str, Any]]] = {}
        for node in self.config.nodes:
            if node.node_id == spec.target_node:
                continue
            submission = self._post_json(
                "/v1/submit",
                {
                    "node_id": node.node_id,
                    "round": request.round,
                    "block_count": self.config.shards_per_node,
                },
                timeout=self.config.request_timeout_seconds,
            )
            observation = submission["observation"]
            if observation["code"] != ErrorCode.OK.value:
                return self._abort_before_attempts(
                    request,
                    tracker,
                    retry_attempts,
                    fault_events,
                    AppError(
                        ErrorCode.INJECTOR_UNAVAILABLE,
                        "A non-target node submission failed",
                        {"node_id": node.node_id, "observation": observation},
                    ),
                )
            non_target_sequences[node.node_id] = observation["submission_sequence"]

        target_sequence: list[dict[str, Any]] | None = None
        attempt_no = 1
        last_code = ErrorCode.OK
        exhausted = False
        backoff_before_ms = 0.0

        while True:
            spec_timeout = float(
                spec.params.get("timeout_seconds", self.config.socket_timeout_seconds)
            )
            response = self._post_json(
                "/v1/inject",
                {
                    "spec": spec.to_dict(),
                    "attempt_no": attempt_no,
                    "block_count": self.config.shards_per_node,
                },
                timeout=max(self.config.request_timeout_seconds, spec_timeout + 2.0),
            )
            observation = response["observation"]
            code = ErrorCode(observation["code"])
            injected = bool(response.get("injected", False))
            last_code = code
            retry_attempts.append(
                {
                    "attempt_no": attempt_no,
                    "injected": injected,
                    "code": code.value,
                    "classification": observation["classification"],
                    "retryable": bool(observation["retryable"]),
                    "elapsed_ms": float(observation["elapsed_ms"]),
                    "backoff_ms": backoff_before_ms,
                    "observation": self._observation_record(observation),
                }
            )

            if code is ErrorCode.OK:
                target_sequence = observation.get("submission_sequence", [])
                break

            if spec.injection_type.value in {
                "duplicate_submission",
                "order_mismatch",
            }:
                ledger_code = self._detect_sequence_fault(
                    [
                        *[
                            block
                            for sequence in non_target_sequences.values()
                            for block in sequence
                        ],
                        *observation.get("submission_sequence", []),
                    ],
                    all_blocks,
                    request.round,
                )
                observation.setdefault("details", {})["aggregator_detection"] = (
                    ledger_code.value if ledger_code else "NOT_DETECTED"
                )
                if ledger_code is None:
                    last_code = ErrorCode.BLOCK_VALIDATION_FAILED
                    code = last_code
                elif ledger_code is not code:
                    last_code = ledger_code
                    code = ledger_code

            fault_events.append(
                self._fault_event(
                    request.batch_id,
                    attempt_no,
                    spec,
                    code,
                    observation,
                )
            )

            if is_retryable(code) and attempt_no <= spec.retry.max_retries:
                if tracker.current is BatchState.INJECTING:
                    tracker.transition(
                        BatchState.RETRYING,
                        reason="Retryable communication failure; declaring retry sequence",
                        code=code,
                    )
                backoff_seconds = spec.retry.backoff_for_retry(attempt_no)
                backoff_before_ms = backoff_seconds * 1000.0
                if backoff_seconds > 0:
                    time.sleep(backoff_seconds)
                attempt_no += 1
                continue

            if is_retryable(code):
                exhausted = True
            tracker.transition(
                BatchState.DEGRADED,
                reason=(
                    "Retry budget exhausted"
                    if exhausted
                    else "Fatal communication failure requires degraded handling"
                ),
                code=code,
            )
            break

        if target_sequence is not None and not exhausted:
            try:
                target_sequence = self._canonical_target_sequence(
                    target_sequence, all_blocks, request.round
                )
                sequences = [*non_target_sequences.values(), target_sequence]
                included_blocks = validate_and_collect_sequences(
                    [block for sequence in sequences for block in sequence],
                    all_blocks,
                    self.config,
                    request.round,
                )
                aggregation = aggregate_blocks(
                    included_blocks, all_blocks, omitted_nodes=[]
                ).to_dict()
                aggregation["fault_ids"] = [
                    event["fault_id"] for event in fault_events
                ]
                if tracker.current is BatchState.RETRYING:
                    tracker.transition(
                        BatchState.VERIFIED,
                        reason="Retry succeeded and the weighted aggregate was verified",
                    )
                else:
                    tracker.transition(
                        BatchState.VERIFIED,
                        reason="Weighted aggregate was verified",
                    )
                final_code = ErrorCode.OK
            except AppError as exc:
                ledger_event_code = exc.code
                if not any(event["attempt_no"] == attempt_no for event in fault_events):
                    fault_events.append(
                        self._fault_event(
                            request.batch_id,
                            attempt_no,
                            spec,
                            ledger_event_code,
                            {"code": ledger_event_code.value, "details": exc.details},
                        )
                    )
                if tracker.current is BatchState.INJECTING:
                    tracker.transition(
                        BatchState.DEGRADED,
                        reason="Aggregator detected a fatal sequence/block fault",
                        code=ledger_event_code,
                    )
                elif tracker.current is BatchState.RETRYING:
                    tracker.transition(
                        BatchState.DEGRADED,
                        reason="Aggregator detected a fatal sequence/block fault",
                        code=ledger_event_code,
                    )
                last_code = ledger_event_code
                return self._finish_degraded(
                    request,
                    spec,
                    policy,
                    tracker,
                    retry_attempts,
                    fault_events,
                    all_blocks,
                    non_target_sequences,
                    last_code=last_code,
                    exhausted=False,
                )
        else:
            return self._finish_degraded(
                request,
                spec,
                policy,
                tracker,
                retry_attempts,
                fault_events,
                all_blocks,
                non_target_sequences,
                last_code=last_code,
                exhausted=exhausted,
            )

        self.store.commit_final(
            batch_id=request.batch_id,
            final_state=tracker.current,
            final_code=final_code,
            transitions=[transition.to_dict() for transition in tracker.history],
            fault_events=fault_events,
            retry_attempts=retry_attempts,
            aggregation=aggregation,
        )
        return self.store.get_final_batch(request.batch_id)

    def _finish_degraded(
        self,
        request: BatchRequest,
        spec: InjectionSpec,
        policy: RoundPolicy,
        tracker: StateTracker,
        retry_attempts: list[dict[str, Any]],
        fault_events: list[dict[str, Any]],
        all_blocks: list[GradientBlock],
        non_target_sequences: dict[str, list[dict[str, Any]]],
        *,
        last_code: ErrorCode,
        exhausted: bool,
    ) -> dict[str, Any]:
        omitted_nodes = [spec.target_node]
        remaining_node_count = len(self.config.nodes) - 1
        can_shorten = (
            policy.on_exhaustion == "shorten"
            and remaining_node_count >= policy.min_participants
        )
        if can_shorten:
            try:
                sequences = [
                    block
                    for node_id, sequence in non_target_sequences.items()
                    if node_id not in omitted_nodes
                    for block in sequence
                ]
                included_blocks = validate_and_collect_sequences(
                    sequences, all_blocks, self.config, request.round
                )
                aggregation = aggregate_blocks(
                    included_blocks, all_blocks, omitted_nodes=omitted_nodes
                ).to_dict()
                aggregation["fault_ids"] = [
                    event["fault_id"] for event in fault_events
                ]
                tracker.transition(
                    BatchState.VERIFIED,
                    reason=(
                        "Participant set was shortened and the compensated aggregate passed"
                    ),
                    code=ErrorCode.DEGRADED_AGGREGATION,
                )
                self.store.commit_final(
                    batch_id=request.batch_id,
                    final_state=tracker.current,
                    final_code=ErrorCode.DEGRADED_AGGREGATION,
                    transitions=[transition.to_dict() for transition in tracker.history],
                    fault_events=fault_events,
                    retry_attempts=retry_attempts,
                    aggregation=aggregation,
                )
                return self.store.get_final_batch(request.batch_id)
            except AppError as exc:
                last_code = exc.code

        tracker.transition(
            BatchState.ABORTED,
            reason="The configured participation policy aborts this degraded batch",
            code=last_code,
        )
        final_code = (
            ErrorCode.RETRY_EXHAUSTED if exhausted else last_code
        )
        if final_code is ErrorCode.OK:
            final_code = ErrorCode.AGGREGATION_ABORTED
        aggregation = {
            "status": "ABORTED",
            "degraded": False,
            "participant_nodes": [],
            "participant_blocks": 0,
            "omitted_nodes": omitted_nodes,
            "sample_weights": {},
            "tensor": None,
            "checksum": None,
            "error_bounds": None,
            "fault_ids": [event["fault_id"] for event in fault_events],
            "reason": "No aggregate was emitted because the participant policy chose abort",
        }
        self.store.commit_final(
            batch_id=request.batch_id,
            final_state=tracker.current,
            final_code=final_code,
            transitions=[transition.to_dict() for transition in tracker.history],
            fault_events=fault_events,
            retry_attempts=retry_attempts,
            aggregation=aggregation,
        )
        return self.store.get_final_batch(request.batch_id)

    def _abort_runtime_error(self, batch_id: str, error: AppError) -> dict[str, Any]:
        tracker = StateTracker()
        tracker.transition(
            BatchState.DEGRADED,
            reason="A handled orchestration/runtime error occurred after intent commit",
            code=error.code,
        )
        tracker.transition(
            BatchState.ABORTED,
            reason="Batch aborted without fabricating in-flight observations",
            code=error.code,
        )
        aggregation = {
            "status": "ABORTED",
            "degraded": False,
            "participant_nodes": [],
            "participant_blocks": 0,
            "omitted_nodes": [],
            "sample_weights": {},
            "tensor": None,
            "checksum": None,
            "error_bounds": None,
            "reason": error.message,
            "fault_ids": [],
        }
        self.store.commit_final(
            batch_id=batch_id,
            final_state=tracker.current,
            final_code=error.code,
            transitions=[transition.to_dict() for transition in tracker.history],
            fault_events=[],
            retry_attempts=[],
            aggregation=aggregation,
        )
        return self.store.get_final_batch(batch_id)

    def _abort_before_attempts(
        self,
        request: BatchRequest,
        tracker: StateTracker,
        retry_attempts: list[dict[str, Any]],
        fault_events: list[dict[str, Any]],
        error: AppError,
    ) -> dict[str, Any]:
        tracker.transition(
            BatchState.DEGRADED,
            reason="A prerequisite normal submission failed",
            code=error.code,
        )
        tracker.transition(
            BatchState.ABORTED,
            reason="Batch aborted before the target injection sequence",
            code=error.code,
        )
        aggregation = {
            "status": "ABORTED",
            "degraded": False,
            "participant_nodes": [],
            "participant_blocks": 0,
            "omitted_nodes": [],
            "sample_weights": {},
            "tensor": None,
            "checksum": None,
            "error_bounds": None,
            "reason": error.message,
            "fault_ids": [],
        }
        self.store.commit_final(
            batch_id=request.batch_id,
            final_state=tracker.current,
            final_code=error.code,
            transitions=[transition.to_dict() for transition in tracker.history],
            fault_events=fault_events,
            retry_attempts=retry_attempts,
            aggregation=aggregation,
        )
        return self.store.get_final_batch(request.batch_id)

    def _detect_sequence_fault(
        self,
        sequences: list[dict[str, Any]],
        all_blocks: list[GradientBlock],
        round_no: int,
    ) -> ErrorCode | None:
        try:
            validate_and_collect_sequences(sequences, all_blocks, self.config, round_no)
        except AppError as exc:
            if exc.code in {
                ErrorCode.FAULT_DUPLICATE_SUBMISSION,
                ErrorCode.FAULT_ORDER_MISMATCH,
                ErrorCode.BLOCK_VALIDATION_FAILED,
            }:
                return exc.code
            raise
        return None

    def _canonical_target_sequence(
        self,
        sequence: list[dict[str, Any]],
        all_blocks: list[GradientBlock],
        round_no: int,
    ) -> list[dict[str, Any]]:
        validate_and_collect_sequences(sequence, all_blocks, self.config, round_no)
        return sequence

    def _fault_event(
        self,
        batch_id: str,
        attempt_no: int,
        spec: InjectionSpec,
        code: ErrorCode,
        observation: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "fault_id": f"fault-{batch_id}-{attempt_no}",
            "attempt_no": attempt_no,
            "injection_type": spec.injection_type.value,
            "target_node": spec.target_node,
            "target_shard": spec.target_shard,
            "injection_round": spec.round,
            "classification": classify_code(code).value,
            "code": code.value,
            "retryable": code in {
                ErrorCode.FAULT_CONNECTION_REFUSED,
                ErrorCode.FAULT_RESPONSE_TIMEOUT,
            },
            "spec": spec.to_dict(),
            "observation": self._observation_record(observation),
        }

    @staticmethod
    def _observation_record(observation: dict[str, Any]) -> dict[str, Any]:
        record = dict(observation)
        sequence = record.get("submission_sequence")
        if isinstance(sequence, list):
            record["submission_sequence"] = [
                {key: value for key, value in block.items() if key != "tensor"}
                if isinstance(block, dict)
                else block
                for block in sequence
            ]
        return record

    def _post_json(self, path: str, payload: dict[str, Any], *, timeout: float) -> dict[str, Any]:
        try:
            with httpx.Client(trust_env=False, timeout=timeout) as client:
                response = client.post(f"{self._injector_url}{path}", json=payload)
            body = response.json()
            if not response.is_success or not body.get("ok", False):
                error = body.get("error", {}) if isinstance(body, dict) else {}
                code = error.get("code")
                raise AppError(
                    ErrorCode(code) if code in set(ErrorCode) else ErrorCode.INJECTOR_UNAVAILABLE,
                    error.get("message", "Injector rejected the request"),
                    {"injector_status": response.status_code, **error.get("details", {})},
                )
            return body["data"]
        except AppError:
            raise
        except Exception as exc:
            raise AppError(
                ErrorCode.INJECTOR_UNAVAILABLE,
                "Injector endpoint is unavailable",
                {"path": path, "reason": str(exc)},
            ) from exc

