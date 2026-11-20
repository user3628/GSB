from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from .models import PlanRecord, RoundRecord
from .percentiles import compute_percentiles
from .randomness import derive_sample_nonce


OUTCOME_SUCCESS = "SUCCESS"
OUTCOME_HTTP_ERROR = "HTTP_ERROR"
OUTCOME_TIMEOUT = "TIMEOUT"
OUTCOME_TRANSPORT_ERROR = "TRANSPORT_ERROR"


@dataclass(frozen=True)
class Sample:
    sequence: int
    latency_ms: float
    outcome: str
    error_kind: str | None
    occurred_at: str

    def as_row(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "latency_ms": self.latency_ms,
            "outcome": self.outcome,
            "error_kind": self.error_kind,
            "occurred_at": self.occurred_at,
        }


@dataclass(frozen=True)
class SamplingOutput:
    samples: tuple[Sample, ...]
    started_at: str
    finished_at: str
    planned_duration_seconds: int
    observed_duration_ms: float
    scheduled_samples: int
    max_in_flight: int


class DeterministicSampler:
    """Paced, closed-loop sampler with a hard per-plan concurrency bound.

    The plan duration and samples/s/worker define a deterministic sample budget.
    Requests are spread over that duration. The server returns a deterministic
    latency sample derived from the plan seed. Actual wall-clock compatibility is
    retained through a real HTTP request and a real per-request timeout.
    """

    async def sample(self, plan: PlanRecord, round_record: RoundRecord) -> SamplingOutput:
        concurrency = round_record.concurrency
        rate = plan.sampling.requests_per_second_per_worker
        per_worker = max(1, round_record.duration_seconds * rate)
        total = per_worker * concurrency
        semaphore = asyncio.Semaphore(concurrency)
        timeout_seconds = plan.request_timeout_ms / 1000.0
        state = {"current": 0, "max": 0}
        wall_start = time.perf_counter()
        started_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")

        limits = httpx.Limits(
            max_connections=concurrency, max_keepalive_connections=0
        )
        timeout = httpx.Timeout(timeout_seconds)
        async with httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            trust_env=False,
            headers={"Connection": "close"},
        ) as client:
            tasks = [
                asyncio.create_task(
                    self._request_one(
                        client=client,
                        semaphore=semaphore,
                        state=state,
                        plan=plan,
                        round_record=round_record,
                        sequence=sequence,
                        start_delay=(sequence // concurrency) / rate,
                        timeout_seconds=timeout_seconds,
                    )
                )
                for sequence in range(total)
            ]
            samples = list(await asyncio.gather(*tasks))
        observed_ms = (time.perf_counter() - wall_start) * 1000.0
        finished_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        return SamplingOutput(
            samples=tuple(samples),
            started_at=started_at,
            finished_at=finished_at,
            planned_duration_seconds=round_record.duration_seconds,
            observed_duration_ms=round(observed_ms, 3),
            scheduled_samples=total,
            max_in_flight=state["max"],
        )

    async def _request_one(
        self,
        *,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        state: dict[str, int],
        plan: PlanRecord,
        round_record: RoundRecord,
        sequence: int,
        start_delay: float,
        timeout_seconds: float,
    ) -> Sample:
        if start_delay > 0:
            await asyncio.sleep(start_delay)
        async with semaphore:
            state["current"] += 1
            state["max"] = max(state["max"], state["current"])
            wall_start = time.perf_counter()
            response: httpx.Response | None = None
            outcome = OUTCOME_SUCCESS
            error_kind: str | None = None
            latency_ms: float | None = None
            try:
                response = await client.post(
                    plan.target_url,
                    json={
                        "nonce": derive_sample_nonce(
                            plan.seed,
                            round_record.stage_index,
                            round_record.attempt,
                            sequence,
                        ),
                        "profile": plan.profile,
                        "concurrency": round_record.concurrency,
                        "round_id": round_record.id,
                    },
                    timeout=timeout_seconds,
                )
                header_latency = response.headers.get("x-sample-latency-ms")
                if header_latency is not None:
                    latency_ms = float(header_latency)
                if response.status_code >= 400:
                    outcome = OUTCOME_HTTP_ERROR
                    error_kind = f"HTTP_{response.status_code}"
            except httpx.TimeoutException:
                outcome = OUTCOME_TIMEOUT
                error_kind = "request_timeout"
                latency_ms = float(plan.request_timeout_ms)
            except httpx.RequestError as exc:
                outcome = OUTCOME_TRANSPORT_ERROR
                error_kind = exc.__class__.__name__
            finally:
                if response is not None:
                    await response.aclose()
                state["current"] -= 1

            if latency_ms is None:
                latency_ms = (time.perf_counter() - wall_start) * 1000.0
            return Sample(
                sequence=sequence,
                latency_ms=round(max(0.0, latency_ms), 6),
                outcome=outcome,
                error_kind=error_kind,
                occurred_at=datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            )


def aggregate_samples(plan: PlanRecord, output: SamplingOutput) -> dict[str, Any]:
    latencies = [sample.latency_ms for sample in output.samples]
    percentiles = compute_percentiles(
        latencies, minimum_samples=plan.sampling.min_samples_for_percentiles
    )
    sample_size = len(output.samples)
    error_count = sum(
        1 for sample in output.samples if sample.outcome != OUTCOME_SUCCESS
    )
    timeout_count = sum(
        1 for sample in output.samples if sample.outcome == OUTCOME_TIMEOUT
    )
    success_count = sample_size - error_count
    # Throughput is tied to the deterministic sample budget and configured
    # window, so identical fixed samples produce an identical capacity verdict.
    # The actual wall-clock window is retained separately for diagnostics.
    throughput = sample_size / max(output.planned_duration_seconds, 1)
    return {
        "sample_size": sample_size,
        "success_count": success_count,
        "error_count": error_count,
        "timeout_count": timeout_count,
        "throughput_rps": round(throughput, 6),
        "p50_ms": percentiles["p50_ms"],
        "p95_ms": percentiles["p95_ms"],
        "p99_ms": percentiles["p99_ms"],
        "error_rate": round(error_count / sample_size, 9),
        "observed_duration_ms": output.observed_duration_ms,
        "percentile_method": "nearest_rank",
        "scheduled_samples": output.scheduled_samples,
        "max_in_flight": output.max_in_flight,
        "planned_duration_seconds": output.planned_duration_seconds,
    }


def evaluate_round(plan: PlanRecord, aggregate: dict[str, Any]) -> dict[str, Any]:
    thresholds = plan.thresholds
    triggered: list[dict[str, Any]] = []
    if aggregate["p99_ms"] > thresholds.p99_ms:
        triggered.append(
            {
                "threshold": "p99_ms",
                "operator": "<=",
                "limit": thresholds.p99_ms,
                "actual": aggregate["p99_ms"],
            }
        )
    if aggregate["error_rate"] > thresholds.error_rate:
        triggered.append(
            {
                "threshold": "error_rate",
                "operator": "<=",
                "limit": thresholds.error_rate,
                "actual": aggregate["error_rate"],
            }
        )
    if aggregate["throughput_rps"] < thresholds.min_throughput_rps:
        triggered.append(
            {
                "threshold": "min_throughput_rps",
                "operator": ">=",
                "limit": thresholds.min_throughput_rps,
                "actual": aggregate["throughput_rps"],
            }
        )
    passed = not triggered
    measured = {
        "sample_size": aggregate["sample_size"],
        "p50_ms": aggregate["p50_ms"],
        "p95_ms": aggregate["p95_ms"],
        "p99_ms": aggregate["p99_ms"],
        "throughput_rps": aggregate["throughput_rps"],
        "error_rate": aggregate["error_rate"],
        "error_count": aggregate["error_count"],
        "timeout_count": aggregate["timeout_count"],
        "percentile_method": "nearest_rank",
        "observed_duration_ms": aggregate["observed_duration_ms"],
        "planned_duration_seconds": aggregate["planned_duration_seconds"],
        "scheduled_samples": aggregate["scheduled_samples"],
        "max_in_flight": aggregate["max_in_flight"],
    }
    if passed:
        rationale = (
            f"All thresholds passed with {aggregate['sample_size']} samples using "
            "nearest-rank percentiles"
        )
    else:
        names = ", ".join(item["threshold"] for item in triggered)
        rationale = f"Rejected by {names} with {aggregate['sample_size']} samples"
    return {
        "passed": passed,
        "verdict": "PASS" if passed else "REJECT",
        "triggered": triggered,
        "measured": measured,
        "sample_size": aggregate["sample_size"],
        "percentile_method": "nearest_rank",
        "rationale": rationale,
    }

