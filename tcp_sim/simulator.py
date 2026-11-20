from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

from .config import SimulatorConfig
from .errors import BusinessError
from .events import ACK, DUP_ACK, LOSS, SEND, TIMEOUT, TraceEvent
from .state import (
    CLOSED,
    ESTABLISHED,
    FIN_WAIT,
    SYN_SENT,
    TIME_WAIT,
    ConnectionState,
    Segment,
    SimulationState,
)


@dataclass
class EventDecision:
    event_index: int
    ts_ms: int
    conn_id: str
    event_type: str
    payload_bytes: int
    ack_seq: int | None
    state_before: str
    state_after: str
    cwnd_before: int
    cwnd_after: int
    ssthresh_before: int
    ssthresh_after: int
    ack_class: str = "NOT_APPLICABLE"
    rtt_sample_us: int | None = None
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_index": self.event_index,
            "ts_ms": self.ts_ms,
            "conn_id": self.conn_id,
            "event_type": self.event_type,
            "payload_bytes": self.payload_bytes,
            "ack_seq": self.ack_seq,
            "state_before": self.state_before,
            "state_after": self.state_after,
            "cwnd_before": self.cwnd_before,
            "cwnd_after": self.cwnd_after,
            "ssthresh_before": self.ssthresh_before,
            "ssthresh_after": self.ssthresh_after,
            "ack_class": self.ack_class,
            "rtt_sample_us": self.rtt_sample_us,
            "reasons": list(self.reasons),
        }


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


class Simulator:
    """Pure, integer-only event simulator.

    All congestion-window and RTT arithmetic uses integer bytes/microseconds.  The
    only non-integer values in the service are wall-clock latency measurements and
    they never enter a checkpoint or a snapshot.
    """

    def __init__(self, config: SimulatorConfig, state: SimulationState):
        self.config = config
        self.state = state

    def apply(self, event: TraceEvent) -> EventDecision:
        existing = self.state.connections.get(event.conn_id)
        candidate = existing or ConnectionState.fresh(event.conn_id, self.config)
        state_before = candidate.state
        state_after = self._next_state(candidate, event)
        if existing is None:
            self.state.connections[event.conn_id] = candidate

        conn = candidate
        cwnd_before = conn.cwnd_bytes
        ssthresh_before = conn.ssthresh_bytes
        decision = EventDecision(
            event_index=event.index,
            ts_ms=event.ts_ms,
            conn_id=event.conn_id,
            event_type=event.event_type,
            payload_bytes=event.payload_bytes,
            ack_seq=event.ack_seq,
            state_before=state_before,
            state_after=state_after,
            cwnd_before=cwnd_before,
            cwnd_after=cwnd_before,
            ssthresh_before=ssthresh_before,
            ssthresh_after=ssthresh_before,
        )

        if state_after != state_before:
            decision.reasons.append(f"STATE_TRANSITION({state_before}->{state_after})")
        conn.state = state_after

        if event.event_type == SEND:
            self._handle_send(conn, event, decision)
        elif event.event_type in {ACK, DUP_ACK}:
            self._handle_ack(conn, event, decision, state_before)
        elif event.event_type == LOSS:
            conn.counters["loss_events"] += 1
            decision.reasons.append("PACKET_LOSS_RECORDED_NO_WINDOW_GROWTH")
        elif event.event_type == TIMEOUT:
            self._handle_timeout(conn, event, decision, state_before)
        else:  # parse_events makes this unreachable, kept as a defensive invariant.
            raise BusinessError(
                "E_TRACE_INVALID",
                context={"event_index": event.index, "event_type": event.event_type},
            )

        conn.last_event_index = event.index
        conn.last_event_ts_ms = event.ts_ms
        self.state.last_event_index = event.index
        self.state.last_event_ts_ms = event.ts_ms
        self.state.committed_event_count = max(
            self.state.committed_event_count,
            event.index + 1,
        )

        decision.cwnd_after = conn.cwnd_bytes
        decision.ssthresh_after = conn.ssthresh_bytes
        return decision

    def _next_state(self, conn: ConnectionState, event: TraceEvent) -> str:
        current = conn.state
        event_type = event.event_type

        if current == CLOSED:
            if event_type == SEND:
                return SYN_SENT
        elif current == SYN_SENT:
            if event_type == SEND:
                return SYN_SENT
            if event_type == ACK:
                return ESTABLISHED
            if event_type in {LOSS, TIMEOUT}:
                return SYN_SENT
        elif current == ESTABLISHED:
            if event_type == SEND:
                return FIN_WAIT if event.payload_bytes == 0 else ESTABLISHED
            if event_type in {ACK, DUP_ACK, LOSS, TIMEOUT}:
                return ESTABLISHED
        elif current == FIN_WAIT:
            if event_type == ACK:
                return TIME_WAIT
            if event_type in {DUP_ACK, LOSS, TIMEOUT}:
                return FIN_WAIT
        elif current == TIME_WAIT:
            if event_type in {ACK, DUP_ACK, LOSS, TIMEOUT}:
                return TIME_WAIT

        raise BusinessError(
            "E_INVALID_TRANSITION",
            context={
                "event_index": event.index,
                "conn_id": event.conn_id,
                "from_state": current,
                "event_type": event.event_type,
                "payload_bytes": event.payload_bytes,
                "rule": "state_machine_transition_not_declared",
            },
        )

    def _handle_send(
        self,
        conn: ConnectionState,
        event: TraceEvent,
        decision: EventDecision,
    ) -> None:
        conn.counters["send_events"] += 1
        now_us = event.ts_ms * 1_000
        if event.payload_bytes > 0:
            start_seq = conn.highest_sent_seq
            conn.highest_sent_seq += event.payload_bytes
            conn.outstanding.append(
                Segment(
                    start_seq=start_seq,
                    end_seq=conn.highest_sent_seq,
                    sent_at_us=now_us,
                )
            )
            decision.reasons.append(
                f"DATA_SEGMENT_ENQUEUED(seq={start_seq}..{conn.highest_sent_seq})"
            )
        else:
            decision.reasons.append("ZERO_LENGTH_SEND_CONTROL_EVENT")
        conn.last_send_at_us = now_us
        conn.last_send_rtt_valid = True

    def _resolve_ack_seq(self, conn: ConnectionState, event: TraceEvent) -> int:
        if event.ack_seq is not None:
            return event.ack_seq
        if event.event_type == DUP_ACK:
            return conn.highest_ack_seq
        return conn.highest_ack_seq + event.payload_bytes

    def _handle_ack(
        self,
        conn: ConnectionState,
        event: TraceEvent,
        decision: EventDecision,
        state_before: str,
    ) -> None:
        conn.counters["ack_events"] += 1
        if event.event_type == DUP_ACK:
            conn.counters["dup_ack_events"] += 1

        acked_seq = self._resolve_ack_seq(conn, event)
        old_highest = conn.highest_ack_seq

        if state_before == SYN_SENT:
            ack_class = "NEW"
            conn.highest_ack_seq = max(conn.highest_ack_seq, acked_seq)
            conn.dup_ack_count = 0
            conn.counters["new_acks"] += 1
            decision.reasons.append("HANDSHAKE_ACK_ESTABLISHES_CONNECTION")
        elif acked_seq < old_highest:
            ack_class = "OUT_OF_ORDER"
            conn.counters["out_of_order_acks"] += 1
            decision.reasons.append(
                f"OUT_OF_ORDER_ACK_IGNORED(ack_seq={acked_seq}<highest_ack_seq={old_highest})"
            )
            decision.ack_class = ack_class
            return
        elif acked_seq == old_highest:
            ack_class = "DUPLICATE"
            conn.dup_ack_count += 1
            conn.counters["duplicate_acks"] += 1
            decision.reasons.append(
                f"DUPLICATE_ACK_COUNTED(ack_seq={acked_seq},count={conn.dup_ack_count})"
            )
        else:
            ack_class = "NEW"
            conn.highest_ack_seq = acked_seq
            conn.dup_ack_count = 0
            conn.counters["new_acks"] += 1
            decision.reasons.append(
                f"NEW_ACK_ADVANCED(highest_ack_seq={old_highest}->{acked_seq})"
            )

        decision.ack_class = ack_class
        active_congestion_control = conn.state == ESTABLISHED

        if ack_class == "NEW":
            exited_recovery = False
            if conn.fast_recovery and active_congestion_control:
                conn.fast_recovery = False
                conn.cwnd_bytes = max(conn.ssthresh_bytes, self.config.min_cwnd_bytes)
                exited_recovery = True
                decision.reasons.append(
                    f"FAST_RECOVERY_EXIT_ON_NEW_ACK(cwnd={conn.cwnd_bytes})"
                )

            sample = self._update_rtt_estimate(conn, event, acked_seq, state_before)
            if sample is not None:
                decision.rtt_sample_us = sample
                decision.reasons.append(f"RTT_SAMPLE_UPDATED(sample_us={sample})")

            if active_congestion_control and not exited_recovery:
                if event.payload_bytes > 0:
                    now_us = event.ts_ms * 1_000
                    if now_us < conn.growth_blocked_until_us:
                        conn.counters["timeout_growth_suppressed"] += 1
                        decision.reasons.append(
                            "TIMEOUT_GROWTH_LAG_SUPPRESSED("
                            f"now_us={now_us}<blocked_until_us={conn.growth_blocked_until_us})"
                        )
                    else:
                        self._grow_cwnd(conn, event.payload_bytes, decision)
                else:
                    decision.reasons.append("NEW_ACK_WITH_ZERO_PAYLOAD_NO_GROWTH")

        elif ack_class == "DUPLICATE" and active_congestion_control:
            now_us = event.ts_ms * 1_000
            if conn.dup_ack_count == 3 and not conn.fast_recovery:
                old_ssthresh = conn.ssthresh_bytes
                old_cwnd = conn.cwnd_bytes
                conn.ssthresh_bytes = max(
                    conn.cwnd_bytes // 2,
                    self.config.min_cwnd_bytes,
                )
                recovery_cwnd = max(
                    conn.ssthresh_bytes + 3 * self.config.mss_bytes,
                    self.config.min_cwnd_bytes,
                )
                if now_us < conn.growth_blocked_until_us:
                    conn.cwnd_bytes = max(
                        self.config.min_cwnd_bytes,
                        min(old_cwnd, recovery_cwnd),
                    )
                    conn.counters["timeout_growth_suppressed"] += 1
                    decision.reasons.append(
                        "FAST_RETRANSMIT_WINDOW_GROWTH_SUPPRESSED_BY_TIMEOUT_LAG("
                        f"now_us={now_us}<blocked_until_us={conn.growth_blocked_until_us})"
                    )
                else:
                    conn.cwnd_bytes = recovery_cwnd
                conn.fast_recovery = True
                conn.counters["fast_retransmits"] += 1
                self._mark_earliest_outstanding_retransmitted(conn)
                decision.reasons.append(
                    "THREE_DUPLICATE_ACKS_FAST_RETRANSMIT("
                    f"cwnd={old_cwnd}->{conn.cwnd_bytes},"
                    f"ssthresh={old_ssthresh}->{conn.ssthresh_bytes})"
                )
            elif conn.dup_ack_count > 3 and conn.fast_recovery:
                old_cwnd = conn.cwnd_bytes
                if now_us < conn.growth_blocked_until_us:
                    conn.counters["timeout_growth_suppressed"] += 1
                    decision.reasons.append(
                        "FAST_RECOVERY_INFLATE_SUPPRESSED_BY_TIMEOUT_LAG("
                        f"now_us={now_us}<blocked_until_us={conn.growth_blocked_until_us})"
                    )
                else:
                    conn.cwnd_bytes = max(
                        conn.cwnd_bytes + self.config.mss_bytes,
                        self.config.min_cwnd_bytes,
                    )
                    conn.counters["fast_recovery_inflates"] += 1
                    decision.reasons.append(
                        "FAST_RECOVERY_DUPLICATE_ACK_INFLATE("
                        f"cwnd={old_cwnd}->{conn.cwnd_bytes})"
                    )
            else:
                decision.reasons.append("DUPLICATE_ACK_BELOW_FAST_RETRANSMIT_THRESHOLD")
        elif ack_class == "DUPLICATE":
            decision.reasons.append("DUPLICATE_ACK_RECORDED_OUTSIDE_ESTABLISHED")

        conn.cwnd_bytes = max(conn.cwnd_bytes, self.config.min_cwnd_bytes)

    def _grow_cwnd(
        self,
        conn: ConnectionState,
        confirmed_bytes: int,
        decision: EventDecision,
    ) -> None:
        old_cwnd = conn.cwnd_bytes
        if conn.cwnd_bytes < conn.ssthresh_bytes:
            conn.cwnd_bytes += confirmed_bytes
            conn.counters["slow_start_acks"] += 1
            decision.reasons.append(
                "SLOW_START_ACK("
                f"cwnd={old_cwnd}->{conn.cwnd_bytes},"
                f"ssthresh={conn.ssthresh_bytes},acked_bytes={confirmed_bytes})"
            )
            if conn.cwnd_bytes >= conn.ssthresh_bytes:
                decision.reasons.append("SLOW_START_THRESHOLD_REACHED")
        else:
            numerator = (
                self.config.mss_bytes
                * confirmed_bytes
                * self.config.base_rtt_us
            )
            denominator = max(1, conn.cwnd_bytes * max(1, conn.srtt_us))
            delta = max(1, numerator // denominator)
            conn.cwnd_bytes += delta
            conn.counters["congestion_avoidance_acks"] += 1
            decision.reasons.append(
                "CONGESTION_AVOIDANCE_RTT_PROPORTIONAL("
                f"cwnd={old_cwnd}->{conn.cwnd_bytes},delta={delta},"
                f"base_rtt_us={self.config.base_rtt_us},srtt_us={conn.srtt_us},"
                f"acked_bytes={confirmed_bytes})"
            )
        conn.cwnd_bytes = max(conn.cwnd_bytes, self.config.min_cwnd_bytes)

    def _update_rtt_estimate(
        self,
        conn: ConnectionState,
        event: TraceEvent,
        acked_seq: int,
        state_before: str,
    ) -> int | None:
        now_us = event.ts_ms * 1_000
        newly_acked: list[Segment] = []
        covered = [segment for segment in conn.outstanding if segment.end_seq <= acked_seq]
        if covered:
            covered_ids = {id(segment) for segment in covered}
            conn.outstanding = [
                segment for segment in conn.outstanding if id(segment) not in covered_ids
            ]
            newly_acked = covered

        sample: int | None = None
        if newly_acked:
            eligible = [segment for segment in newly_acked if not segment.retransmitted]
            if eligible:
                sent_at_us = min(segment.sent_at_us for segment in eligible)
                sample = max(1, now_us - sent_at_us)
        elif (
            state_before == SYN_SENT
            and conn.last_send_at_us is not None
            and conn.last_send_rtt_valid
        ):
            sample = max(1, now_us - conn.last_send_at_us)

        conn.rto_backoff = 1
        if sample is not None:
            if conn.srtt_us == 0:
                conn.srtt_us = sample
                conn.rttvar_us = max(1, sample // 2)
            else:
                error = abs(conn.srtt_us - sample)
                conn.rttvar_us = (3 * conn.rttvar_us + error) // 4
                conn.srtt_us = (7 * conn.srtt_us + sample) // 8
            conn.counters["rtt_updates"] += 1
        self._recompute_rto(conn)
        return sample

    def _recompute_rto(self, conn: ConnectionState) -> None:
        if conn.srtt_us <= 0:
            estimated = self.config.min_rto_us
        else:
            estimated = conn.srtt_us + max(1, 4 * conn.rttvar_us)
        conn.rto_us = _clamp(
            estimated * conn.rto_backoff,
            self.config.min_rto_us,
            self.config.max_rto_us,
        )

    def _handle_timeout(
        self,
        conn: ConnectionState,
        event: TraceEvent,
        decision: EventDecision,
        state_before: str,
    ) -> None:
        conn.counters["timeout_events"] += 1
        if state_before not in {SYN_SENT, ESTABLISHED}:
            decision.reasons.append("TIMEOUT_RECORDED_OUTSIDE_ACTIVE_CONGESTION_STATE")
            return

        old_cwnd = conn.cwnd_bytes
        old_ssthresh = conn.ssthresh_bytes
        conn.ssthresh_bytes = max(conn.cwnd_bytes // 2, self.config.min_cwnd_bytes)
        conn.cwnd_bytes = max(self.config.initial_cwnd_bytes, self.config.min_cwnd_bytes)
        conn.fast_recovery = False
        conn.dup_ack_count = 0
        conn.rto_backoff = min(64, max(1, conn.rto_backoff * 2))
        self._recompute_rto(conn)
        lag_rtt_us = conn.srtt_us if conn.srtt_us > 0 else self.config.base_rtt_us
        timeout_us = event.ts_ms * 1_000
        conn.growth_blocked_until_us = timeout_us + max(1, lag_rtt_us)
        conn.last_timeout_at_us = timeout_us
        conn.last_send_rtt_valid = False
        for segment in conn.outstanding:
            segment.retransmitted = True
        decision.reasons.append(
            "TIMEOUT_RETRANSMIT_HALVE_SSHTHRESH_RESET_CWND("
            f"cwnd={old_cwnd}->{conn.cwnd_bytes},"
            f"ssthresh={old_ssthresh}->{conn.ssthresh_bytes},"
            f"growth_blocked_until_us={conn.growth_blocked_until_us},"
            f"lag_rtt_us={lag_rtt_us},rto_us={conn.rto_us})"
        )

    @staticmethod
    def _mark_earliest_outstanding_retransmitted(conn: ConnectionState) -> None:
        for segment in conn.outstanding:
            if not segment.retransmitted:
                segment.retransmitted = True
                return


def clone_state(state: SimulationState) -> SimulationState:
    return copy.deepcopy(state)


def build_snapshot(state: SimulationState, decision: EventDecision) -> dict[str, Any]:
    config = state.state_config()
    return {
        "schema_version": 1,
        "event_index": decision.event_index,
        "sim_time_ms": decision.ts_ms,
        "decision": decision.to_dict(),
        "connections": {
            conn_id: state.connections[conn_id].public_dict(config)
            for conn_id in sorted(state.connections)
        },
    }
