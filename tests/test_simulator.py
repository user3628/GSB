from __future__ import annotations

from tcp_sim.config import SimulatorConfig
from tcp_sim.events import assign_global_indices, parse_events
from tcp_sim.simulator import Simulator
from tcp_sim.state import ESTABLISHED, FIN_WAIT, SYN_SENT, TIME_WAIT, SimulationState


def simulate_fixed_trace(trace):
    config = SimulatorConfig.from_dict(trace["config"])
    events = assign_global_indices(
        parse_events(trace["events"], max_events=config.max_events_per_batch),
        0,
    )
    state = SimulationState.empty(config)
    simulator = Simulator(config, state)
    decisions = [simulator.apply(event) for event in events]
    return config, state, decisions


def test_fixed_trace_congestion_and_state_invariants(trace):
    config, state, decisions = simulate_fixed_trace(trace)
    conn = state.connections["c1"]

    assert [decision.state_after for decision in decisions if decision.event_index == 0] == [SYN_SENT]
    assert any(decision.state_after == ESTABLISHED for decision in decisions)
    assert any(decision.state_after == FIN_WAIT for decision in decisions)
    assert conn.state == TIME_WAIT
    assert conn.counters["slow_start_acks"] >= 1
    assert conn.counters["congestion_avoidance_acks"] >= 1
    assert conn.counters["fast_retransmits"] == 1
    assert conn.counters["timeout_events"] == 1
    assert conn.counters["out_of_order_acks"] == 2
    assert conn.counters["timeout_growth_suppressed"] == 1
    assert conn.counters["rtt_updates"] == 4
    assert all(decision.cwnd_after >= config.min_cwnd_bytes for decision in decisions)
    assert config.min_rto_us <= conn.rto_us <= config.max_rto_us
    assert all(isinstance(value, int) for value in (conn.cwnd_bytes, conn.srtt_us, conn.rto_us))


def test_out_of_order_ack_not_counted_and_duplicate_ack_no_rtt_update(trace):
    _, state, decisions = simulate_fixed_trace(trace)
    by_index = {decision.event_index: decision for decision in decisions}

    # 14 is out-of-order, 15/16 are duplicate ACKs, 17 is the third duplicate.
    assert by_index[14].ack_class == "OUT_OF_ORDER"
    assert by_index[14].rtt_sample_us is None
    assert by_index[15].ack_class == "DUPLICATE"
    assert by_index[16].ack_class == "DUPLICATE"
    assert by_index[15].rtt_sample_us is None
    assert by_index[16].rtt_sample_us is None
    assert by_index[17].ack_class == "DUPLICATE"
    assert by_index[17].cwnd_after < by_index[17].cwnd_before
    assert "THREE_DUPLICATE_ACKS_FAST_RETRANSMIT" in by_index[17].reasons[-1]
    assert state.connections["c1"].counters["fast_retransmits"] == 1


def test_timeout_halves_threshold_resets_window_and_delays_growth(trace):
    config, _, decisions = simulate_fixed_trace(trace)
    timeout = next(decision for decision in decisions if decision.event_index == 19)
    suppressed = next(decision for decision in decisions if decision.event_index == 20)
    recovered = next(decision for decision in decisions if decision.event_index == 22)

    assert timeout.cwnd_before == 7587
    assert timeout.cwnd_after == config.initial_cwnd_bytes
    assert timeout.ssthresh_after == max(timeout.cwnd_before // 2, config.min_cwnd_bytes)
    assert suppressed.cwnd_after == suppressed.cwnd_before
    assert any(reason.startswith("TIMEOUT_GROWTH_LAG_SUPPRESSED") for reason in suppressed.reasons)
    assert recovered.cwnd_after > recovered.cwnd_before
    assert any(reason.startswith("CONGESTION_AVOIDANCE_RTT_PROPORTIONAL") for reason in recovered.reasons)


def test_fast_recovery_cannot_grow_window_inside_timeout_lag():
    config = SimulatorConfig()
    raw_events = [
        {"ts_ms": 0, "conn_id": "c1", "event_type": "send", "payload_bytes": 0},
        {"ts_ms": 10, "conn_id": "c1", "event_type": "ack", "payload_bytes": 0, "ack_seq": 0},
        {"ts_ms": 20, "conn_id": "c1", "event_type": "send", "payload_bytes": 1000},
        {"ts_ms": 60, "conn_id": "c1", "event_type": "ack", "payload_bytes": 1000, "ack_seq": 1000},
        {"ts_ms": 61, "conn_id": "c1", "event_type": "send", "payload_bytes": 1000},
        {"ts_ms": 200, "conn_id": "c1", "event_type": "timeout", "payload_bytes": 0},
        {"ts_ms": 201, "conn_id": "c1", "event_type": "dup_ack", "payload_bytes": 0, "ack_seq": 1000},
        {"ts_ms": 202, "conn_id": "c1", "event_type": "dup_ack", "payload_bytes": 0, "ack_seq": 1000},
        {"ts_ms": 203, "conn_id": "c1", "event_type": "dup_ack", "payload_bytes": 0, "ack_seq": 1000},
        {"ts_ms": 204, "conn_id": "c1", "event_type": "dup_ack", "payload_bytes": 0, "ack_seq": 1000},
    ]
    events = assign_global_indices(parse_events(raw_events, max_events=config.max_events_per_batch), 0)
    state = SimulationState.empty(config)
    simulator = Simulator(config, state)
    decisions = [simulator.apply(event) for event in events]
    fast_recovery = decisions[8]
    extra_duplicate = decisions[9]
    conn = state.connections["c1"]

    assert fast_recovery.cwnd_after <= fast_recovery.cwnd_before
    assert extra_duplicate.cwnd_after == extra_duplicate.cwnd_before
    assert conn.counters["fast_retransmits"] == 1
    assert conn.counters["timeout_growth_suppressed"] >= 2
    assert any("TIMEOUT_LAG" in reason for reason in fast_recovery.reasons)
    assert any("SUPPRESSED_BY_TIMEOUT_LAG" in reason for reason in extra_duplicate.reasons)


def test_connections_advance_independently_from_one_global_trace():
    config = SimulatorConfig()
    raw_events = [
        {"ts_ms": 0, "conn_id": "a", "event_type": "send", "payload_bytes": 0},
        {"ts_ms": 1, "conn_id": "b", "event_type": "send", "payload_bytes": 0},
        {"ts_ms": 10, "conn_id": "a", "event_type": "ack", "payload_bytes": 0, "ack_seq": 0},
        {"ts_ms": 11, "conn_id": "b", "event_type": "ack", "payload_bytes": 0, "ack_seq": 0},
        {"ts_ms": 20, "conn_id": "a", "event_type": "send", "payload_bytes": 1000},
        {"ts_ms": 21, "conn_id": "b", "event_type": "send", "payload_bytes": 1000},
        {"ts_ms": 60, "conn_id": "a", "event_type": "ack", "payload_bytes": 1000, "ack_seq": 1000},
        {"ts_ms": 61, "conn_id": "b", "event_type": "timeout", "payload_bytes": 0},
    ]
    events = assign_global_indices(parse_events(raw_events, max_events=config.max_events_per_batch), 0)
    state = SimulationState.empty(config)
    simulator = Simulator(config, state)
    for event in events:
        simulator.apply(event)

    assert state.connections["a"].state == ESTABLISHED
    assert state.connections["a"].cwnd_bytes == 5000
    assert state.connections["a"].counters["timeout_events"] == 0
    assert state.connections["b"].state == ESTABLISHED
    assert state.connections["b"].cwnd_bytes == config.initial_cwnd_bytes
    assert state.connections["b"].counters["timeout_events"] == 1
    assert state.connections["a"].last_event_index == 6
    assert state.connections["b"].last_event_index == 7
