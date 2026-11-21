from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from eventbus.aggregation import NeumaierSum
from eventbus.hashing import partition_for
from eventbus.service import EventBus
from eventbus.verifier import StateVerifier


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def clean_sqlite(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        candidate.unlink(missing_ok=True)


class Server:
    def __init__(self, db_path: Path, port: int, log_path: Path):
        self.db_path = db_path
        self.port = port
        self.log_path = log_path
        self.process: subprocess.Popen | None = None
        self.log_handle = None
        self.base_url = f"http://127.0.0.1:{port}"

    def start(self) -> None:
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_handle = self.log_path.open("a", encoding="utf-8")
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "scripts.run_server",
                "--db",
                str(self.db_path),
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--enable-test-hooks",
            ],
            cwd=ROOT,
            stdout=self.log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.time() + 25
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"server exited during startup ({self.process.returncode}):\n"
                    f"{self.log_path.read_text(encoding='utf-8')[-5000:]}"
                )
            try:
                response = requests.get(f"{self.base_url}/health", timeout=0.5)
                if response.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.1)
        raise RuntimeError("server did not become healthy")

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None

    def wait_for_crash(self) -> int:
        assert self.process is not None
        code = self.process.wait(timeout=15)
        return int(code)


class Client:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.observed_errors: dict[str, dict[str, Any]] = {}

    def raw(self, method: str, path: str, **kwargs):
        return self.session.request(method, self.base_url + path, timeout=15, **kwargs)

    def call(
        self,
        method: str,
        path: str,
        *,
        expected_status: int = 200,
        record_error: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        response = self.raw(method, path, **kwargs)
        try:
            payload = response.json()
        except ValueError:
            payload = {"raw": response.text}
        if response.status_code != expected_status:
            raise AssertionError(
                f"{method} {path}: expected {expected_status}, got {response.status_code}: {payload}"
            )
        if record_error and response.status_code >= 400:
            code = payload["error"]["code"]
            self.observed_errors[code] = {
                "http_status": response.status_code,
                "context": payload["error"]["context"],
            }
        return payload

    def expect_error(
        self, method: str, path: str, code: str, *, http_status: int, **kwargs
    ) -> dict[str, Any]:
        payload = self.call(
            method, path, expected_status=http_status, record_error=True, **kwargs
        )
        actual = payload["error"]["code"]
        if actual != code:
            raise AssertionError(f"expected {code}, got {actual}: {payload}")
        return payload


def aggregate_diff(before: list[dict], after: list[dict]) -> list[dict]:
    before_map = {item["key"]: item for item in before}
    after_map = {item["key"]: item for item in after}
    result = []
    for key in sorted(set(before_map) | set(after_map)):
        old = before_map.get(key, {"count": 0, "sum": 0.0})
        new = after_map.get(key, {"count": 0, "sum": 0.0})
        result.append(
            {
                "key": key,
                "count_before": old["count"],
                "count_after": new["count"],
                "count_delta": new["count"] - old["count"],
                "sum_before": old["sum"],
                "sum_after": new["sum"],
                "sum_delta": new["sum"] - old["sum"],
            }
        )
    return result


def pure_aggregate(events: list[dict], routed: dict[str, int], seqs: dict[str, int]) -> list[dict]:
    def measure(event):
        if "measure" in event:
            return event["measure"]
        return event.get("body", {}).get("measure")

    rows = sorted(
        events,
        key=lambda event: (
            event.get("key") or event["body"]["key"],
            routed[event["event_id"]],
            seqs[event["event_id"]],
        ),
    )
    grouped: dict[str, tuple[int, NeumaierSum]] = {}
    for event in rows:
        key = event.get("key") or event["body"]["key"]
        grouped.setdefault(key, (0, NeumaierSum()))
        count, acc = grouped[key]
        acc.add(measure(event))
        grouped[key] = (count + 1, acc)
    return [
        {
            "key": key,
            "count": grouped[key][0],
            "sum": grouped[key][1].value,
            "compensation": grouped[key][1].compensation,
        }
        for key in sorted(grouped)
    ]


def ack_one(client: Client, topic: str, group: str, partition: int, consumer: str) -> dict:
    polled = client.call(
        "POST",
        "/api/v1/consumers/poll",
        json={
            "topic": topic,
            "group_id": group,
            "partition": partition,
            "consumer_id": consumer,
            "lease_seconds": 30,
        },
    )
    if polled["event"] is None:
        return {"event": None, "poll": polled, "ack": None}
    ack = client.call(
        "POST",
        "/api/v1/consumers/ack",
        json={
            "topic": topic,
            "group_id": group,
            "partition": partition,
            "consumer_id": consumer,
            "event_id": polled["event"]["event_id"],
            "lease_token": polled["lease"]["lease_token"],
        },
    )
    return {"event": polled["event"], "poll": polled, "ack": ack}


def consume_partition_until_empty(
    client: Client, topic: str, group: str, partition: int, consumer: str
) -> list[dict]:
    results = []
    while True:
        item = ack_one(client, topic, group, partition, consumer)
        if item["event"] is None:
            return results
        results.append(item)


def consume_reference_all(reference: EventBus, topic: str, group: str, partitions: int) -> None:
    for partition in range(partitions):
        while True:
            polled = reference.poll(
                topic, group, partition, "reference-consumer", lease_seconds=30
            )
            if polled["event"] is None:
                break
            reference.ack(
                topic,
                group,
                partition,
                "reference-consumer",
                polled["event"]["event_id"],
                polled["lease"]["lease_token"],
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the fixed-sample local HTTP end-to-end demo.")
    parser.add_argument("--db", default="var/e2e.db")
    parser.add_argument("--reference-db", default="var/e2e_reference.db")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--artifact", default="artifacts/e2e_result.json")
    args = parser.parse_args()
    db_path = (ROOT / args.db).resolve() if not Path(args.db).is_absolute() else Path(args.db)
    reference_path = (
        (ROOT / args.reference_db).resolve()
        if not Path(args.reference_db).is_absolute()
        else Path(args.reference_db)
    )
    artifact_path = (
        (ROOT / args.artifact).resolve()
        if not Path(args.artifact).is_absolute()
        else Path(args.artifact)
    )
    clean_sqlite(db_path)
    clean_sqlite(reference_path)
    sample = json.loads((ROOT / "data" / "sample_events.json").read_text(encoding="utf-8"))
    topic = sample["topic"]["topic"]
    group = sample["topic"]["group_id"]
    partitions = sample["topic"]["partitions"]
    base_events = sample["events"]
    seqs = dict(sample["expected"]["sequence_by_event_id"])
    routed = {
        event_id: int(partition)
        for partition, event_ids in sample["expected"]["partition_assignments"].items()
        for event_id in event_ids
    }
    partition_events = {
        partition: sorted(
            [event for event in base_events if routed[event["event_id"]] == partition],
            key=lambda event: seqs[event["event_id"]],
        )
        for partition in range(partitions)
    }

    port = args.port or free_port()
    server = Server(db_path, port, ROOT / "var" / "e2e-server.log")
    client = Client(server.base_url)
    steps: list[dict[str, Any]] = []
    order_checks: list[dict[str, Any]] = []
    result: dict[str, Any] = {"ok": False, "seed": sample["seed"], "db_path": str(db_path)}
    try:
        server.start()
        steps.append({"step": "server_started", "port": port})
        topic_response = client.call(
            "POST",
            "/api/v1/topics",
            json={
                "topic": topic,
                "partitions": partitions,
                "group_id": group,
                "min_retained_seq": 1,
            },
        )
        assert topic_response["topic"] == topic
        published = client.call(
            "POST",
            f"/api/v1/topics/{topic}/events",
            json={"batch_id": sample["batch_id"], "events": base_events},
        )
        assert published["accepted"] == len(base_events)
        assert all(
            response["production_seq"] == seqs[response["event_id"]]
            for response in published["events"]
        )
        steps.append(
            {
                "step": "publish_fixed_sample",
                "accepted": published["accepted"],
                "partition_counts": sample["expected"]["partition_counts"],
            }
        )

        client.expect_error(
            "POST",
            f"/api/v1/topics/{topic}/events",
            "E_EVENT_INVALID",
            http_status=400,
            json={
                "batch_id": "invalid",
                "events": [
                    {
                        "event_id": "invalid-event",
                        "partition_key": "bad",
                        "event_time": "2026-01-15T08:00:00Z",
                        "key": "alpha",
                        "measure": "not-number",
                    }
                ],
            },
        )
        conflict_event = dict(base_events[0])
        conflict_event["measure"] = 999999999.0
        client.expect_error(
            "POST",
            f"/api/v1/topics/{topic}/events",
            "E_CONFLICTING_EVENT",
            http_status=409,
            json={"batch_id": "conflict", "events": [conflict_event]},
        )

        first_zero = partition_events[0][0]
        not_dispatched = client.expect_error(
            "POST",
            "/api/v1/consumers/ack",
            "E_NOT_DISPATCHED",
            http_status=409,
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "consumer_id": "early-consumer",
                "event_id": first_zero["event_id"],
                "lease_token": "no-such-lease",
            },
        )
        steps.append({"step": "not_dispatched_guard", "context": not_dispatched["error"]["context"]})

        normal_order = []
        for index in range(2):
            item = ack_one(client, topic, group, 0, "normal-consumer")
            assert item["poll"]["event"]["event_id"] == partition_events[0][index]["event_id"]
            normal_order.append(item["poll"]["event"]["production_seq"])
        steps.append({"step": "normal_consumption", "partition": 0, "sequences": normal_order})

        expected_lease_event = partition_events[0][2]
        slow = client.call(
            "POST",
            "/api/v1/consumers/poll",
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "consumer_id": "slow-consumer",
                "lease_seconds": 0.5,
            },
        )
        assert slow["event"]["event_id"] == expected_lease_event["event_id"]
        held = client.expect_error(
            "POST",
            "/api/v1/consumers/poll",
            "E_LEASE_HELD",
            http_status=409,
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "consumer_id": "fast-consumer",
                "lease_seconds": 30,
            },
        )
        time.sleep(0.65)
        redelivered = client.call(
            "POST",
            "/api/v1/consumers/poll",
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "consumer_id": "fast-consumer",
                "lease_seconds": 30,
            },
        )
        assert redelivered["event"]["event_id"] == expected_lease_event["event_id"]
        assert redelivered["event"]["production_seq"] == seqs[expected_lease_event["event_id"]]
        assert redelivered["lease"]["attempt"] == 2
        client.call(
            "POST",
            "/api/v1/consumers/ack",
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "consumer_id": "fast-consumer",
                "event_id": redelivered["event"]["event_id"],
                "lease_token": redelivered["lease"]["lease_token"],
            },
        )
        order_checks.append(
            {
                "check": "lease_timeout_redelivery_same_sequence",
                "expected_seq": seqs[expected_lease_event["event_id"]],
                "actual_seq": redelivered["event"]["production_seq"],
                "holder": held["error"]["context"]["holder_consumer_id"],
                "attempt": redelivered["lease"]["attempt"],
                "ok": True,
            }
        )

        expected_crash_event = partition_events[0][3]
        crash_poll = client.call(
            "POST",
            "/api/v1/consumers/poll",
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "consumer_id": "crash-consumer",
                "lease_seconds": 30,
            },
        )
        assert crash_poll["event"]["event_id"] == expected_crash_event["event_id"]
        aggregate_before_crash = client.call(
            "GET", "/api/v1/aggregates", params={"topic": topic, "group_id": group}
        )["groups"]
        try:
            client.raw(
                "POST",
                "/api/v1/consumers/ack",
                json={
                    "topic": topic,
                    "group_id": group,
                    "partition": 0,
                    "consumer_id": "crash-consumer",
                    "event_id": crash_poll["event"]["event_id"],
                    "lease_token": crash_poll["lease"]["lease_token"],
                    "test_crash_after_intent": True,
                },
            )
            crashed_by_connection = False
        except requests.RequestException:
            crashed_by_connection = True
        crash_exit_code = server.wait_for_crash()
        assert crash_exit_code == 86
        steps.append(
            {
                "step": "forced_termination_after_intent",
                "event_id": expected_crash_event["event_id"],
                "production_seq": seqs[expected_crash_event["event_id"]],
                "exit_code": crash_exit_code,
                "client_connection_dropped": crashed_by_connection,
            }
        )

        server.start()
        recovery = client.call("GET", "/api/v1/recovery/reports/latest")["report"]
        assert recovery["pending_intents_found"] == 1
        assert recovery["pending_intents_aborted"][0]["kind"] == "ACK"
        aggregate_after_recovery = client.call(
            "GET", "/api/v1/aggregates", params={"topic": topic, "group_id": group}
        )["groups"]
        assert aggregate_after_recovery == aggregate_before_crash
        crashed_event = client.call("GET", f"/api/v1/events/{expected_crash_event['event_id']}")
        assert crashed_event["state"] == "REDELIVERED"
        steps.append(
            {
                "step": "restart_recovery",
                "pending_intents_found": recovery["pending_intents_found"],
                "pending_intents_aborted": recovery["pending_intents_aborted"],
                "released_leases": recovery["released_leases"],
                "aggregate_after_recovery_unchanged": True,
                "event_state_after_recovery": crashed_event["state"],
            }
        )

        resumed = ack_one(client, topic, group, 0, "recovered-consumer")
        assert resumed["poll"]["event"]["event_id"] == expected_crash_event["event_id"]
        assert resumed["poll"]["event"]["production_seq"] == seqs[expected_crash_event["event_id"]]
        order_checks.append(
            {
                "check": "crash_recovery_redelivers_same_sequence",
                "expected_seq": seqs[expected_crash_event["event_id"]],
                "actual_seq": resumed["poll"]["event"]["production_seq"],
                "ok": True,
            }
        )

        remaining_order = []
        while True:
            item = ack_one(client, topic, group, 0, "remaining-consumer")
            if item["event"] is None:
                break
            remaining_order.append(item["poll"]["event"]["production_seq"])
        for partition in (1, 2):
            consume_partition_until_empty(
                client, topic, group, partition, f"partition-{partition}-consumer"
            )
        full_aggregate = client.call(
            "GET", "/api/v1/aggregates", params={"topic": topic, "group_id": group}
        )["groups"]
        assert full_aggregate == list(sample["expected"]["aggregate_by_key"].values())
        order_checks.append(
            {
                "check": "all_partition_delivery_sequences_monotonic",
                "partition_0_remaining": remaining_order,
                "ok": remaining_order == sorted(remaining_order),
            }
        )

        client.expect_error(
            "POST",
            f"/api/v1/events/{first_zero['event_id']}/dead-letter",
            "E_INVALID_TRANSITION",
            http_status=409,
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "event_id": first_zero["event_id"],
                "reason": "must_not_dead_letter_acked",
            },
        )
        client.expect_error(
            "POST",
            "/api/v1/consumers/offset/rewind",
            "E_OFFSET_EXPIRED",
            http_status=410,
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "target_seq": 0,
            },
        )
        client.expect_error(
            "POST",
            "/api/v1/consumers/offset/rewind",
            "E_OFFSET_NOT_FOUND",
            http_status=404,
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "target_seq": 999999,
            },
        )

        full_before_replay = full_aggregate
        rewind = client.call(
            "POST",
            "/api/v1/consumers/offset/rewind",
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "target_seq": 1,
                "reason": "e2e_replay",
            },
        )
        assert rewind["offset"] == 1
        assert rewind["replay_end"] == len(partition_events[0]) + 1
        aggregate_after_rewind = client.call(
            "GET", "/api/v1/aggregates", params={"topic": topic, "group_id": group}
        )["groups"]
        expected_after_rewind = pure_aggregate(
            [event for event in base_events if routed[event["event_id"]] != 0],
            routed,
            seqs,
        )
        assert aggregate_after_rewind == expected_after_rewind
        steps.append(
            {
                "step": "offset_rewind",
                "old_offset": rewind["old_offset"],
                "new_offset": rewind["offset"],
                "replay_end": rewind["replay_end"],
                "rolled_back_confirmations": rewind["rolled_back_confirmations"],
                "aggregate_after_rewind": aggregate_after_rewind,
            }
        )

        during_key = next(
            f"during-replay-{index}"
            for index in range(100000)
            if partition_for(topic, f"during-replay-{index}", partitions) == 0
        )
        extra_event = {
            "event_id": "e2e-during-replay",
            "partition_key": during_key,
            "event_time": "2026-01-15T10:00:00Z",
            "key": "alpha",
            "measure": 7.0,
            "payload": {"source": "e2e-published-during-replay"},
        }
        extra_publish = client.call(
            "POST",
            f"/api/v1/topics/{topic}/events",
            json={"batch_id": "e2e-during-replay", "events": [extra_event]},
        )
        expected_extra_seq = len(partition_events[0]) + 1
        assert extra_publish["events"][0]["partition"] == 0
        assert extra_publish["events"][0]["production_seq"] == expected_extra_seq

        replayed_sequences = []
        for index in range(len(partition_events[0])):
            polled = client.call(
                "POST",
                "/api/v1/consumers/poll",
                json={
                    "topic": topic,
                    "group_id": group,
                    "partition": 0,
                    "consumer_id": "replayer",
                    "lease_seconds": 30,
                },
            )
            assert polled["event"] is not None
            assert polled["event"]["event_id"] != "e2e-during-replay"
            replayed_sequences.append(polled["event"]["production_seq"])
            client.call(
                "POST",
                "/api/v1/consumers/ack",
                json={
                    "topic": topic,
                    "group_id": group,
                    "partition": 0,
                    "consumer_id": "replayer",
                    "event_id": polled["event"]["event_id"],
                    "lease_token": polled["lease"]["lease_token"],
                },
            )
        assert replayed_sequences == list(range(1, len(partition_events[0]) + 1))
        after_barrier = client.call(
            "POST",
            "/api/v1/consumers/poll",
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "consumer_id": "replayer",
                "lease_seconds": 30,
            },
        )
        assert after_barrier["event"]["event_id"] == "e2e-during-replay"
        assert after_barrier["replaying"] is False
        client.call(
            "POST",
            "/api/v1/consumers/ack",
            json={
                "topic": topic,
                "group_id": group,
                "partition": 0,
                "consumer_id": "replayer",
                "event_id": "e2e-during-replay",
                "lease_token": after_barrier["lease"]["lease_token"],
            },
        )
        final_aggregate = client.call(
            "GET", "/api/v1/aggregates", params={"topic": topic, "group_id": group}
        )["groups"]
        order_checks.append(
            {
                "check": "replay_preserved_partition_order_and_barrier",
                "replayed_sequences": replayed_sequences,
                "new_event_held_until_barrier": True,
                "new_event_sequence": expected_extra_seq,
                "ok": True,
            }
        )

        reference = EventBus(reference_path)
        reference.create_topic(topic, partitions=partitions, group_id=group)
        reference.publish(topic, sample["batch_id"], base_events)
        consume_reference_all(reference, topic, group, partitions)
        reference.publish(topic, "e2e-during-replay", [extra_event])
        while True:
            polled = reference.poll(topic, group, 0, "reference-consumer", lease_seconds=30)
            if polled["event"] is None:
                break
            reference.ack(
                topic,
                group,
                0,
                "reference-consumer",
                polled["event"]["event_id"],
                polled["lease"]["lease_token"],
            )
        reference_aggregate = reference.get_aggregates(topic, group)["groups"]
        assert final_aggregate == reference_aggregate

        verification = client.call("GET", "/api/v1/verify", params={"topic": topic})
        direct_verification = StateVerifier(db_path).run(topic_id=topic)
        assert verification["ok"] is True
        assert direct_verification["ok"] is True

        required_codes = {
            "E_EVENT_INVALID",
            "E_CONFLICTING_EVENT",
            "E_INVALID_TRANSITION",
            "E_LEASE_HELD",
            "E_OFFSET_EXPIRED",
            "E_OFFSET_NOT_FOUND",
            "E_NOT_DISPATCHED",
        }
        assert required_codes.issubset(client.observed_errors)
        crash_difference = aggregate_diff(aggregate_before_crash, aggregate_after_recovery)
        rewind_difference = aggregate_diff(full_before_replay, aggregate_after_rewind)
        final_difference = aggregate_diff(full_before_replay, final_aggregate)
        assert all(item["count_delta"] == 0 and abs(item["sum_delta"]) < 1e-12 for item in crash_difference)
        result.update(
            {
                "ok": True,
                "steps": steps,
                "order_checks": order_checks,
                "error_codes_observed": client.observed_errors,
                "recovery_report": recovery,
                "aggregate_before_crash": aggregate_before_crash,
                "aggregate_after_recovery": aggregate_after_recovery,
                "aggregate_recovery_difference": crash_difference,
                "aggregate_full_before_replay": full_before_replay,
                "aggregate_after_rewind": aggregate_after_rewind,
                "aggregate_rewind_difference": rewind_difference,
                "aggregate_final": final_aggregate,
                "aggregate_final_vs_before_rewind_difference": final_difference,
                "reference_equivalence": {
                    "equal": final_aggregate == reference_aggregate,
                    "reference_aggregate": reference_aggregate,
                },
                "verification": {
                    "http": verification,
                    "direct": direct_verification,
                },
                "order_checks_ok": all(check["ok"] for check in order_checks),
            }
        )
    finally:
        client.session.close()
        server.stop()
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    print(
        json.dumps(
            {
                "ok": result["ok"],
                "artifact": str(artifact_path),
                "error_codes": sorted(result.get("error_codes_observed", {})),
                "order_checks": result.get("order_checks"),
                "aggregate_recovery_difference": result.get(
                    "aggregate_recovery_difference"
                ),
                "aggregate_rewind_difference": result.get("aggregate_rewind_difference"),
                "reference_equivalence": result.get("reference_equivalence", {}).get("equal"),
                "verification": result.get("verification", {}).get("direct", {}).get(
                    "summary"
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
