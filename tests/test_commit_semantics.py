from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from capacity_ledger.errors import CapacityError
from capacity_ledger.service import CapacityLedger

from conftest import clone_with_commit_id, load_json, sign


def test_stale_base_changes_nothing(sample_root: Path, ledger: CapacityLedger) -> None:
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    ledger.commit_increment(load_json(sample_root / "increments" / "01_auth_price_increment.json"))
    before = ledger.get_current()
    stale = load_json(sample_root / "increments" / "01_auth_price_increment.json")
    stale["commit_id"] = "stale-commit"
    stale["services"]["auth-service"]["price_core_cents_per_core_hour"] = 21
    stale = sign(stale)
    with pytest.raises(CapacityError) as exc_info:
        ledger.commit_increment(stale)
    assert exc_info.value.code == "E_STALE_BASE"
    assert exc_info.value.details["persisted_state_changed"] is False
    after = ledger.get_current()
    assert after["version"] == before["version"]
    assert after["ledger_checksum"] == before["ledger_checksum"]
    assert len(ledger.list_intents()) == 2


def test_idempotent_commit_reuses_exact_result(sample_root: Path, ledger: CapacityLedger) -> None:
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    increment = load_json(sample_root / "increments" / "01_auth_price_increment.json")
    first = ledger.commit_increment(increment)
    second = ledger.commit_increment(increment)
    assert second == first
    current = ledger.get_current()
    assert current["version"] == 2
    with ledger.store.read_transaction() as connection:
        count = connection.execute("SELECT COUNT(*) FROM ledger_versions").fetchone()[0]
        commits = connection.execute(
            "SELECT COUNT(*) FROM commits WHERE status = 'COMMITTED'"
        ).fetchone()[0]
    assert count == 2
    assert commits == 2


def test_conflicting_commit_id_is_rejected(sample_root: Path, ledger: CapacityLedger) -> None:
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    increment = load_json(sample_root / "increments" / "01_auth_price_increment.json")
    ledger.commit_increment(increment)
    conflicting = copy.deepcopy(increment)
    conflicting["services"]["auth-service"]["price_core_cents_per_core_hour"] = 21
    conflicting = sign(conflicting)
    with pytest.raises(CapacityError) as exc_info:
        ledger.commit_increment(conflicting)
    assert exc_info.value.code == "E_CONFLICTING_COMMIT"
    assert ledger.get_current()["version"] == 2


def test_concurrent_same_base_has_exactly_one_winner(sample_root: Path, ledger: CapacityLedger) -> None:
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    base = load_json(sample_root / "increments" / "01_auth_price_increment.json")
    payloads = [
        clone_with_commit_id(base, f"concurrent-base-{index}") for index in range(8)
    ]

    def submit(payload: dict) -> tuple[str, str | None]:
        try:
            result = ledger.commit_increment(payload)
            return "OK", result["ledger_checksum"]
        except CapacityError as exc:
            return exc.code, None

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = list(executor.map(submit, payloads))
    winners = [item for item in outcomes if item[0] == "OK"]
    stale = [item for item in outcomes if item[0] == "E_STALE_BASE"]
    assert len(winners) == 1
    assert len(stale) == 7
    assert ledger.get_current()["version"] == 2


def test_concurrent_same_commit_id_runs_once(sample_root: Path, ledger: CapacityLedger) -> None:
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    payload = load_json(sample_root / "increments" / "01_auth_price_increment.json")

    def submit(_: int) -> str:
        return ledger.commit_increment(payload)["ledger_checksum"]

    with ThreadPoolExecutor(max_workers=8) as executor:
        checksums = list(executor.map(submit, range(8)))
    assert len(set(checksums)) == 1
    assert ledger.get_current()["version"] == 2
    with ledger.store.read_transaction() as connection:
        versions = connection.execute("SELECT COUNT(*) FROM ledger_versions").fetchone()[0]
        intents = connection.execute("SELECT COUNT(*) FROM intents").fetchone()[0]
    assert versions == 2
    assert intents == 2


def test_reads_observe_only_complete_versions(sample_root: Path, ledger: CapacityLedger) -> None:
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    increments = [
        load_json(sample_root / "increments" / "01_auth_price_increment.json"),
        load_json(sample_root / "increments" / "02_catalog_cores_increment.json"),
    ]

    def writer() -> None:
        for payload in increments:
            ledger.commit_increment(payload)

    observed: list[tuple[int, str]] = []

    def reader() -> None:
        for _ in range(20):
            current = ledger.get_current()
            observed.append((current["version"], current["ledger_checksum"]))

    with ThreadPoolExecutor(max_workers=5) as executor:
        writer_future = executor.submit(writer)
        readers = [executor.submit(reader) for _ in range(4)]
        writer_future.result()
        for future in readers:
            future.result()

    valid = {
        (ledger.get_version(version)["version"], ledger.get_version(version)["ledger_checksum"])
        for version in (1, 2, 3)
    }
    assert observed
    assert set(observed).issubset(valid)

